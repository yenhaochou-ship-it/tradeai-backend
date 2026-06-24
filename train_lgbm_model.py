"""
train_lgbm_model.py — 訓練「做多勝率」LightGBM模型，供 main.py 的 predict_lgbm_confidence() 載入使用

═══════════════════════════════════════════════════════════════════════════
⚠️ 重要：這個檔案的特徵計算邏輯，跟 main.py 裡的 extract_ml_features() 必須完全一致
═══════════════════════════════════════════════════════════════════════════
這裡是獨立腳本，沒有直接 import main.py（main.py一載入就會啟動排程器/連線等副作用，
不適合被當模組import），所以下面複製了一份 main.py 的特徵計算邏輯。
以後改 main.py 的 calc_signal_py / classify_market_regime / advanced_score 這些函式時，
這個檔案要跟著改，順序跟定義都要一模一樣，否則訓練出來的模型在main.py推論時，
餵進去的數字意義會對不上，預測值會是垂圾（這就是機器學習說的 train/serve skew）。

═══════════════════════════════════════════════════════════════════════════
使用方式
═══════════════════════════════════════════════════════════════════════════
1. pip install lightgbm shioaji numpy scikit-learn --break-system-packages
2. 設定環境變數（跟後端連線用的是同一組永豐API金鑰）：
   export SINOPAC_API_KEY="你的api_key"
   export SINOPAC_SECRET_KEY="你的secret_key"
3. 先用小範圍測試（--symbols 2330 --days 10），確認資料抓得到、流程跑得完：
   python train_lgbm_model.py --symbols 2849 2836 2834 --days 30
4. 確認沒問題後，用完整股票池+夠長的歷史跑一次正式訓練：
   python train_lgbm_model.py --days 90
5. 訓練完成會在當前目錄產生 lgbm_model.txt，把這個檔案放到後端服務的工作目錄
   （跟main.py同一層，或設定LGBM_MODEL_PATH環境變數指向它），重啟/重新部署後端即可生效。

標籤定義（Triple Barrier，用「中風險」的停損停利/持倉上限當基準）：
進場後在 max_hold_min 分鐘內，價格先碰到 +tp% 算成功(1)，先碰到 -sl% 算失敗(0)，
兩者都沒碰到就看 max_hold_min 結束時是否有達到 min_profitable_move_pct 算成功，否則失敗。
這個基準是「中風險」設定，不代表你實際運行的風險等級——模型預測的是相對普適的「做多品質」，
各風險等級在main.py用各自的min_conf門檻去比這個機率，門檻越高代表要求越嚴格。
"""
import argparse, os, sys, time
from datetime import datetime, timedelta, timezone

TW_TZ = timezone(timedelta(hours=8))
def tw_now(): return datetime.now(TW_TZ)

# ── 跟main.py完全一致的股票池（訓練資料的股票分佈要跟實際推論時看到的分佈一致，模型才有意義）──
SCAN_UNIVERSE = [
    "2330","2454","2308","2317","3711","2327","2303","2383","2891","3037",
    "2882","2886","2884","2881","2880","2885","2890","5880","2892","2887",
    "2382","3008","2357","2379","4938","2345","2412","1301","1303","2002",
    "3034","3045","2353","2356","6669","3661","3653","2474","2207","1216",
    "2849","2836","2834","2812","2801","2838",
]

# ── 標籤定義基準（中風險，見上方docstring說明）──
LABEL_TP_PCT = 6.0
LABEL_SL_PCT = 3.0
LABEL_MAX_HOLD_MIN = 40
MIN_PROFITABLE_MOVE_PCT = 0.32  # 跟main.py的min_profitable_move_pct()算出來的數字一致(約0.32%)

# ═══════════════════════════════════════════════════════════════════════════
# 以下複製自main.py，保持完全一致 ── 如果main.py改了這幾個函式，這裡要同步改
# ═══════════════════════════════════════════════════════════════════════════

def _calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return [50.0] * len(prices)
    result = [50.0] * period
    for i in range(period, len(prices)):
        w = prices[i-period:i+1]
        g = [max(0, w[j]-w[j-1]) for j in range(1, len(w))]
        l = [max(0, w[j-1]-w[j]) for j in range(1, len(w))]
        ag, al = sum(g)/period, sum(l)/period
        result.append(100.0 if al==0 else 100-100/(1+ag/al))
    return result

def _ema(data, n):
    k, r = 2/(n+1), [data[0]]
    for p in data[1:]: r.append(p*k + r[-1]*(1-k))
    return r

def calc_signal_py(prices, volumes):
    if len(prices) < 30:
        return {"action":"hold","conf":50,"rsi":50,"bad_time":False}
    price = prices[-1]
    ma5   = sum(prices[-5:])/5
    ma20  = sum(prices[-20:])/20
    ma50  = sum(prices[-50:])/50 if len(prices)>=50 else ma20
    rsi_arr = _calc_rsi(prices)
    rsi = rsi_arr[-1]
    vp,vv = prices[-20:], (volumes[-20:] if volumes else [1e6]*20)
    total_v = sum(vv) or 1
    vwap = sum(p*v for p,v in zip(vp,vv))/total_v
    avg_vol = sum(volumes[-20:])/20 if volumes else 1
    vol_ratio = (sum(volumes[-3:])/3 if volumes else 1)/(avg_vol or 1)
    ws=prices[-14:]; wh,wl=max(ws),min(ws)
    w_r=-(wh-price)/(wh-wl)*100 if wh!=wl else -50
    cs=prices[-20:]; cm=sum(cs)/20; cd=sum(abs(p-cm) for p in cs)/20 or 1
    cci=(price-cm)/(0.015*cd)
    r14=prices[-14:]; mp,np2=max(r14),min(r14)
    am=sum(abs(r14[i]-r14[i-1]) for i in range(1,len(r14)))/13 or 1
    trend_str=min(1.0,(mp-np2)/(am*14))
    return {"rsi":round(rsi,1),"ma5":round(ma5,2),"ma20":round(ma20,2),"vwap":round(vwap,2),
            "williams_r":round(w_r,1),"cci":round(cci,1),"trend_str":round(trend_str,2),
            "vol_ratio":round(vol_ratio,2),"bad_time":False}

def classify_market_regime(prices):
    if len(prices) < 20:
        return {"regime":"unknown","confidence":0}
    rets=[(prices[i]-prices[i-1])/prices[i-1] for i in range(1,len(prices)) if prices[i-1]]
    recent=rets[-20:]
    if not recent: return {"regime":"unknown","confidence":0}
    mean_r=sum(recent)/len(recent)
    vol=(sum((r-mean_r)**2 for r in recent)/len(recent))**0.5
    ws=prices[-14:]; am=sum(abs(ws[i]-ws[i-1]) for i in range(1,len(ws)))/13 or 1
    trend_strength=min(1.0,abs(prices[-1]-prices[-14])/(am*13)) if len(prices)>=14 else 0
    cum_ret=(prices[-1]-prices[-10])/prices[-10] if len(prices)>=10 and prices[-10] else 0
    PANIC_VOL,VOLATILE_VOL,TREND_TH=0.012,0.007,0.4
    if vol>=PANIC_VOL and cum_ret<=-0.02: return {"regime":"panic","confidence":min(100,round(vol/PANIC_VOL*60))}
    if vol>=VOLATILE_VOL: return {"regime":"volatile","confidence":min(100,round(vol/VOLATILE_VOL*55))}
    if trend_strength>=TREND_TH and cum_ret>0: return {"regime":"trending_bull","confidence":round(trend_strength*100)}
    if trend_strength>=TREND_TH and cum_ret<0: return {"regime":"trending_bear","confidence":round(trend_strength*100)}
    return {"regime":"range","confidence":round((1-trend_strength)*100)}

def _bucket_closes(entries, bucket_seconds):
    buckets={}
    for e in entries:
        ts=e.get("t")
        if ts is None: continue
        buckets[int(ts//bucket_seconds)]=e["price"]
    return [buckets[k] for k in sorted(buckets.keys())]

def multi_timeframe_direction(entries):
    m1,m5,m15=_bucket_closes(entries,60),_bucket_closes(entries,300),_bucket_closes(entries,900)
    def direction(closes,lookback):
        if len(closes)<lookback+1: return "unknown"
        base=closes[-1-lookback]
        if not base: return "unknown"
        chg=(closes[-1]-base)/base
        return "bull" if chg>0.001 else "bear" if chg<-0.001 else "flat"
    d15,d5,d1=direction(m15,2),direction(m5,3),direction(m1,2)
    def _aligned(forbidden):
        knowns=[d for d in (d15,d5,d1) if d!="unknown"]
        if not knowns: return None
        return all(d!=forbidden for d in knowns)
    return {"aligned_long":_aligned("bear"),"aligned_short":_aligned("bull")}

def orb_range(entries, date_str):
    morning=[]
    for e in entries:
        ts=e.get("t")
        if ts is None: continue
        dt=datetime.fromtimestamp(ts,tz=TW_TZ)
        if dt.strftime("%Y-%m-%d")==date_str and 9*60<=dt.hour*60+dt.minute<9*60+15:
            morning.append(e["price"])
    if not morning: return None
    return {"high":max(morning),"low":min(morning)}

def bid_ask_imbalance(entries):
    recent=[e for e in entries[-10:] if e.get("buy_vol") is not None and e.get("sell_vol") is not None]
    if not recent: return {"imbalance":0.0}
    vals=[(e["buy_vol"]-e["sell_vol"])/((e["buy_vol"]+e["sell_vol"]) or 1) for e in recent]
    return {"imbalance":sum(vals)/len(vals)}

def volume_quality_score(volumes, entries=None):
    if len(volumes)<20: return {"score":50.0}
    avg20=sum(volumes[-20:])/20 or 1
    vol_ratio=(sum(volumes[-3:])/3)/avg20
    baseline=sum(volumes[-60:])/60 if len(volumes)>=60 else avg20
    rel_vol=avg20/(baseline or 1)
    bai=bid_ask_imbalance(entries) if entries else {"imbalance":0.0}
    bai_score=50+bai["imbalance"]*50
    score=min(100.0,max(0.0,40*min(vol_ratio,2)+20*min(rel_vol,2)+0.4*bai_score))
    return {"score":round(score,1),"bid_ask_imbalance":bai["imbalance"]}

def approx_order_flow(entries):
    deltas=[]
    for e in entries:
        vol=e.get("volume")
        if vol is None or vol<=0: continue
        tt=e.get("tick_type",0)
        sign=1 if tt==1 else (-1 if tt==2 else 0)
        deltas.append(sign*vol)
    if not deltas: return {"cvd":0,"delta_recent":0}
    return {"cvd":sum(deltas),"delta_recent":sum(deltas[-10:])}

_REGIME_CODE={"trending_bull":2,"trending_bear":-2,"range":0,"volatile":1,"panic":-3,"unknown":0}

# 必須跟main.py的ML_FEATURE_NAMES順序完全一致
ML_FEATURE_NAMES=[
    "rsi","williams_r","cci","trend_str","vol_ratio",
    "ma5_ma20_dev_pct","price_vwap_dev_pct",
    "regime_code","mtf_aligned_code",
    "volume_quality_score","bid_ask_imbalance","order_flow_delta_scaled","orb_breakout",
    "real_ofi_ema","real_big_trade_scaled",
    # 注意：這兩個一樣會是0(中性值)，跟bid_ask_imbalance/order_flow_delta_scaled同樣的限制——
    # 歷史kbars沒有逐筆tick/委託簿資料，沒辦法回溯算出真實OFI/大單流，訓練時這2個特徵也是常數0，
    # 模型一樣學不到怎麼用它們。等之後有辦法收集到歷史tick資料(api.ticks()可查單日逐筆，
    # 但沒有bid/ask五檔，只有bid_price/ask_price當時的最佳一檔)，可以考慮另外補做歷史OFI重建。
]

def extract_ml_features(sym, prices, volumes, entries, dir_="L"):
    """跟main.py的extract_ml_features()邏輯完全一致，差別只是entries當參數傳入(訓練時逐筆切片效率較好)"""
    if len(prices)<30: return None
    sig=calc_signal_py(prices,volumes)
    regime=classify_market_regime(prices)
    mtf=multi_timeframe_direction(entries)
    vq=volume_quality_score(volumes,entries)
    of=approx_order_flow(entries)
    orb=orb_range(entries,tw_now().strftime("%Y-%m-%d"))
    ma20=sig.get("ma20") or 1
    vwap=sig.get("vwap") or 1
    aligned=mtf.get("aligned_long") if dir_=="L" else mtf.get("aligned_short")
    mtf_code=1 if aligned is True else(-1 if aligned is False else 0)
    orb_hit=1 if(orb is not None and prices[-1]>orb["high"] and dir_=="L") else 0
    return [
        sig.get("rsi",50), sig.get("williams_r",-50), sig.get("cci",0),
        sig.get("trend_str",0), sig.get("vol_ratio",1),
        (sig.get("ma5",ma20)/ma20-1)*100 if ma20 else 0,
        (prices[-1]/vwap-1)*100 if vwap else 0,
        _REGIME_CODE.get(regime["regime"],0), mtf_code,
        vq.get("score",50), vq.get("bid_ask_imbalance",0),
        of.get("delta_recent",0)/1000.0, orb_hit,
        0.0, 0.0,  # real_ofi_ema, real_big_trade_scaled：歷史資料沒有，固定填中性值0
    ]

# ═══════════════════════════════════════════════════════════════════════════
# 抓歷史資料 + 標籤
# ═══════════════════════════════════════════════════════════════════════════

def fetch_1min_bars(api, symbol, days):
    """抓真實1分鐘K棒，用來算特徵跟標籤。比main.py的_fetch_real_history抓更長的歷史，
    因為訓練需要足量資料，不像即時推論只要暖機用最近120根。"""
    contract = api.Contracts.Stocks.get(symbol)
    if not contract:
        print(f"  ⚠️ 找不到合約 {symbol}，跳過")
        return []
    end_d = tw_now()
    start_d = end_d - timedelta(days=days)
    try:
        kb = api.kbars(contract, start=start_d.strftime("%Y-%m-%d"), end=end_d.strftime("%Y-%m-%d"))
    except Exception as e:
        print(f"  ⚠️ 抓取{symbol}失敗: {e}")
        return []
    if not kb or not kb.ts:
        print(f"  ⚠️ {symbol} 沒有歷史資料")
        return []
    n = len(kb.ts)
    bars = [{"ts": kb.ts[i]/1e9, "open": kb.Open[i], "high": kb.High[i], "low": kb.Low[i],
             "close": kb.Close[i], "volume": kb.Volume[i]} for i in range(n)]
    print(f"  ✓ {symbol}: {len(bars)}根1分K棒（{start_d.strftime('%Y-%m-%d')}~{end_d.strftime('%Y-%m-%d')}）")
    return bars

def label_triple_barrier(bars, start_idx):
    """三道門檻標籤法：從start_idx+1開始往後看，看漲到+tp%或跌到-sl%哪個先發生，
    或者撐到max_hold_min分鐘都沒碰到任一道門檻(垂直門檻)，用那時候的報酬率決定成功/失敗。"""
    entry_price = bars[start_idx]["close"]
    tp_price = entry_price * (1 + LABEL_TP_PCT/100)
    sl_price = entry_price * (1 - LABEL_SL_PCT/100)
    horizon_end = min(len(bars)-1, start_idx + LABEL_MAX_HOLD_MIN)
    for j in range(start_idx+1, horizon_end+1):
        if bars[j]["high"] >= tp_price: return 1
        if bars[j]["low"] <= sl_price: return 0
    final_ret = (bars[horizon_end]["close"] - entry_price) / entry_price * 100
    return 1 if final_ret >= MIN_PROFITABLE_MOVE_PCT else 0

def build_dataset(api, symbols, days, stride=5):
    """對每檔股票，每隔stride根棒子取一個樣本點(不用每一根都取，減少高度重疊的樣本、加快訓練)，
    算特徵+標籤，組成完整訓練資料。"""
    X, y, meta = [], [], []
    for sym in symbols:
        bars = fetch_1min_bars(api, sym, days)
        if len(bars) < 60:
            continue
        # 把bars轉成extract_ml_features需要的entries格式（沒有tick_type/buy_vol/sell_vol，
        # 因為kbars歷史資料沒有這些欄位——這跟main.py即時推論時的特徵會有落差，這是已知限制，
        # 見下方"已知限制"說明，先用0/None代替，模型還是能從其他10個特徵學到東西）
        entries = [{"price": b["close"], "volume": b["volume"], "t": b["ts"],
                    "buy_vol": None, "sell_vol": None, "tick_type": 0} for b in bars]
        prices_all = [b["close"] for b in bars]
        volumes_all = [b["volume"] for b in bars]
        n_samples_this_sym = 0
        for i in range(30, len(bars) - LABEL_MAX_HOLD_MIN - 1, stride):
            feats = extract_ml_features(sym, prices_all[:i+1], volumes_all[:i+1], entries[:i+1], "L")
            if feats is None: continue
            label = label_triple_barrier(bars, i)
            X.append(feats); y.append(label)
            meta.append({"sym": sym, "idx": i})
            n_samples_this_sym += 1
        print(f"    -> {sym}: {n_samples_this_sym} 個訓練樣本")
    return X, y, meta

# ═══════════════════════════════════════════════════════════════════════════
# 主程式
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="訓練LightGBM做多勝率模型")
    ap.add_argument("--symbols", nargs="*", default=None, help="只訓練這幾檔(預設用完整SCAN_UNIVERSE)")
    ap.add_argument("--days", type=int, default=90, help="抓多少天的歷史資料(預設90天)")
    ap.add_argument("--stride", type=int, default=5, help="每隔幾根棒子取一個樣本(預設5)")
    ap.add_argument("--output", default="lgbm_model.txt", help="模型輸出路徑")
    args = ap.parse_args()

    symbols = args.symbols or SCAN_UNIVERSE
    print(f"訓練股票池: {len(symbols)}檔 | 歷史天數: {args.days} | 取樣間隔: {args.stride}根")

    api_key = os.environ.get("SINOPAC_API_KEY")
    secret_key = os.environ.get("SINOPAC_SECRET_KEY")
    if not api_key or not secret_key:
        print("❌ 請先設定環境變數 SINOPAC_API_KEY / SINOPAC_SECRET_KEY")
        sys.exit(1)

    import shioaji as sj
    print("連接永豐帳戶...")
    api = sj.Shioaji(simulation=False)
    api.login(api_key=api_key, secret_key=secret_key)
    print("✓ 連線成功，開始抓取歷史資料並建立特徵...")

    X, y, meta = build_dataset(api, symbols, args.days, args.stride)
    print(f"\n總樣本數: {len(X)} | 正樣本(做多成功)比例: {sum(y)/len(y)*100:.1f}%" if y else "⚠️ 沒有抓到任何樣本")
    if len(X) < 200:
        print("⚠️ 樣本數太少(<200)，模型品質會很差，建議增加--days或檢查資料來源是否正常")
        if len(X) == 0:
            sys.exit(1)

    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score, accuracy_score

    X = np.array(X); y = np.array(y)
    # 用時間順序切分train/valid(不能隨機shuffle，否則用未來資料預測過去=資料洩漏)
    split = int(len(X) * 0.8)
    X_train, X_valid = X[:split], X[split:]
    y_train, y_valid = y[:split], y[split:]

    train_data = lgb.Dataset(X_train, label=y_train, feature_name=ML_FEATURE_NAMES)
    valid_data = lgb.Dataset(X_valid, label=y_valid, feature_name=ML_FEATURE_NAMES, reference=train_data)
    params = {
        "objective": "binary", "metric": "auc", "verbosity": -1,
        "learning_rate": 0.05, "num_leaves": 31, "min_data_in_leaf": 30,
        "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 5,
    }
    print("\n開始訓練...")
    model = lgb.train(params, train_data, num_boost_round=500, valid_sets=[valid_data],
                       callbacks=[lgb.early_stopping(30), lgb.log_evaluation(50)])

    pred_valid = model.predict(X_valid)
    auc = roc_auc_score(y_valid, pred_valid)
    acc = accuracy_score(y_valid, (pred_valid >= 0.5).astype(int))
    print(f"\n=== 驗證集表現 ===\nAUC: {auc:.3f} (0.5=瞎猜, 越接近1越好, 通常0.55~0.65已經算有用)")
    print(f"準確率: {acc:.3f}")
    print("\n=== 特徵重要性 ===")
    importance = sorted(zip(ML_FEATURE_NAMES, model.feature_importance()), key=lambda x: -x[1])
    for name, imp in importance:
        print(f"  {name}: {imp}")

    model.save_model(args.output)
    print(f"\n✅ 模型已存到 {args.output}")
    print("把這個檔案放到後端服務工作目錄(跟main.py同一層)，重啟/重新部署後端即可生效。")
    if auc < 0.55:
        print("\n⚠️ AUC接近0.5，代表模型幾乎沒有預測力，不建議直接上線使用——")
        print("   可能是樣本太少、標籤定義不適合這個股票池、或特徵跟結果關聯本來就弱。")
        print("   建議先增加--days、確認資料品質，或重新檢視標籤定義再訓練一次。")

if __name__ == "__main__":
    main()
