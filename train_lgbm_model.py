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

═══════════════════════════════════════════════════════════════════════════
多久該重新訓練一次？要不要排程自動做？
═══════════════════════════════════════════════════════════════════════════
目前刻意設計成「手動執行」，沒有排程自動重訓+自動換模型上線——理由：模型還沒有任何真實
驗證紀錄之前，自動把一個沒人看過的新模型直接換上正在跑的交易系統，等於拿掉了人在最關鍵
決策點上的把關。建議的節奏：累積完一輪20筆/5天的模擬驗證後，手動重新訓練一次，看下面
「新舊模型比較」的結果再決定要不要換。之後如果累積了更穩定的真實績效記錄，要不要做成
自動排程(可以掛在main.py現有的APScheduler裡，跟auto_trade_tick同一套機制)是合理的下一步，
但那時候才有真正的歷史表現可以拿來判斷「自動換模型」這個動作本身值不值得信任。

═══════════════════════════════════════════════════════════════════════════
合併真實交易資料一起訓練（不是續訓，是從零重新訓練；--feature-log-csv）
═══════════════════════════════════════════════════════════════════════════
先從/auto/feature_log.csv下載累積的真實(或模擬)交易紀錄，然後：
   python train_lgbm_model.py --feature-log-csv paper_test_data.csv
這會把真實交易資料跟歷史重建資料合併成同一個訓練集，從零開始一起訓練——不是用LightGBM的
init_model續訓機制。原因是實測過：續訓的方式在「歷史資料這個特徵固定是0、真實資料這個特徵
才有真實數值」的情境下(目前OFI/價差正是這種情況)，新加的樹完全學不會用這個特徵
(驗證集AUC只有0.484，等於瞎猜)；合併後從零訓練則能正確學會(AUC可以到0.8以上)。
真實資料樣本少，用--real-data-weight-fraction調整它在訓練時的權重佔比(預設30%)，
這個數字沒有校準過，是合理起點。

訓練完會自動跟現在部署中的舊模型(或--compare-with指定的檔案)在同一份驗證集上比較AUC，
清楚印出新模型是更好還是更差——這個比較不會自動阻止存檔，但請先看過結果再決定要不要
真的把這次訓練出來的lgbm_model.txt複製過去覆蓋線上的版本。

標籤定義（Triple Barrier，用「中風險」的停損停利/持倉上限當基準）：
進場後在 max_hold_min 分鐘內，價格先碰到 +tp% 算成功(1)，先碰到 -sl% 算失敗(0)，
兩者都沒碰到就看 max_hold_min 結束時是否有達到 min_profitable_move_pct 算成功，否則失敗。
這個基準是「中風險」設定，不代表你實際運行的風險等級——模型預測的是相對普適的「做多品質」，
各風險等級在main.py用各自的min_conf門檻去比這個機率，門檻越高代表要求越嚴格。
"""
import argparse, os, sys, bisect
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
    "real_ofi_interval","real_big_trade_interval_scaled",
    # 注意：這兩個一樣會是0(中性值)，跟bid_ask_imbalance/order_flow_delta_scaled同樣的限制——
    # 歷史kbars沒有逐筆tick/委託簿資料，沒辦法回溯算出真實OFI/大單流，訓練時這2個特徵也是常數0，
    # 模型一樣學不到怎麼用它們。等之後有辦法收集到歷史tick資料(api.ticks()可查單日逐筆，
    # 但沒有bid/ask五檔，只有bid_price/ask_price當時的最佳一檔)，可以考慮另外補做歷史OFI重建。
    "spread_pct",  # 一樣是0：歷史kbars沒有bid/ask，跟上面OFI同樣的限制
    "market_return_5m_pct",  # 這個跟上面不一樣——0050的歷史K棒抓得到，下面build_dataset()有真的算出來，不是固定0
    "time_of_day_pct",  # 跟market_return_5m_pct一樣不是固定值——純時間運算，歷史資料的ts本來就有，直接算
]
MARKET_INDEX_SYMBOL="0050"

def get_time_of_day_pct(ts):
    """跟main.py的get_time_of_day_pct()邏輯完全一致。訓練時傳進來的ts是歷史K棒當時的真實時間戳，
    不是"現在"——這樣訓練出來的「早盤/尾盤」型態學習才會對應到歷史上真實發生的時段，不是亂套用。"""
    dt=datetime.fromtimestamp(ts,tz=TW_TZ)
    minutes=dt.hour*60+dt.minute-9*60
    return round(max(0.0,min(1.0,minutes/270)),3)

def get_market_context_at(market_entries, market_ts_list, current_ts):
    """跟main.py的get_market_context()邏輯一致，但訓練時必須只看「當下這個時間點之前」的0050價格，
    不能用到未來資料(lookahead bias)——不然等於用還沒發生的大盤走勢去訓練模型，評估出來的準確度會是假的。
    效能：market_entries本身就是按時間排好的(來自fetch_1min_bars)，用bisect在已排序時間戳清單裡
    二分搜尋切點是O(log n)，比原本每次呼叫都用list comprehension整個filter一遍的O(n)快得多——
    這個函式會被每一個訓練樣本呼叫一次，資料量大時(例如抓90天、好幾十檔股票)差異會很明顯。"""
    if not market_entries: return None
    idx=bisect.bisect_right(market_ts_list,current_ts)
    past=market_entries[:idx]
    m5=_bucket_closes(past,300)
    if len(m5)<4: return None
    base=m5[-4]
    if not base: return None
    return round((m5[-1]-base)/base*100,3)

def extract_ml_features(sym, prices, volumes, entries, dir_="L", market_return_5m_pct=None, time_of_day_pct=None):
    """跟main.py的extract_ml_features()邏輯完全一致，差別只是entries當參數傳入(訓練時逐筆切片效率較好)，
    market_return_5m_pct/time_of_day_pct是外部算好傳進來的(跨股票共用的0050資料跟時間運算，
    不適合塞在單一股票的特徵函式內部算)"""
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
        0.0, 0.0,  # real_ofi_interval, real_big_trade_interval_scaled：歷史資料沒有，固定填中性值0
        0.0,  # spread_pct：歷史資料沒有，固定填中性值0
        market_return_5m_pct if market_return_5m_pct is not None else 0.0,
        time_of_day_pct if time_of_day_pct is not None else 0.0,
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
    # 先抓0050的歷史K棒，用來算每個樣本點當下的「大盤同步性」特徵——只抓一次，所有股票共用，
    # 不是每檔股票各自抓一份(0050的走勢對所有股票來說是同一份外部資料，不需要重複抓)。
    print("抓取大盤代理指標(0050)歷史資料...")
    market_bars = fetch_1min_bars(api, MARKET_INDEX_SYMBOL, days)
    market_entries = [{"price": b["close"], "t": b["ts"]} for b in market_bars]
    market_ts_list = [e["t"] for e in market_entries]  # 給bisect用，預先抽出時間戳清單，不用每次呼叫都重新建一份
    if not market_entries:
        print("  ⚠️ 抓不到0050資料，market_return_5m_pct這個特徵這次訓練會固定是0(中性值)")
    for sym in symbols:
        bars = fetch_1min_bars(api, sym, days)
        if len(bars) < 60:
            continue
        # 把bars轉成extract_ml_features需要的entries格式（沒有tick_type/buy_vol/sell_vol，
        # 因為kbars歷史資料沒有這些欄位——這跟main.py即時推論時的特徵會有落差，這是已知限制，
        # 見下方"已知限制"說明，先用0/None代替，模型還是能從其他特徵學到東西）
        entries = [{"price": b["close"], "volume": b["volume"], "t": b["ts"],
                    "buy_vol": None, "sell_vol": None, "tick_type": 0} for b in bars]
        prices_all = [b["close"] for b in bars]
        volumes_all = [b["volume"] for b in bars]
        n_samples_this_sym = 0
        for i in range(30, len(bars) - LABEL_MAX_HOLD_MIN - 1, stride):
            mkt_ret = get_market_context_at(market_entries, market_ts_list, bars[i]["ts"])
            tod_pct = get_time_of_day_pct(bars[i]["ts"])
            feats = extract_ml_features(sym, prices_all[:i+1], volumes_all[:i+1], entries[:i+1], "L", mkt_ret, tod_pct)
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

def load_real_feature_log(csv_path):
    """讀取從/auto/feature_log.csv下載的真實交易紀錄，轉成(X_real, y_real)。
    這份資料樣本少，但OFI/價差/大盤同步性都是真實數值(不是歷史重建那種固定填0)，
    是目前唯一能讓模型真正學會用這幾個特徵的資料來源。"""
    import csv as csv_module
    X_real, y_real = [], []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv_module.DictReader(f)
        for row in reader:
            try:
                feats = [float(row[name]) for name in ML_FEATURE_NAMES]
                label = int(row["win"])
            except (KeyError, ValueError):
                continue  # 欄位缺失或格式不對的列跳過，不要讓一筆壞資料擋掉整批
            X_real.append(feats); y_real.append(label)
    return X_real, y_real

def evaluate_model_file(model_path, X_valid, y_valid):
    """載入一個既有模型檔案，在同一份驗證集上算AUC，用來跟新訓練的模型比較。
    檔案不存在/載入失敗都回傳None，呼叫端要當作「沒有舊模型可比較」處理，不要當成錯誤。"""
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score
    if not os.path.exists(model_path): return None
    try:
        old_model = lgb.Booster(model_file=model_path)
        pred = old_model.predict(X_valid)
        return roc_auc_score(y_valid, pred)
    except Exception as e:
        print(f"  ⚠️ 無法載入比較模型 {model_path}: {e}")
        return None


def main():
    ap = argparse.ArgumentParser(description="訓練LightGBM做多勝率模型")
    ap.add_argument("--symbols", nargs="*", default=None, help="只訓練這幾檔(預設用完整SCAN_UNIVERSE)")
    ap.add_argument("--days", type=int, default=90, help="抓多少天的歷史資料(預設90天)")
    ap.add_argument("--stride", type=int, default=5, help="每隔幾根棒子取一個樣本(預設5)")
    ap.add_argument("--output", default="lgbm_model.txt", help="模型輸出路徑")
    ap.add_argument("--feature-log-csv", default=None,
                     help="從/auto/feature_log.csv下載的真實交易紀錄路徑。提供的話會跟歷史重建資料合併"
                          "一起從零訓練(不是續訓)，讓模型有機會學會用OFI/價差這幾個歷史資料裡固定是0的特徵。")
    ap.add_argument("--real-data-weight-fraction", type=float, default=0.3,
                     help="真實交易資料在合併訓練時佔的權重比例(預設0.3=30%%)，不是樣本數比例——"
                          "樣本數通常少很多，用權重讓它在訓練時的影響力不會被歷史資料的數量淹沒。"
                          "這個數字沒有校準過，是合理起點，建議多跑幾次、觀察AUC變化再調整。")
    ap.add_argument("--compare-with", default=None,
                     help="拿新訓練的模型跟這個既有模型檔案比較AUC(預設跟--output同一個路徑比，"
                          "也就是跟現在正在用/即將被覆蓋的那個模型比較)")
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
    print(f"\n歷史重建樣本數: {len(X)} | 正樣本(做多成功)比例: {sum(y)/len(y)*100:.1f}%" if y else "⚠️ 沒有抓到任何樣本")
    if len(X) < 200:
        print("⚠️ 樣本數太少(<200)，模型品質會很差，建議增加--days或檢查資料來源是否正常")
        if len(X) == 0:
            sys.exit(1)

    import numpy as np
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score, accuracy_score

    X = np.array(X); y = np.array(y)
    # 用時間順序切分train/valid(不能隨機shuffle，否則用未來資料預測過去=資料洩漏)。
    # 注意：這裡先把歷史資料自己切好train/valid，還沒跟真實資料合併——
    # 如果是合併完「歷史+真實」整批再切80/20，因為真實資料是後面才append上去的一小段，
    # 時間順序切割會讓真實資料幾乎全部被切進valid那20%，train那80%可能完全沒有真實樣本，
    # 等於白白合併了卻沒有真的拿真實資料去訓練(這個問題用合成資料實測抓到過，AUC會掉到0.475，
    # 跟完全沒合併一樣差，一定要先各自切好才合併，不能合併後再切)。
    split = int(len(X) * 0.8)
    X_train, X_valid = X[:split], X[split:]
    y_train, y_valid = y[:split], y[split:]
    w_train = np.ones(len(X_train))  # 歷史重建資料權重固定1，下面合併真實資料時只調整真實資料那邊的權重

    # 合併真實交易資料(如果有提供)——不是續訓，是把兩份資料當作同一個訓練集，從零開始一起訓練。
    # 上次實測過用init_model續訓的方式，模型完全學不會用OFI這種「歷史資料固定0、真實資料才有值」
    # 的特徵(驗證集AUC只有0.484，等於瞎猜)；合併後從零訓練的方式則能正確學會(AUC 0.862)。
    if args.feature_log_csv:
        print(f"\n讀取真實交易紀錄: {args.feature_log_csv}")
        X_real, y_real = load_real_feature_log(args.feature_log_csv)
        if not X_real:
            print("  ⚠️ 沒有讀到任何真實交易樣本(檔案是空的或格式不對)，本次訓練只用歷史重建資料")
        else:
            print(f"  讀到{len(X_real)}筆真實交易樣本，正樣本比例: {sum(y_real)/len(y_real)*100:.1f}%")
            if len(X_real) < 20:
                print(f"  ⚠️ 真實樣本只有{len(X_real)}筆，遠少於20筆驗證門檻，這次合併訓練的效果還是有限，"
                      f"參考用就好，不要太相信合併後的結果")
            X_real = np.array(X_real); y_real = np.array(y_real)
            # 真實資料也用一樣的時間順序邏輯各自切train/valid，再分別併進對應的train/valid，
            # 不是合併後再切——這樣train一定會看到真實樣本，valid也會看到，兩邊都不會是空的。
            split_real = max(1, int(len(X_real) * 0.8)) if len(X_real) >= 5 else len(X_real)
            X_real_train, X_real_valid = X_real[:split_real], X_real[split_real:]
            y_real_train, y_real_valid = y_real[:split_real], y_real[split_real:]
            # 用權重讓真實資料的「總影響力」達到指定比例，不是直接看樣本數比例——
            # 樣本數通常差幾十倍，沒有調權重的話歷史資料會完全淹沒真實資料的訊號。
            frac = args.real_data_weight_fraction
            w_real_each = (frac * len(X_train)) / max(1,((1-frac) * len(X_real_train)))
            print(f"  真實資料每筆權重: {w_real_each:.2f}(讓真實資料佔總訓練權重約{frac*100:.0f}%)；"
                  f"train併入{len(X_real_train)}筆、valid併入{len(X_real_valid)}筆")
            X_train = np.vstack([X_train, X_real_train]); y_train = np.concatenate([y_train, y_real_train])
            w_train = np.concatenate([w_train, np.full(len(X_real_train), w_real_each)])
            if len(X_real_valid) > 0:
                X_valid = np.vstack([X_valid, X_real_valid]); y_valid = np.concatenate([y_valid, y_real_valid])

    train_data = lgb.Dataset(X_train, label=y_train, weight=w_train, feature_name=ML_FEATURE_NAMES)
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
    print(f"\n=== 新模型驗證集表現 ===\nAUC: {auc:.3f} (0.5=瞎猜, 越接近1越好, 通常0.55~0.65已經算有用)")
    print(f"準確率: {acc:.3f}")
    print("\n=== 特徵重要性 ===")
    importance = sorted(zip(ML_FEATURE_NAMES, model.feature_importance()), key=lambda x: -x[1])
    for name, imp in importance:
        print(f"  {name}: {imp}")

    # 安全網：拿新模型跟現在實際部署/即將被覆蓋的舊模型在同一份驗證集上比較，
    # 不自動阻擋儲存(你可能還是想看看新模型長怎樣)，但會把結果講清楚，不要悶著頭直接換上去。
    compare_path = args.compare_with or args.output
    old_auc = evaluate_model_file(compare_path, X_valid, y_valid)
    print("\n=== 新舊模型比較 ===")
    if old_auc is None:
        print(f"沒有找到既有模型({compare_path})可比較，這應該是第一次訓練。")
    else:
        diff = auc - old_auc
        print(f"舊模型({compare_path}) AUC: {old_auc:.3f}")
        print(f"新模型 AUC: {auc:.3f}（差異 {diff:+.3f}）")
        if diff < -0.02:
            print("⚠️⚠️⚠️ 新模型比舊模型明顯更差，不建議用這次訓練的結果覆蓋現在的模型！")
            print("   建議檢查這次的資料/參數是否有問題，或暫時保留舊模型繼續使用。")
        elif diff < 0:
            print("新模型略差於舊模型，差異不算大，但建議謹慎，可以多訓練幾次看看穩不穩定再決定要不要換。")
        else:
            print("✅ 新模型不差於舊模型，可以考慮用這次的結果替換。")

    model.save_model(args.output)
    print(f"\n✅ 模型已存到 {args.output}（上面的新舊比較結果請先看過，再決定要不要部署這個檔案）")
    print("把這個檔案放到後端服務工作目錄(跟main.py同一層)，重啟/重新部署後端即可生效。")
    if auc < 0.55:
        print("\n⚠️ AUC接近0.5，代表模型幾乎沒有預測力，不建議直接上線使用——")
        print("   可能是樣本太少、標籤定義不適合這個股票池、或特徵跟結果關聯本來就弱。")
        print("   建議先增加--days、確認資料品質，或重新檢視標籤定義再訓練一次。")

if __name__ == "__main__":
    main()
