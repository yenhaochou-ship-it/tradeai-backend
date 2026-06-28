"""
signals.py — 技術指標/市場環境/OFI大單流引擎。從main.py拆出來。

這裡的函式大多是「給什麼資料就算出什麼結果」的純函式，少數需要跨股票共用資料的
(get_market_context, advanced_score)從state.py讀price_cache(就地修改的共用物件，見state.py說明)。

OFI模組的subscribe_ofi_symbols/unsubscribe_all_ofi需要永豐API物件(sinopac_api)，
但main.py的sinopac_api會在連線/斷線時被整個重新賦值(= sj.Shioaji(...) 或 = None)，
不是dict那種就地修改——這種情況下用「from main import sinopac_api」是危險的(會抓到匯入當下
那一刻的舊值，main.py之後重新賦值不會反映過來)，所以這裡改成讓呼叫端(main.py)把目前的
sinopac_api當參數傳進來，每次呼叫都傳最新的，不依賴模組層級共享一個會被整個換掉的物件。
"""
import time, logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple

from config import tw_now, is_tw_market_holiday, TW_TZ
from state import price_cache

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
# Python 信號引擎 v3（與前端 calcSignal 邏輯一致）
# ══════════════════════════════════════════════════════════════════
def _calc_rsi(prices: List[float], period: int = 14) -> List[float]:
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

def _ema(data: List[float], n: int) -> List[float]:
    k, r = 2/(n+1), [data[0]]
    for p in data[1:]: r.append(p*k + r[-1]*(1-k))
    return r

def calc_signal_py(prices: List[float], volumes: List[float]) -> Dict:
    """完整信號計算（12個指標 + 趨勢過濾）"""
    if len(prices) < 30:
        return {"action":"hold","conf":50,"rsi":50}
    price = prices[-1]
    ma5   = sum(prices[-5:])/5
    ma20  = sum(prices[-20:])/20
    ma50  = sum(prices[-50:])/50 if len(prices)>=50 else ma20

    rsi_arr   = _calc_rsi(prices)
    rsi       = rsi_arr[-1]

    vp,vv     = prices[-20:], (volumes[-20:] if volumes else [1e6]*20)
    total_v   = sum(vv) or 1
    vwap      = sum(p*v for p,v in zip(vp,vv))/total_v
    mean20    = ma20
    var       = sum((p-mean20)**2 for p in prices[-20:])/20
    bb_upper  = mean20+2*var**0.5
    bb_lower  = mean20-2*var**0.5
    bb_pct    = (price-bb_lower)/(bb_upper-bb_lower or 1)
    avg_vol   = (sum(volumes[-20:])/20 if volumes else 1) or 1  # 修正：原本只防volumes是空list，沒防「volumes不是空的但全部是0」
    vol_ratio = (sum(volumes[-3:])/3 if volumes else 1)/avg_vol  # (完全沒成交量的冷門股/剛掛牌股票會踩到，這裡用or 1防除以0)

    fresh_golden=fresh_death=False
    macd_val=sig_val=0
    if len(prices)>=26:
        e12=_ema(prices,12); e26=_ema(prices,26)
        ml=[a-b for a,b in zip(e12[-9:],e26[-9:])]
        sl2=_ema(ml,9); macd_val,sig_val=ml[-1],sl2[-1]
        if len(ml)>1:
            fresh_golden=macd_val>sig_val and ml[-2]<=sl2[-2]
            fresh_death =macd_val<sig_val and ml[-2]>=sl2[-2]

    ws=prices[-14:]; wh,wl=max(ws),min(ws)
    w_r=-(wh-price)/(wh-wl)*100 if wh!=wl else -50
    cs=prices[-20:]; cm=sum(cs)/20; cd=sum(abs(p-cm) for p in cs)/20 or 1
    cci=(price-cm)/(0.015*cd)
    r14=prices[-14:]; mp,np2=max(r14),min(r14)
    am=sum(abs(r14[i]-r14[i-1]) for i in range(1,len(r14)))/13 or 1
    trend_str=min(1.0,(mp-np2)/(am*14))
    up_trend  = ma5>ma20 and ma20>ma50
    down_trend= ma5<ma20 and ma20<ma50
    t=tw_now(); tm=t.hour*60+t.minute
    # 與前端一致：09:00-09:30開盤亂流不進場、13:00後不開新倉，12:00-13:00午盤降權但不完全禁止
    bad_time  = tm<9*60+30 or tm>=13*60
    low_quality = 12*60<=tm<13*60

    bull=bear=0.0
    if rsi<25:bull+=20
    elif rsi<33:bull+=13
    elif rsi<42:bull+=6
    elif rsi>75:bear+=20
    elif rsi>67:bear+=13
    elif rsi>58:bear+=6
    if fresh_golden:bull+=25
    elif fresh_death:bear+=25
    elif macd_val>sig_val:bull+=11
    else:bear+=11
    if ma5>ma20:bull+=20
    else:bear+=20
    if price>vwap*1.002:bull+=12
    elif price<vwap*0.998:bear+=12
    else:bull+=5;bear+=5
    if bb_pct<0.12:bull+=8
    elif bb_pct<0.25:bull+=4
    elif bb_pct>0.88:bear+=8
    elif bb_pct>0.75:bear+=4
    if vol_ratio>1.8 and bull>bear:bull+=8
    elif vol_ratio>1.8 and bear>bull:bear+=8
    elif vol_ratio<0.6:bull*=0.85;bear*=0.85
    if w_r<-80:bull+=6
    elif w_r<-60:bull+=3
    elif w_r>-20:bear+=6
    elif w_r>-40:bear+=3
    if cci<-100:bull+=5
    elif cci<-50:bull+=2
    elif cci>100:bear+=5
    elif cci>50:bear+=2
    if trend_str>0.65:
        if ma5>ma20:bull+=7
        else:bear+=7
    if up_trend and bull>bear:bull*=1.15
    elif down_trend and bear>bull:bear*=1.15
    elif up_trend and bear>bull:bear*=0.70
    elif down_trend and bull>bear:bull*=0.70
    if bad_time:bull*=0.72;bear*=0.72
    if low_quality:bull*=0.85;bear*=0.85

    total=bull+bear or 1
    bp=bull/total*100
    action="buy" if bp>=67 else "sell" if bp<=33 else "hold"
    conf=min(95, bp if action=="buy" else 100-bp if action=="sell" else 50)
    return {"action":action,"conf":round(conf,1),"rsi":round(rsi,1),
            "ma5":round(ma5,2),"ma20":round(ma20,2),"vwap":round(vwap,2),
            "williams_r":round(w_r,1),"cci":round(cci,1),"trend_str":round(trend_str,2),
            "vol_ratio":round(vol_ratio,2),
            "bull":round(bull,1),"bear":round(bear,1),
            "up_trend":up_trend,"down_trend":down_trend,"bad_time":bad_time}

# ══════════════════════════════════════════════════════════════════
# 進階訊號引擎（依使用者提供的規格書整合，Phase 1：只用既有OHLCV資料就能算的部分）
#
# 沒做的部分(Phase 2/3，需要額外資料來源，先誠實列出，不假裝已經做了)：
#   - ML「Trade Quality」重新訓練 + Meta Model：需要足量已標記的歷史交易結果才能訓練，
#     目前真實交易/模擬交易的歷史筆數還太少（剛好是上次加的20筆驗證門檻在保護的東西）。
#   - 重大新聞前/法說會前：需要公司行事曆或新聞源，目前沒有接這類資料。
#
# 設計上跟原規格書不同的地方（刻意，不是漏改）：
#   - 原規格「最終進場條件」要求12個條件同時成立(AND)。實際測過，這樣的門檻配上我們刻意選的
#     低波動銀行股池，幾乎不會有任何訊號通過(多個條件各自過關率不到100%，乘起來機率極低)。
#     改成「加權總分」(下面 advanced_score)，精神一樣(多因子都要有一定水準才給高分)，
#     但不會因為單一條件沒過就整個歸零，避免策略變成永遠不交易。
# ══════════════════════════════════════════════════════════════════

def _entry_ts(e:Dict) -> Optional[float]:
    """價格快取裡每筆紀錄的時間戳(epoch秒)，舊資料/還沒補時間戳的情況回傳None"""
    return e.get("t")

def classify_market_regime(prices: List[float]) -> Dict:
    """市場環境分類：trending_bull / trending_bear / range / volatile / panic
    用近20筆報酬率標準差(波動度代用ATR)+近14筆趨勢強度(複用calc_signal_py同一套算法)分類，
    不需要額外資料來源。門檻是合理起點，沒有經過正式校準，建議之後拿真實交易結果回頭驗證調整。"""
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
    if vol>=PANIC_VOL and cum_ret<=-0.02:
        return {"regime":"panic","confidence":min(100,round(vol/PANIC_VOL*60)),"vol":round(vol,4)}
    if vol>=VOLATILE_VOL:
        return {"regime":"volatile","confidence":min(100,round(vol/VOLATILE_VOL*55)),"vol":round(vol,4)}
    if trend_strength>=TREND_TH and cum_ret>0:
        return {"regime":"trending_bull","confidence":round(trend_strength*100),"vol":round(vol,4)}
    if trend_strength>=TREND_TH and cum_ret<0:
        return {"regime":"trending_bear","confidence":round(trend_strength*100),"vol":round(vol,4)}
    return {"regime":"range","confidence":round((1-trend_strength)*100),"vol":round(vol,4)}

def _bucket_closes(entries:List[Dict], bucket_seconds:int) -> List[float]:
    """把有時間戳的價格序列依固定秒數分桶，每桶取最後一筆當「收盤價」，近似聚合出較大時間框架的K線收盤序列"""
    buckets={}
    for e in entries:
        ts=_entry_ts(e)
        if ts is None: continue
        buckets[int(ts//bucket_seconds)]=e["price"]
    return [buckets[k] for k in sorted(buckets.keys())]

def multi_timeframe_direction(entries:List[Dict]) -> Dict:
    """用price_cache裡的時間戳，分桶聚合成近似1分/5分/15分收盤序列，檢查三個時間框架方向是否一致。
    資料不足(例如剛啟動、還沒累積15分鐘的歷史)時對應框架回報unknown，aligned_long/short會是None(不確定)，
    不會被當成「自動算過關」——呼叫端要把None當成中性看待，不是當成True。"""
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
        if not knowns: return None  # 完全沒資料可判斷，回傳None(不確定)，不要預設True或False
        return all(d!=forbidden for d in knowns)
    return {"d15":d15,"d5":d5,"d1":d1,
            "aligned_long":_aligned("bear"),
            "aligned_short":_aligned("bull")}

MARKET_INDEX_SYMBOL="0050"  # 用市值型ETF(0050)的走勢當大盤方向的免費代理指標，不用另外處理期貨轉倉的複雜度

def get_market_context() -> Dict:
    """大盤同步性：用0050最近5分鐘的報酬率代表「現在大盤往哪個方向走」，
    給其他個股的特徵向量參考——個股訊號方向跟大盤對著做時，實際勝率通常會打折，
    但這只是參考特徵，不是硬性gate，因為逆勢仍然有機會是對的，不該直接擋掉。
    資料不足時回傳None，呼叫端要當中性值處理，不要預設成0(0本身就是「大盤平盤」的有效答案，跟"沒資料"是不同的兩件事，
    所以這裡用None跟0.0分開，呼叫端再決定怎麼處理)。"""
    entries=price_cache.get(MARKET_INDEX_SYMBOL,[])
    m5=_bucket_closes(entries,300)
    if len(m5)<4: return {"return_5m_pct":None}
    base=m5[-4]
    if not base: return {"return_5m_pct":None}
    return {"return_5m_pct":round((m5[-1]-base)/base*100,3)}

def get_time_of_day_pct(ts:Optional[float]=None) -> float:
    """距離開盤(09:00)經過的時間，正規化成0~1(以270分鐘=09:00~13:30當一個交易日的長度估算)。
    跟前面OFI/價差/大盤同步性這幾個新特徵不一樣的地方：這個不需要任何即時訂閱或外部資料，
    純粹是時間運算，現在就能直接用真實數值，不會有「歷史資料沒有、只能填中性值0」的限制——
    SHAP分析文件原本提的「依時段做OFI標準化」需要先有足量歷史資料才能校準，這裡換個做法：
    直接把時間本身當一個特徵讓模型自己學「早盤跟尾盤的型態不一樣」，現在就能用，不用等資料累積。"""
    if ts is None: ts=time.time()
    dt=datetime.fromtimestamp(ts,tz=TW_TZ)
    minutes=dt.hour*60+dt.minute-9*60
    return round(max(0.0,min(1.0,minutes/270)),3)

def orb_range(entries:List[Dict], date_str:str) -> Optional[Dict]:
    """取得當天09:00~09:15開盤區間的高低點(ORB)。沒有當天時間戳資料(例如太晚才連線、price_cache還沒暖機到含這段)回傳None"""
    morning=[]
    for e in entries:
        ts=_entry_ts(e)
        if ts is None: continue
        dt=datetime.fromtimestamp(ts,tz=TW_TZ)
        if dt.strftime("%Y-%m-%d")==date_str and 9*60<=dt.hour*60+dt.minute<9*60+15:
            morning.append(e["price"])
    if not morning: return None
    return {"high":max(morning),"low":min(morning)}

def volume_quality_score(volumes: List[float], entries:Optional[List[Dict]]=None) -> Dict:
    """成交量品質分數(0~100)。Phase 1只有Volume Ratio+Relative Volume；
    Phase 2新增Bid-Ask Imbalance(用snapshot的buy_vol/sell_vol欄位，不需要額外訂閱)。
    Large Order Ratio仍然沒做：那個需要看每一筆成交的實際大小去分類「大單」，
    snapshot只給我們最後一筆成交+目前委買委賣量，看不到整天的逐筆成交分佈，
    沒有訂閱tick資料流的話沒辦法做這個，不是不想做，是現有資料真的算不出來。"""
    if len(volumes)<20: return {"score":50.0,"note":"資料不足，給中性分數"}
    avg20=sum(volumes[-20:])/20 or 1
    vol_ratio=(sum(volumes[-3:])/3)/avg20
    baseline=sum(volumes[-60:])/60 if len(volumes)>=60 else avg20
    rel_vol=avg20/(baseline or 1)
    bai=bid_ask_imbalance(entries) if entries else {"imbalance":0.0,"note":"無資料"}
    # 買賣力道失衡(-1~1)轉成0~100分：失衡越偏多方(正值)分數越高
    bai_score=50+bai["imbalance"]*50
    score=min(100.0,max(0.0,40*min(vol_ratio,2)+20*min(rel_vol,2)+0.4*bai_score))
    return {"score":round(score,1),"vol_ratio":round(vol_ratio,2),"rel_vol":round(rel_vol,2),
            "bid_ask_imbalance":bai["imbalance"],
            "note":"未含Large Order Ratio(需逐筆成交明細，僅snapshot資料無法計算)"}

def bid_ask_imbalance(entries: List[Dict]) -> Dict:
    """買賣力道失衡：用snapshot的委買量(buy_vol)/委賣量(sell_vol)算，近10筆平均，範圍-1~1。
    這是目前最佳買賣各一檔的委託量比較，不是逐筆成交流，但不需要額外訂閱tick/bidask資料流就能算，
    每30秒輪詢時snapshot本來就會回傳這兩個欄位。"""
    recent=[e for e in entries[-10:] if e.get("buy_vol") is not None and e.get("sell_vol") is not None]
    if not recent: return {"imbalance":0.0,"note":"無資料"}
    vals=[(e["buy_vol"]-e["sell_vol"])/((e["buy_vol"]+e["sell_vol"]) or 1) for e in recent]
    return {"imbalance":round(sum(vals)/len(vals),3),"n":len(recent)}

def approx_order_flow(entries: List[Dict]) -> Dict:
    """用snapshot的tick_type(最近一筆成交是外盤買進/內盤賣出)＋每筆紀錄本身的區間成交量(volume欄位，
    現在統一是「這段期間的增量」，不是累計量——見_update_cache的修正)，近似估算Delta(買賣力道淨額)/CVD(累積Delta)。
    重要警示：這是「輪詢近似」，不是真正逐筆Delta——30秒之間發生的所有成交，我們只看得到
    輪詢當下最後一筆的方向，中間被吃掉的買賣力道完全看不到，所以這個值有偏差，只能當參考，
    不是精確值。真正精確的Delta/CVD需要訂閱tick資料流逐筆累加，這裡沒有做(見上方說明)。"""
    deltas=[]
    for e in entries:
        vol=e.get("volume")
        if vol is None or vol<=0: continue
        tt=e.get("tick_type",0)
        sign=1 if tt==1 else (-1 if tt==2 else 0)
        deltas.append(sign*vol)
    if not deltas: return {"cvd":0,"delta_recent":0,"note":"無逐筆方向資料(僅輪詢近似，非精確值)"}
    return {"cvd":sum(deltas),"delta_recent":sum(deltas[-10:]),"note":"輪詢近似值，非精確逐筆Delta"}

def detect_absorption(prices: List[float], entries: List[Dict]) -> Dict:
    """規格書第六層的「假突破偵測」概念：價格創近期新高，但買賣力道(Delta)沒有同步增加甚至衰退，
    判定為可能的假突破(Potential Fake Breakout)。用approx_order_flow算的近似Delta，
    精準度受限於上面說明的輪詢近似問題，當作風險警示用，不是100%準確的判定。"""
    if len(prices)<15: return {"fake_breakout_risk":False}
    making_new_high=prices[-1]>=max(prices[-15:-1])
    of=approx_order_flow(entries[-15:])
    return {"fake_breakout_risk":making_new_high and of["delta_recent"]<=0,
            "delta_recent":of["delta_recent"],"note":of["note"]}

# ══════════════════════════════════════════════════════════════════
# 模組一：即時大單流 OFI 引擎（真逐筆tick/bidask訂閱，取代上面approx_order_flow的輪詢近似版）
#
# 重要更正：一開始我以為Tick資料本身就有bid_price/ask_price可以用，省掉另外訂閱BidAsk——
# 這是錯的。我去查證Shioaji官方文件完整欄位列表後發現，TickSTKv1根本沒有bid_price/ask_price
# 這些欄位(它只有volume/tick_type/bid_side_total_vol這類彙總資訊)，bid_price/bid_volume/
# ask_price/ask_volume只存在於另一條獨立的BidAskSTKv1資料流。所以這裡需要分開訂閱+處理：
#   - Tick資料流(TickSTKv1)：volume(這筆成交量) + tick_type(1=外盤買進 2=內盤賣出) → 算大單流
#   - BidAsk資料流(BidAskSTKv1)：bid_price/bid_volume/ask_price/ask_volume(五檔，這裡只用最佳一檔)
#     → 算OFI(委託簿不平衡)
#
# ⚠️ 跟之前所有Phase1/2引擎一樣的免責聲明：這段程式碼沒有真實永豐連線測試過，
# 欄位名稱/訂閱語法都查證自官方文件並對照過真實範例輸出，但實際訂閱/callback行為
# （會不會準時收到、46檔×2條資料流=92個訂閱會不會卡額度、斷線重連)沒有人驗證過。
# 上線後第一件事是看log確認「OFI訂閱成功N/M檔」，並觀察是否持續收到tick/bidask更新。
# ══════════════════════════════════════════════════════════════════
class RealTimeOFI:
    """單一股票的即時大單流計算器。OFI跟大單流分別來自BidAsk跟Tick兩條不同資料流(見上方說明)。
    修正：原本用EMA/衰減加總平滑，但這樣「現在讀到的值」混雜了好幾個30秒週期以前的資訊，
    跟決策引擎每30秒才讀一次的節奏對不齊。改成「累積這個週期內的所有事件，被讀取(flush)時
    立刻歸零，開始累積下一個週期」——這樣每次決策拿到的就是「乾乾淨淨剛剛這30秒發生的事」，
    跟bad_time/no_trade_zone這些每30秒重新評估一次的其他風控邏輯節奏一致。"""
    def __init__(self, big_trade_threshold:int=50):
        self.big_trade_threshold=big_trade_threshold  # 台股大單張數門檻，預設單筆超過50張算大單
        self._prev_bidask=None       # 上一筆BidAsk最佳一檔快照，算OFI delta用(不隨週期重置，要連續比較)
        self.interval_ofi=0.0        # 這個週期內累積的OFI
        self.interval_big_trade=0.0  # 這個週期內累積的大單流

    def update_bidask(self, bidask):
        """bidask是Shioaji的BidAskSTKv1物件，把這次更新的OFI累加進這個週期的累積值"""
        cur={"bid_price":float(bidask.bid_price[0]),"bid_volume":int(bidask.bid_volume[0]),
             "ask_price":float(bidask.ask_price[0]),"ask_volume":int(bidask.ask_volume[0])}
        prev=self._prev_bidask
        self._prev_bidask=cur
        if prev is None: return  # 第一筆沒有對照，跳過這次(不影響累積值)
        # OFI核心公式：買一價上漂等於買方掛單整批新增，賣一價下跌等於賣方掛單整批新增，
        # 價格不變則看掛單量的淨變化；買方淨增量-賣方淨增量
        if cur["bid_price"]>prev["bid_price"]: delta_bid=cur["bid_volume"]
        elif cur["bid_price"]==prev["bid_price"]: delta_bid=cur["bid_volume"]-prev["bid_volume"]
        else: delta_bid=0
        if cur["ask_price"]<prev["ask_price"]: delta_ask=cur["ask_volume"]
        elif cur["ask_price"]==prev["ask_price"]: delta_ask=cur["ask_volume"]-prev["ask_volume"]
        else: delta_ask=0
        self.interval_ofi+=float(delta_bid-delta_ask)

    def update_tick(self, tick):
        """tick是Shioaji的TickSTKv1物件，把這筆成交的大單流(若有)累加進這個週期的累積值。
        修正(感謝使用者文件提醒)：tick_type==0(無法判定/集合競價)原本被歸進else當成賣出方向算，
        這是錯的——無法判定不等於賣出，應該完全不計入方向，否則會把中性事件誤判成賣壓。"""
        vol=int(tick.volume)
        if vol<self.big_trade_threshold: return
        tt=int(tick.tick_type)
        if tt==1: self.interval_big_trade+=float(vol)        # 外盤(買方主動敲進)
        elif tt==2: self.interval_big_trade-=float(vol)       # 內盤(賣方主動殺出)
        # tt==0(無法判定)：不計入，維持原值不變

    def flush(self) -> Tuple[float,float]:
        """由_update_cache每30秒呼叫一次：取出這個週期累積的(ofi, big_trade)，並立刻歸零開始下一輪。
        _prev_bidask不歸零，因為那是用來比較「下一筆bidask跟上一筆」的連續狀態，不是週期累積值。"""
        ofi,big_trade=self.interval_ofi,self.interval_big_trade
        self.interval_ofi=0.0; self.interval_big_trade=0.0
        return ofi,big_trade

_ofi_engines: Dict[str,"RealTimeOFI"] = {}
_ofi_latest: Dict[str,Dict] = {}     # {sym: {"ofi":上一個完整30秒週期的OFI累積值, "big_trade":同期大單流累積值, "updated_at":epoch秒}}
_ofi_subscribed: set = set()         # 已成功訂閱tick+bidask的股票，避免重複訂閱

def _on_bidask_stk_v1(exchange, bidask):
    """Shioaji BidAsk callback：訂閱股票的委託簿(五檔)有變動就會被呼叫一次，事件驅動、隨時觸發。
    注意：跑在Shioaji自己的內部執行緒，這裡只做簡單累加，不做重運算/I/O，避免拖慢回調速度。"""
    try:
        sym=bidask.code
        if sym not in _ofi_engines: _ofi_engines[sym]=RealTimeOFI()
        _ofi_engines[sym].update_bidask(bidask)
    except Exception as e:
        logger.warning(f"OFI bidask callback處理失敗: {e}")

def _on_tick_stk_v1(exchange, tick):
    """Shioaji Tick callback：每收到一筆訂閱股票的逐筆成交就會被呼叫一次，事件驅動、隨時觸發。
    注意：這個函式跑在Shioaji自己的內部執行緒，不是FastAPI/排程器那條主執行緒——
    這裡只做簡單的累加(在CPython的GIL保護下，單一物件的屬性更新是安全的)，
    不做任何重運算或I/O，避免拖慢回調速度或引入跨執行緒的競爭風險。"""
    try:
        sym=tick.code
        if sym not in _ofi_engines: _ofi_engines[sym]=RealTimeOFI()
        _ofi_engines[sym].update_tick(tick)
    except Exception as e:
        logger.warning(f"OFI tick callback處理失敗: {e}")

def _flush_all_ofi():
    """由_update_cache每30秒呼叫一次（橋接「事件驅動的tick/bidask」跟「定時驅動的決策」兩種節奏）：
    把每檔已訂閱股票這個週期累積的OFI/大單流取出存進_ofi_latest，並讓引擎歸零開始下一輪累積。
    對「所有已訂閱」的股票都做，不是只對這次有進入候選名單的股票做——這樣才能保證每檔股票
    的累積週期都乾淨對齊30秒節奏，不會因為某次被no_trade_zone等規則篩掉就漏了一次flush，
    導致下次讀到的其實是跨了好幾個30秒週期的累積值。"""
    for sym in list(_ofi_subscribed):
        engine=_ofi_engines.get(sym)
        if engine is None: continue
        ofi,big_trade=engine.flush()
        _ofi_latest[sym]={"ofi":ofi,"big_trade":big_trade,"updated_at":time.time()}

def subscribe_ofi_symbols(symbols, api, auto_state) -> int:
    """訂閱即時Tick+BidAsk資料流以啟用真實OFI計算(每檔股票需要2個訂閱)。
    api/auto_state由main.py呼叫時傳入目前最新的值(見檔案最上面說明，sinopac_api會被整個重新賦值，
    不能靠模組層級共享同一個物件，每次呼叫都要傳最新的進來)。
    逐檔try/except，一檔失敗不影響其他檔，回傳成功訂閱數量——
    如果這個數字遠小於預期，代表可能撞到訂閱數上限(46檔*2=92個訂閱不算少)，要去看log warning。
    修正：原本失敗只寫進log warning，使用者想知道「具體是哪幾檔」得去翻後端log——
    現在額外記錄進auto_state["ofi_failed_symbols"]，前端點log那一行就能直接看到清單。"""
    import shioaji as sj
    if not api: return 0
    ok_count=0
    failed=[]
    for sym in symbols:
        if sym in _ofi_subscribed: continue
        try:
            contract=api.Contracts.Stocks.get(sym)
            if not contract: failed.append(sym); continue
            api.quote.subscribe(contract,quote_type=sj.constant.QuoteType.Tick,version=sj.constant.QuoteVersion.v1)
            api.quote.subscribe(contract,quote_type=sj.constant.QuoteType.BidAsk,version=sj.constant.QuoteVersion.v1)
            _ofi_subscribed.add(sym)
            ok_count+=1
        except Exception as e:
            failed.append(sym)
            logger.warning(f"OFI訂閱{sym}失敗(可能撞到訂閱數上限): {e}")
    if failed:
        auto_state["ofi_failed_symbols"]=failed
    return ok_count

def unsubscribe_ofi_symbols(symbols, api):
    """取消訂閱「指定的這幾檔」，跟unsubscribe_all_ofi(取消全部)不一樣——用在自選股被移除時，
    只清掉被移除那幾檔的訂閱，不影響還留著的其他股票。修正：原本watchlist更新只會幫新增的股票
    補訂閱，被移除的股票訂閱會一直留著(只有整個斷線登出才會清)，使用者如果常常換自選股，
    訂閱數會隨時間一路往上累積，最終可能撞到Shioaji的訂閱數上限，而且原因跟「目前」自選股
    清單大小完全無關，難以察覺。"""
    if not api or not symbols: return
    import shioaji as sj
    for sym in symbols:
        if sym not in _ofi_subscribed: continue
        try:
            contract=api.Contracts.Stocks.get(sym)
            if contract:
                api.quote.unsubscribe(contract,quote_type=sj.constant.QuoteType.Tick,version=sj.constant.QuoteVersion.v1)
                api.quote.unsubscribe(contract,quote_type=sj.constant.QuoteType.BidAsk,version=sj.constant.QuoteVersion.v1)
            _ofi_subscribed.discard(sym)
        except Exception as e:
            logger.warning(f"OFI取消訂閱{sym}失敗: {e}")

def unsubscribe_all_ofi(api):
    if not api: return
    import shioaji as sj
    for sym in list(_ofi_subscribed):
        try:
            contract=api.Contracts.Stocks.get(sym)
            if contract:
                api.quote.unsubscribe(contract,quote_type=sj.constant.QuoteType.Tick,version=sj.constant.QuoteVersion.v1)
                api.quote.unsubscribe(contract,quote_type=sj.constant.QuoteType.BidAsk,version=sj.constant.QuoteVersion.v1)
        except Exception as e:
            logger.warning(f"OFI取消訂閱{sym}失敗: {e}")
    _ofi_subscribed.clear(); _ofi_engines.clear(); _ofi_latest.clear()

def get_real_ofi(sym:str) -> Optional[Dict]:
    """回傳上一個完整30秒週期的OFI/大單流快照。沒有訂閱這支股票回傳None；
    已訂閱但這段時間沒有任何tick/bidask事件，會合理地拿到{"ofi":0.0,"big_trade":0.0}
    (代表這30秒確實沒有方向性流動，0是有意義的真實值，不是「沒資料」，不需要額外的過時判定)。"""
    if sym not in _ofi_subscribed: return None
    return _ofi_latest.get(sym,{"ofi":0.0,"big_trade":0.0,"updated_at":time.time()})

def get_latest_spread_pct(sym:str) -> Optional[float]:
    """買賣價差(以中價%表示)：用RealTimeOFI算OFI時已經存著的最近一筆bid/ask快照直接算，
    不需要另外多存一份資料。沒有訂閱/還沒收到任何bidask回傳None，呼叫端要當「未知」不是「價差是0」。"""
    engine=_ofi_engines.get(sym)
    if engine is None or engine._prev_bidask is None: return None
    ba=engine._prev_bidask
    mid=(ba["bid_price"]+ba["ask_price"])/2
    if mid<=0: return None
    return round((ba["ask_price"]-ba["bid_price"])/mid*100,4)

def is_no_trade_zone(prices:List[float], volumes:List[float]) -> Tuple[bool,str]:
    """禁止交易區：國定假日(沿用既有行事曆)、連續假日前收盤前時段、月結算日(每月第三個星期三)、
    成交量過低、波動度過低(代用指標，沒有逐筆資料算不出真正ATR)。
    原規格還有「重大新聞前/法說會前」，需要公司行事曆或新聞源，目前沒有資料來源，沒有做這兩項。"""
    now=tw_now()
    if is_tw_market_holiday(now): return True,"國定假日"
    tomorrow=now+timedelta(days=1)
    if (tomorrow.weekday()>=5 or is_tw_market_holiday(tomorrow)) and now.hour*60+now.minute>=12*60+30:
        return True,"連續假日前收盤前時段"
    if now.weekday()==2 and 15<=now.day<=21:  # 每月第三個星期三＝台指期/選擇權結算日
        return True,"月結算日"
    if len(volumes)>=60:
        recent20=sum(volumes[-20:])/20
        baseline60=sum(volumes[-60:])/60
        if baseline60>0 and recent20/baseline60<0.3:
            return True,"成交量過低(相對近期明顯萎縮)"
    elif len(volumes)>=20 and sum(volumes[-20:])==0:
        return True,"成交量過低(完全無交易)"
    if len(prices)>=20:
        rets=[(prices[i]-prices[i-1])/prices[i-1] for i in range(1,len(prices)) if prices[i-1]]
        recent=rets[-20:]
        if recent:
            mean_r=sum(recent)/len(recent)
            vol=(sum((r-mean_r)**2 for r in recent)/len(recent))**0.5
            if vol<0.0008: return True,"波動度過低(代用ATR指標)"
    return False,""

# 加權配分Phase2更新：Order Flow現在有近似資料可用(見approx_order_flow)，加回來給15%權重。
# ML Probability 15%仍然沒有(沒有訓練管線/沒有足夠標記資料)，先把這15%依比例分配給其他6項真實資料項目。
ADV_SCORE_WEIGHTS={"regime":23,"mtf":17,"vwap":12,"orb":12,"volume_quality":18,"order_flow":18}
ADV_SCORE_GRADES=[(95,"S",1.0),(90,"A",0.75),(85,"B",0.5),(80,"C",0.25)]  # 由高到低檢查，沒達標都不交易

def advanced_score(sym:str, prices:List[float], volumes:List[float], dir_:str) -> Dict:
    """多因子加權綜合評分(0~100)+交易分級(S/A/B/C)。分級門檻(80/85/90/95)直接沿用規格書原始數字，
    沒有經過正式校準，是起點不是定論——建議用模擬模式累積足夠交易後，回頭比對實際勝率/PF調整這些門檻。"""
    entries=price_cache.get(sym,[])
    regime=classify_market_regime(prices)
    mtf=multi_timeframe_direction(entries)
    vwap_val=sum(p*v for p,v in zip(prices[-20:],volumes[-20:]))/(sum(volumes[-20:]) or 1) if len(prices)>=20 else prices[-1]
    vwap_ok = prices[-1]>=vwap_val if dir_=="L" else prices[-1]<=vwap_val
    orb=orb_range(entries, tw_now().strftime("%Y-%m-%d"))
    vq=volume_quality_score(volumes,entries)
    of=approx_order_flow(entries)
    absorb=detect_absorption(prices,entries)

    regime_score = 100 if regime["regime"] in ("trending_bull","trending_bear") else (40 if regime["regime"]=="range" else 0)
    mtf_ok = mtf["aligned_long"] if dir_=="L" else mtf["aligned_short"]
    mtf_score = 100 if mtf_ok is True else (30 if mtf_ok is False else 55)  # None=資料不足，給中性分數，不當成過關也不當成沒過關
    vwap_score = 100 if vwap_ok else 0
    orb_breakout = orb is not None and ((dir_=="L" and prices[-1]>orb["high"]) or (dir_=="S" and prices[-1]<orb["low"]))
    orb_score = 100 if orb_breakout else 40
    vq_score = vq["score"]
    # Order Flow分數：方向跟近似Delta一致給高分；偵測到「可能假突破」(創新高但買力衰退)直接重罰
    of_aligned = (dir_=="L" and of["delta_recent"]>0) or (dir_=="S" and of["delta_recent"]<0)
    order_flow_score = 15 if absorb["fake_breakout_risk"] else (90 if of_aligned else 45)

    total=(regime_score*ADV_SCORE_WEIGHTS["regime"]+mtf_score*ADV_SCORE_WEIGHTS["mtf"]+
           vwap_score*ADV_SCORE_WEIGHTS["vwap"]+orb_score*ADV_SCORE_WEIGHTS["orb"]+
           vq_score*ADV_SCORE_WEIGHTS["volume_quality"]+order_flow_score*ADV_SCORE_WEIGHTS["order_flow"])/100

    grade,size_mult="none",0.0
    for th,g,m in ADV_SCORE_GRADES:
        if total>=th: grade,size_mult=g,m; break

    return {"total":round(total,1),"grade":grade,"size_mult":size_mult,"vwap_ok":vwap_ok,
            "regime":regime["regime"],"regime_conf":regime.get("confidence",0),
            "mtf":mtf,"orb_breakout":orb_breakout,"volume_quality":vq,
            "order_flow":of,"fake_breakout_risk":absorb["fake_breakout_risk"]}
