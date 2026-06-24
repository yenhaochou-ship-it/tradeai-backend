"""
TradeAI Pro 後端 v2.0 — 24小時自動交易機器人
台股時段：09:30-13:00主動交易 | 08:00盤前準備 | 13:00停開新倉 | 13:20強制平倉 | 14:30盤後總結
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Tuple
import shioaji as sj
import os, logging, base64, tempfile, time, sqlite3, json
from datetime import datetime, timedelta
import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
# 資料庫持久化（SQLite + Railway Volume）
# 重要：必須在 Railway 後台幫這個服務「掛載一個 Volume」，路徑設為 /data，
# 否則資料庫檔案還是會在每次重新部署時被清空（跟之前記憶體版本同樣的問題）。
# 沒有掛 Volume 時會自動退回容器內的暫存路徑，程式仍可正常運作，只是一樣不會跨部署保留。
# ══════════════════════════════════════════════════════════════════
DB_PATH = "/data/tradeai.db" if os.path.isdir("/data") else "/tmp/tradeai.db"

def db_init():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    conn.commit()
    conn.close()
    using_volume = os.path.isdir("/data")
    logger.info(f"資料庫已初始化：{DB_PATH}（{'已使用Railway Volume，跨部署保留' if using_volume else '未掛載Volume，重新部署仍會清空'}）")

def db_save(key: str, value: dict):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO kv_store (key,value,updated_at) VALUES (?,?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                     (key, json.dumps(value), datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"資料庫寫入失敗 [{key}]: {e}")

def db_load(key: str, default=None):
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
        conn.close()
        if row: return json.loads(row[0])
    except Exception as e:
        logger.warning(f"資料庫讀取失敗 [{key}]: {e}")
    return default

db_init()
TW_TZ = pytz.timezone('Asia/Taipei')

# ══════════════════════════════════════════════════════════════════
# 台股當沖真實交易成本（含手續費+證交稅，2026年現行費率）
# ══════════════════════════════════════════════════════════════════
FEE_RATE = 0.001425      # 手續費 0.1425%（買賣各收一次，未打折）
FEE_DISCOUNT = 0.6       # 一般網路下單券商折扣約6折
DAYTRADE_TAX_RATE = 0.0015  # 當沖證交稅 0.15%（優惠至2027年底，賣出收一次）

def calc_round_trip_cost(entry_price:float, exit_price:float, qty:int) -> dict:
    """計算當沖一買一賣的真實成本（手續費x2 + 當沖證交稅）"""
    buy_amt  = entry_price*qty
    sell_amt = exit_price*qty
    buy_fee  = max(20, buy_amt*FEE_RATE*FEE_DISCOUNT)
    sell_fee = max(20, sell_amt*FEE_RATE*FEE_DISCOUNT)
    tax      = sell_amt*DAYTRADE_TAX_RATE
    total_cost = buy_fee+sell_fee+tax
    gross_pnl  = sell_amt-buy_amt
    net_pnl    = gross_pnl-total_cost
    return {"gross_pnl":gross_pnl,"total_cost":total_cost,"net_pnl":net_pnl,
            "buy_fee":buy_fee,"sell_fee":sell_fee,"tax":tax}

def min_profitable_move_pct() -> float:
    """當沖至少要漲跌多少%才能扣成本後還有獲利（含手續費折扣後約0.32%）"""
    return (FEE_RATE*FEE_DISCOUNT*2 + DAYTRADE_TAX_RATE)*100

# ══════════════════════════════════════════════════════════════════
# 台股時段工具
# ══════════════════════════════════════════════════════════════════
def tw_now():
    return datetime.now(TW_TZ)

# ══════════════════════════════════════════════════════════════════
# 國定假日休市表：原本只用 weekday()>=5 排掉週末，國定假日（春節、228、兒童節等）落在平日時
# 會被誤判成「開盤」，導致系統那天繼續嘗試交易，可能拿到空/過期的歷史資料卻誤判出訊號。
# 資料來源：證交所「市場開休市日期」官方公告(2026/115年)，僅收錄會落在平日、真正需要額外排除的休市日；
# 春節連假已知是落在平日的部分（2/16~2/20）跟結算日(2/12,2/13)都包含在內。
# 注意：這份清單需要每年手動更新一次（通常證交所會在前一年底公布次年行事曆）。
# ══════════════════════════════════════════════════════════════════
TW_MARKET_HOLIDAYS = {
    2026: {
        "2026-01-01",  # 元旦
        "2026-02-12","2026-02-13",  # 春節前結算交割（無交易）
        "2026-02-16","2026-02-17","2026-02-18","2026-02-19","2026-02-20",  # 農曆春節（含補假）
        "2026-02-27","2026-02-28",  # 228和平紀念日（含補假）
        "2026-04-03","2026-04-04","2026-04-06",  # 兒童節/民族掃墓節（含補假，4/5本身是週日）
        "2026-05-01",   # 勞動節
        "2026-06-19",   # 端午節
        "2026-09-25",   # 中秋節
        "2026-09-28",   # 教師節
        "2026-10-09","2026-10-10",  # 國慶日（含補假）
        "2026-10-26",   # 臺灣光復暨金門古寧頭大捷紀念日補假（10/25本身是週日）
        "2026-12-25",   # 行憲紀念日
    },
}
_holiday_warn_logged_years=set()

def is_tw_market_holiday(now=None) -> bool:
    """檢查是否為國定假日休市（不含週末，週末由market_status另外的weekday()檢查處理）"""
    now=now or tw_now()
    year=now.year
    if year not in TW_MARKET_HOLIDAYS and year not in _holiday_warn_logged_years:
        _holiday_warn_logged_years.add(year)
        logger.warning(f"⚠️ 尚未維護{year}年的國定假日清單(TW_MARKET_HOLIDAYS)，目前無法排除國定假日造成的誤判，請更新代碼")
    return now.strftime("%Y-%m-%d") in TW_MARKET_HOLIDAYS.get(year,set())

def market_status() -> dict:
    now = tw_now()
    if now.weekday() >= 5:
        return {"status": "closed", "session": "weekend", "label": "週末休市"}
    if is_tw_market_holiday(now):
        return {"status": "closed", "session": "holiday", "label": "國定假日休市"}
    t = now.hour * 60 + now.minute
    if   t < 8*60+30:     return {"status":"closed",     "session":"before_market",    "label":"盤前"}
    elif t < 9*60:         return {"status":"pre_market",  "session":"order_collection", "label":"委託收集 08:30-09:00"}
    elif t < 9*60+30:      return {"status":"open_vol",    "session":"opening_volatile", "label":"開盤波動期 09:00-09:30（只監控）"}
    elif t < 13*60:        return {"status":"open",        "session":"regular_trading",  "label":"主動交易 09:30-13:00"}
    elif t < 13*60+20:     return {"status":"open_late",   "session":"late_trading",     "label":"尾盤監控 13:00-13:20（不開新倉）"}
    elif t < 13*60+25:     return {"status":"closing",     "session":"force_close",      "label":"強制平倉 13:20-13:25"}
    elif t < 13*60+30:     return {"status":"closing",     "session":"closing_auction",  "label":"收盤集合競價 13:25-13:30"}
    elif t < 14*60+30:     return {"status":"after",       "session":"after_market",     "label":"盤後定價 13:30-14:30"}
    elif t < 21*60:        return {"status":"closed",      "session":"analysis",         "label":"資料分析時段"}
    elif t < 23*60:        return {"status":"closed",      "session":"ml_training",      "label":"ML訓練時段 21:00-23:00"}
    else:                  return {"status":"closed",      "session":"idle",             "label":"休眠"}

def is_trading_time() -> bool:
    return market_status()["status"] == "open"

def can_open_new_position() -> bool:
    """只有主動交易時段才開新倉（09:30-13:00）"""
    return market_status()["status"] == "open"

def should_force_close() -> bool:
    """13:20-13:25強制平倉所有部位"""
    return market_status()["session"] == "force_close"

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
    rsi_win   = rsi_arr[-14:]
    rmin,rmax = min(rsi_win),max(rsi_win)
    stoch_rsi = (rsi-rmin)/(rmax-rmin)*100 if rmax>rmin else 50

    vp,vv     = prices[-20:], (volumes[-20:] if volumes else [1e6]*20)
    total_v   = sum(vv) or 1
    vwap      = sum(p*v for p,v in zip(vp,vv))/total_v
    mean20    = ma20
    var       = sum((p-mean20)**2 for p in prices[-20:])/20
    bb_upper  = mean20+2*var**0.5
    bb_lower  = mean20-2*var**0.5
    bb_pct    = (price-bb_lower)/(bb_upper-bb_lower or 1)
    avg_vol   = sum(volumes[-20:])/20 if volumes else 1
    vol_ratio = (sum(volumes[-3:])/3 if volumes else 1)/avg_vol

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
#   - Order Flow Engine (Delta/CVD/Absorption)：需要逐筆成交+委託簿(tick/bidask)資料，
#     目前只訂閱/輪詢一般報價快照，沒有接這類資料源。
#   - Volume Quality裡的 Large Order Ratio、Bid-Ask Imbalance：同樣需要逐筆/委託簿資料。
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
# 使用者參考文件裡的tick格式(bid_p1/bid_v1/is_market_buy合併在一個tick裡)是範例性質的假格式，
# 不是Shioaji真正的資料結構，已經改成上面查證過的真實雙資料流設計。
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

def subscribe_ofi_symbols(symbols) -> int:
    """訂閱即時Tick+BidAsk資料流以啟用真實OFI計算(每檔股票需要2個訂閱)。
    逐檔try/except，一檔失敗不影響其他檔，回傳成功訂閱數量——
    如果這個數字遠小於預期，代表可能撞到訂閱數上限(46檔*2=92個訂閱不算少)，要去看log warning。"""
    if not sinopac_api: return 0
    ok_count=0
    for sym in symbols:
        if sym in _ofi_subscribed: continue
        try:
            contract=sinopac_api.Contracts.Stocks.get(sym)
            if not contract: continue
            sinopac_api.quote.subscribe(contract,quote_type=sj.constant.QuoteType.Tick,version=sj.constant.QuoteVersion.v1)
            sinopac_api.quote.subscribe(contract,quote_type=sj.constant.QuoteType.BidAsk,version=sj.constant.QuoteVersion.v1)
            _ofi_subscribed.add(sym)
            ok_count+=1
        except Exception as e:
            logger.warning(f"OFI訂閱{sym}失敗(可能撞到訂閱數上限): {e}")
    return ok_count

def unsubscribe_all_ofi():
    if not sinopac_api: return
    for sym in list(_ofi_subscribed):
        try:
            contract=sinopac_api.Contracts.Stocks.get(sym)
            if contract:
                sinopac_api.quote.unsubscribe(contract,quote_type=sj.constant.QuoteType.Tick,version=sj.constant.QuoteVersion.v1)
                sinopac_api.quote.unsubscribe(contract,quote_type=sj.constant.QuoteType.BidAsk,version=sj.constant.QuoteVersion.v1)
        except Exception as e:
            logger.warning(f"OFI取消訂閱{sym}失敗: {e}")
    _ofi_subscribed.clear(); _ofi_engines.clear(); _ofi_latest.clear()

def get_real_ofi(sym:str) -> Optional[Dict]:
    """回傳上一個完整30秒週期的OFI/大單流快照。沒有訂閱這支股票回傳None；
    已訂閱但這段時間沒有任何tick/bidask事件，會合理地拿到{"ofi":0.0,"big_trade":0.0}
    (代表這30秒確實沒有方向性流動，0是有意義的真實值，不是「沒資料」，不需要額外的過時判定)。"""
    if sym not in _ofi_subscribed: return None
    return _ofi_latest.get(sym,{"ofi":0.0,"big_trade":0.0,"updated_at":time.time()})

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

def build_entry_reason(lgbm_conf:float, adv:Dict, dir_:str) -> str:
    """把進場當下的LightGBM預測+輔助資訊組成一句人看得懂的話，存進trade_history，
    讓使用者不用回頭翻log或猜，直接在前端看到「為什麼這筆會被買進」。"""
    parts=[f"LightGBM模型預測做多勝率{lgbm_conf:.0f}%"]
    if adv["vwap_ok"]: parts.append("VWAP上方" if dir_=="L" else "VWAP下方")
    if adv["regime"] in ("trending_bull","trending_bear"):
        parts.append(REGIME_LABEL_ZH.get(adv["regime"],adv["regime"]))
    if adv["orb_breakout"]: parts.append("突破開盤區間高點" if dir_=="L" else "跌破開盤區間低點")
    vq=adv.get("volume_quality",{})
    if vq.get("score",0)>=70: parts.append(f"量能佳({vq['score']:.0f}分)")
    parts.append(f"{lgbm_grade(lgbm_conf)}級")
    return "、".join(parts)

def build_exit_reason(tag:str, cfg:Dict, held_min:float, pp:float) -> str:
    """出場tag(止盈/停損/持倉超時/反轉)本身已經是分類，這裡補上具體數字，讓使用者看得到「差多少觸發」"""
    if tag=="止盈": return f"漲幅達{pp:.2f}%，觸及止盈線({cfg['tp']}%)"
    if tag=="停損": return f"跌幅達{pp:.2f}%，觸及停損線({cfg['sl']}%，含移動停利調整後的價位)"
    if tag=="持倉超時": return f"已持倉{held_min:.0f}分鐘(上限{cfg['max_hold_min']}分)，漲幅{pp:.2f}%已蓋過成本，獲利了結"
    if tag=="反轉": return f"持倉{held_min:.0f}分鐘後技術指標轉向，訊號由多翻空提前出場"
    if tag=="強制平倉": return "13:20當沖規定強制平倉，不留倉過夜"
    return tag

REGIME_LABEL_ZH={"trending_bull":"多頭趨勢","trending_bear":"空頭趨勢","range":"區間盤整","volatile":"高波動","panic":"恐慌"}

# ══════════════════════════════════════════════════════════════════
# LightGBM 進場模型：取代原本的「12項技術指標綜合評分」與「進階評分(C級)」做為進場判斷主力。
# 風控（國定假日/處置股/當日3筆上限/VWAP之上）維持獨立硬性檢查，不受模型影響。
#
# 重要：extract_ml_features() 的特徵定義/順序，train_lgbm_model.py 訓練腳本必須完全一致，
# 否則訓練出來的模型在這裡推論時，餵進去的數字意義會對不上，預測值會是垂圾——
# 兩邊都用同一份 ML_FEATURE_NAMES 常數核對順序，新增/刪除特徵時兩邊要一起改。
# ══════════════════════════════════════════════════════════════════
ML_FEATURE_NAMES=[
    "rsi","williams_r","cci","trend_str","vol_ratio",
    "ma5_ma20_dev_pct","price_vwap_dev_pct",
    "regime_code","mtf_aligned_code",
    "volume_quality_score","bid_ask_imbalance","order_flow_delta_scaled","orb_breakout",
    "real_ofi_interval","real_big_trade_interval_scaled",  # 模組一：上一個30秒週期累積的真實OFI/大單流，沒訂閱時給0(中性)
]
_REGIME_CODE={"trending_bull":2,"trending_bear":-2,"range":0,"volatile":1,"panic":-3,"unknown":0}

def extract_ml_features(sym:str, prices:List[float], volumes:List[float], dir_:str="L") -> Optional[List[float]]:
    """把已經算好的技術指標/Phase1-2引擎輸出，統一抽成固定順序的特徵向量餵給LightGBM模型。
    回傳None代表資料不足，呼叫端應該跳過這個候選，不要硬塞進模型。"""
    if len(prices)<30: return None
    sig=calc_signal_py(prices,volumes)
    if sig.get("rsi") is None: return None
    regime=classify_market_regime(prices)
    entries=price_cache.get(sym,[])
    mtf=multi_timeframe_direction(entries)
    vq=volume_quality_score(volumes,entries)
    of=approx_order_flow(entries)
    orb=orb_range(entries,tw_now().strftime("%Y-%m-%d"))
    real_ofi=get_real_ofi(sym)  # 模組一：真實tick訂閱資料，沒有(還沒訂閱/還沒收到tick/太舊)就是None

    ma20=sig.get("ma20") or 1
    vwap=sig.get("vwap") or 1
    aligned=mtf.get("aligned_long") if dir_=="L" else mtf.get("aligned_short")
    mtf_code=1 if aligned is True else(-1 if aligned is False else 0)
    orb_hit=1 if(orb is not None and prices[-1]>orb["high"] and dir_=="L") or \
                 (orb is not None and prices[-1]<orb["low"] and dir_=="S") else 0

    return [
        sig.get("rsi",50), sig.get("williams_r",-50), sig.get("cci",0),
        sig.get("trend_str",0), sig.get("vol_ratio",1),
        (sig.get("ma5",ma20)/ma20-1)*100 if ma20 else 0,
        (prices[-1]/vwap-1)*100 if vwap else 0,
        _REGIME_CODE.get(regime["regime"],0), mtf_code,
        vq.get("score",50), vq.get("bid_ask_imbalance",0),
        of.get("delta_recent",0)/1000.0, orb_hit,
        real_ofi["ofi"] if real_ofi else 0.0,
        (real_ofi["big_trade"]/1000.0) if real_ofi else 0.0,
    ]

_lgbm_model={"booster":None,"loaded":False,"path":None,"error":None}
LGBM_MODEL_PATH=os.environ.get("LGBM_MODEL_PATH","lgbm_model.txt")

def _load_lgbm_model():
    """惰性載入模型(第一次用到時才載入)，且把lightgbm的import包在function裡面而不是放在檔案最上面——
    這樣即使尚未在requirements.txt加上lightgbm套件、或還沒訓練出模型檔案，整個後端服務照樣能正常啟動，
    只是LightGBM進場判斷會自然停用(predict_lgbm_confidence回傳None)，不會讓整個API掛掉。"""
    if _lgbm_model["loaded"]: return _lgbm_model["booster"]
    _lgbm_model["loaded"]=True
    if not os.path.exists(LGBM_MODEL_PATH):
        _lgbm_model["error"]=f"模型檔案不存在: {LGBM_MODEL_PATH}（請先用 train_lgbm_model.py 訓練並放到這個路徑）"
        logger.warning(f"⚠️ LightGBM{_lgbm_model['error']}")
        return None
    try:
        import lightgbm as lgb
        _lgbm_model["booster"]=lgb.Booster(model_file=LGBM_MODEL_PATH)
        _lgbm_model["path"]=LGBM_MODEL_PATH
        logger.info(f"✅ LightGBM模型已載入：{LGBM_MODEL_PATH}")
    except ImportError:
        _lgbm_model["error"]="lightgbm套件未安裝（requirements.txt需加上lightgbm並重新部署）"
        logger.warning(f"⚠️ {_lgbm_model['error']}")
    except Exception as e:
        _lgbm_model["error"]=f"模型載入失敗: {e}"
        logger.warning(f"⚠️ LightGBM{_lgbm_model['error']}")
    return _lgbm_model["booster"]

def predict_lgbm_confidence(sym:str, prices:List[float], volumes:List[float], dir_:str="L") -> Optional[float]:
    """回傳LightGBM模型預測的「做多勝率信心度」(0~100)。模型不存在/特徵不足/預測失敗都回傳None，
    呼叫端看到None就該跳過該候選——不會也不該悄悄退回舊的規則式評分，那樣等於沒有真的換掉。"""
    booster=_load_lgbm_model()
    if booster is None: return None
    feats=extract_ml_features(sym,prices,volumes,dir_)
    if feats is None: return None
    try:
        prob=booster.predict([feats])[0]
        return float(max(0.0,min(100.0,prob*100)))
    except Exception as e:
        logger.warning(f"LightGBM預測失敗 {sym}: {e}")
        return None

def lgbm_grade(conf:float) -> str:
    """純粹顯示用的等級標籤(沿用S/A/B/C視覺風格)，不再決定部位大小——
    部位大小現在由下面的連續線性公式決定，這裡只是給前端一個好懂的徽章。"""
    if conf>=90: return "S"
    if conf>=83: return "A"
    if conf>=77: return "B"
    return "C"

def calculate_dynamic_position_ratio(win_prob_pct:float, min_conf_pct:float, max_alloc_pct:float) -> float:
    """LightGBM機率動態部位縮放——連續線性比例，取代原本S/A/B/C離散分級跳階。
    勝率剛好等於門檻時給最大曝險額度的30%當基本底倉，勝率趨近100%時線性放大到最大曝險上限，
    公式：(勝率-門檻)/(100-門檻)，限制在0~1之間，再用 0.3+0.7*scaling_factor 算出實際比例。

    注意：max_alloc_pct沿用你既有RISK_CFG的alloc(低5%/中10%/高20%)，不是規格書建議的30%/20%/10%——
    那組數字跟現有的「總曝險30%上限」疊在一起，會讓低風險單筆信心夠高時就用滿30%總曝險額度，
    等於低風險變成最集中而不是最分散，跟風險等級的一般直覺相反。如果你還是想要規格書那組數字，
    跟我說一聲，我可以照那組數字改，這裡先用跟現有風控設計一致的數字。"""
    if win_prob_pct < min_conf_pct: return 0.0
    scaling_factor = (win_prob_pct - min_conf_pct) / (100.0 - min_conf_pct)
    scaling_factor = max(0.0, min(1.0, scaling_factor))
    return max_alloc_pct/100 * (0.3 + 0.7*scaling_factor)

# ══════════════════════════════════════════════════════════════════
# 全域狀態
# ══════════════════════════════════════════════════════════════════
sinopac_api    = None
stock_account  = None
future_account = None

_auto_state_defaults = {
    "enabled":False,"risk":"low","cap_pct":100,"paper_mode":True,  # 預設模擬下單，須使用者明確選擇才會送真實委託
    "watchlist":[],"positions":[],"log":[],"trade_history":[],  # trade_history: 每筆已完成交易的明細記錄
    "capital":1_000_000,
    "daily_pnl":0.0,"daily_win":0,"daily_trades":0,
    "consec_loss":0,"pause_until":0,"trade_date":None,
    "_loss_stop_logged":False,"_profit_lock_logged":False,"_afford_warn_logged":False,
    # 模擬驗證進度：累積筆數/交易日不會被每日重置清空（跟trade_history不一樣），
    # 做為「切換成真實下單」前的最低驗證門檻依據，見 PAPER_VALIDATION_MIN_*。
    "paper_validation":{"trade_count":0,"trading_days":[]},
}
auto_state = db_load("auto_state", None) or dict(_auto_state_defaults)
# 資料庫存的舊資料可能缺少新版新增的欄位，補齊避免KeyError
for _k,_v in _auto_state_defaults.items():
    auto_state.setdefault(_k,_v)

def _persist_auto_state():
    """把目前的auto_state存進資料庫，重新部署/當機後可以接續，而不是每次都從零開始"""
    db_save("auto_state", auto_state)

RISK_CFG = {
    "low":  {"min_conf":72,"alloc":0.05,"sl":2.0,"tp":4.0,"max_pos":3,"max_hold_min":25},
    "mid":  {"min_conf":68,"alloc":0.10,"sl":3.0,"tp":6.0,"max_pos":5,"max_hold_min":40},
    "high": {"min_conf":65,"alloc":0.20,"sl":5.0,"tp":10.0,"max_pos":8,"max_hold_min":60},
}

# ══════════════════════════════════════════════════════════════════
# 真實下單前的最低模擬驗證門檻：避免剛調整完邏輯/股票池/風險參數就直接切真錢，
# 至少要先讓「模擬模式」（用真實股價算損益，不花真錢）實際跑過一段時間、看過足夠多筆完整交易結果，
# 確認新邏輯/新標的真的可行，而不是憑感覺直接賭一次真實委託。
# 門檻可依需求調整；若使用者很清楚自己在做什麼，可在/auto/start帶force_real=true跳過此檢查。
# ══════════════════════════════════════════════════════════════════
PAPER_VALIDATION_MIN_TRADES=20
PAPER_VALIDATION_MIN_DAYS=5
MAX_TRADES_PER_DAY=3        # 規格書新增風控：當日最多交易3次，避免訊號反覆觸發造成過度交易、手續費侵蝕獲利
CONSEC_LOSS_STOP=2          # 規格書收緊：原本連虧3次冷靜15分鐘，改成連虧2次直接停止當日交易(更保守)

def _record_paper_validation():
    """每筆模擬交易平倉後呼叫：累積驗證進度，trade_count/trading_days不會被_reset_daily清空"""
    pv=auto_state.setdefault("paper_validation",{"trade_count":0,"trading_days":[]})
    pv["trade_count"]=pv.get("trade_count",0)+1
    today_str=tw_now().strftime("%Y-%m-%d")
    if today_str not in pv.get("trading_days",[]):
        pv.setdefault("trading_days",[]).append(today_str)

def _paper_validation_progress() -> dict:
    pv=auto_state.get("paper_validation",{"trade_count":0,"trading_days":[]})
    trades=pv.get("trade_count",0); days=len(pv.get("trading_days",[]))
    ok = trades>=PAPER_VALIDATION_MIN_TRADES and days>=PAPER_VALIDATION_MIN_DAYS
    return {"trades":trades,"days":days,"min_trades":PAPER_VALIDATION_MIN_TRADES,
            "min_days":PAPER_VALIDATION_MIN_DAYS,"ready_for_real":ok}

price_cache: Dict[str,List[Dict]] = {}
_last_cum_volume: Dict[str,int] = {}  # 修正：追蹤每檔股票上次輪詢時的累計成交量，用來換算成「這次輪詢期間」的增量
MAX_BARS = 120
ml_predictions: Dict[str,float] = {}  # {symbol: 0~1 預測勝率} 由前端訓練完成後同步

# ══════════════════════════════════════════════════════════════════
# 全市場掃描股票池（0050成分股 + 主要流動性最好的台股，非使用者自選股）
# 用於「飆股雷達」全市場掃描，跟使用者的自選股/真實交易清單(auto_state["watchlist"])完全分開
# ══════════════════════════════════════════════════════════════════
SCAN_UNIVERSE = [
    "2330","2454","2308","2317","3711","2327","2303","2383","2891","3037",  # 0050前10大成分股
    "2882","2886","2884","2881","2880","2885","2890","5880","2892","2887",  # 金融股
    "2382","3008","2357","2379","4938","2345","2412","1301","1303","2002", # 科技/傳產龍頭
    "3034","3045","2353","2356","6669","3661","3653","2474","2207","1216", # 其他大型股
    # ── 可負擔低價股池（2026/06盤面實際股價約NT$12~25，1張曝險約1.2萬~2.5萬，
    #    對應目前資金規模在「高風險」設定下單筆預算約NT$2萬內可負擔；上面的大型/高價股
    #    在修正②上線後不會再強迫買，只是現在資金規模還買不起，等資金/cap_pct提高會自動恢復可用）──
    "2849","2836","2834","2812","2801","2838",  # 中小型銀行：價格親民(約NT$12~23)、流動性夠、可當沖
    # 註：5880合庫金、2887台新新光金已在上面的金融股清單中，價格約NT$24~33，資金/cap_pct提高後也買得起
]

# 股票所屬板塊（用於進場時的板塊分散檢查，避免同一板塊集中持倉風險過高）
SECTOR_MAP = {
    "2330":"半導體","2303":"半導體",
    "2454":"IC設計","2379":"IC設計","3034":"IC設計","3661":"IC設計",
    "2308":"電源工業電子","3711":"封測","2327":"被動元件","2383":"PCB連接器","3037":"PCB",
    "2317":"電子代工","2382":"電子代工","4938":"電子代工","2356":"電子代工","6669":"伺服器",
    "2891":"金融","2882":"金融","2886":"金融","2884":"金融","2881":"金融",
    "2880":"金融","2885":"金融","2890":"金融","5880":"金融","2892":"金融","2887":"金融",
    "3008":"光學","2345":"網通設備","2412":"電信","3045":"電信",
    "1301":"塑化","1303":"塑化","2002":"鋼鐵","2353":"筆電",
    "3653":"散熱","2474":"機殼","2207":"汽車","1216":"食品",
    # 可負擔低價股池（皆歸金融板塊，會跟既有金控/銀行股互相排擠，避免重倉同一族群）
    "2849":"金融","2836":"金融","2834":"金融","2812":"金融","2801":"金融","2838":"金融",
}

# ── 個股合約資訊快取（當沖資格 day_trade、漲跌停價，每日更新一次，避免重複查詢）──
contract_info_cache: Dict[str,Dict] = {}  # {symbol: {day_trade, limit_up, limit_down, cached_date}}

def get_contract_info(symbol:str) -> dict:
    """取得股票當沖資格與漲跌停價（真實資料，來自永豐合約），有快取避免重複查詢"""
    today = tw_now().strftime("%Y%m%d")
    cached = contract_info_cache.get(symbol)
    if cached and cached.get("cached_date")==today:
        return cached
    info = {"day_trade":"Unknown","limit_up":0.0,"limit_down":0.0,"cached_date":today}
    try:
        contract = sinopac_api.Contracts.Stocks.get(symbol) if sinopac_api else None
        if contract:
            info["day_trade"] = str(getattr(contract,"day_trade",""))
            info["limit_up"] = float(getattr(contract,"limit_up",0) or 0)
            info["limit_down"] = float(getattr(contract,"limit_down",0) or 0)
    except Exception as e:
        logger.warning(f"取得{symbol}合約資訊失敗: {e}")
    contract_info_cache[symbol]=info
    return info

def can_day_trade(symbol:str, is_sell_first:bool=False) -> tuple:
    """檢查股票今日是否可當沖，回傳(可否,原因)。No=不可當沖(處置股票/零股/權證等)，OnlyBuy=僅限先買後賣"""
    info = get_contract_info(symbol)
    dt = info["day_trade"]
    if "No" in dt: return (False, "今日不可當沖（處置股票或不符資格）")
    if "OnlyBuy" in dt and is_sell_first: return (False, "今日僅限先買後賣，不可先賣後買")
    return (True, "")

# ══════════════════════════════════════════════════════════════════
# 三大法人買賣超（真實公開資料，來源：台灣證交所開放資料 T86）
# ══════════════════════════════════════════════════════════════════
institutional_cache: Dict = {"date":None,"data":{}}  # {date, data:{symbol:{foreign,trust,dealer,total}}}

def fetch_institutional_flows(date_str:str=None) -> Dict:
    """從台灣證交所公開資料抓三大法人買賣超（真實數據，無需驗證）"""
    if date_str is None:
        date_str = tw_now().strftime("%Y%m%d")
    try:
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&selectType=ALL"
        r = requests.get(url, timeout=10)
        d = r.json()
        if d.get("stat") != "OK" or not d.get("data"):
            return {}
        result = {}
        for row in d["data"]:
            try:
                code = row[0].strip()
                name = row[1].strip()
                foreign_net = int(row[4].replace(",",""))   # 外資買賣超股數
                trust_net   = int(row[10].replace(",",""))  # 投信買賣超股數
                dealer_net  = int(row[11].replace(",",""))  # 自營商買賣超股數
                result[code] = {
                    "name": name,
                    "foreign": foreign_net, "trust": trust_net, "dealer": dealer_net,
                    "total": foreign_net+trust_net+dealer_net,
                }
            except (IndexError, ValueError):
                continue
        return result
    except Exception as e:
        logger.warning(f"抓取三大法人資料失敗: {e}")
        return {}

def update_institutional_cache():
    today = tw_now().strftime("%Y%m%d")
    if institutional_cache["date"] == today and institutional_cache["data"]:
        return  # 今天已經抓過了
    data = fetch_institutional_flows(today)
    if data:
        institutional_cache["date"] = today
        institutional_cache["data"] = data
        logger.info(f"三大法人資料已更新 | {len(data)}支股票 | {today}")

# ══════════════════════════════════════════════════════════════════
# 排程任務
# ══════════════════════════════════════════════════════════════════
def _log(msg:str, sym:str="系統"):
    entry={"ts":tw_now().strftime("%H:%M:%S"),"sym":sym,"msg":msg}
    auto_state["log"].insert(0,entry)
    auto_state["log"]=auto_state["log"][:100]
    logger.info(f"[Auto] {sym}: {msg}")

def _snapshot(sym:str) -> Optional[Dict]:
    """回傳完整快照欄位(price/volume/buy_vol/sell_vol/tick_type)，不再只取price+volume兩個欄位。
    這些欄位都是snapshots()本來就會回傳的，之前只解出close跟total_volume，其他被丟掉了——
    Phase 2(Order Flow近似/Bid-Ask Imbalance)會用到buy_vol/sell_vol/tick_type，不需要額外訂閱tick資料流。"""
    if not sinopac_api: return None
    try:
        c=sinopac_api.Contracts.Stocks.get(sym)
        if not c: return None
        s=sinopac_api.snapshots([c])
        if not s: return None
        snap=s[0]
        tt=getattr(snap,"tick_type",None)
        tt_val = (1 if str(tt).endswith("Buy") else 2 if str(tt).endswith("Sell") else 0) if tt is not None else 0
        return {
            "price":float(snap.close),
            "volume":int(getattr(snap,"total_volume",0) or 0),
            "buy_vol":float(getattr(snap,"buy_volume",0) or 0),
            "sell_vol":float(getattr(snap,"sell_volume",0) or 0),
            "tick_type":tt_val,  # 1=外盤(買進成交) 2=內盤(賣出成交) 0=無法判定
        }
    except: return None

def _update_cache():
    # 模擬下單模式：除了使用者自選清單，也持續更新40檔股票池的價格快取，
    # 因為模擬模式下AI可以從這個更大的股票池找當沖機會（真實下單仍只看使用者自選清單，較保守）
    symbols = set(auto_state["watchlist"])
    if auto_state.get("paper_mode"):
        symbols |= set(SCAN_UNIVERSE)
    for sym in symbols:
        snap=_snapshot(sym)
        if snap:
            cum_vol=snap["volume"]  # snapshot的volume其實是「今天累計成交量」(total_volume)，不是這次輪詢期間的量
            prev_cum=_last_cum_volume.get(sym)
            # 修正：bootstrap歷史K棒存的是「每根棒子的成交量」(區間量)，但這裡原本直接存累計量，
            # 兩種語意混在同一個volume欄位裡，導致vol_ratio/量能指標在即時交易階段幾乎完全失真
            # (累計量逐筆輪詢時彼此非常接近，比值永遠趨近1，偵測不到真正的爆量)。
            # 改成：用累計量前後差，換算出「這次輪詢期間」真正成交的量，跟bootstrap的語意一致。
            if prev_cum is None or cum_vol<prev_cum:
                interval_vol=0  # 第一次輪詢這支股票，或跨日累計量重置：沒有基準可以算增量，給0避免單筆假爆量
            else:
                interval_vol=cum_vol-prev_cum
            _last_cum_volume[sym]=cum_vol
            if sym not in price_cache: price_cache[sym]=[]
            price_cache[sym].append({"price":snap["price"],"volume":interval_vol,"t":time.time(),
                                      "buy_vol":snap["buy_vol"],"sell_vol":snap["sell_vol"],"tick_type":snap["tick_type"]})
            price_cache[sym]=price_cache[sym][-MAX_BARS:]
    # 模組一橋接：把這30秒內(事件驅動)累積的OFI/大單流取出存進_ofi_latest並歸零，
    # 跟上面的價格輪詢共用同一個30秒心跳，確保每檔股票的累積週期跟決策節奏對齊
    _flush_all_ofi()

def _reset_daily():
    today=tw_now().strftime("%Y-%m-%d")
    if auto_state["trade_date"]!=today:
        auto_state.update({"trade_date":today,"daily_pnl":0.0,
            "daily_win":0,"daily_trades":0,"consec_loss":0,"pause_until":0,
            "_loss_stop_logged":False,"_profit_lock_logged":False,"_afford_warn_logged":False,"trade_history":[]})
        _log("每日計數器重置 ✓")

def _place_real_order(sym:str, action, qty:int):
    if auto_state.get("paper_mode"):
        _log(f"[模擬]未送出真實委託 {sym} {action} {qty}張",sym)
        return
    if not sinopac_api or not stock_account: return
    try:
        c=sinopac_api.Contracts.Stocks.get(sym)
        if not c: return
        o=sinopac_api.Order(price=0,quantity=qty,action=action,
            price_type=sj.constant.StockPriceType.MKT,
            order_type=sj.constant.OrderType.ROD,account=stock_account)
        sinopac_api.place_order(c,o)
    except Exception as e:
        logger.warning(f"委託失敗 {sym}: {e}")

def auto_trade_tick():
    """每30秒執行 — 核心自動交易"""
    if not auto_state["enabled"] or not sinopac_api or not is_trading_time(): return
    _reset_daily()
    _update_cache()
    if auto_state["pause_until"]>time.time(): return
    if abs(min(0,auto_state["daily_pnl"]))/auto_state["capital"]*100>=3:
        if not auto_state.get("_loss_stop_logged"):
            _log("[停止]今日虧損達3%，停止交易"); auto_state["_loss_stop_logged"]=True
        return
    if auto_state["daily_pnl"]/auto_state["capital"]*100>=8:
        if not auto_state.get("_profit_lock_logged"):
            _log("[鎖定]今日獲利達8%，鎖定獲利"); auto_state["_profit_lock_logged"]=True
        return
    cfg=RISK_CFG[auto_state["risk"]]
    # 出場檢查
    to_close=[]
    for pos in auto_state["positions"]:
        sym=pos["sym"]; snap=_snapshot(sym)
        if not snap: continue
        p=snap["price"]
        pp=(p-pos["entry"])/pos["entry"]*100*(1 if pos["dir"]=="L" else -1)
        held_min=(time.time()-pos.get("opened_at",time.time()))/60
        # 修正①：原本只要求 pp>-0.1（沒有明顯虧損）就會被超時平倉，
        # 但完全沒檢查漲跌幅有沒有蓋過來回手續費+當沖證交稅（min_profitable_move_pct，約0.32%），
        # 導致價格幾乎沒動也會被「持倉超時」強制出場，穩定倒貼一次完整成本（例：原價買賣的台積電單，0%價差卻虧NT$8,089手續費）。
        # 改成要求至少要爬到「扣成本後打平」的幅度才放它走；沒到的話繼續抱著，交給停損/止盈/反轉訊號決定，
        # 最晚 13:20 還有 force_close_all 強制收尾，不會有隔夜風險。
        min_move=min_profitable_move_pct()
        time_up=held_min>=cfg.get("max_hold_min",40) and pp>=min_move
        # 移動停利：獲利超過1.5%後啟動，停損價跟隨移動鎖定部分獲利（新倉只會是多單，但保留方向判斷以正確處理限制前就存在的舊空單）
        # 修正：原本回檔寬度1.2%，跟1.5%啟動門檻太接近，剛啟動時的安全margin只有0.3%，
        # 連來回成本(~0.32%)都蓋不過——峰值漲幅落在1.5%~1.6%之間反轉的單子，扣成本後幾乎是平的或微虧，
        # 移動停利等於沒有真正鎖到任何淨利。收緊到0.5%，啟動瞬間就有約0.67%的淨利margin，不再卡在成本線上。
        if pp>=1.5:
            if pos["dir"]=="L":
                trail_sl=round(p*0.995,2)  # 距目前價0.5%
                if trail_sl>pos["sl"]: pos["sl"]=trail_sl
            else:
                trail_sl=round(p*1.005,2)
                if trail_sl<pos["sl"]: pos["sl"]=trail_sl
        close=(p<=pos["sl"] if pos["dir"]=="L" else p>=pos["sl"]) or pp>=cfg["tp"] or time_up
        # 修正③：剛進場的訊號雜訊較大（每30秒就重算一次），給它一段穩定期再讓「反轉」訊號出場生效，
        # 避免像國泰金那筆只抱4分鐘就被自己的指標雜訊洗出場（持倉設計目標是 max_hold_min 分鐘，不該4分鐘就被打翻）。
        MIN_HOLD_BEFORE_REVERSAL=5
        if not close and held_min>=MIN_HOLD_BEFORE_REVERSAL:
            hist=price_cache.get(sym,[])
            if len(hist)>=30:
                sig=calc_signal_py([h["price"] for h in hist],[h["volume"] for h in hist])
                if (pos["dir"]=="L" and sig["action"]=="sell") or \
                   (pos["dir"]=="S" and sig["action"]=="buy"): close=True
        if close:
            # 計算真實淨損益（扣除手續費+當沖證交稅）
            # 重要修正：pos["qty"] 是「張」數（1張=1000股），所有金額計算必須換算成股數，
            # 否則損益會被低估1000倍，導致每日虧損/獲利風控門檻形同虛設
            shares = pos["qty"] * 1000
            entry_p,exit_p = (pos["entry"],p) if pos["dir"]=="L" else (p,pos["entry"])
            cost=calc_round_trip_cost(entry_p,exit_p,shares)
            gross=(p-pos["entry"])*shares*(1 if pos["dir"]=="L" else -1)
            net_pnl=gross-cost["total_cost"]
            auto_state["daily_pnl"]+=net_pnl; auto_state["daily_trades"]+=1
            if net_pnl>0: auto_state["daily_win"]+=1; auto_state["consec_loss"]=0
            else:
                auto_state["consec_loss"]+=1
                if auto_state["consec_loss"]>=CONSEC_LOSS_STOP:
                    # 規格書收緊版：連虧2次不是冷靜15分鐘繼續試，是直接停止「當天」交易，
                    # 設pause_until到今天收盤時間，明天會自然恢復(時間比較用的是timestamp，跨日就失效了)
                    today_close=tw_now().replace(hour=13,minute=30,second=0,microsecond=0).timestamp()
                    auto_state["pause_until"]=max(today_close,time.time()+1)
                    _log(f"連虧{auto_state['consec_loss']}次，今日停止交易")
            tag="止盈" if pp>=cfg["tp"] else "停損" if pp<=-cfg["sl"] else "持倉超時" if time_up else "反轉"
            exit_reason=build_exit_reason(tag,cfg,held_min,pp)
            _log(f"{tag} {pos['dir']} @{p:.2f} 淨損益${net_pnl:.0f}(毛利${gross:.0f}-成本${cost['total_cost']:.0f})",sym)
            auto_state["trade_history"].insert(0,{
                "sym":sym,"dir":pos["dir"],"qty":pos["qty"],"shares":shares,
                "entry":pos["entry"],"exit":round(p,2),
                "total_cost_basis":round(pos["entry"]*shares,0),  # 進場總成本（股數×進場價）
                "gross_pnl":round(gross,0),"fees":round(cost["total_cost"],0),
                "pnl":round(net_pnl,0),"pct":round(pp,2),"tag":tag,
                "open_time":pos.get("open_time",""),"close_time":tw_now().strftime("%H:%M:%S"),
                "from_pool": sym not in auto_state["watchlist"],
                "grade":pos.get("grade","-"),"regime":pos.get("regime","-"),
                "entry_reason":pos.get("entry_reason","-"),"exit_reason":exit_reason,
            })
            auto_state["trade_history"]=auto_state["trade_history"][:50]
            if auto_state.get("paper_mode"): _record_paper_validation()
            # 漲跌停鎖死風險提示（不對稱）：做空遇漲停鎖死是真正危險（違約交割+借券費），
            # 做多遇跌停沖不掉則只是變成一般T+2持股，風險輕微，用不同等級的提示
            cinfo=get_contract_info(sym)
            if pos["dir"]=="S" and cinfo["limit_up"]>0 and p>=cinfo["limit_up"]*0.998:
                _log(f"[高風險] {sym} 接近/觸及漲停，做空回補可能掛不掉，可能產生借券費用與違約交割風險",sym)
            if pos["dir"]=="L" and cinfo["limit_down"]>0 and p<=cinfo["limit_down"]*1.002:
                _log(f"[提示] {sym} 接近/觸及跌停，今日可能無法賣出，將自動變成一般持股待T+2交割（非違約風險）",sym)
            close_act=sj.constant.Action.Sell if pos["dir"]=="L" else sj.constant.Action.Buy
            _place_real_order(sym,close_act,pos["qty"])
            to_close.append(sym)
    auto_state["positions"]=[p for p in auto_state["positions"] if p["sym"] not in to_close]
    # 開倉（僅在09:30-13:00主動交易時段，避開開盤亂流與尾盤風險）
    if not can_open_new_position():
        existing=set()
        candidates_pool=[]
    else:
        existing={p["sym"] for p in auto_state["positions"]}
        # 模擬下單：從40檔股票池找機會；真實下單：僅限使用者自選清單（較保守，避免真錢自動擴大選股範圍）
        candidates_pool=list(set(auto_state["watchlist"]) | set(SCAN_UNIVERSE)) if auto_state.get("paper_mode") else list(auto_state["watchlist"])

    # 先逐一檢查資格、算出每檔的「AI預估利潤分數」，蒐集成候選清單
    candidates=[]
    for sym in candidates_pool:
        if sym in existing: continue
        hist=price_cache.get(sym,[])
        if len(hist)<30: continue
        prices_h=[h["price"] for h in hist]; vols_h=[h["volume"] for h in hist]
        # 修正⑩：進場判斷主力換成LightGBM模型——sig還是要算，但只用bad_time(開盤前30分/收盤前)
        # 這個時間風控欄位，不再用sig['conf']/sig['action']決定要不要進場(那是被取代的"12項指標綜合評分")
        sig=calc_signal_py(prices_h,vols_h)
        if sig.get("bad_time"): continue
        dir_="L"  # 規格要求：只看LightGBM「做多勝率」，當沖只做多單
        # 禁止交易區：國定假日/連續假日前收盤前/月結算日/量過低/波動度過低，這些情況下不進場(維持獨立硬性檢查)
        ntz,ntz_reason=is_no_trade_zone(prices_h,vols_h)
        if ntz: continue
        # 真實當沖資格檢查（用永豐合約真實資料：day_trade=No的處置股票/不符資格股票直接跳過，維持獨立硬性檢查）
        ok,reason = can_day_trade(sym, is_sell_first=False)
        if not ok: continue
        cinfo=get_contract_info(sym)
        p_now=hist[-1]["price"]
        if cinfo["limit_down"]>0 and p_now<=cinfo["limit_down"]*1.005: continue  # 接近跌停，做多賣不掉風險高
        # 還是要算advanced_score：①VWAP硬否決規格明確要求保留 ②regime/orb/volume_quality等輸出
        # 現在角色變成「餵給LightGBM的特徵」+「事後顯示用的輔助資訊」，不再是獨立的進場門檻
        adv=advanced_score(sym,prices_h,vols_h,dir_)
        if not adv["vwap_ok"]: continue  # 規格明確要求保留的VWAP硬否決
        # LightGBM做多勝率信心度——取代原本的12項指標評分跟進階評分(C級)門檻
        lgbm_conf=predict_lgbm_confidence(sym,prices_h,vols_h,dir_)
        if lgbm_conf is None:
            continue  # 模型還沒訓練/載入失敗：誠實地不交易，不要悄悄退回舊邏輯製造「好像在運作」的假象
        if lgbm_conf<cfg["min_conf"]: continue  # 沿用既有風險等級門檻(低72%/中68%/高65%)，只是現在門檻比的是模型機率
        est_profit_score=lgbm_conf*cfg["tp"]
        candidates.append({"sym":sym,"sig":sig,"dir":dir_,"price":p_now,"est_profit_score":est_profit_score,
                            "adv":adv,"lgbm_conf":lgbm_conf})

    # 依LightGBM信心度排序(信心最高=模型最有把握)，同分才看AI預估利潤分數
    candidates.sort(key=lambda c:(c["lgbm_conf"],c["est_profit_score"]),reverse=True)
    held_sectors={SECTOR_MAP.get(p["sym"]) for p in auto_state["positions"] if SECTOR_MAP.get(p["sym"])}
    skipped_unaffordable=[]
    opened_this_tick=0
    # 風控新增：所有持倉合計曝險不得超過資金的30%，避免每筆都在per-trade budget之內、
    # 但同時開好幾筆疊加起來總曝險還是過大(高風險max_pos=8時，理論上8筆*20%alloc=160%，沒有總量上限的話會嚴重超額)
    total_exposure=sum(p["entry"]*p["qty"]*1000 for p in auto_state["positions"])
    MAX_AGGREGATE_EXPOSURE_PCT=30
    for c in candidates:
        if len(auto_state["positions"])>=cfg["max_pos"]: break
        if auto_state["daily_trades"]>=MAX_TRADES_PER_DAY: break  # 風控新增：當日交易次數上限，避免訊號反覆觸發過度交易
        sym,sig,dir_,p,adv,lgbm_conf=c["sym"],c["sig"],c["dir"],c["price"],c["adv"],c["lgbm_conf"]
        # 板塊分散：同板塊已有持倉就跳過這個候選，避免集中壓在同一個產業（如同時押好幾家金融股）
        this_sector=SECTOR_MAP.get(sym)
        if this_sector and this_sector in held_sectors: continue
        # 修正②：原本用 max(1, …) 強制至少買1張，對高價股（如台積電2500+、健策3800+）來說，
        # 1張的曝險動輒幾百萬，遠超過風險設定要動用的資金（例：低風險alloc=5%，100K資金只想動用5,000元，
        # 卻被迫買下252萬元台積電，曝險是帳戶資金的25倍），導致小幅價格波動就被放大成鉅額虧損。
        # 改成：算出來的張數不到1張，就代表這檔股票對目前資金規模太貴，直接跳過、不強迫超額曝險。
        # 修正⑩：部位大小依LightGBM信心度分級加碼(S/A/B/C=100%/75%/50%/25%)，取代原本的進階評分分級。
        # 模組二：LightGBM機率動態部位縮放（連續線性比例），取代原本的離散分級加碼
        alloc_ratio=calculate_dynamic_position_ratio(lgbm_conf,cfg["min_conf"],cfg["alloc"]*100)
        budget=auto_state["capital"]*(auto_state["cap_pct"]/100)*alloc_ratio
        max_aggregate=auto_state["capital"]*MAX_AGGREGATE_EXPOSURE_PCT/100
        budget=min(budget,max(0,max_aggregate-total_exposure))  # 受總曝險上限約束
        qty=int(budget/(p*1000))
        if qty<1:
            skipped_unaffordable.append((sym,p))
            continue
        grade=lgbm_grade(lgbm_conf)
        auto_state["positions"].append({
            "sym":sym,"dir":dir_,"qty":qty,"entry":p,
            "sl":round(p*(1-cfg["sl"]/100) if dir_=="L" else p*(1+cfg["sl"]/100),2),
            "tp":round(p*(1+cfg["tp"]/100) if dir_=="L" else p*(1-cfg["tp"]/100),2),
            "open_time":tw_now().strftime("%H:%M:%S"),
            "opened_at":time.time(),  # 數值時間戳，用於計算持倉分鐘數（短炒持倉時間上限判斷）
            "grade":grade,"regime":adv["regime"],"entry_reason":build_entry_reason(lgbm_conf,adv,dir_),
        })
        act=sj.constant.Action.Buy if dir_=="L" else sj.constant.Action.Sell
        _place_real_order(sym,act,qty)
        pool_tag="[股票池]" if (sym not in auto_state["watchlist"]) else ""
        _log(f"{pool_tag}{'做多▲' if dir_=='L' else '做空▼'} {qty}張@{p:.2f} LightGBM信心{lgbm_conf:.0f}% "
             f"({grade}級) 市場環境:{adv['regime']}",sym)
        existing.add(sym)
        opened_this_tick+=1
        total_exposure+=p*qty*1000
        if this_sector: held_sectors.add(this_sector)
    # 透明度提示：修正②上線後，資金規模配不上整張交易（台股當沖只能整張，零股不開放當沖）時，
    # AI可能會整天都找不到「買得起」的標的而完全不下單——這不是bug，是修正後誠實反映風險，
    # 但每天只提示一次，讓使用者知道發生了什麼、該調整資金配置或股票池，而不是誤以為系統當機。
    if opened_this_tick==0 and skipped_unaffordable and not auto_state.get("_afford_warn_logged"):
        budget=auto_state["capital"]*(auto_state["cap_pct"]/100)*cfg["alloc"]
        cheapest=min(skipped_unaffordable,key=lambda x:x[1])
        _log(f"[提示]本輪{len(skipped_unaffordable)}檔候選股價超出目前資金配置上限（{auto_state['risk']}風險單筆預算NT${budget:.0f}，"
             f"最低價候選{cheapest[0]}@{cheapest[1]:.1f}仍需NT${cheapest[1]*1000:.0f}/張），暫無買得起的標的。"
             f"可提高cap_pct/資金規模，或將股票池調整為更低價的標的。")
        auto_state["_afford_warn_logged"]=True

def pre_market_prep():
    _reset_daily(); _update_cache()
    _log("08:00 開盤前準備，更新價格快取與昨日資料 ✓")

def force_close_all():
    if not auto_state["positions"]: return
    _log(f"13:20 強制平倉 {len(auto_state['positions'])} 筆（當沖規則：禁止留倉過夜）")
    for pos in list(auto_state["positions"]):
        sym=pos["sym"]
        try:
            hist=price_cache.get(sym,[])
            p=hist[-1]["price"] if hist else pos["entry"]  # 拿不到最新價時退回進場價，避免崩潰但盡量準確記錄
            shares=pos["qty"]*1000
            gross=(p-pos["entry"])*shares*(1 if pos["dir"]=="L" else -1)
            cost=calc_round_trip_cost(
                pos["entry"] if pos["dir"]=="L" else p,
                p if pos["dir"]=="L" else pos["entry"],
                shares
            )["total_cost"]
            net_pnl=gross-cost
            auto_state["daily_pnl"]+=net_pnl; auto_state["daily_trades"]+=1
            if net_pnl>0: auto_state["daily_win"]+=1; auto_state["consec_loss"]=0
            else: auto_state["consec_loss"]+=1
            _log(f"[強制平倉] {pos['dir']} @{p:.2f} 淨損益${net_pnl:.0f}",sym)
            pp_fc=(p-pos["entry"])/pos["entry"]*100*(1 if pos["dir"]=="L" else -1)
            auto_state["trade_history"].insert(0,{
                "sym":sym,"dir":pos["dir"],"qty":pos["qty"],"shares":shares,
                "entry":pos["entry"],"exit":round(p,2),
                "total_cost_basis":round(pos["entry"]*shares,0),
                "gross_pnl":round(gross,0),"fees":round(cost,0),
                "pnl":round(net_pnl,0),"pct":round(pp_fc,2),"tag":"強制平倉",
                "open_time":pos.get("open_time",""),"close_time":tw_now().strftime("%H:%M:%S"),
                "from_pool": sym not in auto_state["watchlist"],
                "grade":pos.get("grade","-"),"regime":pos.get("regime","-"),
                "entry_reason":pos.get("entry_reason","-"),"exit_reason":"13:20當沖規定強制平倉，不留倉過夜",
            })
            auto_state["trade_history"]=auto_state["trade_history"][:50]
            if auto_state.get("paper_mode"): _record_paper_validation()
        except Exception as e:
            logger.warning(f"強制平倉{sym}損益計算失敗（委託仍會送出）: {e}")
        act=sj.constant.Action.Sell if pos["dir"]=="L" else sj.constant.Action.Buy
        _place_real_order(sym,act,pos["qty"])
    auto_state["positions"]=[]
    _persist_auto_state()

def post_market_summary():
    d=auto_state["daily_trades"] or 1
    wr=auto_state["daily_win"]/d*100
    _log(f"盤後總結 | P&L:${auto_state['daily_pnl']:.0f} | 勝率:{wr:.0f}%({auto_state['daily_win']}/{auto_state['daily_trades']})")

def ml_training_window():
    _log("21:00 ML訓練時段開始（前端訓練請在學習分頁執行）")

# ══════════════════════════════════════════════════════════════════
# 全市場飆股雷達：定期掃描SCAN_UNIVERSE，排序技術面動能最強的股票
# 老實說明：這是技術指標排序，不是漲跌預測保證，已在前端文案標明
# ══════════════════════════════════════════════════════════════════
scan_cache = {"results": [], "updated": None, "scanning": False}

def scan_top_stocks():
    """掃描股票池，排出技術面動能最強的股票，含建議進出場點。排程定期執行，避免每次請求都要重新掃描40檔太慢"""
    if not sinopac_api or scan_cache["scanning"]: return
    if not is_trading_time(): return  # 非交易時段不掃描，避免浪費資源且資料無意義
    scan_cache["scanning"] = True
    results = []
    try:
        for sym in SCAN_UNIVERSE:
            try:
                bars = _fetch_real_history(sym, bars=90)
                if len(bars) < 30: continue
                prices = [b["close"] for b in bars]
                volumes = [b["volume"] for b in bars]
                sig = calc_signal_py(prices, volumes)
                if sig["action"] == "hold": continue  # 觀望的不列入排行，只看有明確方向的
                price = prices[-1]
                momentum = (sig["conf"]-50)*1.0 + (sig.get("vol_ratio",1)-1)*15 + (sig.get("trend_str",0)*20)
                # 建議進出場點（用中風險參數試算，僅供參考）
                cfg = RISK_CFG["mid"]
                is_long = sig["action"]=="buy"
                sl = price*(1-cfg["sl"]/100) if is_long else price*(1+cfg["sl"]/100)
                tp = price*(1+cfg["tp"]/100) if is_long else price*(1-cfg["tp"]/100)
                results.append({
                    "symbol": sym, "price": round(price,2), "action": sig["action"],
                    "conf": sig["conf"], "rsi": sig["rsi"], "momentum": round(momentum,1),
                    "entry": round(price,2), "stop_loss": round(sl,2), "take_profit": round(tp,2),
                })
            except Exception as e:
                logger.warning(f"掃描{sym}失敗: {e}")
        results.sort(key=lambda x: abs(x["momentum"]), reverse=True)
        scan_cache["results"] = results[:10]  # 多存幾筆，前端可依需要取TOP5或更多
        scan_cache["updated"] = tw_now().strftime("%H:%M:%S")
        _log(f"全市場掃描完成，{len(results)}檔有明確信號（股票池共{len(SCAN_UNIVERSE)}檔）")
    finally:
        scan_cache["scanning"] = False

def _periodic_persist():
    """定期把auto_state存進資料庫（不放在auto_trade_tick裡面，因為那個函式有很多提早return的分支，
    放在獨立排程更不容易漏掉某個情況沒存到）"""
    _persist_auto_state()

def _refresh_capital_from_account():
    """內部函式：從永豐真實帳戶刷新可用資金（扣除T+2交割款），供API端點與排程器共用，
    確保即使前端沒開著，後端排程也會定期自己刷新，不會用過時的資金數字下單。
    （注意：此函式定義必須放在 scheduler.add_job 註冊之前，否則模組載入時會NameError崩潰）"""
    if not sinopac_api: return None
    try:
        bal=sinopac_api.account_balance()
        balance=float(getattr(bal,"acc_balance",None) or getattr(bal,"balance",0))
        avail  =float(getattr(bal,"available_balance",None) or getattr(bal,"available",0))
        pending_settlement = 0.0
        try:
            setts = sinopac_api.settlements(stock_account)
            today_str = tw_now().strftime("%Y-%m-%d")
            for s in setts:
                t_date = str(getattr(s,"t_date","") or getattr(s,"date",""))
                if t_date and t_date >= today_str:
                    pending_settlement += abs(float(s.amount))
        except Exception as se:
            logger.warning(f"無法取得交割資料，風控將以可用餘額為準（未扣交割款）: {se}")
        available_after_settlement = max(0.0, avail - pending_settlement)
        if available_after_settlement>0:
            auto_state["capital"]=available_after_settlement
        return {"account":str(stock_account),"balance":balance,"available":avail,
                "pending_settlement":pending_settlement,
                "available_after_settlement":available_after_settlement}
    except Exception as e:
        logger.warning(f"刷新可用資金失敗: {e}")
        return None

# ══════════════════════════════════════════════════════════════════
# 排程器啟動
# ══════════════════════════════════════════════════════════════════
scheduler=BackgroundScheduler(timezone='Asia/Taipei')
scheduler.add_job(auto_trade_tick,    'interval',seconds=30,id='tick',replace_existing=True)
scheduler.add_job(_periodic_persist,  'interval',seconds=60,id='persist',replace_existing=True) # 每分鐘把auto_state存進資料庫
scheduler.add_job(scan_top_stocks,    'interval',minutes=5,id='scan',replace_existing=True) # 每5分鐘掃描全市場股票池
scheduler.add_job(_refresh_capital_from_account, 'interval',minutes=5,id='capital_refresh',replace_existing=True) # 確保後端自己定期跟永豐核對真實可用資金，不依賴前端
scheduler.add_job(pre_market_prep,    CronTrigger(hour=8, minute=0, day_of_week='mon-fri',timezone='Asia/Taipei'),id='prep')
scheduler.add_job(force_close_all,    CronTrigger(hour=13,minute=20,day_of_week='mon-fri',timezone='Asia/Taipei'),id='force_close')
scheduler.add_job(post_market_summary,CronTrigger(hour=14,minute=30,day_of_week='mon-fri',timezone='Asia/Taipei'),id='post')
scheduler.add_job(ml_training_window, CronTrigger(hour=21,minute=0, day_of_week='mon-fri',timezone='Asia/Taipei'),id='ml')
scheduler.add_job(update_institutional_cache, CronTrigger(hour=15,minute=30,day_of_week='mon-fri',timezone='Asia/Taipei'),id='inst_flow') # 證交所約15:00後公布當日三大法人資料
scheduler.start()
logger.info("TradeAI Pro 後端 v2.0 排程器啟動 — 台股時段自動交易就緒")

# ══════════════════════════════════════════════════════════════════
# FastAPI 應用
# ══════════════════════════════════════════════════════════════════
app=FastAPI(title="TradeAI Pro 後端 v2.0",version="2.0.0")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

class ConnectRequest(BaseModel):
    api_key:str; secret_key:str
    ca_path:str=""; ca_password:str=""; person_id:str=""

class OrderRequest(BaseModel):
    symbol:str; direction:str; quantity:int; price:float; order_type:str="市價"

class CancelRequest(BaseModel):
    order_id:str

class AutoStartRequest(BaseModel):
    risk:str="low"; cap_pct:int=100; watchlist:List[str]=[]
    paper_mode:bool=True  # True=模擬下單(用真實股價算損益但不送真實委託)，False=真實下單
    force_real:bool=False  # 修正⑥安全閥：未達模擬驗證門檻時，切換成真實下單預設會被擋下；
                            # 使用者很清楚自己在做什麼、想跳過檢查時，明確帶這個true才會放行

# ── 健康 ──────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status":"ok","version":"2.0","connected":sinopac_api is not None,
            "market":market_status(),"auto_enabled":auto_state["enabled"]}

@app.get("/health")
def health():
    return {"healthy":True,"connected":sinopac_api is not None}

# ── iPhone Widget 公開狀態端點 ──────────────────────────────────────
@app.get("/status")
def get_status():
    ms=market_status()
    d=auto_state["daily_trades"] or 1
    wr=round(auto_state["daily_win"]/d*100,1)
    return {
        "market_status":ms["label"],
        "market_open":ms["status"]=="open",
        "connected":sinopac_api is not None,
        "auto_enabled":auto_state["enabled"],
        "paper_mode":auto_state.get("paper_mode",True),
        "daily_pnl":round(auto_state["daily_pnl"],0),
        "daily_pnl_pct":round(auto_state["daily_pnl"]/auto_state["capital"]*100,2),
        "win_rate":wr,
        "daily_trades":auto_state["daily_trades"],
        "positions_count":len(auto_state["positions"]),
        "risk_level":auto_state["risk"],
        "consec_loss":auto_state["consec_loss"],
        "paused":auto_state["pause_until"]>time.time(),
        "updated":tw_now().strftime("%H:%M:%S"),
    }

# ── 連接 / 斷線 ────────────────────────────────────────────────────
@app.post("/connect")
async def connect(req:ConnectRequest, background_tasks:BackgroundTasks):
    global sinopac_api,stock_account,future_account
    try:
        api=sj.Shioaji(simulation=False)
        accounts=api.login(api_key=req.api_key,secret_key=req.secret_key)
        sinopac_api=api
        ca_b64=os.environ.get("SINOPAC_CA_BASE64","").strip()
        ca_pwd=os.environ.get("SINOPAC_CA_PASSWORD","").strip()
        person_id=os.environ.get("SINOPAC_PERSON_ID","").strip()
        ca_ok=False
        if ca_b64 and ca_pwd and person_id:
            ca_temp=None
            try:
                ca_bytes=base64.b64decode(ca_b64)
                with tempfile.NamedTemporaryFile(suffix=".pfx",delete=False) as f:
                    f.write(ca_bytes); ca_temp=f.name
                api.activate_ca(ca_path=ca_temp,ca_passwd=ca_pwd,person_id=person_id)
                ca_ok=True; logger.info("CA 憑證啟用成功")
            except Exception as ca_e:
                logger.warning(f"CA 啟用失敗: {ca_e}")
            finally:
                if ca_temp:
                    try: os.unlink(ca_temp)
                    except: pass
        for acc in accounts:
            tn=type(acc).__name__; at=str(getattr(acc,"account_type","")).lower()
            if "stock" in tn.lower() or "stock" in at: stock_account=acc
            elif "future" in tn.lower() or "future" in at: future_account=acc
        if stock_account is None and accounts: stock_account=accounts[0]
        # 修正④：原本連線成功後資金(auto_state["capital"])要等5分鐘排程或使用者剛好打開/account頁面才會刷新，
        # 這段空窗期所有跟資金有關的風控（單筆預算、每日3%停損/8%鎖利）都可能拿著舊資金數字（甚至是預設的100萬）在算，
        # 跟修正②的「買不起就跳過」邏輯互相矛盾——預算算錢用的資金不準，跳過判斷自然也不準。改成連線後立刻同步刷新。
        cap_info=_refresh_capital_from_account()
        if cap_info:
            _log(f"資金已同步：可用NT${cap_info['available_after_settlement']:.0f}（扣除待交割款）")
        _log(f"永豐帳戶連接成功 CA={'✓' if ca_ok else '✗'}")
        background_tasks.add_task(scan_top_stocks)  # 連線成功後立即在背景開始掃描，不用等5分鐘排程
        # 模組一：註冊tick+bidask callback並訂閱即時OFI資料流(watchlist+股票池)，背景執行避免拖慢連線回應
        try:
            api.quote.set_on_tick_stk_v1_callback(_on_tick_stk_v1)
            api.quote.set_on_bidask_stk_v1_callback(_on_bidask_stk_v1)
        except Exception as e:
            logger.warning(f"OFI callback註冊失敗，真實OFI將停用(extract_ml_features會用0中性值): {e}")
        ofi_symbols=list(set(auto_state["watchlist"]) | set(SCAN_UNIVERSE))
        def _do_ofi_subscribe():
            n_ok=subscribe_ofi_symbols(ofi_symbols)
            _log(f"OFI即時訂閱完成：{n_ok}/{len(ofi_symbols)}檔成功"+("" if n_ok==len(ofi_symbols) else "（有股票訂閱失敗，那些股票的order_flow特徵會用0中性值代替，看log warning找原因）"))
        background_tasks.add_task(_do_ofi_subscribe)
        return {"success":True,"stock_account":str(stock_account) if stock_account else None,
                "future_account":str(future_account) if future_account else None,
                "total_accounts":len(accounts),"ca_activated":ca_ok}
    except Exception as e:
        logger.error(f"連接失敗：{e}")
        raise HTTPException(status_code=401,detail=f"連接失敗：{str(e)}")

@app.post("/disconnect")
async def disconnect():
    global sinopac_api,stock_account,future_account
    auto_state["enabled"]=False
    try:
        unsubscribe_all_ofi()  # 模組一：登出前先取消tick訂閱、清空OFI狀態，避免殘留訂閱占用額度
        if sinopac_api: sinopac_api.logout()
        sinopac_api=stock_account=future_account=None
        return {"success":True}
    except Exception as e:
        return {"success":False,"error":str(e)}

# ── 自動交易控制 ────────────────────────────────────────────────────
@app.post("/auto/start")
async def auto_start(req:AutoStartRequest):
    if not sinopac_api:
        raise HTTPException(status_code=401,detail="請先連接永豐帳戶")
    # 修正⑥：剛調整完邏輯/股票池/風險參數，不應該直接切真錢下單賭一次。
    # 要求至少先用「模擬模式」實際跑出 PAPER_VALIDATION_MIN_TRADES 筆完整交易、
    # 且跨過 PAPER_VALIDATION_MIN_DAYS 個不同交易日，才放行切換成真實下單；
    # 這個驗證進度（trade_count/trading_days）不會被每天的_reset_daily清空，是累積值。
    # 使用者明確知道自己在做什麼、想跳過時，可帶 force_real=true。
    if not req.paper_mode and not req.force_real:
        progress=_paper_validation_progress()
        if not progress["ready_for_real"]:
            _log(f"[擋下] 嘗試切換真實下單但模擬驗證不足（{progress['trades']}/{progress['min_trades']}筆，"
                 f"{progress['days']}/{progress['min_days']}天），已阻止啟動")
            raise HTTPException(status_code=400,detail={
                "message":"尚未達到真實下單的最低模擬驗證門檻，已阻止啟動",
                "progress":progress,
                "hint":"先用模擬模式繼續跑，或如果你很清楚自己在做什麼，帶 force_real=true 可跳過此檢查",
            })
    _refresh_capital_from_account()  # 修正④：啟動自動交易前再次確保資金是最新的，不要用舊資料算風控/部位大小
    auto_state["enabled"]=True
    auto_state["risk"]=req.risk
    auto_state["cap_pct"]=req.cap_pct
    auto_state["paper_mode"]=req.paper_mode
    if req.watchlist: auto_state["watchlist"]=req.watchlist
    _bootstrap_price_cache()  # 用真實歷史K棒暖機，避免重啟後空等15分鐘、且與前端顯示的訊號不一致
    mode_label="模擬下單（真實股價，不花真錢）" if req.paper_mode else "真實下單"
    _log(f"後端自動交易啟動 | {mode_label} | 風險:{req.risk} | 資金:{req.cap_pct}% | 自選股:{len(auto_state['watchlist'])}支 | 僅做多單")
    _persist_auto_state()
    return {"success":True,"message":"後端自動交易已啟動","state":auto_state}

@app.post("/auto/stop")
async def auto_stop():
    auto_state["enabled"]=False
    _log("後端自動交易已停止")
    _persist_auto_state()
    return {"success":True}

@app.post("/auto/reset-daily")
async def reset_daily_stats():
    """手動清空今日損益/勝率/交易紀錄，從現在開始重新計算（不影響真實永豐帳戶，只清這個系統自己記錄的統計數字）"""
    auto_state.update({
        "daily_pnl":0.0,"daily_win":0,"daily_trades":0,
        "consec_loss":0,"pause_until":0,
        "_loss_stop_logged":False,"_profit_lock_logged":False,"_afford_warn_logged":False,
        "trade_history":[],
    })
    _log("使用者手動清空今日統計，重新開始記錄")
    _persist_auto_state()
    return {"success":True,"state":auto_state}

@app.get("/auto/status")
async def auto_status():
    return {**auto_state,"market":market_status(),
            "price_cache_size":{k:len(v) for k,v in price_cache.items()}}

@app.get("/auto/validation")
async def get_paper_validation():
    """真實下單前的模擬驗證進度，供前端顯示「還差幾筆/幾天」"""
    return _paper_validation_progress()

@app.put("/auto/watchlist")
async def update_watchlist(watchlist:List[str]):
    auto_state["watchlist"]=watchlist
    _persist_auto_state()
    if sinopac_api:  # 模組一：自選股新增的股票也要補訂閱OFI，不然extract_ml_features對它們永遠拿不到即時OFI
        n_ok=subscribe_ofi_symbols(watchlist)
        if n_ok>0: _log(f"自選股更新，補訂閱OFI {n_ok}檔")
    return {"success":True,"watchlist":watchlist}

# ── ML 模型預測同步（前端訓練完成後呼叫，讓後端下單邏輯參考ML判斷）──────
class MLPredictionsRequest(BaseModel):
    predictions: Dict[str,float]  # {symbol: 0~1 預測勝率}

@app.post("/ml/predictions")
async def update_ml_predictions(req:MLPredictionsRequest):
    global ml_predictions
    ml_predictions=req.predictions
    _log(f"ML模型預測已同步 | {len(ml_predictions)}支股票")
    return {"success":True,"count":len(ml_predictions)}

@app.get("/ml/predictions")
async def get_ml_predictions():
    return ml_predictions

# ── AI學習狀態持久化（信心度/勝率/連勝/自適應權重，存在後端不會因換裝置或清快取而重置）──
learn_state_store: Dict = db_load("learn_state", {})  # 啟動時從資料庫還原（若已掛載Volume，跨重新部署也保留）

@app.post("/learn/state")
async def save_learn_state(state: Dict):
    global learn_state_store
    learn_state_store = state
    db_save("learn_state", state)
    return {"success": True}

@app.get("/learn/state")
async def get_learn_state():
    return learn_state_store or {}

# ── ML 神經網路模型權重持久化（存資料庫，換裝置/清瀏覽器快取/後端重新部署都不會丟失訓練成果，前提是已掛載Volume）──
ml_model_store: Dict = db_load("ml_model", {})

@app.post("/ml/model")
async def save_ml_model(model: Dict):
    global ml_model_store
    ml_model_store = model
    db_save("ml_model", model)
    return {"success": True}

@app.get("/ml/model")
async def get_ml_model():
    return ml_model_store or {}

# ── 交易成本試算（含手續費+當沖證交稅）──────────────────────────────
@app.get("/fees/calc")
async def calc_fees(entry_price:float,exit_price:float,qty:int=1000):
    return calc_round_trip_cost(entry_price,exit_price,qty)

@app.get("/contract/{symbol}")
async def get_contract_eligibility(symbol:str):
    """查詢個股當沖資格與漲跌停價（真實永豐合約資料），供前端下單前提示風險"""
    symbol=symbol.replace(".TW","").replace(".TWO","")
    info=get_contract_info(symbol)
    ok,reason=can_day_trade(symbol)
    return {"symbol":symbol,"day_trade":info["day_trade"],"can_day_trade":ok,"reason":reason,
            "limit_up":info["limit_up"],"limit_down":info["limit_down"]}

@app.get("/fees/min-move")
async def get_min_move():
    return {"min_profitable_move_pct":round(min_profitable_move_pct(),3),
            "note":"當沖一買一賣的價格漲跌幅至少要超過此百分比，才能扣除手續費與證交稅後真正獲利"}

# ── 三大法人買賣超（真實公開資料）─────────────────────────────────────
@app.get("/institutional/flows")
async def get_institutional_flows(top:int=10):
    """回傳今日外資/投信/自營商買賣超排行（依合計買賣超股數排序）"""
    update_institutional_cache()  # 若快取是舊的會自動嘗試更新
    data = institutional_cache["data"]
    if not data:
        return {"date":None,"top_buy":[],"top_sell":[],"note":"今日資料尚未公布（證交所約15:00後公布）或非交易日"}
    sorted_items = sorted(data.items(), key=lambda x: x[1]["total"], reverse=True)
    top_buy  = [{"symbol":k,**v} for k,v in sorted_items[:top] if v["total"]>0]
    top_sell = [{"symbol":k,**v} for k,v in sorted_items[::-1][:top] if v["total"]<0]
    return {"date":institutional_cache["date"],"top_buy":top_buy,"top_sell":top_sell}

@app.get("/institutional/flows/{symbol}")
async def get_institutional_flow_for_symbol(symbol:str):
    """查詢特定股票的三大法人買賣超"""
    update_institutional_cache()
    symbol = symbol.replace(".TW","").replace(".TWO","")
    data = institutional_cache["data"].get(symbol)
    if not data:
        return {"symbol":symbol,"found":False,"date":institutional_cache["date"]}
    return {"symbol":symbol,"found":True,"date":institutional_cache["date"],**data}

@app.get("/scan/topstocks")
async def get_scan_results(top:int=5, background_tasks:BackgroundTasks=None):
    """回傳全市場掃描結果（技術面動能排行，非預測保證）。若還沒掃描過會在背景觸發一次，不會卡住這次請求"""
    if not sinopac_api:
        raise HTTPException(status_code=401,detail="請先連接永豐帳戶")
    if scan_cache["updated"] is None and not scan_cache["scanning"] and background_tasks is not None:
        background_tasks.add_task(scan_top_stocks)  # 背景執行，不卡住這次API回應
    return {
        "results": scan_cache["results"][:top],
        "updated": scan_cache["updated"],
        "scanning": scan_cache["scanning"],
        "universe_size": len(SCAN_UNIVERSE),
        "note": "依RSI/MACD/量能/趨勢等技術指標綜合排序，反映目前技術面動能強度，非漲跌預測保證",
    }

@app.get("/auto/log")
async def get_log():
    return auto_state["log"]

# ── 帳戶餘額 ──────────────────────────────────────────────────────

@app.get("/account")
async def get_account():
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    result=_refresh_capital_from_account()
    if result is None: raise HTTPException(status_code=500,detail="無法取得帳戶資訊")
    return result

# ── 持倉明細 ──────────────────────────────────────────────────────
@app.get("/positions")
async def get_positions():
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    try:
        try: positions=sinopac_api.list_positions(stock_account,unit=sj.constant.Unit.Share)
        except AttributeError:
            try: positions=sinopac_api.list_positions(stock_account)
            except: positions=sinopac_api.list_positions()
        if not positions:
            try: positions=sinopac_api.list_positions()
            except: pass
        if not positions: return []
        result=[]
        for p in positions:
            try:
                code=getattr(p,"code",None) or getattr(p,"symbol","")
                qty =int(getattr(p,"quantity",0))
                ap  =float(getattr(p,"price",0) or getattr(p,"avg_price",0))
                lp  =float(getattr(p,"last_price",0) or getattr(p,"close",0))
                pnl =float(getattr(p,"pnl",0))
                pnlp=float(getattr(p,"pnl_percent",0))
                result.append({"symbol":code,"name":getattr(p,"name",code),"quantity":qty,
                    "avg_price":ap,"current_price":lp,"pnl":pnl,"pnl_percent":pnlp,
                    "direction":str(getattr(p,"direction","Buy")),"value":lp*qty if lp else ap*qty})
            except Exception as pe:
                logger.warning(f"解析持倉失敗: {pe}")
        return result
    except Exception as e:
        logger.error(f"list_positions 失敗: {e}")
        raise HTTPException(status_code=500,detail=f"持倉查詢失敗: {str(e)}")

# ── 委託記錄 ──────────────────────────────────────────────────────
@app.get("/orders")
async def get_orders():
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    try:
        sinopac_api.update_status(stock_account)
        trades=sinopac_api.list_trades()
        return [{"order_id":t.order.id,"symbol":t.contract.code,
                 "action":str(t.order.action),"quantity":int(t.order.quantity),
                 "price":float(t.order.price),"status":str(t.status.status),
                 "time":str(t.status.order_datetime)} for t in trades]
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

# ── 下單 ──────────────────────────────────────────────────────────
@app.post("/order")
async def place_order(req:OrderRequest):
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    try:
        contract=sinopac_api.Contracts.Stocks.get(req.symbol)
        if not contract: raise HTTPException(status_code=404,detail=f"找不到股票 {req.symbol}")
        action=sj.constant.Action.Buy if req.direction in ["做多","買進","buy"] else sj.constant.Action.Sell
        # 真實當沖資格檢查（依永豐合約資料的 day_trade 欄位，Yes/No/OnlyBuy）
        # 法規規定：零股、權證、ETN、處置股票皆不可當沖，僅整張可當沖（已用張為下單單位確保此規則）
        day_trade_status = str(getattr(contract,"day_trade",""))
        if "No" in day_trade_status:
            raise HTTPException(status_code=400,detail=f"{req.symbol} 今日不可當沖（可能為處置股票或不符當沖資格），已阻止下單")
        if "OnlyBuy" in day_trade_status and action==sj.constant.Action.Sell:
            raise HTTPException(status_code=400,detail=f"{req.symbol} 今日僅限先買後賣（OnlyBuy），不可先賣後買，已阻止下單")
        # 漲跌停鎖死風險檢查：若是做空（賣出）時遇漲停鎖死最危險（買不回會有違約交割+借券費風險）
        limit_up=float(getattr(contract,"limit_up",0) or 0)
        limit_down=float(getattr(contract,"limit_down",0) or 0)
        if limit_up>0 and req.price>=limit_up*0.998:
            logger.warning(f"{req.symbol} 接近漲停價({limit_up})，若為做空回補方向可能無法成交，請留意")
        if limit_down>0 and req.price<=limit_down*1.002:
            logger.warning(f"{req.symbol} 接近跌停價({limit_down})，若為做多賣出方向可能無法成交")
        ptype=sj.constant.StockPriceType.MKT if req.order_type=="市價" else sj.constant.StockPriceType.LMT
        order=sinopac_api.Order(price=req.price,quantity=req.quantity,action=action,
            price_type=ptype,order_type=sj.constant.OrderType.ROD,account=stock_account)
        trade=sinopac_api.place_order(contract,order)
        return {"success":True,"order_id":trade.order.id,"status":str(trade.status.status),
                "symbol":req.symbol,"direction":req.direction,"quantity":req.quantity,"price":req.price}
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(status_code=500,detail=f"下單失敗：{str(e)}")

# ── 取消委託 ──────────────────────────────────────────────────────
@app.post("/cancel")
async def cancel_order(req:CancelRequest):
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    try:
        sinopac_api.update_status(stock_account)
        trades=sinopac_api.list_trades()
        target=next((t for t in trades if t.order.id==req.order_id),None)
        if not target: raise HTTPException(status_code=404,detail="找不到此委託單")
        sinopac_api.cancel_order(target)
        return {"success":True,"order_id":req.order_id}
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

# ── 即時股價 ──────────────────────────────────────────────────────
@app.get("/price/{symbol}")
async def get_price(symbol:str):
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    symbol=symbol.replace(".TW","").replace(".TWO","")  # 永豐合約代碼不含交易所後綴
    try:
        contract=sinopac_api.Contracts.Stocks.get(symbol)
        if not contract: raise HTTPException(status_code=404,detail=f"找不到 {symbol}")
        snap=sinopac_api.snapshots([contract])
        if not snap: raise HTTPException(status_code=404,detail="無法取得股價")
        s=snap[0]
        return {"symbol":symbol,"name":str(getattr(contract,"name","")),"price":float(s.close),"change":float(s.change_price),
                "change_percent":float(s.change_rate),"volume":int(s.total_volume),
                "open":float(s.open),"high":float(s.high),"low":float(s.low)}
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

# ── 交割記錄 ──────────────────────────────────────────────────────
# ── 真實歷史K棒（取代前端亂數模擬，讓RSI/MACD/ML訓練都基於真實價格歷史）──────
def _fetch_real_history(symbol:str, bars:int=90):
    """內部函式：抓真實歷史K棒，供API端點與後端自身的price_cache暖機共用"""
    if not sinopac_api: return []
    symbol=symbol.replace(".TW","").replace(".TWO","")
    try:
        contract=sinopac_api.Contracts.Stocks.get(symbol)
        if not contract: return []
        end_d=tw_now()
        start_d=end_d-timedelta(days=10)
        kb=sinopac_api.kbars(contract,start=start_d.strftime("%Y-%m-%d"),end=end_d.strftime("%Y-%m-%d"))
        if not kb or not kb.ts: return []
        n=len(kb.ts)
        raw=[{"ts":kb.ts[i],"open":kb.Open[i],"high":kb.High[i],"low":kb.Low[i],
              "close":kb.Close[i],"volume":kb.Volume[i]} for i in range(n)]
        raw=raw[-(bars*5):]
        agg=[]
        for i in range(0,len(raw),5):
            chunk=raw[i:i+5]
            if not chunk: continue
            t=datetime.fromtimestamp(chunk[0]["ts"]/1e9,tz=TW_TZ)
            agg.append({
                "time":t.strftime("%H:%M"),"ts":t.timestamp(),
                "price":chunk[-1]["close"],"close":chunk[-1]["close"],
                "open":chunk[0]["open"],
                "high":max(c["high"] for c in chunk),
                "low":min(c["low"] for c in chunk),
                "volume":sum(c["volume"] for c in chunk),
            })
        return agg[-bars:]
    except Exception as e:
        logger.warning(f"抓取{symbol}歷史K棒失敗: {e}")
        return []

def _bootstrap_price_cache():
    """後端啟動自動交易時，用真實歷史K棒預先填充price_cache，避免重啟後要空等15分鐘才開始判斷信號
    （這段空窗期內前端可能已經根據真實歷史顯示訊號，但後端因為price_cache是空的只會回報觀望，造成兩邊不一致）"""
    symbols = set(auto_state["watchlist"])
    if auto_state.get("paper_mode"):
        symbols |= set(SCAN_UNIVERSE)  # 模擬模式：股票池也一起暖機，AI才能立即從更大範圍找機會
    for sym in symbols:
        if sym in price_cache and len(price_cache[sym])>=30: continue
        bars=_fetch_real_history(sym,bars=MAX_BARS)
        if bars:
            price_cache[sym]=[{"price":b["close"],"volume":b["volume"],"t":b.get("ts",time.time())} for b in bars]
            logger.info(f"{sym} price_cache 已用真實歷史K棒暖機，{len(bars)}筆")

@app.get("/history/{symbol}")
async def get_history(symbol:str, bars:int=90):
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    agg=_fetch_real_history(symbol,bars)
    if not agg:
        return {"symbol":symbol,"bars":[],"note":"無歷史資料（可能非交易日或新股）"}
    return {"symbol":symbol,"bars":agg,"source":"real_kbars"}

@app.get("/settlements")
async def get_settlements():
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    try:
        data=sinopac_api.settlements(stock_account)
        return [{"date":str(s.date),"amount":float(s.amount),"t_date":str(getattr(s,"t_date",""))} for s in data]
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

# ── 完整投資組合 ──────────────────────────────────────────────────
@app.get("/portfolio")
async def get_portfolio():
    if not sinopac_api: raise HTTPException(status_code=401,detail="尚未連接")
    acc=await get_account()
    pos=await get_positions()
    ords=await get_orders()
    setts=await get_settlements()
    return {"account":acc,"positions":pos,"orders":ords,"settlements":setts}
