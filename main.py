"""
TradeAI Pro 後端 v2.0 — 24小時自動交易機器人
台股時段：09:30-13:00主動交易 | 08:00盤前準備 | 13:00停開新倉 | 13:20強制平倉 | 14:30盤後總結
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict
import shioaji as sj
import os, logging, base64, tempfile, time
from datetime import datetime, timedelta
import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
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

def market_status() -> dict:
    now = tw_now()
    if now.weekday() >= 5:
        return {"status": "closed", "session": "weekend", "label": "週末休市"}
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
            "bull":round(bull,1),"bear":round(bear,1),
            "up_trend":up_trend,"down_trend":down_trend,"bad_time":bad_time}

# ══════════════════════════════════════════════════════════════════
# 全域狀態
# ══════════════════════════════════════════════════════════════════
sinopac_api    = None
stock_account  = None
future_account = None

auto_state = {
    "enabled":False,"risk":"low","cap_pct":100,"paper_mode":True,  # 預設模擬下單，須使用者明確選擇才會送真實委託
    "watchlist":[],"positions":[],"log":[],
    "capital":1_000_000,
    "daily_pnl":0.0,"daily_win":0,"daily_trades":0,
    "consec_loss":0,"pause_until":0,"trade_date":None,
}

RISK_CFG = {
    "low":  {"min_conf":72,"alloc":0.05,"sl":2.0,"tp":4.0,"max_pos":3,"max_hold_min":25},
    "mid":  {"min_conf":68,"alloc":0.10,"sl":3.0,"tp":6.0,"max_pos":5,"max_hold_min":40},
    "high": {"min_conf":65,"alloc":0.20,"sl":5.0,"tp":10.0,"max_pos":8,"max_hold_min":60},
}

price_cache: Dict[str,List[Dict]] = {}
MAX_BARS = 120
ml_predictions: Dict[str,float] = {}  # {symbol: 0~1 預測勝率} 由前端訓練完成後同步

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

def _snapshot(sym:str) -> Optional[tuple]:
    """回傳 (price, volume)，volume用真實成交量，不再用假數字（之前固定填1,000,000導致量比指標完全失效）"""
    if not sinopac_api: return None
    try:
        c=sinopac_api.Contracts.Stocks.get(sym)
        if not c: return None
        s=sinopac_api.snapshots([c])
        if not s: return None
        return (float(s[0].close), int(getattr(s[0],"total_volume",0) or 0))
    except: return None

def _update_cache():
    for sym in auto_state["watchlist"]:
        snap=_snapshot(sym)
        if snap:
            price,volume=snap
            if sym not in price_cache: price_cache[sym]=[]
            price_cache[sym].append({"price":price,"volume":volume or 1_000_000})
            price_cache[sym]=price_cache[sym][-MAX_BARS:]

def _reset_daily():
    today=tw_now().strftime("%Y-%m-%d")
    if auto_state["trade_date"]!=today:
        auto_state.update({"trade_date":today,"daily_pnl":0.0,
            "daily_win":0,"daily_trades":0,"consec_loss":0,"pause_until":0})
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
        _log("[停止]今日虧損達3%，停止交易"); return
    if auto_state["daily_pnl"]/auto_state["capital"]*100>=8:
        _log("[鎖定]今日獲利達8%，鎖定獲利"); return
    cfg=RISK_CFG[auto_state["risk"]]
    # 出場檢查
    to_close=[]
    for pos in auto_state["positions"]:
        sym=pos["sym"]; snap=_snapshot(sym)
        if not snap: continue
        p=snap[0]
        pp=(p-pos["entry"])/pos["entry"]*100*(1 if pos["dir"]=="L" else -1)
        held_min=(time.time()-pos.get("opened_at",time.time()))/60
        time_up=held_min>=cfg.get("max_hold_min",40) and pp>-0.1  # 短炒持倉時間上限：超時且未明顯虧損就先了結
        close=pp<=-cfg["sl"] or pp>=cfg["tp"] or time_up
        if not close:
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
                if auto_state["consec_loss"]>=3:
                    auto_state["pause_until"]=time.time()+15*60
                    _log(f"連虧{auto_state['consec_loss']}次，冷靜15分鐘")
            tag="止盈" if pp>=cfg["tp"] else "停損" if pp<=-cfg["sl"] else "持倉超時" if time_up else "反轉"
            _log(f"{tag} {pos['dir']} @{p:.2f} 淨損益${net_pnl:.0f}(毛利${gross:.0f}-成本${cost['total_cost']:.0f})",sym)
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
    else:
        existing={p["sym"] for p in auto_state["positions"]}
    for sym in (auto_state["watchlist"] if can_open_new_position() else []):
        if sym in existing or len(auto_state["positions"])>=cfg["max_pos"]: continue
        hist=price_cache.get(sym,[])
        if len(hist)<30: continue
        sig=calc_signal_py([h["price"] for h in hist],[h["volume"] for h in hist])
        if sig["conf"]<cfg["min_conf"] or sig["action"]=="hold" or sig.get("bad_time"): continue
        # 真實當沖資格檢查（用永豐合約真實資料：day_trade=No的處置股票/不符資格股票直接跳過）
        dir_=  "L" if sig["action"]=="buy" else "S"
        ok,reason = can_day_trade(sym, is_sell_first=(dir_=="S"))
        if not ok:
            continue  # 不符當沖資格，AI不會對此股票下單（避免送出必定失敗或違規的委託）
        # 漲跌停鎖死風險（不對稱）：做空最怕遇到漲停鎖死買不回（會產生違約交割風險、借券費用），
        # 做多遇到跌停沖不掉則只是變成一般T+2持股，風險相對輕微。故做空進場用更寬的安全距離。
        cinfo=get_contract_info(sym)
        p_now=hist[-1]["price"]
        if dir_=="S" and cinfo["limit_up"]>0 and p_now>=cinfo["limit_up"]*0.985:
            continue  # 做空：價格已偏向漲停（即使還沒鎖死），避免進場後被軋空鎖死買不回
        if dir_=="L" and cinfo["limit_down"]>0 and p_now<=cinfo["limit_down"]*1.005:
            continue  # 做多：價格已接近跌停，避免追空進場
        # ML 模型加成：若前端已訓練模型對此股有預測，納入信心參考（不足以單獨決定方向，僅加成/減弱）
        ml_pred=ml_predictions.get(sym)
        if ml_pred is not None:
            agree = (ml_pred>=0.55 and sig["action"]=="buy") or (ml_pred<=0.45 and sig["action"]=="sell")
            disagree = (ml_pred<=0.40 and sig["action"]=="buy") or (ml_pred>=0.60 and sig["action"]=="sell")
            if disagree: continue  # ML與技術指標方向衝突，跳過避免假信號
            if agree: sig["conf"]=min(95,sig["conf"]*1.08)  # ML同意方向，信心加成8%
        p=hist[-1]["price"]
        qty=max(1,int(auto_state["capital"]*(auto_state["cap_pct"]/100)*cfg["alloc"]/(p*1000)))
        auto_state["positions"].append({
            "sym":sym,"dir":dir_,"qty":qty,"entry":p,
            "sl":round(p*(1-cfg["sl"]/100) if dir_=="L" else p*(1+cfg["sl"]/100),2),
            "tp":round(p*(1+cfg["tp"]/100) if dir_=="L" else p*(1-cfg["tp"]/100),2),
            "open_time":tw_now().strftime("%H:%M:%S"),
            "opened_at":time.time(),  # 數值時間戳，用於計算持倉分鐘數（短炒持倉時間上限判斷）
        })
        act=sj.constant.Action.Buy if dir_=="L" else sj.constant.Action.Sell
        _place_real_order(sym,act,qty)
        _log(f"{'做多▲' if dir_=='L' else '做空▼'} {qty}張@{p:.2f} 信心{sig['conf']:.0f}%",sym)
        existing.add(sym)

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
        except Exception as e:
            logger.warning(f"強制平倉{sym}損益計算失敗（委託仍會送出）: {e}")
        act=sj.constant.Action.Sell if pos["dir"]=="L" else sj.constant.Action.Buy
        _place_real_order(sym,act,pos["qty"])
    auto_state["positions"]=[]

def post_market_summary():
    d=auto_state["daily_trades"] or 1
    wr=auto_state["daily_win"]/d*100
    _log(f"盤後總結 | P&L:${auto_state['daily_pnl']:.0f} | 勝率:{wr:.0f}%({auto_state['daily_win']}/{auto_state['daily_trades']})")

def ml_training_window():
    _log("21:00 ML訓練時段開始（前端訓練請在學習分頁執行）")

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
scheduler.add_job(_refresh_capital_from_account, 'interval',minutes=5,id='capital_refresh',replace_existing=True) # 確保後端自己定期跟永豐核對真實可用資金，不依賴前端
scheduler.add_job(pre_market_prep,    CronTrigger(hour=8, minute=0, day_of_week='mon-fri'),id='prep')
scheduler.add_job(force_close_all,    CronTrigger(hour=13,minute=20,day_of_week='mon-fri'),id='force_close')
scheduler.add_job(post_market_summary,CronTrigger(hour=14,minute=30,day_of_week='mon-fri'),id='post')
scheduler.add_job(ml_training_window, CronTrigger(hour=21,minute=0, day_of_week='mon-fri'),id='ml')
scheduler.add_job(update_institutional_cache, CronTrigger(hour=15,minute=30,day_of_week='mon-fri'),id='inst_flow') # 證交所約15:00後公布當日三大法人資料
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
async def connect(req:ConnectRequest):
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
        _log(f"永豐帳戶連接成功 CA={'✓' if ca_ok else '✗'}")
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
    auto_state["enabled"]=True
    auto_state["risk"]=req.risk
    auto_state["cap_pct"]=req.cap_pct
    auto_state["paper_mode"]=req.paper_mode
    if req.watchlist: auto_state["watchlist"]=req.watchlist
    _bootstrap_price_cache()  # 用真實歷史K棒暖機，避免重啟後空等15分鐘、且與前端顯示的訊號不一致
    mode_label="模擬下單（真實股價，不花真錢）" if req.paper_mode else "真實下單"
    _log(f"後端自動交易啟動 | {mode_label} | 風險:{req.risk} | 資金:{req.cap_pct}% | 自選股:{len(auto_state['watchlist'])}支")
    return {"success":True,"message":"後端自動交易已啟動","state":auto_state}

@app.post("/auto/stop")
async def auto_stop():
    auto_state["enabled"]=False
    _log("後端自動交易已停止")
    return {"success":True}

@app.get("/auto/status")
async def auto_status():
    return {**auto_state,"market":market_status(),
            "price_cache_size":{k:len(v) for k,v in price_cache.items()}}

@app.put("/auto/watchlist")
async def update_watchlist(watchlist:List[str]):
    auto_state["watchlist"]=watchlist
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
learn_state_store: Dict = {}  # 簡易持久化：存在記憶體，搭配 Railway 持續運行不重啟即可長期保留

@app.post("/learn/state")
async def save_learn_state(state: Dict):
    global learn_state_store
    learn_state_store = state
    return {"success": True}

@app.get("/learn/state")
async def get_learn_state():
    return learn_state_store or {}

# ── ML 神經網路模型權重持久化（存在後端，換裝置/清瀏覽器快取也不會丟失訓練成果）──
ml_model_store: Dict = {}

@app.post("/ml/model")
async def save_ml_model(model: Dict):
    global ml_model_store
    ml_model_store = model
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
                "time":t.strftime("%H:%M"),
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
    for sym in auto_state["watchlist"]:
        if sym in price_cache and len(price_cache[sym])>=30: continue
        bars=_fetch_real_history(sym,bars=MAX_BARS)
        if bars:
            price_cache[sym]=[{"price":b["close"],"volume":b["volume"]} for b in bars]
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
