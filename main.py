"""
TradeAI Pro 後端 v2.0 — 24小時自動交易機器人
台股時段：09:30-13:00主動交易 | 08:00盤前準備 | 13:00停開新倉 | 13:20強制平倉 | 14:30盤後總結

模組拆分說明：原本這個檔案近2000行所有邏輯都擠在一起，現在拆成：
  config.py   — 純常數、時間/假日工具、股票池/板塊對照表
  db.py       — SQLite持久化
  state.py    — 跨模組共用的price_cache(就地修改，不可整個重新賦值)
  signals.py  — 技術指標/市場環境/OFI大單流引擎
  ml_model.py — LightGBM特徵抽取/推論/動態部位縮放/進出場原因說明
main.py本身保留：全域連線狀態(sinopac_api等會被整個重新賦值的物件)、auto_state交易狀態、
交易引擎主迴圈、看門狗、全市場掃描、FastAPI所有路由——這些彼此高度耦合、且跟main.py的
生命週期(誰連線了/誰啟動了自動交易)綁在一起，拆出去風險大於好處，刻意留在這裡。
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Tuple
import shioaji as sj
import os, logging, base64, tempfile, time, csv, io
from datetime import datetime, timedelta
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from db import db_init, db_save, db_load
from config import (
    TW_TZ, tw_now, FEE_RATE, FEE_DISCOUNT, DAYTRADE_TAX_RATE,
    calc_round_trip_cost, min_profitable_move_pct,
    market_status, is_trading_time, can_open_new_position,
    SCAN_UNIVERSE, SECTOR_MAP,
    PAPER_VALIDATION_MIN_TRADES, PAPER_VALIDATION_MIN_DAYS,
    MAX_TRADES_PER_DAY, CONSEC_LOSS_STOP,
    max_allowed_spread_pct, next_settlement_date, tick_size,
)
from state import price_cache
from signals import (
    calc_signal_py, is_no_trade_zone, advanced_score,
    MARKET_INDEX_SYMBOL, get_market_context,
    subscribe_ofi_symbols, unsubscribe_all_ofi, unsubscribe_ofi_symbols, get_latest_spread_pct,
    _on_tick_stk_v1, _on_bidask_stk_v1, _flush_all_ofi,
)
from ml_model import (
    ML_FEATURE_NAMES, predict_lgbm_confidence,
    lgbm_grade, calculate_dynamic_position_ratio,
    build_entry_reason, build_exit_reason, get_lgbm_model_status,
)

db_init()

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
    # 模擬資金：完全獨立於真實帳戶餘額的虛擬現金帳本，不會被真實帳戶的待交割款卡住——
    # 不然模擬模式的部位大小被真實帳戶的T+2交割綁住，完全不合理(模擬本來就不該花真錢，
    # 也就不該被真錢的交割時間表限制)。預設1000萬，可以透過/auto/paper-capital調整。
    # 跟「真實capital」一樣的可用/已交割概念：paper_capital是總帳，paper_pending_settlements
    # 記錄每筆平倉後還在交割中(T+2)的金額，可用金額=paper_capital減去還沒交割完成的部分。
    "paper_capital":10_000_000.0,
    "paper_pending_settlements":[],  # [{"amount":float,"settle_date":"YYYY-MM-DD"}, ...]
    # 券商手續費折扣：每個人跟永豐談的實際折扣不一樣(常見1折~無折扣都有)，預設用config.py
    # 的6折只是個起始假設，不是每個人的真實數字——可透過/auto/fee-discount設定成自己實際拿到的折扣，
    # 這個數字會直接影響所有損益試算跟「至少要漲跌多少%才划算」的門檻是否準確。
    "fee_discount":FEE_DISCOUNT,
    "daily_pnl":0.0,"daily_win":0,"daily_trades":0,
    "consec_loss":0,"pause_until":0,"trade_date":None,
    "_loss_stop_logged":False,"_profit_lock_logged":False,"_afford_warn_logged":False,"_zero_capital_logged":False,
    # 模擬驗證進度：累積筆數/交易日不會被每日重置清空（跟trade_history不一樣），
    # 做為「切換成真實下單」前的最低驗證門檻依據，見 PAPER_VALIDATION_MIN_*。
    "paper_validation":{"trade_count":0,"trading_days":[]},
    # 權益曲線：每個交易日收盤後記錄一筆{date, equity, mode}，這是算最大回撤跟年化報酬率的唯一資料來源——
    # 原本只累積總贏/總輸金額跟勝負次數，沒有「隨時間變化的帳戶總值」這個時間序列，根本算不出
    # 最大回撤(需要知道帳戶價值從哪個高點跌到哪個低點)，年化報酬率也只能用不夠嚴謹的線性外推。
    "equity_curve":[],  # [{"date":"YYYY-MM-DD","equity":float,"mode":"paper"|"real"}, ...]
    "drawdown_halt":False,"drawdown_halt_info":None,  # 累積回撤保護觸發狀態，需要手動resume才會解除
    # 每日交易漏斗：記錄每個候選股票在每一關被擋下的原因次數，每天重置(_reset_daily)。
    # 直接回答「今天為什麼沒有交易」，不用再去猜log文字背後的原因。
    "funnel":{"scanned":0,"bad_time":0,"no_trade_zone":0,"not_eligible":0,"limit_down_risk":0,
              "vwap_reject":0,"wide_spread":0,"extreme_recent_move":0,"no_model":0,"low_confidence":0,"sector_dup":0,"unaffordable":0,
              "daily_cap_reached":0,"per_tick_cap_reached":0,"opened":0,"order_failed":0},
    "ofi_failed_symbols":[],  # OFI訂閱失敗的股票代號清單，點log時顯示具體是哪幾檔
    "capital_info":None,  # 最近一次資金同步的完整明細(balance/available/pending_settlement/available_after_settlement)
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
# (PAPER_VALIDATION_MIN_TRADES/MIN_DAYS/MAX_TRADES_PER_DAY/CONSEC_LOSS_STOP 已從config.py匯入)
# ══════════════════════════════════════════════════════════════════

def _record_paper_validation(pnl:float=0.0, pct:float=0.0, held_min:float=0.0):
    """每筆模擬交易平倉後呼叫：累積驗證進度，trade_count/trading_days/累積損益都不會被_reset_daily清空。
    修正：原本只記trade_count/trading_days，沒記損益細節——但trade_history每天會被_reset_daily清空，
    等20筆+5天驗證期跑完時，根本沒有資料可以回頭算「這段期間整體的Profit Factor/勝率」，
    只能看到「今天」的數字。改成額外累積總贏/總輸金額跟勝負次數，才能算出真正跨天的驗證報告。
    修正②：補上「專業」績效報告通常會有、但這裡原本完全沒追蹤的幾項——單筆最大贏/最大輸、
    最長連勝/連敗紀錄(歷史最高，不是現在風控用的當前連敗數)、總持倉時間(用來算平均持倉分鐘)。
    這些數字本身不難算，難的是「資料根本沒被存下來」，trade_history每天被清空，這些統計量
    必須在這裡(不會被每日重置的地方)逐筆累積，不能事後從trade_history回頭算。"""
    pv=auto_state.setdefault("paper_validation",{"trade_count":0,"trading_days":[],
                              "total_win_pnl":0.0,"total_loss_pnl":0.0,"wins":0,"losses":0,
                              "win_pct_sum":0.0,"loss_pct_sum":0.0,
                              "largest_win_pnl":0.0,"largest_loss_pnl":0.0,
                              "current_streak":0,"max_win_streak":0,"max_loss_streak":0,
                              "total_held_min":0.0})
    pv["trade_count"]=pv.get("trade_count",0)+1
    pv["total_held_min"]=pv.get("total_held_min",0.0)+max(0.0,held_min)
    today_str=tw_now().strftime("%Y-%m-%d")
    if today_str not in pv.get("trading_days",[]):
        pv.setdefault("trading_days",[]).append(today_str)
    if pnl>0:
        pv["wins"]=pv.get("wins",0)+1
        pv["total_win_pnl"]=pv.get("total_win_pnl",0.0)+pnl
        pv["win_pct_sum"]=pv.get("win_pct_sum",0.0)+pct
        pv["largest_win_pnl"]=max(pv.get("largest_win_pnl",0.0),pnl)
        cur=pv.get("current_streak",0)
        pv["current_streak"]=cur+1 if cur>=0 else 1
        pv["max_win_streak"]=max(pv.get("max_win_streak",0),pv["current_streak"])
    elif pnl<0:
        pv["losses"]=pv.get("losses",0)+1
        pv["total_loss_pnl"]=pv.get("total_loss_pnl",0.0)+pnl  # 存負數，方便後面算profit factor時取絕對值
        pv["loss_pct_sum"]=pv.get("loss_pct_sum",0.0)+pct
        pv["largest_loss_pnl"]=min(pv.get("largest_loss_pnl",0.0),pnl)
        cur=pv.get("current_streak",0)
        pv["current_streak"]=cur-1 if cur<=0 else -1
        pv["max_loss_streak"]=max(pv.get("max_loss_streak",0),abs(pv["current_streak"]))

def _record_feature_log(sym:str, pos:Dict, pnl:float, pct:float, tag:str):
    """每筆交易平倉時記錄當時餵給LightGBM的完整特徵向量+結果，跨天累積不會被_reset_daily清空。
    這是5天驗證期跑完後做SHAP特徵歸因分析(到底哪個特徵真的有用、哪個是雜訊)的原始資料——
    沒有這個log，模型預測完特徵向量就丟了，事後完全沒有資料可以回頭分析，分析計畫根本做不了。
    上限500筆，20筆驗證門檻的數十倍，不會無限長大，又遠超過5天能累積的真實交易量。"""
    log=auto_state.setdefault("feature_log",[])
    entry={"ts":tw_now().strftime("%Y-%m-%d %H:%M:%S"),"sym":sym,"pnl":round(pnl,0),"pct":round(pct,2),
           "tag":tag,"lgbm_conf":pos.get("lgbm_conf"),"win":1 if pnl>0 else 0}
    entry.update(pos.get("features",{}))
    log.append(entry)
    auto_state["feature_log"]=log[-500:]

def _paper_validation_progress() -> dict:
    """真實下單前的驗證進度查詢：累積勝率/獲利因子/平均贏輸%，供/auto/validation跟/auto/start
    的真實模式門檻檢查共用。"""
    pv=auto_state.get("paper_validation",{"trade_count":0,"trading_days":[]})
    trades=pv.get("trade_count",0); days=len(pv.get("trading_days",[]))
    ok = trades>=PAPER_VALIDATION_MIN_TRADES and days>=PAPER_VALIDATION_MIN_DAYS
    wins=pv.get("wins",0); losses=pv.get("losses",0)
    total_win=pv.get("total_win_pnl",0.0); total_loss=pv.get("total_loss_pnl",0.0)
    # Profit Factor：總贏的錢/總輸的錢(取絕對值)，業界常用門檻是>1.5才算「具備實盤印鈔能力」
    profit_factor=round(total_win/abs(total_loss),2) if total_loss!=0 else (float("inf") if total_win>0 else 0.0)
    win_rate=round(wins/(wins+losses)*100,1) if (wins+losses)>0 else 0.0
    avg_win_pct=round(pv.get("win_pct_sum",0.0)/wins,2) if wins>0 else 0.0
    avg_loss_pct=round(pv.get("loss_pct_sum",0.0)/losses,2) if losses>0 else 0.0
    # 期望值(Expectancy)：平均每一筆交易賺賠多少錢，是判斷一個策略「平均每動一次手會發生什麼事」
    # 最直接的數字，比單獨看勝率或獲利因子更貼近實際盈虧感受——勝率高但期望值是負的，代表小贏多次
    # 但偶爾大賠，整體還是賠錢；這個數字才是真正該優化的目標。
    expectancy=round((total_win+total_loss)/trades,1) if trades>0 else 0.0
    avg_held_min=round(pv.get("total_held_min",0.0)/trades,1) if trades>0 else 0.0
    return {"trades":trades,"days":days,"min_trades":PAPER_VALIDATION_MIN_TRADES,
            "min_days":PAPER_VALIDATION_MIN_DAYS,"ready_for_real":ok,
            "win_rate":win_rate,"profit_factor":profit_factor,
            "avg_win_pct":avg_win_pct,"avg_loss_pct":avg_loss_pct,
            "total_pnl":round(total_win+total_loss,0),
            "expectancy_per_trade":expectancy,
            "largest_win_pnl":round(pv.get("largest_win_pnl",0.0),0),
            "largest_loss_pnl":round(pv.get("largest_loss_pnl",0.0),0),
            "max_win_streak":pv.get("max_win_streak",0),"max_loss_streak":pv.get("max_loss_streak",0),
            "avg_held_min":avg_held_min}

ANNUALIZED_RELIABLE_MIN_DAYS = 20  # 少於這個交易日數，年化外推會被單一極端的好/壞日子放大到失真，不該被當真

MAX_DRAWDOWN_HALT_PCT = 15.0  # 累積回撤達這個百分比，停止開新倉直到使用者手動確認恢復——這跟「今日虧損3%」
                              # 是不同層級的保護：每日停損防的是單日急殺，這個防的是「連續好幾天慢慢流血」，
                              # 慢性虧損可能永遠不會單日觸發3%，但累積起來一樣會把帳戶掏空。

def _check_drawdown_circuit_breaker() -> bool:
    """檢查目前的估計權益，相對歷史最高點的回撤是否已經達到需要停手的程度。回傳True代表已經
    觸發暫停(或本來就已經暫停中)，False代表可以正常開新倉。
    用「昨天收盤權益(來自equity_curve) + 今天目前為止的daily_pnl」當即時估計值，不用等到
    今天14:30盤後才更新——回撤這種風險，越晚發現代價越高，不該每天才檢查一次。
    一旦觸發，需要呼叫/auto/resume-after-drawdown手動清除才會恢復，不會像每日停損那樣
    隔天自動恢復——累積回撤代表的問題比單日虧損更系統性，值得停下來認真檢視，不該自動繼續。
    修正：只看「目前模式(模擬/真實)」自己的權益曲線，不要跟另一種模式混在一起比——模擬預設
    1000萬起始資金，真實帳戶規模通常完全不同，混在同一條曲線上比「歷史最高點」會被其中一種
    模式的絕對金額永遠壓著，另一種模式的回撤判斷會完全失真(可能誤觸發，也可能永遠不會觸發)。"""
    if auto_state.get("drawdown_halt"):
        return True
    cur_mode="paper" if auto_state.get("paper_mode") else "real"
    curve=[c for c in auto_state.get("equity_curve",[]) if c.get("mode")==cur_mode]
    if not curve:
        return False  # 還沒有任何歷史權益資料，無從判斷回撤，不阻擋(跟年化報酬率同樣的資料不足限制)
    peak=max(c["equity"] for c in curve)
    last_closed=curve[-1]["equity"]
    current_est=last_closed+auto_state.get("daily_pnl",0.0)
    if peak<=0: return False
    dd_pct=(peak-current_est)/peak*100
    if dd_pct>=MAX_DRAWDOWN_HALT_PCT:
        auto_state["drawdown_halt"]=True
        auto_state["drawdown_halt_info"]={"peak_equity":round(peak,2),"current_equity_est":round(current_est,2),
                                           "drawdown_pct":round(dd_pct,2),"mode":cur_mode,
                                           "triggered_at":tw_now().strftime("%Y-%m-%d %H:%M:%S")}
        _log(f"⚠️⚠️⚠️ [回撤保護觸發] {'模擬' if cur_mode=='paper' else '真實'}帳戶估計權益較歷史高點(${peak:,.0f})回撤{dd_pct:.1f}%，已達{MAX_DRAWDOWN_HALT_PCT}%門檻，"
             f"停止開新倉(現有持倉仍會正常出場)。需要到系統分頁手動確認後才會恢復，不會隔天自動恢復。")
        _persist_auto_state()
        return True
    return False

def compute_performance_metrics(mode:Optional[str]=None) -> dict:
    """從equity_curve(每個交易日收盤後記錄的帳戶總值)算出最大回撤、年化報酬率、Sharpe比率、
    Calmar比率這些標準績效指標。
    重要：年化報酬率是用複利公式把『目前累積的報酬率』外推成『一年的話會是多少』，這個外推方式
    在交易日數還很少的時候統計上極不可靠——比如只跑了3天，其中1天剛好大賺3%，外推成年化可能
    變成一個荒謬的三位數百分比，那不是「這個策略真的能賺那麼多」，是「樣本太少，被單一個事件
    放大到失真」。這裡明確標記is_annualized_reliable跟附上原因，畫面顯示時應該照這個標記決定
    要不要用警示色或加註說明，不能直接秀一個算出來的數字就讓人誤以為這是可信的預期報酬率。
    Sharpe/Calmar比率依賴同一份年化外推，可靠度標記同樣適用，不是獨立算出來就比較可信。
    修正：mode參數讓呼叫端可以指定只看"paper"或"real"——模擬跟真實帳戶的資金規模通常完全不同
    (模擬預設1000萬，真實帳戶可能小很多)，equity_curve如果不分開看，混在一起算「歷史最高點」
    跟「回撤」會變得沒有意義(隨時可能因為兩種模式的資金規模差異，製造出跟實際績效無關的假回撤)。
    不指定mode時(None)用全部資料，僅供「還沒有任何真實/模擬切換經驗」的舊資料相容，新的呼叫端
    應該明確指定要看哪一種。"""
    curve=auto_state.get("equity_curve",[])
    if mode is not None:
        curve=[c for c in curve if c.get("mode")==mode]
    if len(curve)<2:
        return {"available":False,"mode":mode,"reason":"權益曲線資料不足(至少需要2個交易日才能算出任何報酬率)",
                "trading_days":len(curve)}
    equities=[c["equity"] for c in curve]
    start_equity=equities[0]; end_equity=equities[-1]
    n_days=len(curve)
    if start_equity<=0:
        return {"available":False,"mode":mode,"reason":"起始權益為0或負數，無法計算報酬率","trading_days":n_days}
    total_return_pct=(end_equity/start_equity-1)*100
    try:
        annualized_return_pct=((end_equity/start_equity)**(252/n_days)-1)*100
    except (OverflowError,ZeroDivisionError):
        annualized_return_pct=None
    # 最大回撤：從權益曲線逐筆掃過去，追蹤「目前為止的歷史新高」，每個點都跟這個新高比，
    # 記錄過程中出現過的最大跌幅(不是只看頭尾兩點，要看過程中真正最深的那一次)
    peak=equities[0]; peak_date=curve[0]["date"]
    max_dd=0.0; worst_peak_date=peak_date; worst_trough_date=peak_date
    for c in curve:
        e=c["equity"]
        if e>peak: peak=e; peak_date=c["date"]
        dd=(peak-e)/peak*100 if peak>0 else 0.0
        if dd>max_dd: max_dd=dd; worst_peak_date=peak_date; worst_trough_date=c["date"]
    # Sharpe比率：用每日報酬率(不是每日損益金額)的平均值/標準差，年化用sqrt(252)——
    # 標準差至少要兩個報酬率才算得出意義(n_days>=3才有2個日報酬率)，無風險利率簡化設為0
    # (短線當沖策略的波動規模遠大於無風險利率，這個簡化對結果影響很小)。
    daily_returns=[]
    for i in range(1,len(equities)):
        if equities[i-1]>0:
            daily_returns.append((equities[i]-equities[i-1])/equities[i-1])
    sharpe_ratio=None
    if len(daily_returns)>=2:
        mean_r=sum(daily_returns)/len(daily_returns)
        var_r=sum((r-mean_r)**2 for r in daily_returns)/(len(daily_returns)-1)
        std_r=var_r**0.5
        if std_r>0:
            sharpe_ratio=round(mean_r/std_r*(252**0.5),2)
    # Calmar比率：年化報酬率/最大回撤，衡量「每承受1%回撤風險，換到多少年化報酬」，
    # 業界沒有Sharpe那麼通用，但對回撤風險的敏感度比Sharpe更直接(Sharpe用標準差，不分上漲下跌波動)
    calmar_ratio=None
    if annualized_return_pct is not None and max_dd>0:
        calmar_ratio=round(annualized_return_pct/max_dd,2)
    is_reliable=n_days>=ANNUALIZED_RELIABLE_MIN_DAYS
    return {
        "available":True,"trading_days":n_days,"mode":mode,
        "start_equity":round(start_equity,2),"end_equity":round(end_equity,2),
        "total_return_pct":round(total_return_pct,3),
        "annualized_return_pct":round(annualized_return_pct,2) if annualized_return_pct is not None else None,
        "max_drawdown_pct":round(max_dd,3),
        "max_drawdown_peak_date":worst_peak_date,"max_drawdown_trough_date":worst_trough_date,
        "sharpe_ratio":sharpe_ratio,"calmar_ratio":calmar_ratio,
        "is_annualized_reliable":is_reliable,
        "reliability_note":None if is_reliable else
            f"只有{n_days}個交易日的資料，年化報酬率/Sharpe/Calmar比率用複利外推會被單一好/壞日子放大到失真，"
            f"建議累積到至少{ANNUALIZED_RELIABLE_MIN_DAYS}個交易日後才認真參考這些數字",
    }

# ══════════════════════════════════════════════════════════════════
# 看門狗：Shioaji官方自己的參考交易終端機文件寫得很清楚——
# 「停損/停利為客戶端觸價單，只在頁面開啟時監控」，沒有真正的券商/交易所端OCO或觸價單給股票用。
# 也就是說，這支程式現在做的事(伺服器端輪詢價格、碰到條件就送出真實委託)就是業界唯一的標準做法，
# 不是少了什麼「雲端智慧單」可以接。真正的風險不是「沒有券商端保護」，是「萬一我自己這個監控
# 程式卡住/斷線了，停損停利就不會被執行」——這個才是看門狗該解決的問題。
# ══════════════════════════════════════════════════════════════════
_watchdog_state={"last_tick_at":time.time(),"alerted":False}
WATCHDOG_THRESHOLD_SECONDS=90  # 連續3次30秒心跳沒更新(=90秒)就視為主迴圈可能卡住

def watchdog_check():
    """獨立排程，每30秒檢查一次auto_trade_tick有沒有按時跳動。只負責「警示」，不自動砍倉——
    用時間落差判斷「主迴圈是不是卡住」本身就有誤判風險(網路一次延遲就可能誤觸發)，
    強制砍倉這種有實際後果的動作，留給人看到警示後自己判斷，不要讓看門狗自己先動手。"""
    if not auto_state["enabled"] or not is_trading_time():
        _watchdog_state["alerted"]=False
        return
    gap=time.time()-_watchdog_state["last_tick_at"]
    if gap>WATCHDOG_THRESHOLD_SECONDS:
        if not _watchdog_state["alerted"]:
            _log(f"⚠️⚠️⚠️ [看門狗警示] 主交易迴圈已經{gap:.0f}秒沒有心跳(正常應該30秒一次)，"
                 f"可能已經停止監控停損停利！目前{'有' if auto_state['positions'] else '沒有'}持倉，"
                 f"請立刻檢查伺服器狀態，必要時手動到永豐app/網頁平倉。")
            _watchdog_state["alerted"]=True
    else:
        _watchdog_state["alerted"]=False


POSITION_RECONCILE_GRACE_PERIOD_SECONDS = 90  # 開倉後至少等90秒才核對，給永豐端的持倉回報一點緩衝時間，避免單純的回報延遲被誤判成沒成交

def reconcile_real_positions():
    """真實模式下的第二道防線：_place_real_order()只能確認委託『沒有被立即拒絕』，沒辦法確認
    委託後來是不是真的成交(可能送出後才被取消/部分成交/其他原因失敗)。這裡定期拿永豐回報的
    『真實』持倉，跟系統自己以為持有的清單核對，抓出帳本跟真實帳戶脫鉤的情況。
    完整的做法應該是註冊on_order callback或對每筆委託呼叫update_status()盯著後續狀態變化，
    這裡先用『定期跟真實持倉核對』這種更簡單、不依賴追蹤個別委託ID的方式頂著——
    抓不到「為什麼」不一致，但至少能抓到「有沒有」不一致。
    重要：確認是幻影持倉後直接從追蹤清單移除，不是放著不管——如果留著，正常出場邏輯之後會
    嘗試賣出根本不存在的持倉，而賣出委託失敗會被_place_real_order的另一個防線擋下、留在清單裡
    等下次重試，等於陷入永遠賣不掉一筆根本不存在的股票的無限重試迴圈。"""
    if auto_state.get("paper_mode") or not sinopac_api or not auto_state["positions"]:
        return
    try:
        try: real_positions=sinopac_api.list_positions(stock_account,unit=sj.constant.Unit.Share)
        except AttributeError:
            try: real_positions=sinopac_api.list_positions(stock_account)
            except: real_positions=sinopac_api.list_positions()
    except Exception as e:
        logger.warning(f"持倉核對：抓取真實持倉失敗，跳過這次核對: {e}")
        return
    real_shares={}
    for p in (real_positions or []):
        try:
            code=getattr(p,"code",None) or getattr(p,"symbol","")
            real_shares[code]=real_shares.get(code,0)+int(getattr(p,"quantity",0))
        except Exception:
            continue
    now=time.time()
    suspect=[pos for pos in auto_state["positions"]
             if now-pos.get("opened_at",now)>=POSITION_RECONCILE_GRACE_PERIOD_SECONDS
             and real_shares.get(pos["sym"],0)<pos["qty"]*1000]
    if suspect:
        for pos in suspect:
            _log(f"[持倉核對異常]系統記錄持有{pos['qty']}張，但永豐真實持倉查不到對應數量，"
                 f"這筆委託可能後來沒有真的成交，已從追蹤清單移除，請自行確認永豐帳戶實際狀況",pos["sym"])
        auto_state["positions"]=[p for p in auto_state["positions"] if p not in suspect]
        fn=auto_state["funnel"]; fn["order_failed"]=fn.get("order_failed",0)+len(suspect)
        _persist_auto_state()

def _get_real_held_symbols() -> set:
    """真實模式專用：查詢真實帳戶目前實際持有(不論零股或整股、不論是bot買的還是使用者自己手動買的)
    的股票代號集合。用在開新倉前排除掉這些股票，避免bot完全不知道使用者帳戶裡已經有這檔的
    手動持倉(例如零股)，疊加買進整張造成超出預期的集中風險——bot自己的auto_state["positions"]
    只記錄它自己開的倉，對使用者在同一個真實帳戶裡自行操作的持倉完全沒有概念，這個函式補上這一層。"""
    if not sinopac_api: return set()
    try:
        try: positions=sinopac_api.list_positions(stock_account,unit=sj.constant.Unit.Share)
        except AttributeError:
            try: positions=sinopac_api.list_positions(stock_account)
            except: positions=sinopac_api.list_positions()
        held=set()
        for p in (positions or []):
            try:
                if int(getattr(p,"quantity",0))>0:
                    held.add(getattr(p,"code",None) or getattr(p,"symbol",""))
            except Exception:
                continue
        return held
    except Exception as e:
        logger.warning(f"查詢真實持倉(用於避免疊加手動持倉)失敗，這次tick會跳過這層保護: {e}")
        return set()


_last_cum_volume: Dict[str,int] = {}  # 修正：追蹤每檔股票上次輪詢時的累計成交量，用來換算成「這次輪詢期間」的增量
MAX_BARS = 120
# (SCAN_UNIVERSE/SECTOR_MAP 已從config.py匯入)

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
        return _parse_snapshot(s[0])
    except: return None

def _parse_snapshot(snap) -> Dict:
    tt=getattr(snap,"tick_type",None)
    tt_val = (1 if str(tt).endswith("Buy") else 2 if str(tt).endswith("Sell") else 0) if tt is not None else 0
    return {
        "price":float(snap.close),
        "volume":int(getattr(snap,"total_volume",0) or 0),
        "buy_vol":float(getattr(snap,"buy_volume",0) or 0),
        "sell_vol":float(getattr(snap,"sell_volume",0) or 0),
        "tick_type":tt_val,  # 1=外盤(買進成交) 2=內盤(賣出成交) 0=無法判定
    }

def _snapshot_batch(syms) -> Dict[str,Dict]:
    """一次幫一批股票打一個snapshots() API，而不是每檔股票各打一次——Shioaji原生支援
    傳入一個contracts list、一次回傳全部結果(用.code欄位對應回是哪一檔，不依賴順序)，
    47檔股票原本要47次來回，現在1次打完。回傳{symbol: 解析後的dict}，抓不到/解析失敗的symbol不會出現在結果裡，
    呼叫端要用.get(sym)處理缺漏，跟原本_snapshot()回傳None時的處理方式一致。"""
    if not sinopac_api or not syms: return {}
    out={}
    try:
        contracts=[]; sym_by_code={}
        for sym in syms:
            c=sinopac_api.Contracts.Stocks.get(sym)
            if c:
                contracts.append(c)
                sym_by_code[getattr(c,"code",sym)]=sym
        if not contracts: return {}
        snaps=sinopac_api.snapshots(contracts)
        for snap in (snaps or []):
            code=getattr(snap,"code",None)
            sym=sym_by_code.get(code)
            if sym is None: continue
            try: out[sym]=_parse_snapshot(snap)
            except: continue
    except Exception as e:
        logger.warning(f"批次抓取快照失敗，這個tick的價格更新會跳過: {e}")
    return out

def _update_cache():
    # 模擬下單模式：除了使用者自選清單，也持續更新40檔股票池的價格快取，
    # 因為模擬模式下AI可以從這個更大的股票池找當沖機會（真實下單仍只看使用者自選清單，較保守）
    symbols = set(auto_state["watchlist"])
    if auto_state.get("paper_mode"):
        symbols |= set(SCAN_UNIVERSE)
    # 大盤同步性特徵需要的市場指標(0050)：不管使用者自選清單或paper_mode設定如何，一律持續追蹤價格，
    # 跟OFI訂閱一樣是核心基礎設施，不該因為使用者改了自選股清單就斷掉——這支不會被當成交易標的
    # (是否可交易仍然只看candidates_pool，跟這裡的價格追蹤是分開的兩件事)。
    symbols.add(MARKET_INDEX_SYMBOL)
    # 修正：原本每檔股票各自呼叫一次_snapshot()單獨打API，paper_mode下symbols可能高達47檔，
    # 等於每30秒的tick都要做47次序列化的網路來回——改成一次批次打完，大幅減少tick的延遲跟
    # 對Shioaji行情API的呼叫次數(雖然5秒500次的額度本來就夠用，但能省則省，也降低單檔逾時拖累整批的風險)。
    snaps=_snapshot_batch(symbols)
    for sym in symbols:
        snap=snaps.get(sym)
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
            "_loss_stop_logged":False,"_profit_lock_logged":False,"_afford_warn_logged":False,"_zero_capital_logged":False,"trade_history":[],
            "funnel":{"scanned":0,"bad_time":0,"no_trade_zone":0,"not_eligible":0,"limit_down_risk":0,
                      "vwap_reject":0,"wide_spread":0,"extreme_recent_move":0,"no_model":0,"low_confidence":0,"sector_dup":0,"unaffordable":0,
                      "daily_cap_reached":0,"per_tick_cap_reached":0,"opened":0,"order_failed":0}})
        _prune_settled_paper_pending()
        _log("每日計數器重置 ✓")

_order_events: Dict[str,dict] = {}  # {order_id: {"op_type":str,"op_code":str,"op_msg":str,"received_at":float,"raw_state":str}}

def _on_order_callback(stat, msg):
    """Shioaji委託/成交事件callback——重要：這個函式是Shioaji自己管理的背景執行緒在呼叫，
    不是主交易迴圈(APScheduler BackgroundScheduler)的執行緒。故意設計成只做一件最單純的事：
    把事件存進_order_events這個dict，不在這裡直接修改auto_state["positions"]——因為兩個不同
    執行緒同時讀寫同一份共用可變狀態(auto_state)有競爭風險，真正的判斷跟移除動作交給主迴圈
    自己的執行緒處理(見_apply_order_events)，這裡單純記錄、不做決策，把單一dict key賦值這種
    GIL底下相對安全的操作留給callback，複合的讀-改-寫操作留給主執行緒。
    委託狀態(StockOrder/TFTOrder事件)的失敗/取消資訊在msg["operation"]["op_type"]/["op_code"]裡，
    不是在stat(OrderState enum)的名稱本身——這個結構是照Shioaji官方文件的範例payload寫的，
    但沒有真實委託流量驗證過，部署後第一筆真實單建議對照log確認欄位真的長這樣。"""
    try:
        if not isinstance(msg,dict): return
        order_id=None
        op_type=op_code=op_msg=""
        if "order" in msg:  # StockOrder/TFTOrder事件：委託狀態變化(New/Cancel/UpdateQty等)
            order_id=str((msg.get("order") or {}).get("id") or "")
            op=msg.get("operation") or {}
            op_type=str(op.get("op_type") or ""); op_code=str(op.get("op_code") or ""); op_msg=str(op.get("op_msg") or "")
        elif "trade_id" in msg:  # StockDeal/TFTDeal事件：實際成交回報
            order_id=str(msg.get("trade_id") or "")
            op_type="Deal"
        if order_id:
            _order_events[order_id]={"op_type":op_type,"op_code":op_code,"op_msg":op_msg,
                                      "received_at":time.time(),"raw_state":str(stat)}
    except Exception as e:
        logger.warning(f"委託回報callback處理失敗(不影響交易繼續執行，reconcile_real_positions仍是最後防線): {e}")

def _apply_order_events():
    """主交易迴圈呼叫(在auto_trade_tick裡)，把_order_events裡『委託被取消/失敗』的事件套用到
    auto_state["positions"]——跟reconcile_real_positions()是互補關係，不是取代：這裡是『有收到
    明確的取消/失敗回報』時的快速反應(數秒內)，reconcile_real_positions()是『不管有沒有收到
    回報，每2分鐘定期核對真實持倉』的最後一道防線，防範這個callback本身漏接事件
    (例如websocket斷線重連那段空窗期回報會收不到)。"""
    if not auto_state["positions"]: return
    removed=[]
    for pos in list(auto_state["positions"]):
        oid=pos.get("order_id")
        if not oid: continue
        evt=_order_events.get(oid)
        if not evt: continue
        is_cancelled=evt["op_type"]=="Cancel"
        is_failed=bool(evt["op_code"]) and evt["op_code"]!="00" and evt["op_type"]!="Deal"
        if is_cancelled or is_failed:
            _log(f"[委託回報]收到委託{'取消' if is_cancelled else '失敗'}通知"
                 f"(op_code={evt['op_code']},{evt['op_msg']})，移除這筆持倉追蹤",pos["sym"])
            removed.append(pos)
    if removed:
        auto_state["positions"]=[p for p in auto_state["positions"] if p not in removed]
        fn=auto_state["funnel"]; fn["order_failed"]=fn.get("order_failed",0)+len(removed)
        _persist_auto_state()

def _place_real_order(sym:str, action, qty:int) -> Tuple[bool,Optional[str]]:
    """回傳(成功與否, order_id)。成功只代表「委託至少沒有立即被拒絕」，不代表「已經真的成交」——
    完整的成交確認交給on_order callback(_on_order_callback)非同步處理，這裡回傳的order_id就是
    用來讓callback事件進來時，可以對應回是哪一筆持倉的關鍵(存進pos["order_id"])。
    模擬模式/連線失敗/找不到合約的情況沒有真正的order_id，回傳None，呼叫端不能假設一定拿得到。"""
    if auto_state.get("paper_mode"):
        _log(f"[模擬]未送出真實委託 {sym} {action} {qty}張",sym)
        return True,None
    if not sinopac_api or not stock_account: return False,None
    try:
        c=sinopac_api.Contracts.Stocks.get(sym)
        if not c: return False,None
        o=sinopac_api.Order(price=0,quantity=qty,action=action,
            price_type=sj.constant.StockPriceType.MKT,
            order_type=sj.constant.OrderType.ROD,account=stock_account)
        trade=sinopac_api.place_order(c,o)
        status=str(getattr(getattr(trade,"status",None),"status",""))
        order_id=str(getattr(getattr(trade,"order",None),"id","")) or None
        if "fail" in status.lower():
            logger.warning(f"委託被立即拒絕 {sym}: status={status}")
            return False,order_id
        return True,order_id
    except Exception as e:
        logger.warning(f"委託失敗 {sym}: {e}")
        return False,None

def auto_trade_tick():
    """每30秒執行 — 核心自動交易"""
    _watchdog_state["last_tick_at"]=time.time()  # 安全機制：記錄心跳，下面不管哪個分支return都已經記過了
    if not auto_state["enabled"] or not sinopac_api or not is_trading_time(): return
    _reset_daily()
    _update_cache()
    _apply_order_events()  # 處理委託回報callback記錄到的取消/失敗事件，盡快反應(不用等reconcile_real_positions的2分鐘週期)
    if auto_state["pause_until"]>time.time(): return
    if auto_state["capital"]<=0:
        # 修正：可用資金被待交割款吃光時capital會是0(這是真實狀況，不是bug，見main.py的_refresh_capital_from_account)，
        # 這裡要明確擋下來當成「今天沒錢可動用，暫停」處理——不只是防呆，0元帳戶本來就不該繼續嘗試任何新倉位計算，
        # 而且下面daily_pnl/capital這個除法，capital=0會直接讓整個tick crash，每30秒crash一次，必須在這裡先擋掉。
        if not auto_state.get("_zero_capital_logged"):
            _log("[暫停]目前可用資金為0(待交割款佔用)，暫停今日交易直到資金恢復"); auto_state["_zero_capital_logged"]=True
        return
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
      try:
        sym=pos["sym"]
        # 修正：這裡原本每筆持倉都重新呼叫一次_snapshot(sym)單獨打API，但同一個tick裡
        # _update_cache()早幾行就已經幫所有自選股/股票池(持倉幾乎一定包含在內)抓過一次新鮮報價了，
        # 等於同一個symbol在同一個30秒週期內打兩次一樣的API——改成直接讀price_cache最新一筆，
        # 完全不犧牲新鮮度(中間只隔幾行純運算，沒有任何I/O)，省掉重複的網路來回。
        cached=price_cache.get(sym)
        snap=({"price":cached[-1]["price"]} if cached else None) or _snapshot(sym)
        if not snap: continue
        p=snap["price"]
        pp=(p-pos["entry"])/pos["entry"]*100*(1 if pos["dir"]=="L" else -1)
        held_min=(time.time()-pos.get("opened_at",time.time()))/60
        # 修正①：原本只要求 pp>-0.1（沒有明顯虧損）就會被超時平倉，
        # 但完全沒檢查漲跌幅有沒有蓋過來回手續費+當沖證交稅（min_profitable_move_pct，約0.32%），
        # 導致價格幾乎沒動也會被「持倉超時」強制出場，穩定倒貼一次完整成本（例：原價買賣的台積電單，0%價差卻虧NT$8,089手續費）。
        # 改成要求至少要爬到「扣成本後打平」的幅度才放它走；沒到的話繼續抱著，交給停損/止盈/反轉訊號決定，
        # 最晚 13:20 還有 force_close_all 強制收尾，不會有隔夜風險。
        min_move=min_profitable_move_pct(auto_state.get("fee_discount",FEE_DISCOUNT))
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
            close_act=sj.constant.Action.Sell if pos["dir"]=="L" else sj.constant.Action.Buy
            close_ok,_close_oid=_place_real_order(sym,close_act,pos["qty"])
            if not close_ok:
                # 修正：平倉委託被拒絕時絕對不能繼續記錄成「已經平倉」——這比開倉那邊的同類問題更危險，
                # 因為這樣會讓系統以為這筆持倉已經結束、不再監控，但永豐那邊這筆其實還真實開著，
                # 完全沒人在看著它的停損停利。不往下走帳本邏輯，留在positions裡，下個tick再試一次平倉。
                auto_state["funnel"]["order_failed"]=auto_state["funnel"].get("order_failed",0)+1
                _log(f"[委託失敗]{sym} 平倉委託被拒絕或送出失敗，持倉保留，下次tick重試",sym)
                continue
            # 計算真實淨損益（扣除手續費+當沖證交稅）
            # 重要修正：pos["qty"] 是「張」數（1張=1000股），所有金額計算必須換算成股數，
            # 否則損益會被低估1000倍，導致每日虧損/獲利風控門檻形同虛設
            shares = pos["qty"] * 1000
            fill_p=_apply_slippage(p,is_buy=False)  # 模擬滑價：賣出假設要少收1個tick(觸發出場後的真實成交價，不是觸發判斷本身)
            entry_p,exit_p = (pos["entry"],fill_p) if pos["dir"]=="L" else (fill_p,pos["entry"])
            cost=calc_round_trip_cost(entry_p,exit_p,shares,auto_state.get("fee_discount",FEE_DISCOUNT))
            gross=(fill_p-pos["entry"])*shares*(1 if pos["dir"]=="L" else -1)
            net_pnl=gross-cost["total_cost"]
            settlement_date=None
            if auto_state.get("paper_mode") and pos["dir"]=="L":
                settlement_date=_record_paper_close(fill_p,shares,tw_now().strftime("%Y-%m-%d"))
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
            _log(f"{tag} {pos['dir']} @{fill_p:.2f} 淨損益${net_pnl:.0f}(毛利${gross:.0f}-成本${cost['total_cost']:.0f})",sym)
            auto_state["trade_history"].insert(0,{
                "sym":sym,"dir":pos["dir"],"qty":pos["qty"],"shares":shares,
                "entry":pos["entry"],"exit":round(fill_p,2),
                "total_cost_basis":round(pos["entry"]*shares,0),  # 進場總成本（股數×進場價）
                "gross_pnl":round(gross,0),"fees":round(cost["total_cost"],0),
                "pnl":round(net_pnl,0),"pct":round(pp,2),"tag":tag,
                "open_time":pos.get("open_time",""),"close_time":tw_now().strftime("%H:%M:%S"),
                "settlement_date":settlement_date,
                "from_pool": sym not in auto_state["watchlist"],
                "grade":pos.get("grade","-"),"regime":pos.get("regime","-"),
                "entry_reason":pos.get("entry_reason","-"),"exit_reason":exit_reason,
            })
            auto_state["trade_history"]=auto_state["trade_history"][:50]
            if auto_state.get("paper_mode"):
                _record_paper_validation(net_pnl,pp,held_min)
                _record_feature_log(sym,pos,net_pnl,pp,tag)
            # 漲跌停鎖死風險提示（不對稱）：做空遇漲停鎖死是真正危險（違約交割+借券費），
            # 做多遇跌停沖不掉則只是變成一般T+2持股，風險輕微，用不同等級的提示
            cinfo=get_contract_info(sym)
            if pos["dir"]=="S" and cinfo["limit_up"]>0 and p>=cinfo["limit_up"]*0.998:
                _log(f"[高風險] {sym} 接近/觸及漲停，做空回補可能掛不掉，可能產生借券費用與違約交割風險",sym)
            if pos["dir"]=="L" and cinfo["limit_down"]>0 and p<=cinfo["limit_down"]*1.002:
                _log(f"[提示] {sym} 接近/觸及跌停，今日可能無法賣出，將自動變成一般持股待T+2交割（非違約風險）",sym)
            to_close.append(sym)
      except Exception as e:
        # 防禦性隔離：跟下面候選評估迴圈一樣的道理，但這裡更關鍵——一檔持倉檢查時crash，
        # 不能連帶讓其他持倉的停損停利都沒被檢查到，那樣等於本來該出場的單子可能一直卡著不出場。
        logger.warning(f"檢查持倉{pos.get('sym','?')}出場條件時發生例外，跳過這筆繼續檢查其他持倉: {e}")
        continue
    auto_state["positions"]=[p for p in auto_state["positions"] if p["sym"] not in to_close]
    # 開倉（僅在09:30-13:00主動交易時段，避開開盤亂流與尾盤風險）
    if not can_open_new_position() or _check_drawdown_circuit_breaker():
        existing=set()
        candidates_pool=[]
    else:
        existing={p["sym"] for p in auto_state["positions"]}
        if not auto_state.get("paper_mode"):
            # 真實模式：額外排除「真實帳戶裡已經有持倉」的股票，不管是bot自己買的還是使用者自己
            # 手動操作的(包括零股)——bot自己的清單對使用者手動持倉完全沒有概念，這層保護避免
            # 在使用者已經手動持有某檔股票時，bot又買進整張疊加上去，造成超出預期的集中風險。
            existing |= _get_real_held_symbols()
        # 模擬下單：從40檔股票池找機會；真實下單：僅限使用者自選清單（較保守，避免真錢自動擴大選股範圍）
        candidates_pool=list(set(auto_state["watchlist"]) | set(SCAN_UNIVERSE)) if auto_state.get("paper_mode") else list(auto_state["watchlist"])

    # 先逐一檢查資格、算出每檔的「AI預估利潤分數」，蒐集成候選清單
    candidates=[]
    fn=auto_state["funnel"]
    # 效能：大盤同步性跟個股無關，這個tick只要算一次，不要讓底下每檔股票各自重算一次一樣的結果
    market_ctx=get_market_context()
    # 模擬模式用獨立的模擬資金算部位大小，不要被真實帳戶的T+2交割卡住；真實模式才用真實capital。
    effective_capital=get_paper_available_capital() if auto_state.get("paper_mode") else auto_state["capital"]
    for sym in candidates_pool:
        try:
            if sym in existing: continue
            hist=price_cache.get(sym,[])
            if len(hist)<30: continue
            fn["scanned"]+=1
            prices_h=[h["price"] for h in hist]; vols_h=[h["volume"] for h in hist]
            # 修正⑩：進場判斷主力換成LightGBM模型——sig還是要算，但只用bad_time(開盤前30分/收盤前)
            # 這個時間風控欄位，不再用sig['conf']/sig['action']決定要不要進場(那是被取代的"12項指標綜合評分")
            sig=calc_signal_py(prices_h,vols_h)
            if sig.get("bad_time"): fn["bad_time"]+=1; continue
            dir_="L"  # 規格要求：只看LightGBM「做多勝率」，當沖只做多單
            # 禁止交易區：國定假日/連續假日前收盤前/月結算日/量過低/波動度過低，這些情況下不進場(維持獨立硬性檢查)
            ntz,ntz_reason=is_no_trade_zone(prices_h,vols_h)
            if ntz: fn["no_trade_zone"]+=1; continue
            # 真實當沖資格檢查（用永豐合約真實資料：day_trade=No的處置股票/不符資格股票直接跳過，維持獨立硬性檢查）
            ok,reason = can_day_trade(sym, is_sell_first=False)
            if not ok: fn["not_eligible"]+=1; continue
            cinfo=get_contract_info(sym)
            p_now=hist[-1]["price"]
            if cinfo["limit_down"]>0 and p_now<=cinfo["limit_down"]*1.005: fn["limit_down_risk"]+=1; continue  # 接近跌停，做多賣不掉風險高
            # 瞬間價格穩定機制(集合競價鎖死)風險的保守代理指標：台股規定若成交價超過前5分鐘滾動
            # 加權均價±3.5%，會強制進入2分鐘集合競價(暫緩撮合)。這裡沒有逐筆資料能驗證精確複製
            # 交易所的加權均價公式對不對，寧可保守一點不假裝精確——改用「最近5分鐘漲跌幅度」當代理：
            # 已經在這麼短時間內動這麼大，不管有沒有真的觸發這個機制，追上去都不是好時機(可能買進後
            # 剛好鎖住2分鐘沒辦法成交，停損停利也一起卡住，沒有任何即時保護)。
            target_ts=hist[-1]["t"]-300  # 5分鐘前的目標時間點
            ref_entry=min(hist,key=lambda h:abs(h["t"]-target_ts))
            # 安全閥：只有真的找到「接近5分鐘前」的資料點才比對，避免今天剛開盤、5分鐘的歷史
            # 還不存在時，誤抓到昨天收盤價當參考點，把正常的隔夜跳空誤判成「短時間內暴漲暴跌」
            if abs(ref_entry["t"]-target_ts)<=60 and ref_entry["price"]>0:
                recent_move_pct=abs(p_now-ref_entry["price"])/ref_entry["price"]*100
                if recent_move_pct>=3.0:
                    fn["extreme_recent_move"]=fn.get("extreme_recent_move",0)+1; continue
            # 還是要算advanced_score：①VWAP硬否決規格明確要求保留 ②regime/orb/volume_quality等輸出
            # 現在角色變成「餵給LightGBM的特徵」+「事後顯示用的輔助資訊」，不再是獨立的進場門檻
            adv=advanced_score(sym,prices_h,vols_h,dir_)
            if not adv["vwap_ok"]: fn["vwap_reject"]+=1; continue  # 規格明確要求保留的VWAP硬否決
            # 買賣價差過濾：價差太寬，光是進場那一下就先吃掉一截停損空間，當沖薄利策略對這個特別敏感。
            # 修正：原本固定0.3%門檻，驗證時發現NT$12~16的便宜銀行股(2849/2836/2834，剛好是這個系統
            # 特地加入解決資金不足問題的那幾檔)，連最小可能的1個tick價差都已經超過0.3%，等於不管
            # 流動性多好，這幾檔永遠被擋下來。改成依股價換算tick大小再設門檻(max_allowed_spread_pct)，
            # 不同價位的股票才能公平比較。還沒訂閱到bidask資料時回傳None，這時候選擇不卡(寧可讓
            # LightGBM評分繼續判斷)，避免訂閱還沒就位時就把全部候選都擋掉。
            spread_pct=get_latest_spread_pct(sym)
            if spread_pct is not None and spread_pct>max_allowed_spread_pct(p_now):
                fn["wide_spread"]=fn.get("wide_spread",0)+1; continue
            # LightGBM做多勝率信心度——取代原本的12項指標評分跟進階評分(C級)門檻
            lgbm_conf,lgbm_feats=predict_lgbm_confidence(sym,prices_h,vols_h,dir_,market_ctx,spread_pct)
            if lgbm_conf is None:
                fn["no_model"]+=1
                continue  # 模型還沒訓練/載入失敗：誠實地不交易，不要悄悄退回舊邏輯製造「好像在運作」的假象
            if lgbm_conf<cfg["min_conf"]: fn["low_confidence"]+=1; continue  # 沿用既有風險等級門檻(低72%/中68%/高65%)，只是現在門檻比的是模型機率
            est_profit_score=lgbm_conf*cfg["tp"]
            candidates.append({"sym":sym,"sig":sig,"dir":dir_,"price":p_now,"est_profit_score":est_profit_score,
                                "adv":adv,"lgbm_conf":lgbm_conf,"lgbm_feats":lgbm_feats})
        except Exception as e:
            # 防禦性隔離：單一股票的資料異常(任何原因，不只是已知的零成交量這種)絕對不能讓整個tick
            # 連帶評估失敗——少了這層保護，一檔有問題的股票會讓當次30秒週期裡其他45檔全部陪著一起跳過，
            # 而且看門狗的心跳是在函式最前面就先記過了，不會因為這裡crash而被偵測到「卡住」，
            # 等於這種錯誤可能默默重複發生好幾天都不會被任何警示機制抓到。
            logger.warning(f"候選評估{sym}時發生例外，跳過這檔繼續處理其他股票: {e}")
            continue

    # 依LightGBM信心度排序(信心最高=模型最有把握)，同分才看AI預估利潤分數
    candidates.sort(key=lambda c:(c["lgbm_conf"],c["est_profit_score"]),reverse=True)
    held_sectors={SECTOR_MAP.get(p["sym"]) for p in auto_state["positions"] if SECTOR_MAP.get(p["sym"])}
    skipped_unaffordable=[]
    opened_this_tick=0
    # 風控新增：所有持倉合計曝險不得超過資金的30%，避免每筆都在per-trade budget之內、
    # 但同時開好幾筆疊加起來總曝險還是過大(高風險max_pos=8時，理論上8筆*20%alloc=160%，沒有總量上限的話會嚴重超額)
    total_exposure=sum(p["entry"]*p["qty"]*1000 for p in auto_state["positions"])
    MAX_AGGREGATE_EXPOSURE_PCT=30
    # 風控新增：單次30秒週期最多開2檔新倉，跟max_pos(總同時持倉上限)是不同的限制——
    # 如果同一個tick有3、5檔同時信心度達標(例如台指期突然暴衝)，全部一次性送單會放大滑價跟頻寬延遲，
    # 只取當下排序後最強的1~2檔進場，其餘留給下一個30秒週期重新評估，不是浪費機會，是控制單次衝擊。
    MAX_NEW_ENTRIES_PER_TICK=2
    for c in candidates:
        if len(auto_state["positions"])>=cfg["max_pos"]: break
        if auto_state["daily_trades"]>=MAX_TRADES_PER_DAY: fn["daily_cap_reached"]+=1; break  # 風控新增：當日交易次數上限，避免訊號反覆觸發過度交易
        if opened_this_tick>=MAX_NEW_ENTRIES_PER_TICK: fn["per_tick_cap_reached"]+=1; break
        sym,sig,dir_,p,adv,lgbm_conf,lgbm_feats=c["sym"],c["sig"],c["dir"],c["price"],c["adv"],c["lgbm_conf"],c["lgbm_feats"]
        # 板塊分散：同板塊已有持倉就跳過這個候選，避免集中壓在同一個產業（如同時押好幾家金融股）
        this_sector=SECTOR_MAP.get(sym)
        if this_sector and this_sector in held_sectors: fn["sector_dup"]+=1; continue
        # 修正②：原本用 max(1, …) 強制至少買1張，對高價股（如台積電2500+、健策3800+）來說，
        # 1張的曝險動輒幾百萬，遠超過風險設定要動用的資金（例：低風險alloc=5%，100K資金只想動用5,000元，
        # 卻被迫買下252萬元台積電，曝險是帳戶資金的25倍），導致小幅價格波動就被放大成鉅額虧損。
        # 改成：算出來的張數不到1張，就代表這檔股票對目前資金規模太貴，直接跳過、不強迫超額曝險。
        # 修正⑩：部位大小依LightGBM信心度分級加碼(S/A/B/C=100%/75%/50%/25%)，取代原本的進階評分分級。
        # 模組二：LightGBM機率動態部位縮放（連續線性比例），取代原本的離散分級加碼
        alloc_ratio=calculate_dynamic_position_ratio(lgbm_conf,cfg["min_conf"],cfg["alloc"]*100)
        budget=effective_capital*(auto_state["cap_pct"]/100)*alloc_ratio
        max_aggregate=effective_capital*MAX_AGGREGATE_EXPOSURE_PCT/100
        budget=min(budget,max(0,max_aggregate-total_exposure))  # 受總曝險上限約束
        qty=int(budget/(p*1000))
        if qty<1:
            fn["unaffordable"]+=1
            skipped_unaffordable.append((sym,p))
            continue
        grade=lgbm_grade(lgbm_conf)
        fill_p=_apply_slippage(p,is_buy=True)  # 模擬滑價：當沖只做多，買進假設要多付1個tick才成交
        pool_tag="[股票池]" if (sym not in auto_state["watchlist"]) else ""
        act=sj.constant.Action.Buy if dir_=="L" else sj.constant.Action.Sell
        order_ok,order_id=_place_real_order(sym,act,qty)
        if not order_ok:
            # 修正：委託被立即拒絕時，絕對不能繼續往下記錄成持倉/扣模擬資金——
            # 不然會變成系統以為自己買到了，但永豐那邊根本沒有這筆交易，帳本直接對不起來。
            fn["order_failed"]=fn.get("order_failed",0)+1
            _log(f"[委託失敗]{sym} 買進委託被拒絕或送出失敗，不記錄為持倉",sym)
            continue
        if auto_state.get("paper_mode"): _record_paper_open(fill_p,qty*1000)
        auto_state["positions"].append({
            "sym":sym,"dir":dir_,"qty":qty,"entry":fill_p,"order_id":order_id,
            "sl":round(fill_p*(1-cfg["sl"]/100) if dir_=="L" else fill_p*(1+cfg["sl"]/100),2),
            "tp":round(fill_p*(1+cfg["tp"]/100) if dir_=="L" else fill_p*(1-cfg["tp"]/100),2),
            "open_time":tw_now().strftime("%H:%M:%S"),
            "opened_at":time.time(),  # 數值時間戳，用於計算持倉分鐘數（短炒持倉時間上限判斷）
            "grade":grade,"regime":adv["regime"],"entry_reason":build_entry_reason(lgbm_conf,adv,dir_),
            "lgbm_conf":round(lgbm_conf,1),"features":dict(zip(ML_FEATURE_NAMES,lgbm_feats)) if lgbm_feats else {},
        })
        _log(f"{pool_tag}{'做多▲' if dir_=='L' else '做空▼'} {qty}張@{p:.2f} LightGBM信心{lgbm_conf:.0f}% "
             f"({grade}級) 市場環境:{adv['regime']}",sym)
        existing.add(sym)
        opened_this_tick+=1
        fn["opened"]+=1
        total_exposure+=p*qty*1000
        if this_sector: held_sectors.add(this_sector)
    # 透明度提示：修正②上線後，資金規模配不上整張交易（台股當沖只能整張，零股不開放當沖）時，
    # AI可能會整天都找不到「買得起」的標的而完全不下單——這不是bug，是修正後誠實反映風險，
    # 但每天只提示一次，讓使用者知道發生了什麼、該調整資金配置或股票池，而不是誤以為系統當機。
    if opened_this_tick==0 and skipped_unaffordable and not auto_state.get("_afford_warn_logged"):
        budget=effective_capital*(auto_state["cap_pct"]/100)*cfg["alloc"]
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
    still_open=[]
    for pos in list(auto_state["positions"]):
        sym=pos["sym"]
        act=sj.constant.Action.Sell if pos["dir"]=="L" else sj.constant.Action.Buy
        fc_ok,_fc_oid=_place_real_order(sym,act,pos["qty"])
        if not fc_ok:
            # 修正：強制平倉失敗(例如剛好跌停鎖死)是真正需要使用者注意的情況——已經是13:20了，
            # 不像一般時段還有很多次30秒tick可以重試，視窗快關了。不往下記錄成已平倉，
            # 保留在positions裡讓使用者在畫面上看得到「這筆沒有真的平倉成功」，需要去手動處理。
            fn=auto_state["funnel"]; fn["order_failed"]=fn.get("order_failed",0)+1
            _log(f"[強制平倉失敗]{sym} 委託被拒絕(可能漲跌停鎖死)，這筆會留倉過夜，請手動確認",sym)
            still_open.append(pos)
            continue
        try:
            hist=price_cache.get(sym,[])
            p=hist[-1]["price"] if hist else pos["entry"]  # 拿不到最新價時退回進場價，避免崩潰但盡量準確記錄
            shares=pos["qty"]*1000
            fill_p=_apply_slippage(p,is_buy=False)  # 強制平倉也是真實出場，一樣套用滑價假設
            gross=(fill_p-pos["entry"])*shares*(1 if pos["dir"]=="L" else -1)
            cost=calc_round_trip_cost(
                pos["entry"] if pos["dir"]=="L" else fill_p,
                fill_p if pos["dir"]=="L" else pos["entry"],
                shares,
                auto_state.get("fee_discount",FEE_DISCOUNT)
            )["total_cost"]
            net_pnl=gross-cost
            settlement_date=None
            if auto_state.get("paper_mode") and pos["dir"]=="L":
                settlement_date=_record_paper_close(fill_p,shares,tw_now().strftime("%Y-%m-%d"))
            auto_state["daily_pnl"]+=net_pnl; auto_state["daily_trades"]+=1
            if net_pnl>0: auto_state["daily_win"]+=1; auto_state["consec_loss"]=0
            else: auto_state["consec_loss"]+=1
            _log(f"[強制平倉] {pos['dir']} @{fill_p:.2f} 淨損益${net_pnl:.0f}",sym)
            pp_fc=(fill_p-pos["entry"])/pos["entry"]*100*(1 if pos["dir"]=="L" else -1)
            auto_state["trade_history"].insert(0,{
                "sym":sym,"dir":pos["dir"],"qty":pos["qty"],"shares":shares,
                "entry":pos["entry"],"exit":round(fill_p,2),
                "total_cost_basis":round(pos["entry"]*shares,0),
                "gross_pnl":round(gross,0),"fees":round(cost,0),
                "pnl":round(net_pnl,0),"pct":round(pp_fc,2),"tag":"強制平倉",
                "open_time":pos.get("open_time",""),"close_time":tw_now().strftime("%H:%M:%S"),
                "settlement_date":settlement_date,
                "from_pool": sym not in auto_state["watchlist"],
                "grade":pos.get("grade","-"),"regime":pos.get("regime","-"),
                "entry_reason":pos.get("entry_reason","-"),"exit_reason":"13:20當沖規定強制平倉，不留倉過夜",
            })
            auto_state["trade_history"]=auto_state["trade_history"][:50]
            if auto_state.get("paper_mode"):
                _record_paper_validation(net_pnl,pp_fc,(time.time()-pos.get("opened_at",time.time()))/60)
                _record_feature_log(sym,pos,net_pnl,pp_fc,"強制平倉")
        except Exception as e:
            logger.warning(f"強制平倉{sym}損益計算失敗（委託仍已送出成功）: {e}")
    auto_state["positions"]=still_open
    _persist_auto_state()

def post_market_summary():
    d=auto_state["daily_trades"] or 1
    wr=auto_state["daily_win"]/d*100
    today_trades=auto_state.get("trade_history",[])
    wins=[t for t in today_trades if t.get("pnl",0)>0]
    losses=[t for t in today_trades if t.get("pnl",0)<0]
    total_win=sum(t["pnl"] for t in wins); total_loss=sum(t["pnl"] for t in losses)
    pf=round(total_win/abs(total_loss),2) if total_loss!=0 else (float("inf") if total_win>0 else 0.0)
    avg_win=round(sum(t["pct"] for t in wins)/len(wins),2) if wins else 0.0
    avg_loss=round(sum(t["pct"] for t in losses)/len(losses),2) if losses else 0.0
    pf_str="∞" if pf==float("inf") else f"{pf:.2f}"
    _log(f"盤後總結 | P&L:${auto_state['daily_pnl']:.0f} | 勝率:{wr:.0f}%({auto_state['daily_win']}/{auto_state['daily_trades']}) "
         f"| 獲利因子:{pf_str}(>1.5算及格) | 平均贏{avg_win:+.2f}% 平均輸{avg_loss:+.2f}%")
    # 記錄今天收盤的權益曲線快照——不管今天有沒有交易都記，缺一天權益曲線就斷掉一截，
    # 之後算最大回撤/年化報酬率會失準。用全帳本價值(paper_capital/capital)，不是「扣除待交割款
    # 後的可用金額」，因為權益曲線要反映「帳戶實際總值」，不是「現在能不能馬上動用」這個流動性問題。
    today_str=tw_now().strftime("%Y-%m-%d")
    mode="paper" if auto_state.get("paper_mode") else "real"
    equity=auto_state.get("paper_capital",0.0) if mode=="paper" else auto_state.get("capital",0.0)
    curve=auto_state.setdefault("equity_curve",[])
    if not curve or curve[-1].get("date")!=today_str:
        curve.append({"date":today_str,"equity":round(equity,2),"mode":mode})
        auto_state["equity_curve"]=curve[-365:]  # 留最近365筆，避免無限增長(一年的交易日數綽綽有餘)
        _persist_auto_state()

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

def get_paper_available_capital() -> float:
    """模擬資金的「可用」金額=總帳(paper_capital)減去還在T+2交割中、還不能拿來開新倉的部分。
    跟真實帳戶的available_after_settlement是同一個概念，只是這裡的錢全部都是假的，
    只用來讓模擬模式的部位大小計算更貼近真實情況(剛平倉的錢不會馬上又能用)。"""
    today=tw_now().strftime("%Y-%m-%d")
    pending=sum(p["amount"] for p in auto_state["paper_pending_settlements"] if p["settle_date"]>today)
    return max(0.0,auto_state["paper_capital"]-pending)

def _prune_settled_paper_pending():
    """把已經過了交割日的紀錄從pending清單移除，避免清單無限長下去——
    這些錢已經算進paper_capital裡了(平倉時就立刻+=過)，這裡只是清掉「不再需要鎖住」的舊紀錄。"""
    today=tw_now().strftime("%Y-%m-%d")
    auto_state["paper_pending_settlements"]=[p for p in auto_state["paper_pending_settlements"] if p["settle_date"]>today]

def _apply_slippage(price:float, is_buy:bool) -> float:
    """模擬真實成交的滑價：用tick_size()算出這個價位的最小跳動單位，買進假設要多付1個tick才能成交
    (吃掉賣方掛單)，賣出假設要少收1個tick(被買方掛單吃掉)。原本模擬模式假設「完全用看到的報價成交」，
    等於零滑價的理想情況，會讓模擬績效系統性地比真實交易樂觀——現實中下單到成交之間有延遲，
    而且OFI訊號爆量時，其他人也看得到，最佳五檔的對手價很可能瞬間被掃掉一截。1個tick是
    相對保守、有實際根據的起點(不是衝著最壞情況的「地獄級」假設)，不是憑感覺挑的數字。"""
    tick=tick_size(price)
    return round(price+tick if is_buy else price-tick, 2)

def _record_paper_open(entry_price:float, shares:int):
    """模擬模式開倉：立刻從paper_capital扣掉成本+買進手續費(買進不像賣出有交割延遲，
    下單當下就需要這筆可動用資金，跟真實當沖券商的概念一致)。"""
    buy_fee=max(20,entry_price*shares*FEE_RATE*FEE_DISCOUNT)
    auto_state["paper_capital"]-=(entry_price*shares+buy_fee)

def _record_paper_close(exit_price:float, shares:int, close_date_str:str):
    """模擬模式平倉：賣出所得(扣掉賣出手續費+證交稅)立刻計入paper_capital的總帳(讓「總資產」
    隨時反映真實損益)，但同時登記一筆T+2交割中紀錄，這筆金額在交割完成前不算進「可用資金」，
    跟真實當沖規則一致——模擬資金要像真實交易一樣跑，不能讓使用者以為平倉的錢馬上能再拿來開倉。"""
    sell_fee=max(20,exit_price*shares*FEE_RATE*FEE_DISCOUNT)
    tax=exit_price*shares*DAYTRADE_TAX_RATE
    net_proceeds=exit_price*shares-sell_fee-tax
    auto_state["paper_capital"]+=net_proceeds
    settle_date=next_settlement_date(tw_now())
    auto_state["paper_pending_settlements"].append({"amount":net_proceeds,"settle_date":settle_date})
    return settle_date

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
        settlement_schedule = []  # [{"date":"YYYY-MM-DD","amount":float,"type":"payable"|"receivable"}, ...]
        try:
            setts = sinopac_api.settlements(stock_account)
            today_str = tw_now().strftime("%Y-%m-%d")
            for s in setts:
                t_date = str(getattr(s,"t_date","") or getattr(s,"date",""))
                # 修正：原本這裡用abs(s.amount)，把正負號直接抹掉——但用真實對帳單核對過，
                # 永豐的交割金額本來就有正負號：買進是負數(應付，你欠的錢)，賣出是正數(應收，
                # 還沒入帳的錢)。原本不分正負一律加總取絕對值，會把「還沒收到的賣出款」也當成
                # 「要扣的錢」算進pending_settlement，嚴重高估真正該扣掉的金額(實測案例：應付
                # 66,934加應收35,394，原本錯誤算成102,328要扣，正確應該只扣應付的66,934)。
                # 應收(賣出)的錢還在T+2交割中、還沒真的到帳，不該拿來『增加』可用資金，但也不該
                # 被誤當成又一筆要扣的負擔——所以只有應付(買進，負數)才計入pending_settlement，
                # 應收(賣出，正數)只放進明細給你參考「之後會收到多少」，不影響可用資金的計算。
                raw_amt = float(s.amount)
                if t_date and t_date >= today_str and raw_amt != 0:
                    if raw_amt < 0:
                        pending_settlement += abs(raw_amt)
                        settlement_schedule.append({"date":t_date,"amount":abs(raw_amt),"type":"payable"})
                    else:
                        settlement_schedule.append({"date":t_date,"amount":raw_amt,"type":"receivable"})
            settlement_schedule.sort(key=lambda x:x["date"])
        except Exception as se:
            logger.warning(f"無法取得交割資料，風控將以可用餘額為準（未扣交割款）: {se}")
        available_after_settlement = max(0.0, avail - pending_settlement)
        # 修正：原本只在>0時才更新capital，導致「真的可用資金剛好是0」這個最需要被正確反映的情況，
        # 反而被當成「不可信」而忽略，capital留在舊的(可能是預設100萬)數字上——
        # 等於系統不知道自己沒錢，還拿著虛高的資金去算部位大小。這裡是成功抓到真實餘額後算出來的結果，
        # 0本身就是有效、真實的答案(代表現在真的不能再買)，應該無條件更新，不是只在>0時才更新。
        auto_state["capital"]=available_after_settlement
        cap_info={"account":str(stock_account),"balance":balance,"available":avail,
                "pending_settlement":pending_settlement,
                "available_after_settlement":available_after_settlement,
                "settlement_schedule":settlement_schedule,
                "synced_at":time.time()}
        auto_state["capital_info"]=cap_info  # 持久化存起來，前端點"資金已同步"那行log時可以隨時查詢最新明細
        return cap_info
    except Exception as e:
        logger.warning(f"刷新可用資金失敗: {e}")
        return None

# ══════════════════════════════════════════════════════════════════
# 排程器啟動
# ══════════════════════════════════════════════════════════════════
scheduler=BackgroundScheduler(timezone='Asia/Taipei')
scheduler.add_job(auto_trade_tick,    'interval',seconds=30,id='tick',replace_existing=True)
scheduler.add_job(watchdog_check,     'interval',seconds=30,id='watchdog',replace_existing=True)
scheduler.add_job(reconcile_real_positions,'interval',seconds=120,id='reconcile',replace_existing=True) # 真實模式持倉核對，每2分鐘一次(安全網而非主要機制，不需要跟30秒tick一樣頻繁)
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
        "daily_pnl_pct":round(auto_state["daily_pnl"]/(auto_state["capital"] or 1)*100,2),
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
        try:
            api.set_order_callback(_on_order_callback)
        except Exception as e:
            logger.warning(f"委託回報callback註冊失敗，真實下單時的委託取消/失敗通知將完全依賴reconcile_real_positions(每2分鐘核對): {e}")
        ofi_symbols=list(set(auto_state["watchlist"]) | set(SCAN_UNIVERSE))
        def _do_ofi_subscribe():
            n_ok=subscribe_ofi_symbols(ofi_symbols, api, auto_state)
            already=auto_state.get("ofi_already_subscribed_count",0)
            n_failed=len(ofi_symbols)-n_ok-already
            if n_failed<=0:
                # 修正：原本只看n_ok!=總數就說「有股票訂閱失敗」，但如果這次呼叫的股票早就被
                # 之前一次呼叫訂閱過(例如/connect短時間內被呼叫兩次)，n_ok會是0，卻不是真的失敗，
                # 是「沒有新的要訂閱」——要扣掉already_count才知道是不是真的有股票訂閱不到。
                msg=f"OFI即時訂閱完成：{n_ok+already}/{len(ofi_symbols)}檔涵蓋"+(f"(其中{already}檔先前已訂閱過)" if already>0 else "")
            else:
                msg=f"OFI即時訂閱完成：{n_ok+already}/{len(ofi_symbols)}檔涵蓋，{n_failed}檔訂閱失敗，那些股票的order_flow特徵會用0中性值代替，看log warning找原因"
            _log(msg)
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
        unsubscribe_all_ofi(sinopac_api)  # 模組一：登出前先取消tick訂閱、清空OFI狀態，避免殘留訂閱占用額度
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
    if req.risk not in RISK_CFG:
        # 修正：原本沒驗證就直接存進auto_state["risk"]，無效值(打錯字/前端bug/任何非low/mid/high的字串)
        # 會在下一次auto_trade_tick讀RISK_CFG[auto_state["risk"]]時直接KeyError，每30秒crash一次，
        # 而且要等到那個時候才會發現問題——在這裡就先擋掉，啟動當下就清楚告知，不要讓壞資料進到狀態裡。
        raise HTTPException(status_code=400,detail=f"無效的risk參數: {req.risk!r}，必須是 {list(RISK_CFG.keys())} 其中之一")
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
        "_loss_stop_logged":False,"_profit_lock_logged":False,"_afford_warn_logged":False,"_zero_capital_logged":False,
        "trade_history":[],
    })
    _log("使用者手動清空今日統計，重新開始記錄")
    _persist_auto_state()
    return {"success":True,"state":auto_state}

_model_status_logged=False

@app.get("/auto/status")
async def auto_status():
    global _model_status_logged
    lgbm_status=get_lgbm_model_status()
    if not _model_status_logged:
        _model_status_logged=True
        if lgbm_status["loaded"]:
            _log(f"✅ LightGBM模型已載入：{lgbm_status['path']}")
        else:
            _log(f"⚠️ LightGBM模型尚未載入：{lgbm_status['error']}")
    return {**auto_state,"market":market_status(),
            "paper_available_capital":round(get_paper_available_capital(),2),
            "min_profitable_move_pct":round(min_profitable_move_pct(auto_state.get("fee_discount",FEE_DISCOUNT)),3),
            "paper_validation_min_trades":PAPER_VALIDATION_MIN_TRADES,
            "paper_validation_min_days":PAPER_VALIDATION_MIN_DAYS,
            "paper_validation_progress":_paper_validation_progress(),
            "performance_metrics":compute_performance_metrics("paper" if auto_state.get("paper_mode") else "real"),
            "performance_metrics_real":compute_performance_metrics("real"),
            "performance_metrics_paper":compute_performance_metrics("paper"),
            "lgbm_model":lgbm_status,
            "price_cache_size":{k:len(v) for k,v in price_cache.items()},
            "watchdog":{"seconds_since_tick":round(time.time()-_watchdog_state["last_tick_at"],1),
                        "alerted":_watchdog_state["alerted"]}}

@app.get("/auto/validation")
async def get_paper_validation():
    """真實下單前的模擬驗證進度，供前端顯示「還差幾筆/幾天」"""
    return _paper_validation_progress()

@app.get("/auto/performance")
async def get_performance_metrics(mode:Optional[str]=None):
    """年化報酬率/最大回撤等標準績效指標，從每日收盤後記錄的權益曲線算出來。
    mode可指定"paper"或"real"只看該模式自己的權益曲線，不指定則用目前正在運行的模式。"""
    return compute_performance_metrics(mode or ("paper" if auto_state.get("paper_mode") else "real"))

@app.post("/auto/resume-after-drawdown")
async def resume_after_drawdown():
    """手動清除累積回撤保護的暫停狀態——刻意不做成自動恢復，累積回撤代表的問題比單日虧損
    更系統性(可能是模型本身失準、市場環境系統性不利等)，值得人看過數字後自己決定要不要繼續，
    不應該隔天自動繼續嘗試同一套邏輯。"""
    if not auto_state.get("drawdown_halt"):
        return {"success":True,"message":"目前沒有處於回撤保護暫停狀態"}
    info=auto_state.get("drawdown_halt_info")
    auto_state["drawdown_halt"]=False
    auto_state["drawdown_halt_info"]=None
    _persist_auto_state()
    _log("[回撤保護解除]使用者已手動確認，恢復正常開新倉")
    return {"success":True,"message":"已解除回撤保護暫停","previous_trigger":info}

@app.get("/auto/feature_log.csv")
async def export_feature_log():
    """匯出累積的特徵紀錄成CSV，供5天驗證期結束後做SHAP特徵歸因分析(見analyze_shap.py)。
    每一列是一筆實際發生的交易：進場當時餵給LightGBM的完整特徵向量+這筆交易的結果(pnl/win)。"""
    log=auto_state.get("feature_log",[])
    if not log:
        return Response(content="沒有資料，至少要有一筆模擬交易平倉後才會有紀錄\n",media_type="text/plain")
    fieldnames=["ts","sym","tag","lgbm_conf","pnl","pct","win"]+ML_FEATURE_NAMES
    buf=io.StringIO()
    writer=csv.DictWriter(buf,fieldnames=fieldnames,extrasaction="ignore")
    writer.writeheader()
    writer.writerows(log)
    return Response(content=buf.getvalue(),media_type="text/csv",
                     headers={"Content-Disposition":"attachment; filename=paper_test_data.csv"})

@app.put("/auto/watchlist")
async def update_watchlist(watchlist:List[str]):
    old_watchlist=set(auto_state.get("watchlist",[]))
    removed=old_watchlist-set(watchlist)
    auto_state["watchlist"]=watchlist
    _persist_auto_state()
    if sinopac_api:  # 模組一：自選股新增的股票也要補訂閱OFI，不然extract_ml_features對它們永遠拿不到即時OFI
        n_ok=subscribe_ofi_symbols(watchlist, sinopac_api, auto_state)
        if n_ok>0: _log(f"自選股更新，補訂閱OFI {n_ok}檔")
        if removed:
            # 修正：被移除的自選股，OFI訂閱原本會一直留著(只有整個斷線才清)，長期下來訂閱數
            # 會跟「目前」自選股清單大小脫鉤、一路往上累積，最終可能不明原因撞到訂閱數上限。
            unsubscribe_ofi_symbols(removed, sinopac_api)
            _log(f"自選股更新，取消訂閱已移除的{len(removed)}檔OFI")
    return {"success":True,"watchlist":watchlist}

class PaperCapitalRequest(BaseModel):
    amount:float

class FeeDiscountRequest(BaseModel):
    discount:float  # 0~1之間，例如6折就填0.6，1折填0.1，無折扣填1.0

@app.put("/auto/fee-discount")
async def set_fee_discount(req:FeeDiscountRequest):
    """設定使用者實際從永豐拿到的手續費折扣，取代config.py裡6折的通用假設。
    這個數字會直接影響所有損益試算(含已經模擬中的舊持倉，下次平倉時就會用新數字算)跟
    「至少要漲跌多少%才划算」的門檻——折扣設得不準，整套損益估算都會系統性偏差，
    模擬驗證的可信度也會打折扣。"""
    if not (0<req.discount<=1):
        raise HTTPException(status_code=400,detail="折扣必須在0~1之間(例如6折填0.6)")
    auto_state["fee_discount"]=req.discount
    _persist_auto_state()
    _log(f"手續費折扣已設定為{req.discount*10:.1f}折")
    return {"success":True,"fee_discount":auto_state["fee_discount"],
            "min_profitable_move_pct":round(min_profitable_move_pct(req.discount),3)}

@app.put("/auto/paper-capital")
async def set_paper_capital(req:PaperCapitalRequest):
    """設定模擬資金總額。這個資金完全獨立於真實永豐帳戶，模擬模式的部位大小用這個算，
    不會被真實帳戶的T+2交割卡住——也就是使用者可以放心拿一個夠大的模擬資金(例如預設1000萬)
    去跑驗證，不需要真的有那麼多錢在永豐帳戶裡。"""
    if req.amount<=0:
        raise HTTPException(status_code=400,detail="模擬資金必須大於0")
    if auto_state["positions"]:
        _log(f"[提示]目前有{len(auto_state['positions'])}筆模擬持倉仍開著，調整模擬資金不會回溯影響這些持倉的成本計算，"
             f"建議先平倉或等自然出場後再調整，避免帳本對不齊")
    auto_state["paper_capital"]=req.amount
    auto_state["paper_pending_settlements"]=[]  # 重設基準，連帶清空交割中紀錄，避免新舊金額混在一起搞混
    _persist_auto_state()
    _log(f"模擬資金已調整為NT${req.amount:,.0f}")
    return {"success":True,"paper_capital":auto_state["paper_capital"]}

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
    return {"min_profitable_move_pct":round(min_profitable_move_pct(auto_state.get("fee_discount",FEE_DISCOUNT)),3),
            "fee_discount":auto_state.get("fee_discount",FEE_DISCOUNT),
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
                # 修正：Shioaji的direction是Action enum(<Action.Buy: 'Buy'>)，Python對enum預設
                # str()出來是"Action.Buy"不是單純"Buy"，跟"Buy"做exact match永遠不會相等，
                # 導致這裡一直誤判成空單方向，連帶讓下面pnl_percent的正負號整個顛倒——
                # 螢幕上看到0050明明虧錢卻顯示正的百分比，009816明明賺錢卻顯示負的，就是這個原因。
                # 改成看字串裡有沒有包含"buy"(不分大小寫)，不管Shioaji回傳的是"Buy"、"Action.Buy"
                # 哪一種形式都抓得到，同時把存進結果的direction清理成乾淨的"Buy"/"Sell"給前端用。
                direction_raw=str(getattr(p,"direction","Buy"))
                is_long="buy" in direction_raw.lower()
                direction="Buy" if is_long else "Sell"
                # 修正：原本pnl_percent是直接用均價/現價的價差自己算，跟pnl(直接讀Shioaji回傳的金額)
                # 是兩條獨立算出來的數字——螢幕上會同時看到「-5222」跟「-7.58%」，但600股×(103.10-111.56)
                # 算出來其實是-5076，不是-5222，代表Shioaji自己的pnl金額包含了價差以外的東西(可能是
                # 預估手續費/證交稅之類的成本)，跟純粹用價差算的百分比不是同一件事，兩個數字擺在一起
                # 卻對不起來，看起來像是哪裡算錯了。改成直接拿pnl金額去除以成本反推百分比，保證
                # 兩個數字永遠互相對得上，不管Shioaji的pnl金額裡實際包含了什麼我們不清楚的調整項目。
                cost_basis=ap*qty
                pnlp=(pnl/cost_basis*100) if cost_basis else 0.0
                result.append({"symbol":code,"name":getattr(p,"name",code),"quantity":qty,
                    "avg_price":ap,"current_price":lp,"pnl":pnl,"pnl_percent":round(pnlp,2),
                    "direction":direction,"value":lp*qty if lp else ap*qty})
            except Exception as pe:
                logger.warning(f"解析持倉失敗: {pe}")
        return result
    except Exception as e:
        logger.error(f"list_positions 失敗: {e}")
        raise HTTPException(status_code=500,detail=f"持倉查詢失敗: {str(e)}")

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
