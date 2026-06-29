"""
config.py — 純常數跟時間/假日工具，從main.py拆出來。
這裡的東西完全不依賴任何交易狀態(auto_state/price_cache等)，
任何其他模組都可以安心import，不會有循環依賴或共享可變狀態的風險。
"""
import logging
import pytz
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

TW_TZ = pytz.timezone('Asia/Taipei')

def tw_now():
    return datetime.now(TW_TZ)

# ══════════════════════════════════════════════════════════════════
# 台股當沖真實交易成本（含手續費+證交稅，2026年現行費率）
# ══════════════════════════════════════════════════════════════════
FEE_RATE = 0.001425      # 手續費 0.1425%（買賣各收一次，未打折）
FEE_DISCOUNT = 0.6       # 預設折扣6折，僅作為「使用者還沒自己設定真實折扣前」的起始值——
                         # 實際折扣每個人跟永豐談的不一樣(常見從1折到完全沒折扣都有)，
                         # 真正生效的數字是auto_state["fee_discount"](可透過/auto/fee-discount設定)，
                         # 這個常數只在main.py完全沒呼叫過設定、auto_state還沒這個欄位時當預設值用。
DAYTRADE_TAX_RATE = 0.0015  # 當沖證交稅 0.15%（優惠至2027年底，賣出收一次）

def calc_round_trip_cost(entry_price:float, exit_price:float, qty:int, fee_discount:float=FEE_DISCOUNT) -> dict:
    """計算當沖一買一賣的真實成本（手續費x2 + 當沖證交稅）。fee_discount給未指定時退回模組預設值，
    main.py應該傳入auto_state["fee_discount"](使用者實際設定的折扣)，而不是依賴這個預設值，
    避免每個使用者實際拿到的券商折扣不一樣，卻全部都套用同一個假設的6折，造成損益估算系統性偏差。"""
    buy_amt  = entry_price*qty
    sell_amt = exit_price*qty
    buy_fee  = max(20, buy_amt*FEE_RATE*fee_discount)
    sell_fee = max(20, sell_amt*FEE_RATE*fee_discount)
    tax      = sell_amt*DAYTRADE_TAX_RATE
    total_cost = buy_fee+sell_fee+tax
    gross_pnl  = sell_amt-buy_amt
    net_pnl    = gross_pnl-total_cost
    return {"gross_pnl":gross_pnl,"total_cost":total_cost,"net_pnl":net_pnl,
            "buy_fee":buy_fee,"sell_fee":sell_fee,"tax":tax}

def min_profitable_move_pct(fee_discount:float=FEE_DISCOUNT) -> float:
    """當沖至少要漲跌多少%才能扣成本後還有獲利。同樣接受fee_discount覆寫，理由跟上面一致。"""
    return (FEE_RATE*fee_discount*2 + DAYTRADE_TAX_RATE)*100

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

def is_tw_business_day(d) -> bool:
    """單純檢查某一天是不是台股交易日(非週末+非國定假日)，給結算日計算用"""
    if d.weekday()>=5: return False
    return not is_tw_market_holiday(d)

def next_settlement_date(from_date) -> str:
    """計算T+2交割日（從from_date起算，往後數2個「交易日」，不是單純2個日曆天——
    遇到週末或國定假日要跳過，例如週四成交，T+2交割日是下週一(週五、週六、週日都不算交易日)。
    模擬資金要「像真實交易一樣跑」，這個函式讓模擬資金的交割也遵守一樣的規則，不是憑感覺給2天。"""
    d = from_date
    counted = 0
    while counted < 2:
        d = d + timedelta(days=1)
        if is_tw_business_day(d):
            counted += 1
    return d.strftime("%Y-%m-%d")

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
    # ── 使用者要求加入：大盤型ETF，讓LightGBM訓練資料/全市場掃描/候選池都涵蓋到這兩檔 ──
    # 00631L 元大台灣50正2：2倍槓桿ETF，追蹤台灣50指數2倍。重要：槓桿代表同樣的指數%變動，
    # 這檔的價格%變動大約是2倍，但RISK_CFG的停損停利%(低風險2%/4%這種)是針對一般個股校準的，
    # 沒有特別為槓桿ETF放寬——同一個%門檻對這檔會更容易被觸發(停損/停利都會)，等於實際持倉
    # 時間會比一般個股更短、進出更頻繁，這是槓桿產品的自然特性，不是bug，但训练/上線後的
    # 績效統計如果想分開看「一般股」vs「槓桿ETF」的表現，要自己另外篩選symbol，系統目前
    # 不會自動區分槓桿與非槓桿來分組統計。
    # 009816 凱基台灣TOP50：2026年初才掛牌的市值型ETF(不配息)，追蹤特選台灣TOP50指數，
    # 非槓桿(1倍)。掛牌時間短，訓練時如果--days抓太長(例如180天以上)，這檔能用的真實K棒
    # 會比其他老牌股票少，train_lgbm_model.py會自然照樣納入訓練(沒有額外篩選新股的邏輯)，
    # 只是這檔貢獻的樣本數比較少，不會出錯，只是統計power較弱。
    "00631L","009816",
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
    # 大盤型ETF獨立歸一類(不歸入任何個股產業)：00631L跟009816追蹤的指數高度重疊(都是台灣大型權值股)，
    # 歸同一個板塊代號，板塊分散檢查才會把這兩檔當成「同一群」，不會讓AI同時重倉兩檔本質上
    # 高度相關的大盤型商品，誤以為這是兩筆獨立分散的持倉。
    "00631L":"大盤ETF","009816":"大盤ETF",
}

# ══════════════════════════════════════════════════════════════════
# 風控門檻常數
# ══════════════════════════════════════════════════════════════════
def tick_size(price:float) -> float:
    """台股股價升降單位(最小跳動單位)，依證交所6級距規定，每股市價越低、跳動單位佔股價比例越高。
    來源：證交所現行規定(未滿10元0.01、10~50元0.05、50~100元0.10、100~500元0.50、500~1000元1、1000元以上5)。"""
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.10
    if price < 500: return 0.50
    if price < 1000: return 1.0
    return 5.0

def max_allowed_spread_pct(price:float, max_ticks:int=2) -> float:
    """買賣價差過寬的門檻，換算成「最多容許幾個tick」而不是固定百分比——
    原本用固定0.3%門檻，結果驗證時發現：股價NT$12~16的便宜銀行股（剛好是這個系統特地加入
    解決資金不足問題的那幾檔），連最小可能的1個tick價差都已經佔股價0.32~0.40%，超過0.3%門檻，
    等於不管市場流動性再好，這幾檔永遠會被擋下來，跟「資金不足」問題一樣，無形中又把
    好不容易買得起的股票擋掉了。改成依股價換算對應的tick大小，再用tick數設門檻，
    才能在不同股價區間都公平比較。max_ticks=2是合理起點，不是校準過的數字。"""
    if price<=0: return 0.3  # 異常價格時退回舊的固定值，不要算出負數或除以0
    return (max_ticks * tick_size(price)) / price * 100

PAPER_VALIDATION_MIN_TRADES=20
PAPER_VALIDATION_MIN_DAYS=5
MAX_TRADES_PER_DAY=3        # 規格書新增風控：當日最多交易3次，避免訊號反覆觸發造成過度交易、手續費侵蝕獲利
CONSEC_LOSS_STOP=2          # 規格書收緊：原本連虧3次冷靜15分鐘，改成連虧2次直接停止當日交易(更保守)
