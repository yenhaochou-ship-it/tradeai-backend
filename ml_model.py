"""
ml_model.py — LightGBM進場模型：特徵抽取、推論、動態部位縮放、人看得懂的進出場原因說明。
取代原本main.py裡的「12項技術指標綜合評分」與「進階評分(C級)」做為進場判斷主力。

重要：extract_ml_features() 的特徵定義/順序，跟 train_lgbm_model.py / analyze_shap.py
裡的 ML_FEATURE_NAMES 必須三邊完全一致，否則訓練/推論/SHAP分析互相對不上，新增/刪除
特徵時三個檔案要一起改。
"""
import os, logging
from typing import Optional, List, Dict, Tuple

from config import tw_now
from state import price_cache
from signals import (
    calc_signal_py, classify_market_regime, multi_timeframe_direction,
    volume_quality_score, approx_order_flow, orb_range,
    get_real_ofi, get_latest_spread_pct, get_market_context, get_time_of_day_pct,
)

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
# LightGBM 進場模型：取代原本的「12項技術指標綜合評分」與「進階評分(C級)」做為進場判斷主力。
# 風控（國定假日/處置股/當日3筆上限/VWAP之上）維持獨立硬性檢查，不受模型影響。
# ══════════════════════════════════════════════════════════════════
ML_FEATURE_NAMES=[
    "rsi","williams_r","cci","trend_str","vol_ratio",
    "ma5_ma20_dev_pct","price_vwap_dev_pct",
    "regime_code","mtf_aligned_code",
    "volume_quality_score","bid_ask_imbalance","order_flow_delta_scaled","orb_breakout",
    "real_ofi_interval","real_big_trade_interval_scaled",  # 模組一：上一個30秒週期累積的真實OFI/大單流，沒訂閱時給0(中性)
    "spread_pct",  # 買賣價差(中價%)，沒資料時給0；數值越大代表流動性越差、進出場成本越高
    "market_return_5m_pct",  # 大盤同步性：0050最近5分鐘報酬率，沒資料時給0(中性，不等於"大盤平盤"，只是未知)
    "time_of_day_pct",  # 距開盤經過時間正規化(0~1)，這個永遠有真實值，沒有"缺資料只能填0"的問題
]
_REGIME_CODE={"trending_bull":2,"trending_bear":-2,"range":0,"volatile":1,"panic":-3,"unknown":0}

def extract_ml_features(sym:str, prices:List[float], volumes:List[float], dir_:str="L",
                         market_ctx:Optional[Dict]=None, spread_pct:Optional[float]=None) -> Optional[List[float]]:
    """把已經算好的技術指標/Phase1-2引擎輸出，統一抽成固定順序的特徵向量餵給LightGBM模型。
    回傳None代表資料不足，呼叫端應該跳過這個候選，不要硬塞進模型。
    效能修正：market_ctx(大盤同步性)跟sym無關，46檔股票每檔都各自呼叫一次get_market_context()
    等於對同一份0050資料重複算46次一樣的結果——改成讓呼叫端(候選迴圈)每個tick算一次後傳進來，
    這裡只在沒收到才自己算一次(標準呼叫/獨立測試時還是能正常運作，不依賴呼叫端一定要傳)。
    spread_pct同理：候選迴圈已經算過一次(用在硬性過濾)，這裡不用再重算一次。"""
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
    if spread_pct is None: spread_pct=get_latest_spread_pct(sym)  # 買賣價差，沒資料是None
    mkt=market_ctx if market_ctx is not None else get_market_context()  # 大盤同步性(0050)

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
        spread_pct if spread_pct is not None else 0.0,
        mkt["return_5m_pct"] if mkt.get("return_5m_pct") is not None else 0.0,
        get_time_of_day_pct(entries[-1].get("t") if entries else None),
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

def predict_lgbm_confidence(sym:str, prices:List[float], volumes:List[float], dir_:str="L",
                             market_ctx:Optional[Dict]=None, spread_pct:Optional[float]=None) -> Tuple[Optional[float],Optional[List[float]]]:
    """回傳(LightGBM預測的「做多勝率信心度」(0~100), 餵進模型的特徵向量)。模型不存在/特徵不足/預測失敗
    都回傳(None,None)，呼叫端看到None就該跳過該候選——不會也不該悄悄退回舊的規則式評分，那樣等於沒有真的換掉。
    修正：原本只回傳信心度，特徵向量算完就丟，事後沒有任何資料可以回頭做SHAP特徵歸因分析——
    5天驗證期跑完想分析「到底哪個特徵真的有用、哪個是雜訊」時，會發現根本沒有歷史特徵資料可以分析。"""
    booster=_load_lgbm_model()
    if booster is None: return None,None
    feats=extract_ml_features(sym,prices,volumes,dir_,market_ctx,spread_pct)
    if feats is None: return None,None
    try:
        prob=booster.predict([feats])[0]
        return float(max(0.0,min(100.0,prob*100))),feats
    except Exception as e:
        logger.warning(f"LightGBM預測失敗 {sym}: {e}")
        return None,None

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

    注意：max_alloc_pct沿用既有RISK_CFG的alloc(低5%/中10%/高20%)，不是規格書建議的30%/20%/10%——
    那組數字跟現有的「總曝險30%上限」疊在一起，會讓低風險單筆信心夠高時就用滿30%總曝險額度，
    等於低風險變成最集中而不是最分散，跟風險等級的一般直覺相反。"""
    if win_prob_pct < min_conf_pct: return 0.0
    scaling_factor = (win_prob_pct - min_conf_pct) / (100.0 - min_conf_pct)
    scaling_factor = max(0.0, min(1.0, scaling_factor))
    return max_alloc_pct/100 * (0.3 + 0.7*scaling_factor)

REGIME_LABEL_ZH={"trending_bull":"多頭趨勢","trending_bear":"空頭趨勢","range":"區間盤整","volatile":"高波動","panic":"恐慌"}

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
