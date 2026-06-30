"""
tests/test_paper_mode_allocation.py — 鎖住「模擬模式不受風險等級5%/10%/20%單筆配置上限限制」這個規則。

背景：使用者明確要求「虛擬帳戶內所有的錢你都可以動，不用5%」——模擬模式(paper_mode=True)
是假錢，沒有必要被低風險alloc=5%這種保守的單筆配置比例卡住，導致股價較高的股票(如台積電)
即使有NT$10,000,000模擬資金，也常常被「資金不足，買不起1張」擋下。

修正：main.py的budget計算改成 alloc_pct = 100.0 if paper_mode else cfg["alloc"]*100，
讓calculate_dynamic_position_ratio()在模擬模式下用100%當配置上限(不是risk tier的5/10/20)，
信心度仍然照原本的0.3~1.0線性比例縮放(高信心度部位更大)，邏輯不變，只是上限放寬。

真實下單(paper_mode=False)完全不受影響，繼續用risk tier的alloc%——這是保護真錢曝險的
關鍵風控，不能因為這個要求被連帶放寬。

MAX_AGGREGATE_EXPOSURE_PCT(總曝險30%上限)兩種模式都繼續適用，沒有被這次修改放寬——
這不是保守限制，是「模擬帳戶不能花超過自己有的錢」這個基本要求本身：同一個tick裡
開好幾筆新倉時，如果完全不設曝險上限，每一筆都會各自以為「全部資金都還沒被用掉」，
加總起來可能讓模擬帳戶的曝險超過100%本金。

執行方式：
    pip install pytest --break-system-packages
    cd tradeai-backend && pytest tests/ -v
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ml_model import calculate_dynamic_position_ratio  # noqa: E402


def test_paper_mode_uses_100pct_allocation_ceiling():
    """核心回歸測試：相同信心度下，paper_mode的配置比例上限應該是100%，不是risk tier的5%。"""
    conf, min_conf = 80.0, 72.0  # 低風險門檻72%，信心度80%
    real_ratio = calculate_dynamic_position_ratio(conf, min_conf, 5.0)   # 真實模式：低風險alloc=5%
    paper_ratio = calculate_dynamic_position_ratio(conf, min_conf, 100.0)  # 模擬模式：100%
    assert paper_ratio > real_ratio
    assert paper_ratio == real_ratio * 20  # 100/5=20倍，因為calculate_dynamic_position_ratio對max_alloc_pct是線性的


def test_paper_mode_can_afford_high_priced_stock_real_mode_cannot():
    """驗證實際金額：NT$10,000,000本金、信心度80%，模擬模式應該負擔得起NT$1000/股的股票(1張NT$1,000,000)，
    真實模式(低風險5%)則不行——這就是使用者實際回報「資金不足，買不起1張」的情境。"""
    capital = 10_000_000.0
    conf, min_conf = 80.0, 72.0
    price = 1000.0
    lot_cost = price * 1000  # 台股1張=1000股

    real_budget = capital * calculate_dynamic_position_ratio(conf, min_conf, 5.0)
    paper_budget = capital * calculate_dynamic_position_ratio(conf, min_conf, 100.0)

    assert real_budget < lot_cost  # 真實模式(低風險)買不起
    assert paper_budget >= lot_cost  # 模擬模式買得起


def test_below_min_conf_still_returns_zero_regardless_of_mode():
    """信心度沒過門檻的話，不管是不是模擬模式，配置比例都該是0——「不用5%」是放寬上限，
    不是跳過信心度門檻檢查，這兩件事不能混在一起。"""
    assert calculate_dynamic_position_ratio(70.0, 72.0, 100.0) == 0.0
    assert calculate_dynamic_position_ratio(70.0, 72.0, 5.0) == 0.0


def test_paper_mode_ratio_still_scales_with_confidence():
    """確認拿掉5%上限後，「信心度越高、部位越大」這個既有邏輯沒有被破壞——
    只是上限從5%放寬到100%，不是整個信心度縮放機制被拿掉變成always 100%。"""
    min_conf = 72.0
    low_conf_ratio = calculate_dynamic_position_ratio(73.0, min_conf, 100.0)
    high_conf_ratio = calculate_dynamic_position_ratio(99.0, min_conf, 100.0)
    assert high_conf_ratio > low_conf_ratio
