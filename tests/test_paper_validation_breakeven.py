"""
tests/test_paper_validation_breakeven.py — 鎖住「打平的模擬交易不會悄悄從統計裡消失」這個規則。

背景：使用者實際回報過的bug——畫面顯示「模擬驗證 1/20筆」，但「累積勝率」卡片顯示
「0%、0勝0敗」，看起來自相矛盾。根因：_record_paper_validation()原本對pnl恰好等於0
(扣完成本後完全打平)的交易，既不算贏(if pnl>0)也不算輸(elif pnl<0)，trade_count
照樣+1，但wins/losses都沒加——_paper_validation_progress()算win_rate時分母用
wins+losses(=0)，不是trades(=1)，所以「1筆已完成」卻「0勝0敗、勝率0%」。

修正：加一個breakeven桶子接住pnl==0的情況，win_rate分母改用trades(完整已平倉筆數)。

這支測試需要import main.py(含shioaji/apscheduler等完整依賴)，跟其他測試
(test_capital_separation.py/test_etf_fees.py，刻意只import不會啟動排程器的模組)
不一樣——如果還沒pip install -r requirements.txt，這支測試會被跳過而不是噴一堆錯誤。

執行方式：
    pip install -r requirements.txt -r requirements-dev.txt --break-system-packages
    cd tradeai-backend && pytest tests/ -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

shioaji = pytest.importorskip("shioaji", reason="main.py需要shioaji才能import，還沒裝的話跳過這支測試")

import main  # noqa: E402


def setup_function(_):
    """每個測試開始前重置paper_validation累積狀態，測試之間不互相汙染。"""
    main.auto_state["paper_validation"] = {"trade_count": 0, "trading_days": []}


def test_breakeven_trade_is_counted_but_not_a_win_or_loss():
    main._record_paper_validation(pnl=0.0, pct=0.0, held_min=10.0)
    progress = main._paper_validation_progress()
    assert progress["trades"] == 1
    assert progress["wins"] == 0
    assert progress["losses"] == 0
    assert progress["breakeven"] == 1


def test_win_rate_denominator_is_trades_not_wins_plus_losses():
    """核心回歸測試：1筆打平的交易，trade_count=1但wins+losses=0——
    勝率分母如果用wins+losses會除以0顯示成0%(看起來矛盾)，用trades算才正確反映「1筆裡0勝」。"""
    main._record_paper_validation(pnl=0.0, pct=0.0, held_min=10.0)
    progress = main._paper_validation_progress()
    assert progress["win_rate"] == 0.0
    assert progress["trades"] == 1  # 確保「1/20筆」這個畫面數字跟win_rate用的是同一個分母


def test_one_win_one_breakeven_gives_50_percent_not_100_percent():
    main._record_paper_validation(pnl=500.0, pct=1.2, held_min=15.0)
    main._record_paper_validation(pnl=0.0, pct=0.0, held_min=10.0)
    progress = main._paper_validation_progress()
    assert progress["trades"] == 2
    assert progress["wins"] == 1
    assert progress["breakeven"] == 1
    assert progress["win_rate"] == 50.0  # 不是1勝/(1勝+0敗)=100%


def test_breakeven_breaks_current_streak():
    main._record_paper_validation(pnl=500.0, pct=1.0, held_min=10.0)
    main._record_paper_validation(pnl=300.0, pct=0.6, held_min=10.0)
    pv = main.auto_state["paper_validation"]
    assert pv["current_streak"] == 2  # 連勝2次
    main._record_paper_validation(pnl=0.0, pct=0.0, held_min=10.0)
    assert pv["current_streak"] == 0  # 打平打斷連勝紀錄，不是繼續算成第3次贏
