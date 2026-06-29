"""
tests/test_capital_separation.py — 鎖住「虛擬(模擬)帳戶跟真實帳戶資金永遠互不影響」這個規則。

背景：曾經發生過的bug——所有風控安全閥一律檢查真實帳戶的capital，導致真實帳戶餘額
剛好是0(待交割款卡住，這是正常會發生的真實狀況)時，連模擬模式(完全用獨立的paper_capital
虛擬帳本)也被一起卡死，整天掃描0次、開倉0筆。已經在capital.py修好(get_effective_capital
依paper_mode分流)，這支測試確保以後不會有人不小心又寫回「不分模式一律看capital」。

執行方式：
    pip install pytest --break-system-packages
    cd tradeai-backend && pytest tests/ -v
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from capital import get_paper_available_capital, get_effective_capital  # noqa: E402


def _make_state(**overrides):
    state = {
        "paper_mode": True,
        "paper_capital": 10_000_000.0,
        "paper_pending_settlements": [],
        "capital": 0.0,
    }
    state.update(overrides)
    return state


def test_paper_mode_ignores_zero_real_capital():
    """核心回歸測試：真實帳戶餘額是0，不該影響模擬模式的可用資金——
    這就是使用者實際遇過、要求「不要再出現」的那個bug。"""
    state = _make_state(paper_mode=True, capital=0.0, paper_capital=5_000_000.0)
    assert get_effective_capital(state) == 5_000_000.0
    assert get_effective_capital(state) > 0


def test_real_mode_uses_real_capital_not_paper():
    state = _make_state(paper_mode=False, capital=123_456.0, paper_capital=10_000_000.0)
    assert get_effective_capital(state) == 123_456.0


def test_real_mode_zero_capital_blocks_real_only_not_paper_ledger():
    """真實模式下，真實帳戶0元理當被擋下來(這是正確行為，不是bug)，
    但不該連帶改動paper_capital這個獨立帳本的數字——兩個帳本要完全互不干擾。"""
    state = _make_state(paper_mode=False, capital=0.0, paper_capital=8_000_000.0)
    assert get_effective_capital(state) == 0.0
    assert state["paper_capital"] == 8_000_000.0


def test_paper_available_capital_subtracts_pending_settlement():
    today = datetime.now().strftime("%Y-%m-%d")
    future = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
    state = _make_state(
        paper_capital=1_000_000.0,
        paper_pending_settlements=[
            {"amount": 200_000.0, "settle_date": future},   # 還在交割中，不能用
            {"amount": 50_000.0, "settle_date": today},      # 今天已交割完成，可以用
        ],
    )
    assert get_paper_available_capital(state) == 800_000.0


def test_paper_available_capital_never_negative():
    """理論上不該發生(pending不該超過總帳)，但算式本身要有下限保護，不要讓可用資金變成負數
    去影響後面的部位大小計算(負的budget算出來的qty會是負數或0，行為未定義)。"""
    state = _make_state(paper_capital=100.0, paper_pending_settlements=[
        {"amount": 99_999.0, "settle_date": "2099-01-01"},
    ])
    assert get_paper_available_capital(state) == 0.0
