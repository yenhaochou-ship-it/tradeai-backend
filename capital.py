"""
capital.py — 虛擬(模擬)帳戶與真實帳戶的可用資金計算，從main.py拆出來。

⚠️ 虛實分離規則只有一個，所有要「現在能不能買/風控門檻」的地方都要走get_effective_capital()，
不要在別處自己重新寫一次if paper_mode判斷——這支檔案存在的唯一理由，就是把這個判斷集中成
一個函式，physically讓「真實帳戶0元卻把模擬模式卡死」這種bug無法再悄悄發生：
    - paper_mode=True（模擬）：只看 auto_state["paper_capital"]（獨立虛擬帳本，
      不管真實帳戶餘額是多少、甚至還沒連線/連線失敗，模擬都照常能跑）。
    - paper_mode=False（真實）：看 auto_state["capital"]（真實帳戶可用餘額，
      由main.py的_refresh_capital_from_account()從永豐同步過來）。

拆成獨立檔案還有一個好處：這裡不依賴main.py(會在載入時啟動排程器/連資料庫)，
可以單獨被tests/test_capital_separation.py import來寫單元測試，鎖住這個規則不會回歸。
"""
from typing import Dict

from config import tw_now


def get_paper_available_capital(auto_state: Dict) -> float:
    """模擬資金的「可用」金額 = 總帳(paper_capital) - 還在T+2交割中、還不能拿來開新倉的部分。
    跟真實帳戶的available_after_settlement是同一個概念，只是這裡的錢全部都是假的，
    只用來讓模擬模式的部位大小計算更貼近真實情況(剛平倉的錢不會馬上又能用)。"""
    today = tw_now().strftime("%Y-%m-%d")
    pending = sum(
        p["amount"]
        for p in auto_state.get("paper_pending_settlements", [])
        if p["settle_date"] > today
    )
    return max(0.0, auto_state.get("paper_capital", 0.0) - pending)


def get_effective_capital(auto_state: Dict) -> float:
    """目前用於開倉預算/風控門檻計算的可用資金——虛實分離規則「唯一」的判斷點。
    模擬模式回傳虛擬帳本(paper_capital扣交割中)，真實模式回傳真實帳戶可用餘額(capital)。
    呼叫端(main.py的auto_trade_tick/候選評估迴圈/auto/status等)全部都呼叫這支，
    不要自己另外寫一份`auto_state["capital"] if ... else ...`的判斷。"""
    if auto_state.get("paper_mode"):
        return get_paper_available_capital(auto_state)
    return auto_state.get("capital", 0.0)
