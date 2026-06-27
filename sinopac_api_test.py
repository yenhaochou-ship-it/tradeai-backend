"""
sinopac_api_test.py — 完成永豐官方要求的「Python API測試」(一次性，跟main.py無關)

這是永豐自己要求的審核流程：用 simulation=True 連到他們真正的模擬環境，
做一次登入+一次下單測試，他們的系統會記錄下來，審核通過後，
你的API金鑰才會真正被授權可以下單(不管是真錢還是透過main.py的paper_mode)。

═══════════════════════════════════════════════════════════════════════════
使用方式
═══════════════════════════════════════════════════════════════════════════
1. pip install shioaji --break-system-packages
2. 設定環境變數(跟main.py用的是同一組永豐API金鑰)：
   $env:SINOPAC_API_KEY="你的api_key"
   $env:SINOPAC_SECRET_KEY="你的secret_key"
3. 只能在營業日 08:00~20:00 執行(永豐官方規定的測試時段)
4. python sinopac_api_test.py
5. 跑完回到 https://sinotrade.com.tw/newweb/signCenter/S_openAPI/ 重新整理，
   狀態通常當天就會審核完畢(官方文件寫「隨到隨審」)
"""
import os, sys

api_key = os.environ.get("SINOPAC_API_KEY")
secret_key = os.environ.get("SINOPAC_SECRET_KEY")
if not api_key or not secret_key:
    print("❌ 請先設定環境變數 SINOPAC_API_KEY / SINOPAC_SECRET_KEY")
    sys.exit(1)

import shioaji as sj

print("="*60)
print("步驟1：連線到永豐模擬環境 (simulation=True，這裡跟main.py不一樣)")
print("="*60)
api = sj.Shioaji(simulation=True)
accounts = api.login(api_key=api_key, secret_key=secret_key)
print(accounts)
print()

# 修正：signed這個欄位反映的是「API測試本身有沒有通過」，不是網站上「簽署與否」那個勾選——
# 兩個是不同的東西。現在還沒做測試單，所以signed=False是正常的、預期中的狀態，不是哪裡設定錯誤，
# 不需要因為看到這個就去檢查網站簽署狀態。真正要看的是下面這筆測試單能不能成功送出。
signed_ok = any(getattr(a, "signed", False) for a in accounts)
if signed_ok:
    print("ℹ️ 帳戶已經是signed=True，可能這把金鑰之前就通過測試了，這次純粹再測一次下單也無妨")
else:
    print("ℹ️ signed=False 是測試前的正常狀態(這個欄位代表「API測試有沒有通過」，不是「有沒有簽署」)，"
          "繼續往下測下單，成功送出後永豐審核通過就會變成True，不用因為這個去檢查網站。")
print()

print("="*60)
print("步驟2：下一筆測試單(模擬環境，不是真錢，金額/股數隨便都可以)")
print("="*60)
# 修正：原本用 a.__class__.__name__=="StockAccount" 完全比對class名稱，
# 但實測(shioaji 1.5.3)發現帳戶清單裡明明印出StockAccount(...)，這個比對卻找不到——
# 可能不同版本內部的類別命名/模組路徑不完全一樣。改成看型別字串裡有沒有包含"stock"，
# 比逐字比對寬鬆、更不容易因為版本差異就找不到帳戶。
# 修正：原本用 a.__class__.__name__=="StockAccount" 完全比對class名稱，
# 但實測(shioaji 1.5.3)發現帳戶清單裡明明印出StockAccount(...)，這個比對卻找不到——
# 代表shioaji內部的實際類別跟印出來的repr字串不是同一回事(可能用了自訂__repr__或Pydantic判別欄位)。
# 改成直接看repr字串本身有沒有出現"StockAccount"，這是我們從你貼的輸出親眼確認過真的存在的字串，
# 比猜內部類別結構可靠；account_type欄位當第二道備援。
def _is_stock_account(a):
    if "stockaccount" in repr(a).lower(): return True
    if str(getattr(a,"account_type","")).upper() in ("S","STOCK"): return True
    return False
stock_account = next((a for a in accounts if _is_stock_account(a)), None)
if not stock_account:
    print("❌ 找不到證券帳戶，無法測試下單。請確認你的永豐帳戶有開證券戶。")
    print(f"   目前抓到的帳戶: {[repr(a) for a in accounts]}")
    sys.exit(1)

contract = api.Contracts.Stocks["2890"]  # 永豐金自己的股票，隨便挑一支流動性夠的測試用
print(f"測試合約: {contract}")

order = api.Order(
    price=contract.reference,           # 用平盤價，盡量確保模擬環境會接受
    quantity=1,
    action=sj.constant.Action.Buy,
    price_type=sj.constant.StockPriceType.LMT,
    order_type=sj.constant.OrderType.ROD,
    account=stock_account,
)
trade = api.place_order(contract, order)
print(trade)
print()

status_str = str(getattr(trade.status, "status", ""))
# 修正：trade.status.status是一個Enum，Python預設的str()會印成"OrderStatus.PendingSubmit"
# 這種「類別名.成員名」格式，不是單純的"PendingSubmit"，原本的精確比對因此永遠對不上，
# 即使委託明明成功了也會被判定成警告。改成看子字串，不管是哪種格式都抓得到。
if any(s in status_str for s in ("PendingSubmit", "Submitted", "Filled")):
    print(f"✅ 下單測試成功，狀態={status_str}")
    print("這次的登入+下單紀錄，永豐系統會自動記下來，等他們審核通過，")
    print("回到簽署頁面重新整理，「Python測試與否」應該會從「未測試」變成已通過。")
else:
    print(f"⚠️ 委託狀態是「{status_str}」，不是預期的成功狀態，可能需要檢查委託內容或稍後再試一次。")

api.logout()
