# 部署設定（這次更新後新增/變動的環境變數）

這次更新把「永豐金鑰」跟「對外API保護」都改成由後端環境變數管理，前端不再經手任何敏感金鑰。
請到 Railway 專案的 Variables 設定以下項目：

## 必須設定（沒設定的話功能會被擋下或印警告）

| 變數 | 說明 |
|---|---|
| `SINOPAC_API_KEY` | 永豐 API Key。**改成只從這裡讀**，不再接受前端傳來的值（前端的輸入框已移除）。 |
| `SINOPAC_SECRET_KEY` | 永豐 Secret Key，同上。 |
| `BACKEND_API_KEY` | 自己取一個夠長、夠隨機的字串（例如用 `openssl rand -hex 32` 產生）。**這是擋住外人直接呼叫這支 API 的關鍵**——沒設定的話，任何知道這個後端網址的人都能對你的真實帳戶下指令。設定後，前端的 Vercel 專案要用同一個值設定 `BACKEND_API_KEY`（見 tradeai-pro 那邊的說明）。 |

## 建議設定（沒設定也能跑，但安全性/體驗會差一點）

| 變數 | 說明 |
|---|---|
| `FRONTEND_ORIGIN` | 你的前端網址，例如 `https://tradeai-pro-theta.vercel.app`。沒設定的話 CORS 會開放所有來源（仍然有 `BACKEND_API_KEY` 擋著，但多一層總是好）。可填多個，用逗號分隔。 |

## 維持不變（CA憑證，原本就是環境變數）

`SINOPAC_CA_BASE64` / `SINOPAC_CA_PASSWORD` / `SINOPAC_PERSON_ID` — 沒有變動。

## 變更後的連線方式

- 以前：前端要手動輸入 API Key/Secret Key，存在瀏覽器 localStorage。
- 現在：後端直接從環境變數讀，前端只要按「連接真實帳戶」（或開啟網站就自動連線），完全不需要輸入金鑰，金鑰也不會出現在瀏覽器裡。

## 跑測試（選用）

```bash
pip install -r requirements-dev.txt --break-system-packages
pytest tests/ -v
```

目前只有 `tests/test_capital_separation.py`，鎖住「模擬帳戶跟真實帳戶資金永遠互不影響」這個規則，
避免之前「真實帳戶 0 元卡死模擬模式」的 bug 再出現。後續可以陸續補上 `calc_round_trip_cost`、
`compute_performance_metrics` 等跟損益計算有關的函式的測試。
