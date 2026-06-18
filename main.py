from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import shioaji as sj
import os, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TradeAI Pro 後端", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

sinopac_api    = None
stock_account  = None
future_account = None

class ConnectRequest(BaseModel):
    api_key: str
    secret_key: str

class OrderRequest(BaseModel):
    symbol:     str
    direction:  str
    quantity:   int
    price:      float
    order_type: str = "市價"

class CancelRequest(BaseModel):
    order_id: str

# ── 健康檢查 ─────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "TradeAI Pro 後端", "connected": sinopac_api is not None}

@app.get("/health")
def health():
    return {"healthy": True, "connected": sinopac_api is not None}

# ── 連接 / 斷線 ──────────────────────────────────────────────────────────
@app.post("/connect")
async def connect(req: ConnectRequest):
    global sinopac_api, stock_account, future_account
    try:
        api = sj.Shioaji(simulation=False)
        accounts = api.login(api_key=req.api_key, secret_key=req.secret_key)
        sinopac_api = api
        for acc in accounts:
            # 相容不同版本 shioaji 的帳戶類型判斷
            type_name = type(acc).__name__
            acc_type_str = str(getattr(acc, 'account_type', '')).lower()
            if 'stock' in type_name.lower() or 'stock' in acc_type_str:
                stock_account = acc
            elif 'future' in type_name.lower() or 'future' in acc_type_str:
                future_account = acc
        # 如果都沒分到，預設第一個帳戶給 stock
        if stock_account is None and accounts:
            stock_account = accounts[0]
        return {
            "success": True,
            "stock_account":  str(stock_account)  if stock_account  else None,
            "future_account": str(future_account) if future_account else None,
            "total_accounts": len(accounts)
        }
    except Exception as e:
        logger.error(f"連接失敗：{e}")
        raise HTTPException(status_code=401, detail=f"連接失敗：{str(e)}")

@app.post("/disconnect")
async def disconnect():
    global sinopac_api, stock_account, future_account
    try:
        if sinopac_api:
            sinopac_api.logout()
        sinopac_api = stock_account = future_account = None
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ── 帳戶餘額 ─────────────────────────────────────────────────────────────
@app.get("/account")
async def get_account():
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    try:
        bal = sinopac_api.account_balance()
        # 相容不同屬性名稱
        balance   = float(getattr(bal, "acc_balance", None) or getattr(bal, "balance", 0))
        available = float(getattr(bal, "available_balance", None) or getattr(bal, "available", 0))
        return {
            "account":   str(stock_account),
            "balance":   balance,
            "available": available,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── 持倉明細 ─────────────────────────────────────────────────────────────
@app.get("/positions")
async def get_positions():
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    try:
        # 相容不同版本 shioaji：先試帶 unit 參數，失敗則不帶
        try:
            positions = sinopac_api.list_positions(stock_account, unit=sj.constant.Unit.Share)
        except AttributeError:
            try:
                positions = sinopac_api.list_positions(stock_account)
            except Exception:
                positions = sinopac_api.list_positions()
        if not positions:
            return []
        result = []
        for p in positions:
            try:
                code    = getattr(p, "code", None) or getattr(p, "symbol", "")
                qty     = int(getattr(p, "quantity", 0))
                avg_p   = float(getattr(p, "price", 0) or getattr(p, "avg_price", 0))
                last_p  = float(getattr(p, "last_price", 0) or getattr(p, "close", 0))
                pnl     = float(getattr(p, "pnl", 0))
                pnl_pct = float(getattr(p, "pnl_percent", 0))
                value   = last_p * qty if last_p else avg_p * qty
                result.append({
                    "symbol":        code,
                    "name":          getattr(p, "name", code),
                    "quantity":      qty,
                    "avg_price":     avg_p,
                    "current_price": last_p,
                    "pnl":           pnl,
                    "pnl_percent":   pnl_pct,
                    "direction":     str(getattr(p, "direction", "Buy")),
                    "value":         value,
                })
            except Exception as pe:
                logger.warning(f"解析持倉 {p} 失敗: {pe}")
        return result
    except Exception as e:
        logger.error(f"list_positions 失敗: {e}")
        raise HTTPException(status_code=500, detail=f"持倉查詢失敗: {str(e)}")

# ── 今日委託 ─────────────────────────────────────────────────────────────
@app.get("/orders")
async def get_orders():
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    try:
        sinopac_api.update_status(stock_account)
        trades = sinopac_api.list_trades()
        return [{
            "order_id": t.order.id,
            "symbol":   t.contract.code,
            "action":   str(t.order.action),
            "quantity": int(t.order.quantity),
            "price":    float(t.order.price),
            "status":   str(t.status.status),
            "time":     str(t.status.order_datetime),
        } for t in trades]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── 下單 ─────────────────────────────────────────────────────────────────
@app.post("/order")
async def place_order(req: OrderRequest):
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    try:
        contract = sinopac_api.Contracts.Stocks.get(req.symbol)
        if not contract:
            raise HTTPException(status_code=404, detail=f"找不到股票 {req.symbol}")

        action = sj.constant.Action.Buy if req.direction in ["做多","買進","buy"] else sj.constant.Action.Sell
        ptype  = sj.constant.StockPriceType.MKT if req.order_type == "市價" else sj.constant.StockPriceType.LMT

        order = sinopac_api.Order(
            price=req.price,
            quantity=req.quantity,
            action=action,
            price_type=ptype,
            order_type=sj.constant.OrderType.ROD,
            account=stock_account
        )
        trade = sinopac_api.place_order(contract, order)
        return {
            "success":    True,
            "order_id":   trade.order.id,
            "status":     str(trade.status.status),
            "symbol":     req.symbol,
            "direction":  req.direction,
            "quantity":   req.quantity,
            "price":      req.price,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"下單失敗：{str(e)}")

# ── 取消委託 ─────────────────────────────────────────────────────────────
@app.post("/cancel")
async def cancel_order(req: CancelRequest):
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    try:
        sinopac_api.update_status(stock_account)
        trades = sinopac_api.list_trades()
        target = next((t for t in trades if t.order.id == req.order_id), None)
        if not target:
            raise HTTPException(status_code=404, detail="找不到此委託單")
        sinopac_api.cancel_order(target)
        return {"success": True, "order_id": req.order_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── 即時股價 ─────────────────────────────────────────────────────────────
@app.get("/price/{symbol}")
async def get_price(symbol: str):
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    try:
        contract = sinopac_api.Contracts.Stocks.get(symbol)
        if not contract:
            raise HTTPException(status_code=404, detail=f"找不到股票 {symbol}")
        snap = sinopac_api.snapshots([contract])
        if not snap:
            raise HTTPException(status_code=404, detail="無法取得股價")
        s = snap[0]
        return {
            "symbol":         symbol,
            "price":          float(s.close),
            "change":         float(s.change_price),
            "change_percent": float(s.change_rate),
            "volume":         int(s.total_volume),
            "open":           float(s.open),
            "high":           float(s.high),
            "low":            float(s.low),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── 交割記錄 ─────────────────────────────────────────────────────────────
@app.get("/settlements")
async def get_settlements():
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    try:
        data = sinopac_api.settlements(stock_account)
        return [{
            "date":   str(s.date),
            "amount": float(s.amount),
            "t_date": str(getattr(s, "t_date", "")),
        } for s in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── 完整投資組合 ──────────────────────────────────────────────────────────
@app.get("/portfolio")
async def get_portfolio():
    if not sinopac_api:
        raise HTTPException(status_code=401, detail="尚未連接永豐帳戶")
    acc   = await get_account()
    pos   = await get_positions()
    ords  = await get_orders()
    setts = await get_settlements()
    return {
        "account":     acc,
        "positions":   pos,
        "orders":      ords,
        "settlements": setts,
    }
