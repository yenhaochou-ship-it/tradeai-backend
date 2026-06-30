"""
db.py — SQLite持久化(Railway Volume)。從main.py拆出來，這一塊跟交易邏輯完全無關，
獨立測試/替換儲存後端(例如以後想換成Postgres)都不會牽動到main.py其他部分。
"""
import os, sqlite3, json, logging, threading
from datetime import datetime

logger = logging.getLogger(__name__)

# 重要：必須在 Railway 後台幫這個服務「掛載一個 Volume」，路徑設為 /data，
# 否則資料庫檔案還是會在每次重新部署時被清空（跟之前記憶體版本同樣的問題）。
# 沒有掛 Volume 時會自動退回容器內的暫存路徑，程式仍可正常運作，只是一樣不會跨部署保留。
DB_PATH = "/data/tradeai.db" if os.path.isdir("/data") else "/tmp/tradeai.db"

# 優化：原本db_save/db_load每次呼叫都各自開一個新的SQLite連線、用完就關掉——main.py一整個
# 交易日下來會呼叫db_save()好幾百次(每60秒一次的定期持久化_periodic_persist，加上main.py裡
# 另外15個地方在特定事件發生時——每筆交易平倉、每次設定變更——額外觸發的即時持久化)，每次都
# 重新開關連線是不必要的反覆開銷；在Railway Volume這種網路掛載儲存上，連線開啟的延遲又比
# 本機磁碟更明顯。改成模組層級維護一個共用連線，整個process生命週期正常情況下只開一次。
# 執行緒安全：main.py的排程器已經改成只用1個工作執行緒(見main.py排程器設定的註解)，不會有
# 多執行緒同時呼叫db_save/db_load的情況，但這裡仍然加鎖+check_same_thread=False保險——
# 一來避免初次建立連線時的競爭(雖然目前不會真的並發呼叫，但寫成不依賴這個前提更穩妥)，
# 二來避免未來如果排程器設定被改回多執行緒，連線物件被不同執行緒呼叫時直接因為sqlite3
# 預設的單執行緒限制而拋例外。
_conn = None
_conn_lock = threading.Lock()

def _get_conn():
    global _conn
    if _conn is None:
        with _conn_lock:
            if _conn is None:  # double-checked locking：避免兩個執行緒同時通過外層判斷、各自開一條連線
                _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return _conn

def _reset_conn():
    """連線疑似壞掉(操作拋例外)時呼叫——清掉目前的連線物件，下次_get_conn()會重新開一條新的，
    避免一次暫時性錯誤(例如Railway Volume短暫抖動)就讓接下來整個process的存檔/讀取永久壞掉。"""
    global _conn
    try:
        if _conn is not None: _conn.close()
    except Exception:
        pass
    _conn = None

def db_init():
    conn = _get_conn()
    conn.execute("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    conn.commit()
    using_volume = os.path.isdir("/data")
    logger.info(f"資料庫已初始化：{DB_PATH}（{'已使用Railway Volume，跨部署保留' if using_volume else '未掛載Volume，重新部署仍會清空'}）")

def db_save(key: str, value: dict):
    try:
        conn = _get_conn()
        conn.execute("INSERT INTO kv_store (key,value,updated_at) VALUES (?,?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                     (key, json.dumps(value), datetime.now().isoformat()))
        conn.commit()
    except Exception as e:
        logger.warning(f"資料庫寫入失敗 [{key}]: {e}")
        _reset_conn()  # 這次的連線狀態不可信，丟掉讓下次重開，不要讓一次失敗拖累後面所有呼叫

def db_load(key: str, default=None):
    try:
        conn = _get_conn()
        row = conn.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
        if row: return json.loads(row[0])
    except Exception as e:
        logger.warning(f"資料庫讀取失敗 [{key}]: {e}")
        _reset_conn()
    return default
