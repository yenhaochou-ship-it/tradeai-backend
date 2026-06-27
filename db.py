"""
db.py — SQLite持久化(Railway Volume)。從main.py拆出來，這一塊跟交易邏輯完全無關，
獨立測試/替換儲存後端(例如以後想換成Postgres)都不會牽動到main.py其他部分。
"""
import os, sqlite3, json, logging
from datetime import datetime

logger = logging.getLogger(__name__)

# 重要：必須在 Railway 後台幫這個服務「掛載一個 Volume」，路徑設為 /data，
# 否則資料庫檔案還是會在每次重新部署時被清空（跟之前記憶體版本同樣的問題）。
# 沒有掛 Volume 時會自動退回容器內的暫存路徑，程式仍可正常運作，只是一樣不會跨部署保留。
DB_PATH = "/data/tradeai.db" if os.path.isdir("/data") else "/tmp/tradeai.db"

def db_init():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    conn.commit()
    conn.close()
    using_volume = os.path.isdir("/data")
    logger.info(f"資料庫已初始化：{DB_PATH}（{'已使用Railway Volume，跨部署保留' if using_volume else '未掛載Volume，重新部署仍會清空'}）")

def db_save(key: str, value: dict):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO kv_store (key,value,updated_at) VALUES (?,?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                     (key, json.dumps(value), datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"資料庫寫入失敗 [{key}]: {e}")

def db_load(key: str, default=None):
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
        conn.close()
        if row: return json.loads(row[0])
    except Exception as e:
        logger.warning(f"資料庫讀取失敗 [{key}]: {e}")
    return default
