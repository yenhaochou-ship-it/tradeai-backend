"""
state.py — 跨模組共用的可變狀態。

只放price_cache，因為這是唯一一個被多個模組(signals.py的advanced_score/get_market_context、
ml_model.py的extract_ml_features)直接讀取的全域字典，其他像auto_state這種主要只在main.py
本體的交易迴圈裡使用的狀態，刻意不搬過來——用得到的地方用explicit參數傳，不要為了搬而搬，
動得越少，風險越低。

⚠️ 重要：這裡只能用「就地修改」(price_cache[sym]=...、.update()、.clear())，
不能整個重新賦值(price_cache={...})，否則其他檔案手上拿著的會是舊的字典物件，
新賦值不會反映過去，等於資料同步斷掉——這就是Python多模組共享可變狀態時最容易踩的坑。
main.py/signals.py/ml_model.py都用 `from state import price_cache` 拿到同一個物件參照。
"""
from typing import List, Dict

price_cache: Dict[str, List[Dict]] = {}
