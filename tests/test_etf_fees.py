"""
tests/test_etf_fees.py — 鎖住「ETF跟個股的當沖證交稅率不一樣」這個規則。

背景：00631L(元大台灣50正2)、009816(凱基台灣TOP50)加入交易池後才發現的bug——
calc_round_trip_cost()/min_profitable_move_pct()原本不分股票/ETF，一律套用個股當沖稅率0.15%，
但台股ETF當沖實際稅率是0.1%(這是ETF本來就有的優惠稅率，「當沖證交稅減半」這個規則不適用在ETF上，
ETF不管是不是當沖都是0.1%)。一律用0.15%算，會讓這兩檔ETF的模擬損益/可行性門檻系統性偏嚴格。
已經在config.py加上is_etf()判斷修好，這支測試鎖住這個規則，避免以後又被改回「不分股票/ETF」。

執行方式：
    pip install pytest --break-system-packages
    cd tradeai-backend && pytest tests/ -v
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import calc_round_trip_cost, min_profitable_move_pct, is_etf  # noqa: E402


def test_is_etf_recognizes_00_prefix():
    assert is_etf("00631L") is True
    assert is_etf("009816") is True
    assert is_etf("0050") is True
    assert is_etf("2330") is False
    assert is_etf("2454") is False


def test_is_etf_strips_market_suffix():
    assert is_etf("00631L.TW") is True
    assert is_etf("2330.TW") is False


def test_etf_tax_is_lower_than_stock_tax():
    """核心回歸測試：同樣的進出場價格/股數，ETF的稅務成本必須比個股低，
    因為ETF當沖稅率0.1%，個股當沖稅率0.15%——這就是00631L/009816加入後才發現的那個bug。"""
    stock_cost = calc_round_trip_cost(100, 101, 1000, fee_discount=0.6, symbol="2330")
    etf_cost   = calc_round_trip_cost(100, 101, 1000, fee_discount=0.6, symbol="00631L")
    assert etf_cost["tax"] < stock_cost["tax"]
    assert etf_cost["total_cost"] < stock_cost["total_cost"]
    # 手續費(buy_fee/sell_fee)不該因為是不是ETF而不同，只有稅率(tax)有差
    assert etf_cost["buy_fee"] == stock_cost["buy_fee"]
    assert etf_cost["sell_fee"] == stock_cost["sell_fee"]


def test_calc_round_trip_cost_without_symbol_keeps_old_behavior():
    """沒有傳symbol時(舊呼叫方式)要退回個股稅率，向後相容，不能因為這次修改讓既有呼叫端悄悄變了行為。"""
    no_symbol = calc_round_trip_cost(100, 101, 1000, fee_discount=0.6)
    stock     = calc_round_trip_cost(100, 101, 1000, fee_discount=0.6, symbol="2330")
    assert no_symbol["tax"] == stock["tax"]


def test_min_profitable_move_pct_etf_is_lower():
    stock_threshold = min_profitable_move_pct(0.6, symbol="2330")
    etf_threshold   = min_profitable_move_pct(0.6, symbol="00631L")
    assert etf_threshold < stock_threshold
    assert round(stock_threshold, 2) == 0.32
    assert round(etf_threshold, 2) == 0.27
