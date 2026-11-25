"""Synthetic fixtures for a separate, clearly labeled SQLite preview."""

from datetime import date, timedelta
from decimal import Decimal
from urllib.parse import unquote


def seed_demo(store, profiles):
    db_file = unquote(store.engine.url.database or "")
    if store.backend != "sqlite" or "demo" not in db_file.rsplit("/", 1)[-1].lower():
        raise ValueError("请为演示使用文件名含 demo 的独立 SQLite 库")
    data = {"stock_basic": [], "daily": [], "daily_basic": [], "adj_factor": [], "index_daily": []}
    instruments = [("600001.SH", "甲辰科技", "SSE", Decimal("18.65")),
                   ("000001.SZ", "海原金融", "SZSE", Decimal("27.48")),
                   ("920001.BJ", "北曜电气", "BSE", Decimal("12.40"))]
    trades = []
    day = date(2026, 10, 9)
    for offset in range(44):
        current = day + timedelta(days=offset)
        if current.weekday() < 5:
            trades.append(current.strftime("%Y%m%d"))
    for index, (code, name, exchange, base) in enumerate(instruments):
        data["stock_basic"].append({"ts_code": code, "symbol": code.split(".")[0], "name": name, "exchange": exchange,
                                    "market": "主板" if index < 2 else "北交所", "list_status": "L", "list_date": "20200102",
                                    "area": ["上海", "广东", "北京"][index], "industry": ["电子", "金融", "电气设备"][index]})
        previous = base
        for sequence, trade in enumerate(trades):
            close = base + Decimal(sequence) * Decimal("0.09") + Decimal((sequence * 13 + index * 7) % 17 - 8) / 10
            open_price = previous + Decimal((sequence * 7) % 5 - 2) / 10
            data["daily"].append({"ts_code": code, "trade_date": trade, "open": open_price, "high": max(close, open_price) + Decimal("0.32"),
                                  "low": min(close, open_price) - Decimal("0.27"), "close": close, "pre_close": previous,
                                  "change": close - previous, "pct_chg": (((close - previous) / previous) * 100).quantize(Decimal(".0001")),
                                  "vol": 120000 + sequence * 2371, "amount": (close * (120000 + sequence * 2371)).quantize(Decimal(".01"))})
            data["daily_basic"].append({"ts_code": code, "trade_date": trade, "close": close, "turnover_rate": Decimal("1.35"),
                                        "volume_ratio": Decimal("0.96"), "pe": Decimal("19.51"), "pb": Decimal("2.11")})
            data["adj_factor"].append({"ts_code": code, "trade_date": trade, "adj_factor": Decimal("1.0000")})
            previous = close
    for sequence, trade in enumerate(trades):
        close = Decimal("3412.55") + Decimal(sequence * 265 + (sequence % 7 - 3) * 630) / 100
        data["index_daily"].append({"ts_code": "000001.SH", "trade_date": trade, "open": close - 11, "high": close + 8,
                                    "low": close - 16, "close": close, "pre_close": close - 5, "change": 5,
                                    "pct_chg": Decimal("0.1463"), "vol": 3100000 + sequence * 22700, "amount": 35241000 + sequence * 100500})
    data["cn_cpi"] = [{"month": f"2026{month:02d}", "nt_val": Decimal("101.1") + Decimal(month % 3) / 10,
                       "nt_yoy": Decimal("1.10") + Decimal(month % 4) / 10, "nt_mom": Decimal("0.20"),
                       "nt_accu": Decimal("101.34")} for month in range(1, 11)]
    result = {}
    for dataset_id, rows in data.items():
        if dataset_id in profiles:
            report = store.commit_partition(profiles[dataset_id], f"synthetic-{dataset_id}", {"source": "synthetic"}, rows)
            result[dataset_id] = report["row_count"]
    return {"synthetic": True, "datasets": result}
