"""
同花顺「龙虎榜机构与游资观察」数据组装

使用 dragon-tiger-list 的 all / org / hot_money 三种榜单，并按 trade_date 对齐。
只做事实汇总：净额保留正负号，不把上榜或净买入扩写成推荐。
"""

from chanlun.tools.ths_fuyao import ThsFuyaoError, to_local_code

SOURCE_ENDPOINTS = [
    "/api/a-share/special-data/dragon-tiger-list?board_type=all",
    "/api/a-share/special-data/dragon-tiger-list?board_type=org",
    "/api/a-share/special-data/dragon-tiger-list?board_type=hot_money",
]

PASS_THROUGH_FIELDS = [
    "change",
    "net_value",
    "net_rate",
    "hot_rank",
    "buy_value",
    "sell_value",
    "limit_reason",
    "range_days",
]

ORG_FIELDS = [
    "org_net_value",
    "org_net_rate",
    "org_buy_num",
    "org_sell_num",
]

HOT_FIELDS = [
    "hot_money_net_value",
    "hot_money_net_rate",
]


def _num(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _concept_names(item) -> list[str]:
    concepts = item.get("concept_list")
    if not isinstance(concepts, list):
        return []
    names = []
    for concept in concepts:
        if isinstance(concept, dict):
            name = concept.get("name")
        else:
            name = concept
        if name and str(name).strip():
            names.append(str(name).strip())
    return names


def _merge_fields(record: dict, item: dict, fields: list[str]):
    for field in fields:
        value = item.get(field)
        if value is not None:
            record[field] = value


def _record_key(item: dict) -> tuple[str, int]:
    return (
        str(item.get("thscode") or ""),
        int(item.get("range_days") or 1),
    )


def _is_a_share(item: dict) -> bool:
    """过滤龙虎榜里的可转债/基金等非股票标的"""
    ticker = str(item.get("ticker") or "")
    if len(ticker) != 6:
        return False
    return ticker[:1] in {"0", "3", "4", "6", "8"} or ticker.startswith("92")


def _build_records(
    all_data: dict,
    org_data: dict,
    hot_data: dict,
) -> tuple[dict, list[dict]]:
    records: dict[tuple[str, int], dict] = {}

    for item in all_data.get("stock_items") or []:
        if not _is_a_share(item):
            continue
        key = _record_key(item)
        record = records.setdefault(
            key,
            {
                "thscode": item.get("thscode") or "",
                "ticker": item.get("ticker") or "",
                "name": item.get("name") or "",
                "range_days": key[1],
                "hot_seats": [],
                "concepts": [],
            },
        )
        _merge_fields(record, item, PASS_THROUGH_FIELDS + ORG_FIELDS + HOT_FIELDS)
        record["concepts"] = _concept_names(item)

    for item in org_data.get("stock_items") or []:
        if not _is_a_share(item):
            continue
        key = _record_key(item)
        record = records.setdefault(
            key,
            {
                "thscode": item.get("thscode") or "",
                "ticker": item.get("ticker") or "",
                "name": item.get("name") or "",
                "range_days": key[1],
                "hot_seats": [],
                "concepts": [],
            },
        )
        _merge_fields(record, item, PASS_THROUGH_FIELDS + ORG_FIELDS + HOT_FIELDS)
        if not record.get("concepts"):
            record["concepts"] = _concept_names(item)

    seats: list[dict] = []
    for group in hot_data.get("hot_money_items") or []:
        seat_name = group.get("name") or ""
        seat_rows = group.get("rows") or []
        seat_stocks = []
        seen_stocks = set()
        for item in seat_rows:
            if not _is_a_share(item):
                continue
            key = _record_key(item)
            record = records.setdefault(
                key,
                {
                    "thscode": item.get("thscode") or "",
                    "ticker": item.get("ticker") or "",
                    "name": item.get("name") or "",
                    "range_days": key[1],
                    "hot_seats": [],
                    "concepts": [],
                },
            )
            _merge_fields(record, item, PASS_THROUGH_FIELDS + ORG_FIELDS + HOT_FIELDS)
            if not record.get("concepts"):
                record["concepts"] = _concept_names(item)
            item_net = _num(item.get("hot_money_item_net_value"))
            item_rate = _num(item.get("hot_money_item_net_rate"))
            if seat_name and all(
                seat.get("name") != seat_name for seat in record["hot_seats"]
            ):
                record["hot_seats"].append(
                    {
                        "name": seat_name,
                        "net_value": item_net,
                        "net_rate": item_rate,
                    }
                )
            if item.get("thscode") not in seen_stocks:
                seen_stocks.add(item.get("thscode"))
                seat_stocks.append(
                    {
                        "code": to_local_code(item.get("thscode") or ""),
                        "ticker": item.get("ticker") or "",
                        "name": item.get("name") or "",
                        "net_value": item_net,
                        "net_rate": item_rate,
                        "range_days": key[1],
                    }
                )
        seat_stocks.sort(
            key=lambda row: (
                row.get("net_value") is None,
                -(row.get("net_value") or 0),
            )
        )
        if not seat_stocks:
            continue
        seats.append(
            {
                "name": seat_name,
                "net_value": _num(group.get("buying")),
                "stock_count": len(seen_stocks),
                "stocks": seat_stocks[:6],
            }
        )

    for record in records.values():
        if record.get("hot_money_net_value") is None and record.get("hot_seats"):
            seat_values = [
                seat.get("net_value")
                for seat in record["hot_seats"]
                if seat.get("net_value") is not None
            ]
            if seat_values:
                record["hot_money_net_value"] = sum(seat_values)
        record["code"] = to_local_code(record.get("thscode") or "")
        record["range_label"] = "3日榜" if record.get("range_days") == 3 else "1日榜"
        for field in (
            "change",
            "net_value",
            "net_rate",
            "hot_rank",
            "buy_value",
            "sell_value",
            "org_net_value",
            "org_net_rate",
            "org_buy_num",
            "org_sell_num",
            "hot_money_net_value",
            "hot_money_net_rate",
        ):
            record.setdefault(field, None)

    return records, seats


def _stock_primary_records(records: dict) -> list[dict]:
    by_code: dict[str, dict] = {}
    for record in records.values():
        code = record.get("thscode")
        current = by_code.get(code)
        if current is None or (
            record.get("range_days") == 1 and current.get("range_days") != 1
        ):
            by_code[code] = record

    stocks = []
    for record in by_code.values():
        stock = dict(record)
        stock["records"] = sorted(
            [
                item
                for item in records.values()
                if item.get("thscode") == record.get("thscode")
            ],
            key=lambda item: item.get("range_days") or 1,
        )
        stocks.append(stock)
    return stocks


def _one_day_records(records: dict) -> list[dict]:
    one_day = [
        record for record in records.values() if record.get("range_days") == 1
    ]
    return one_day or list(records.values())


def _sum(items: list[dict], key: str) -> float:
    total = 0.0
    for item in items:
        value = _num(item.get(key))
        if value is not None:
            total += value
    return total


def _sort_seats(seats: list[dict]) -> list[dict]:
    result = sorted(
        seats,
        key=lambda seat: (
            seat.get("net_value") is None,
            -(seat.get("net_value") or 0),
        ),
    )
    return result


def build_dragon_tiger_watch(
    all_data: dict,
    org_data: dict,
    hot_data: dict,
) -> dict:
    trade_date = all_data.get("trade_date") or ""
    warnings = []
    if not trade_date:
        raise ThsFuyaoError(3002, "龙虎榜暂无可用交易日")

    if org_data.get("trade_date") and org_data.get("trade_date") != trade_date:
        warnings.append(
            f"机构榜交易日 {org_data.get('trade_date')} 与全榜 {trade_date} 不一致"
        )
    if hot_data.get("trade_date") and hot_data.get("trade_date") != trade_date:
        warnings.append(
            f"游资榜交易日 {hot_data.get('trade_date')} 与全榜 {trade_date} 不一致"
        )
    if not org_data.get("stock_items"):
        warnings.append("机构榜暂无股票记录")
    if not hot_data.get("hot_money_items"):
        warnings.append("游资榜暂无席位记录")

    records, seats = _build_records(all_data, org_data, hot_data)
    records = records or {}
    stocks = _stock_primary_records(records)
    one_day_records = _one_day_records(records)
    seats = _sort_seats(seats)

    org_stock_count = sum(
        1 for stock in stocks if stock.get("org_net_value") is not None
    )
    hot_stock_count = sum(
        1
        for stock in stocks
        if stock.get("hot_money_net_value") is not None or stock.get("hot_seats")
    )

    stats = {
        "stock_count": len(stocks),
        "record_count": len(records),
        "buy_total": _sum(one_day_records, "buy_value"),
        "sell_total": _sum(one_day_records, "sell_value"),
        "net_total": _sum(one_day_records, "net_value"),
        "org_net_total": _sum(one_day_records, "org_net_value"),
        "hot_net_total": _sum(one_day_records, "hot_money_net_value"),
        "org_stock_count": org_stock_count,
        "hot_stock_count": hot_stock_count,
        "seat_count": len(seats),
    }

    return {
        "trade_date": trade_date,
        "data_time_ms": all_data.get("timestamp"),
        "mode": "real",
        "is_simulated": False,
        "stats": stats,
        "stocks": stocks,
        "seats": seats,
        "warnings": warnings,
        "sources": SOURCE_ENDPOINTS,
        "definitions": {
            "net": "净额 = 买入额 - 卖出额；保留正负号，不把净买入扩写为推荐",
            "board": "全部榜 / 机构榜 / 游资榜均来自 dragon-tiger-list 的对应 board_type",
            "filter": "仅统计 A 股股票，过滤龙虎榜中的可转债/基金等非股票标的",
            "org": "机构净额为机构专用席位买入减卖出；机构家数来自 org_buy_num / org_sell_num",
            "hot_money": "游资净额为游资席位在个股上的净买入；席位聚合值来自 hot_money_items[].buying",
            "range": "同一股票可能同时存在 1 日榜和 3 日榜，汇总金额优先按 1 日榜避免重复",
            "date": "交易日来自接口返回的 trade_date，不使用“今天”作为默认假设",
        },
    }


def get_dragon_tiger_watch(client) -> dict:
    """获取三种龙虎榜并按同一交易日组装看板"""
    all_data = client.dragon_tiger_list(board_type="all")
    trade_date = all_data.get("trade_date")
    if not trade_date:
        raise ThsFuyaoError(3002, "龙虎榜暂无可用交易日")

    try:
        org_data = client.dragon_tiger_list(board_type="org", date=trade_date)
    except ThsFuyaoError as e:
        org_data = {"trade_date": trade_date, "warnings": [e.message]}
    try:
        hot_data = client.dragon_tiger_list(board_type="hot_money", date=trade_date)
    except ThsFuyaoError as e:
        hot_data = {"trade_date": trade_date, "warnings": [e.message]}

    data = build_dragon_tiger_watch(all_data, org_data, hot_data)
    for source in (org_data, hot_data):
        for warning in source.get("warnings") or []:
            data["warnings"].append(warning)
    return data
