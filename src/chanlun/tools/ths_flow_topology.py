"""
同花顺「龙虎榜资金流向拓扑台」数据组装

逐日读取近一年交易日历中的最近 N 个交易日，分别请求龙虎榜 all / org / hot_money，
默认只纳入 range_days=1。股票有多个概念时按概念数等分净额，保证概念聚合总额守恒。
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from chanlun.tools.ths_dragon_tiger import _is_a_share
from chanlun.tools.ths_fuyao import ThsFuyaoError, to_local_code

DEFAULT_DAYS = 20
RANGE_OPTIONS = [5, 10, 20]
BOARDS = ["all", "org", "hot"]
SOURCE_ENDPOINTS = [
    "/api/a-share/calendar/trading-days",
    "/api/a-share/special-data/dragon-tiger-list?board_type=all",
    "/api/a-share/special-data/dragon-tiger-list?board_type=org",
    "/api/a-share/special-data/dragon-tiger-list?board_type=hot_money",
]


def _num(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _concept_names(item) -> list[str]:
    raw = item.get("concept_list")
    if not isinstance(raw, list):
        return []
    names = []
    for concept in raw:
        if isinstance(concept, dict):
            name = concept.get("name")
        else:
            name = concept
        if name and str(name).strip() and str(name).strip() not in names:
            names.append(str(name).strip())
    return names


def _safe_float(value) -> float:
    number = _num(value)
    return 0.0 if number is None else number


def _call_board(client, board: str, date: str) -> dict:
    board_type = "hot_money" if board == "hot" else board
    for attempt in range(3):
        try:
            return client.dragon_tiger_list(board_type=board_type, date=date)
        except ThsFuyaoError as e:
            if e.code == 4001 and attempt < 2:
                time.sleep(0.8 * (attempt + 1))
                continue
            raise
    return {}


def _fetch_days(client, dates: list[str]) -> tuple[dict, list[str]]:
    """并发读取每个交易日的全部/机构/游资榜"""
    data = {board: {} for board in BOARDS}
    warnings = []
    failures = 0
    tasks = [(date, board) for date in dates for board in BOARDS]
    with ThreadPoolExecutor(max_workers=3) as pool:
        future_map = {
            pool.submit(_call_board, client, board, date): (date, board)
            for date, board in tasks
        }
        for future in as_completed(future_map):
            date, board = future_map[future]
            try:
                data[board][date] = future.result() or {}
            except ThsFuyaoError:
                failures += 1
            except Exception:
                failures += 1
    if failures:
        warnings.append(f"{failures} 个交易日/榜单请求失败，缺失部分已跳过")
    return data, warnings


def _new_board(days: int) -> dict:
    return {
        "daily": {},
        "stock_daily": {},
        "stats_daily": [
            {
                "codes": set(),
                "buy": 0.0,
                "sell": 0.0,
                "net": 0.0,
                "org": 0.0,
                "hot": 0.0,
            }
            for _ in range(days)
        ],
    }


def _add_concept(board: dict, concept: str, day_index: int, amount: float):
    board["daily"].setdefault(
        concept, [0.0] * len(board["stats_daily"])
    )
    board["daily"][concept][day_index] += amount


def _add_stock(
    board: dict,
    concept: str,
    code: str,
    day_index: int,
    amount: float,
):
    stocks = board["stock_daily"].setdefault(concept, {})
    stocks.setdefault(code, [0.0] * len(board["stats_daily"]))
    stocks[code][day_index] += amount


def _add_stats(
    board: dict,
    day_index: int,
    code: str,
    buy: float,
    sell: float,
    net: float,
    org: float,
    hot: float,
):
    stats = board["stats_daily"][day_index]
    stats["codes"].add(code)
    stats["buy"] += buy
    stats["sell"] += sell
    stats["net"] += net
    stats["org"] += org
    stats["hot"] += hot


def _top_concepts(board: dict, limit: int = 10) -> list[str]:
    scores = {}
    for concept, values in board["daily"].items():
        cumulative = 0.0
        max_abs = 0.0
        for value in values:
            cumulative += value
            max_abs = max(max_abs, abs(cumulative))
        scores[concept] = max_abs
    # “其他”是未分类标的的守恒桶，不参与概念排行
    candidates = [concept for concept in scores if concept != "其他"] or list(scores)
    return sorted(candidates, key=scores.get, reverse=True)[:limit]


def _top_stocks_for_concept(
    board: dict, concept: str, limit: int = 20
) -> dict[str, list[float]]:
    stocks = board["stock_daily"].get(concept) or {}
    scores = {}
    for code, values in stocks.items():
        cumulative = 0.0
        max_abs = 0.0
        for value in values:
            cumulative += value
            max_abs = max(max_abs, abs(cumulative))
        scores[code] = max_abs
    selected = sorted(scores, key=scores.get, reverse=True)[:limit]
    return {code: stocks[code] for code in selected}


def _range_stats(board: dict, dates: list[str], days: int) -> dict:
    count = min(days, len(dates))
    start = len(dates) - count
    rows = board["stats_daily"][start:]
    codes = set()
    for row in rows:
        codes.update(row["codes"])
    return {
        "days": count,
        "stock_count": len(codes),
        "buy_total": sum(row["buy"] for row in rows),
        "sell_total": sum(row["sell"] for row in rows),
        "net_total": sum(row["net"] for row in rows),
        "org_net_total": sum(row["org"] for row in rows),
        "hot_net_total": sum(row["hot"] for row in rows),
    }


def _build_board_output(
    board: dict,
    dates: list[str],
    all_board: bool = False,
) -> dict:
    concepts = _top_concepts(board)
    daily = {concept: board["daily"][concept] for concept in concepts}
    stock_daily = {
        concept: _top_stocks_for_concept(board, concept) for concept in concepts
    }
    range_stats = {
        str(days): _range_stats(board, dates, days) for days in RANGE_OPTIONS
    }

    output = {
        "concepts": concepts,
        "daily": daily,
        "stock_daily": stock_daily,
        "stats_by_range": range_stats,
        "latest_stats": _range_stats(board, dates, 1),
    }
    if all_board:
        output["category_daily"] = board.get("category_daily", {})
    return output


def build_flow_topology(
    all_days: dict,
    org_days: dict,
    hot_days: dict,
    dates: list[str],
    calendar_count: int,
    latest_timestamp: int | None,
    fetch_warnings: list[str] | None = None,
) -> dict:
    if not dates:
        raise ThsFuyaoError(3002, "交易日历中没有可用的交易日")

    boards = {board: _new_board(len(dates)) for board in BOARDS}
    boards["all"]["category_daily"] = {}
    stock_meta: dict[str, dict] = {}
    warnings = list(fetch_warnings or [])

    def ensure_meta(item: dict, concepts: list[str]) -> str:
        code = to_local_code(item.get("thscode") or "")
        if not code:
            return ""
        meta = stock_meta.setdefault(
            code,
            {
                "code": code,
                "ticker": item.get("ticker") or "",
                "name": item.get("name") or "",
                "concepts": [],
            },
        )
        if not meta.get("name"):
            meta["name"] = item.get("name") or ""
        for concept in concepts:
            if concept not in meta["concepts"]:
                meta["concepts"].append(concept)
        return code

    for index, date in enumerate(dates):
        # 全部榜：用于全市场概念轨迹，以及机构/游资/其他的来源拆分
        for item in (all_days.get(date) or {}).get("stock_items") or []:
            if not _is_a_share(item) or int(item.get("range_days") or 1) != 1:
                continue
            amount = _num(item.get("net_value"))
            if amount is None:
                continue
            concepts = _concept_names(item) or ["其他"]
            code = ensure_meta(item, concepts)
            if not code:
                continue
            org = _num(item.get("org_net_value")) or 0.0
            hot = _num(item.get("hot_money_net_value")) or 0.0
            other = amount - org - hot
            share = amount / len(concepts)
            for concept in concepts:
                _add_concept(boards["all"], concept, index, share)
                _add_stock(boards["all"], concept, code, index, share)
                category = boards["all"]["category_daily"].setdefault(
                    concept,
                    {
                        "org": [0.0] * len(dates),
                        "hot": [0.0] * len(dates),
                        "other": [0.0] * len(dates),
                    },
                )
                category["org"][index] += org / len(concepts)
                category["hot"][index] += hot / len(concepts)
                category["other"][index] += other / len(concepts)
            _add_stats(
                boards["all"],
                index,
                code,
                _safe_float(item.get("buy_value")),
                _safe_float(item.get("sell_value")),
                amount,
                org,
                hot,
            )

        # 机构榜：机构专用席位净额
        for item in (org_days.get(date) or {}).get("stock_items") or []:
            if not _is_a_share(item) or int(item.get("range_days") or 1) != 1:
                continue
            amount = _num(item.get("org_net_value"))
            if amount is None:
                continue
            concepts = _concept_names(item) or ["其他"]
            code = ensure_meta(item, concepts)
            if not code:
                continue
            share = amount / len(concepts)
            for concept in concepts:
                _add_concept(boards["org"], concept, index, share)
                _add_stock(boards["org"], concept, code, index, share)
            _add_stats(
                boards["org"],
                index,
                code,
                _safe_float(item.get("buy_value")),
                _safe_float(item.get("sell_value")),
                amount,
                amount,
                0.0,
            )

        # 游资榜：hot_money_items 下按股票汇总各席位净额
        for group in (hot_days.get(date) or {}).get("hot_money_items") or []:
            for item in group.get("rows") or []:
                if not _is_a_share(item) or int(item.get("range_days") or 1) != 1:
                    continue
                amount = _num(item.get("hot_money_item_net_value"))
                if amount is None:
                    continue
                concepts = _concept_names(item) or ["其他"]
                code = ensure_meta(item, concepts)
                if not code:
                    continue
                share = amount / len(concepts)
                for concept in concepts:
                    _add_concept(boards["hot"], concept, index, share)
                    _add_stock(boards["hot"], concept, code, index, share)
                _add_stats(
                    boards["hot"],
                    index,
                    code,
                    _safe_float(item.get("buy_value")),
                    _safe_float(item.get("sell_value")),
                    amount,
                    0.0,
                    amount,
                )

    output_boards = {}
    for board in BOARDS:
        output_boards[board] = _build_board_output(
            boards[board],
            dates,
            all_board=board == "all",
        )

    # 只保留真正出现在轨迹/个股贡献里的股票元信息
    used_codes = set()
    for board in output_boards.values():
        for concept_stocks in board["stock_daily"].values():
            used_codes.update(concept_stocks.keys())
    stock_meta = {
        code: meta for code, meta in stock_meta.items() if code in used_codes
    }

    if not any(boards[board]["daily"] for board in BOARDS):
        warnings.append("最近交易日区间内没有可用的 1 日榜股票记录")

    return {
        "trade_date": dates[-1],
        "window_start": dates[0],
        "dates": dates,
        "calendar_count": calendar_count,
        "range_options": RANGE_OPTIONS,
        "data_time_ms": latest_timestamp,
        "mode": "real",
        "is_simulated": False,
        "boards": output_boards,
        "stock_meta": stock_meta,
        "warnings": warnings,
        "sources": SOURCE_ENDPOINTS,
        "definitions": {
            "window": "从近一年交易日历中取最近 20 个交易日，页面可切换 5/10/20 日窗口",
            "range": "仅纳入 range_days=1 的 1 日榜记录，避免 1 日榜与 3 日榜重复",
            "split": "一只股票有多个概念时按概念数等分净额，概念聚合总额守恒；未分类标的归入“其他”，不参与概念排行",
            "board": "全部榜 / 机构榜 / 游资榜分别来自 dragon-tiger-list 的对应 board_type",
            "category": "机构/游资/其他来源取全榜行内字段归因，其他 = 全榜净额 - 机构净额 - 游资净额",
            "flow": "轨迹从窗口起点累计，端点标签做防碰撞错位；路径宽度按绝对净额，颜色区分正负",
        },
    }


def get_flow_topology(client, days: int = DEFAULT_DAYS) -> dict:
    latest = client.dragon_tiger_list(board_type="all")
    latest_date = latest.get("trade_date") or ""
    if not latest_date:
        raise ThsFuyaoError(3002, "龙虎榜暂无可用交易日")

    calendar = client.trading_days()
    all_dates = []
    for item in calendar.get("item") or []:
        raw = str(item.get("date") or "")
        if len(raw) == 8 and raw.isdigit():
            all_dates.append(f"{raw[:4]}-{raw[4:6]}-{raw[6:]}")
    all_dates = sorted(set(all_dates))
    all_dates = [date for date in all_dates if date <= latest_date]
    if not all_dates:
        all_dates = [latest_date]
    window_days = max(5, min(20, int(days or DEFAULT_DAYS)))
    dates = all_dates[-window_days:]

    fetched, warnings = _fetch_days(client, dates)
    timestamp = latest.get("timestamp")
    for board in BOARDS:
        for data in fetched[board].values():
            if data.get("timestamp"):
                timestamp = max(timestamp or 0, int(data["timestamp"]))
    return build_flow_topology(
        fetched["all"],
        fetched["org"],
        fetched["hot"],
        dates,
        len(all_dates),
        timestamp,
        warnings,
    )
