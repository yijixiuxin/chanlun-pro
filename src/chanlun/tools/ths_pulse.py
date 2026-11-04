"""
同花顺「涨停情绪市场脉冲屏」数据组装

数据只来自：
  GET /api/a-share/special-data/limit-up-pool
  GET /api/a-share/special-data/limit-up-ladder

不使用炸板池，不把涨停原因改写为行业分布；晋级率仅在天梯样本内按股票代码估算。
"""

import re

from chanlun.tools.ths_fuyao import ThsFuyaoError, to_local_code

BOARD_LEVELS = [
    ("two_board", "2板"),
    ("three_board", "3板"),
    ("four_board", "4板"),
    ("five_board", "5板"),
    ("six_board", "6板"),
    ("seven_over", "7板+"),
]

POOL_LEVELS = [
    ("one", "首板", 1),
    ("two", "2板", 2),
    ("three", "3板", 3),
    ("four", "4板", 4),
    ("five", "5板", 5),
    ("six_plus", "6板+", 6),
]

SEAL_BUCKETS = [
    ("< 0.5 亿", 0, 50000000),
    ("0.5 ~ 1 亿", 50000000, 100000000),
    ("1 ~ 3 亿", 100000000, 300000000),
    ("3 ~ 5 亿", 300000000, 500000000),
    ("5 亿以上", 500000000, None),
]

SOURCE_ENDPOINTS = [
    "/api/a-share/special-data/limit-up-pool",
    "/api/a-share/special-data/limit-up-ladder",
]


def _norm_date(value) -> str:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _time_to_minutes(value) -> int | None:
    if not value:
        return None
    match = re.match(r"^(\d{1,2}):(\d{2})", str(value))
    if not match:
        return None
    return int(match.group(1)) * 60 + int(match.group(2))


def _pct(numerator, denominator) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator * 100


def _sum_values(items, key) -> float:
    return sum(_float(item.get(key)) or 0 for item in items)


def _pool_level(item) -> str:
    count = _int(item.get("continue_day_cnt")) or 1
    if count <= 1:
        return "one"
    if count == 2:
        return "two"
    if count == 3:
        return "three"
    if count == 4:
        return "four"
    if count == 5:
        return "five"
    return "six_plus"


def _fetch_pool(client, date: str) -> tuple[list[dict], int, int | None]:
    """取完涨停池分页，返回 items、total、数据时间"""
    first = client.limit_up_pool(date=date, page=1, size=200)
    items = list(first.get("item") or [])
    pagination = first.get("pagination") or {}
    pages = _int(pagination.get("pages")) or 1
    total = _int(pagination.get("total"))
    if total is None:
        total = len(items)

    for page in range(2, max(1, pages) + 1):
        if len(items) >= total:
            break
        extra = client.limit_up_pool(date=date, page=page, size=200)
        items.extend(extra.get("item") or [])

    return items, total, _int(first.get("timestamp"))


def _pool_stats(items: list[dict], total: int) -> dict:
    sampled = len(items)
    early_count = sum(
        1
        for item in items
        if (_time_to_minutes(item.get("limit_up_time")) or 24 * 60) <= 10 * 60
    )
    board_counts = {key: 0 for key, _, _ in POOL_LEVELS}
    for item in items:
        board_counts[_pool_level(item)] += 1

    max_board = max(
        [_int(item.get("continue_day_cnt")) or 1 for item in items] or [0]
    )
    continue_count = sum(
        1 for item in items if (_int(item.get("continue_day_cnt")) or 1) >= 2
    )
    seal_total = _sum_values(items, "seal_money")
    max_seal_total = _sum_values(items, "max_seal_money")
    early_rate = _pct(early_count, sampled)
    seal_retention = _pct(seal_total, max_seal_total)

    return {
        "limit_up_count": total,
        "sampled_count": sampled,
        "complete": sampled >= total,
        "continue_count": continue_count,
        "max_board": max_board,
        "early_count": early_count,
        "early_rate": early_rate,
        "seal_total": seal_total,
        "max_seal_total": max_seal_total,
        "seal_retention": seal_retention,
        "board_counts": board_counts,
    }


def _delta(current, previous):
    if current is None or previous is None:
        return None
    return current - previous


def _ladder_rows(ladder: dict) -> tuple[list[str], list[dict], dict]:
    """返回升序日期、每日板位数量、每日板位股票代码"""
    item_by_date = {
        _norm_date(item.get("date")): item for item in ladder.get("item") or []
    }
    window_dates = [
        _norm_date(date) for date in (ladder.get("window") or {}).get("date_list") or []
    ]
    item_dates = list(item_by_date.keys())
    dates = sorted({date for date in window_dates + item_dates if date})
    rows = []
    codes = {}
    for date in dates:
        boards = (item_by_date.get(date) or {}).get("boards") or {}
        row = {"date": date, "levels": {}, "total": 0}
        codes[date] = {}
        for key, _ in BOARD_LEVELS:
            members = boards.get(key) or []
            row["levels"][key] = len(members)
            row["total"] += len(members)
            codes[date][key] = {
                str(member.get("thscode") or "") for member in members
            }
        rows.append(row)
    # 开盘前/数据未就绪时，窗口末尾可能是全 0 日期，避免矩阵和晋级率出现空档
    while rows and rows[-1]["total"] == 0:
        empty_date = rows.pop()["date"]
        codes.pop(empty_date, None)
        if empty_date in dates:
            dates.remove(empty_date)
    return dates, rows, codes


def _promotion_stats(dates: list[str], codes: dict) -> dict:
    """天梯样本内估算：N板 -> 次日 N+1板，按股票代码匹配"""
    by_date = []
    promoted_total = 0
    eligible_total = 0
    source_keys = [key for key, _ in BOARD_LEVELS[:-1]]
    for index, date in enumerate(dates):
        next_date = dates[index + 1] if index + 1 < len(dates) else None
        if next_date is None:
            by_date.append(
                {"date": date, "rate": None, "eligible": 0, "promoted": 0}
            )
            continue
        next_has_data = any(
            codes.get(next_date, {}).get(key) for key, _ in BOARD_LEVELS
        )
        if not next_has_data:
            by_date.append(
                {"date": date, "rate": None, "eligible": 0, "promoted": 0}
            )
            continue
        promoted = 0
        eligible = 0
        for source_index, key in enumerate(source_keys):
            next_key = BOARD_LEVELS[source_index + 1][0]
            source_codes = codes.get(date, {}).get(key) or set()
            target_codes = codes.get(next_date, {}).get(next_key) or set()
            eligible += len(source_codes)
            promoted += len(source_codes & target_codes)
        promoted_total += promoted
        eligible_total += eligible
        by_date.append(
            {
                "date": date,
                "rate": _pct(promoted, eligible),
                "eligible": eligible,
                "promoted": promoted,
            }
        )

    return {
        "rate": _pct(promoted_total, eligible_total),
        "eligible": eligible_total,
        "promoted": promoted_total,
        "by_date": by_date,
        "note": "天梯样本内按股票代码匹配 N板→次日N+1板，非全市场晋级率",
    }


def _ladder_panel(items: list[dict]) -> list[dict]:
    groups = {key: [] for key, _, _ in POOL_LEVELS}
    for item in items:
        groups[_pool_level(item)].append(item)

    panel = []
    for key, label, _ in POOL_LEVELS:
        members = sorted(
            groups[key],
            key=lambda item: _float(item.get("seal_money")) or 0,
            reverse=True,
        )
        top = members[:4]
        panel.append(
            {
                "key": key,
                "label": label,
                "count": len(members),
                "items": [
                    {
                        "code": to_local_code(item.get("thscode") or ""),
                        "ticker": item.get("ticker") or "",
                        "name": item.get("name") or "",
                        "seal_money": _float(item.get("seal_money")),
                        "continue_day_text": item.get("continue_day_text") or "",
                    }
                    for item in top
                ],
                "remaining": max(0, len(members) - len(top)),
            }
        )
    return panel


def _seal_buckets(items: list[dict]) -> list[dict]:
    buckets = [
        {"label": label, "count": 0, "amount": 0.0}
        for label, _, _ in SEAL_BUCKETS
    ]
    for item in items:
        amount = _float(item.get("seal_money"))
        if amount is None:
            continue
        for index, (_, lower, upper) in enumerate(SEAL_BUCKETS):
            if amount >= lower and (upper is None or amount < upper):
                buckets[index]["count"] += 1
                buckets[index]["amount"] += amount
                break
    return buckets


def _reason_keywords(items: list[dict], limit: int = 10) -> list[dict]:
    counts: dict[str, int] = {}
    for item in items:
        reason = item.get("limit_up_reason") or ""
        if not reason:
            continue
        for token in re.split(r"[+、,，/;；|]+", reason):
            keyword = token.strip()
            if len(keyword) < 2:
                continue
            counts[keyword] = counts.get(keyword, 0) + 1
    result = [
        {"keyword": keyword, "count": count}
        for keyword, count in counts.items()
    ]
    result.sort(key=lambda item: (-item["count"], item["keyword"]))
    return result[:limit]


def _board_changes(board_counts: dict, previous_counts: dict) -> list[dict]:
    changes = []
    for key, label, _ in POOL_LEVELS:
        current = board_counts.get(key, 0)
        previous = previous_counts.get(key)
        changes.append(
            {
                "key": key,
                "label": label,
                "current": current,
                "previous": previous,
                "delta": _delta(current, previous),
            }
        )
    return changes


def get_market_pulse(client, date: str | None = None) -> dict:
    """读取涨停池与连板天梯，组装市场脉冲屏数据"""
    ladder = client.limit_up_ladder()
    date_list = [
        _norm_date(value)
        for value in (ladder.get("window") or {}).get("date_list") or []
    ]
    date_list = [value for value in date_list if value]
    if not date_list:
        raise ThsFuyaoError(3002, "连板天梯暂无可用交易日")

    warnings = []
    if date:
        selected_date = _norm_date(date)
        if selected_date not in date_list:
            selected_date = date_list[0]
        pool_items, pool_total, data_time_ms = _fetch_pool(client, selected_date)
    else:
        # 开盘前当天可能还没有涨停数据，默认取最近一个有数据的交易日
        selected_date = date_list[0]
        pool_items, pool_total, data_time_ms = [], 0, None
        for candidate in date_list[:5]:
            items, total, timestamp = _fetch_pool(client, candidate)
            if total > 0:
                selected_date = candidate
                pool_items, pool_total, data_time_ms = items, total, timestamp
                break
            if candidate == date_list[0]:
                pool_items, pool_total, data_time_ms = items, total, timestamp
        if selected_date != date_list[0]:
            warnings.append(
                f"{date_list[0]} 暂无涨停数据，已展示最近有数据的交易日 {selected_date}"
            )

    # 前一交易日：日期列表按最新在前
    selected_index = date_list.index(selected_date)
    previous_date = (
        date_list[selected_index + 1]
        if selected_index + 1 < len(date_list)
        else None
    )

    stats = _pool_stats(pool_items, pool_total)
    if pool_total == 0:
        warnings.append(f"{selected_date} 暂无涨停数据")
    previous_stats = None
    previous_counts = {}
    if previous_date:
        try:
            previous_items, previous_total, _ = _fetch_pool(client, previous_date)
            previous_stats = _pool_stats(previous_items, previous_total)
            previous_counts = previous_stats["board_counts"]
        except ThsFuyaoError as e:
            warnings.append(f"前一交易日涨停池暂不可用：{e.message}")
    else:
        warnings.append("没有可用的前一交易日数据")

    if not stats["complete"]:
        warnings.append(
            f"涨停池接口单页最多返回 200 条，本次仅取到 {stats['sampled_count']} / {pool_total} 条"
        )

    deltas = {}
    if previous_stats:
        for key in [
            "limit_up_count",
            "continue_count",
            "max_board",
            "early_rate",
            "seal_retention",
            "seal_total",
        ]:
            deltas[key] = _delta(stats.get(key), previous_stats.get(key))
    else:
        for key in [
            "limit_up_count",
            "continue_count",
            "max_board",
            "early_rate",
            "seal_retention",
            "seal_total",
        ]:
            deltas[key] = None

    dates, ladder_rows, codes = _ladder_rows(ladder)
    promotion = _promotion_stats(dates, codes)

    return {
        "trade_date": selected_date,
        "previous_date": previous_date,
        "date_list": date_list,
        "data_time_ms": data_time_ms,
        "mode": "real",
        "is_simulated": False,
        "stats": stats,
        "previous": previous_stats,
        "deltas": deltas,
        "board_changes": _board_changes(stats["board_counts"], previous_counts),
        "ladder": ladder_rows,
        "ladder_levels": [
            {"key": key, "label": label} for key, label in BOARD_LEVELS
        ],
        "promotion": promotion,
        "ladder_panel": _ladder_panel(pool_items),
        "seal_buckets": _seal_buckets(pool_items),
        "reasons": _reason_keywords(pool_items),
        "reason_total": len(pool_items),
        "definitions": {
            "early_seal_rate": "早封率 = 10:00 前涨停家数 / 涨停家数",
            "seal_retention": "封单留存 = Σ当前封单额 / Σ峰值封单额",
            "promotion": "晋级率 = 天梯样本内 N板→次日 N+1板 股票代码匹配，非全市场晋级率",
            "reason": "涨停原因来自 limit_up_reason 原始文本拆分，不是行业分类",
            "continue": "连板家数 = 连板天数 ≥ 2 的涨停股票数量",
        },
        "sources": SOURCE_ENDPOINTS,
        "warnings": warnings,
    }
