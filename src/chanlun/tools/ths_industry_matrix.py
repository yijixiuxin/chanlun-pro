"""
同花顺「行业强度作战矩阵」数据组装

行业目录 → 行业指数历史日 K；计算 5/20/60 日相对沪深300强度、
成交额脉冲、排名变化与市场宽度。用户选择行业时，再读取当前成分与股票快照，
计算上涨/下跌家数、等权涨跌代理和成交额活跃度。
"""

import datetime
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from chanlun.tools.ths_fuyao import ThsFuyaoError, to_local_code

INDUSTRY_PREFIX = "881"
BENCHMARK_CODE = "000300.SH"
HISTORY_CALENDAR_DAYS = 180
HEATMAP_DAYS = 20
TOP_HEATMAP_ROWS = 12
SOURCE_ENDPOINTS = [
    "/api/a-share-index/catalog/ths-index-list?tag=industry",
    "/api/a-share-index/prices/historical?thscode=881xxx.TI",
    "/api/a-share-index/constituents/ths-stock-list?thscode=...",
    "/api/a-share/prices/snapshot?thscodes=...",
    "/api/a-share-index/prices/historical?thscode=000300.SH",
]
CN_TZ = datetime.timezone(datetime.timedelta(hours=8))


def _num(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_from_ms(value) -> str:
    number = _num(value)
    if number is None:
        return ""
    return datetime.datetime.fromtimestamp(number / 1000, CN_TZ).strftime("%Y-%m-%d")


def _call_index_history(client, code: str, start_ms: int, end_ms: int) -> dict:
    for attempt in range(3):
        try:
            return client.index_historical(code, start_ms, end_ms)
        except ThsFuyaoError as e:
            if e.code == 4001 and attempt < 2:
                time.sleep(0.8 * (attempt + 1))
                continue
            raise
    return {}


def _fetch_histories(
    client,
    codes: list[str],
    start_ms: int,
    end_ms: int,
) -> tuple[dict, list[str]]:
    histories = {}
    failures = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        future_map = {
            pool.submit(_call_index_history, client, code, start_ms, end_ms): code
            for code in codes
        }
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                histories[code] = future.result() or {}
            except ThsFuyaoError:
                failures += 1
            except Exception:
                failures += 1
    warnings = []
    if failures:
        warnings.append(f"{failures} 个行业指数历史 K 线请求失败，已跳过")
    return histories, warnings


def _bars(data: dict) -> list[dict]:
    bars = []
    for item in data.get("item") or []:
        close = _num(item.get("close_price"))
        date = _date_from_ms(item.get("date_ms"))
        if not date or close is None:
            continue
        bars.append(
            {
                "date": date,
                "close": close,
                "turnover": _num(item.get("turnover")) or 0.0,
                "timestamp": item.get("date_ms"),
            }
        )
    bars.sort(key=lambda item: item["date"])
    return bars


def _pct(value) -> float | None:
    number = _num(value)
    return None if number is None else number * 100


def _return(closes: list[float], index: int, days: int) -> float | None:
    if index - days < 0 or closes[index - days] == 0:
        return None
    return closes[index] / closes[index - days] - 1


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _industry_metrics(
    bars: list[dict],
    benchmark_bars: list[dict],
) -> dict | None:
    if len(bars) < 25:
        return None
    bench_map = {item["date"]: item["close"] for item in benchmark_bars}
    industry_dates = [item["date"] for item in bars]
    closes = [item["close"] for item in bars]
    turnovers = [item["turnover"] for item in bars]
    last_index = len(bars) - 1
    last_date = industry_dates[last_index]
    bench_close = bench_map.get(last_date)
    if not bench_close:
        return None

    def relative(days: int):
        if last_index - days < 0:
            return None, None, None
        prev_date = industry_dates[last_index - days]
        prev_bench = bench_map.get(prev_date)
        if not prev_bench or closes[last_index - days] == 0:
            return None, None, None
        industry_return = closes[last_index] / closes[last_index - days] - 1
        bench_return = bench_close / prev_bench - 1
        return (
            _pct(industry_return),
            _pct(bench_return),
            _pct(industry_return - bench_return),
        )

    return5 = relative(5)
    return20 = relative(20)
    return60 = relative(60)
    recent_turnover = _mean(turnovers[-5:])
    base_turnover = _mean(turnovers[-25:-5])
    volume_pulse = recent_turnover / base_turnover if base_turnover else None

    rs20_series = []
    for index in range(20, len(bars)):
        prev_date = industry_dates[index - 20]
        prev_bench = bench_map.get(prev_date)
        current_bench = bench_map.get(industry_dates[index])
        if not prev_bench or not current_bench or closes[index - 20] == 0:
            rs20_series.append(None)
            continue
        industry_return = closes[index] / closes[index - 20] - 1
        bench_return = current_bench / prev_bench - 1
        rs20_series.append(_pct(industry_return - bench_return))

    return {
        "last_date": last_date,
        "last_timestamp": bars[-1].get("timestamp"),
        "return5": return5[0],
        "return20": return20[0],
        "return60": return60[0],
        "benchmark5": return5[1],
        "benchmark20": return20[1],
        "benchmark60": return60[1],
        "rs5": return5[2],
        "rs20": return20[2],
        "rs60": return60[2],
        "volume_pulse": volume_pulse,
        "rs20_series": rs20_series,
        "closes": closes,
        "dates": industry_dates,
        "turnovers": turnovers,
    }


def _list_industries(client) -> tuple[list[dict], int | None, list[str]]:
    catalog = client.index_catalog("industry")
    items = []
    for item in catalog.get("item") or []:
        code = str(item.get("thscode") or "")
        name = str(item.get("name") or "")
        if code.startswith(INDUSTRY_PREFIX) and name:
            items.append({"code": code, "name": name})
    return items, catalog.get("timestamp"), []


def _build_industry_rows(
    industries: list[dict],
    histories: dict,
    benchmark_bars: list[dict],
) -> tuple[list[dict], dict]:
    rows = []
    for industry in industries:
        metrics = _industry_metrics(
            _bars(histories.get(industry["code"]) or {}),
            benchmark_bars,
        )
        if not metrics:
            continue
        rows.append({**industry, **metrics})

    if not rows:
        return [], {}

    ranked = sorted(
        [row for row in rows if row.get("rs20") is not None],
        key=lambda row: row["rs20"],
        reverse=True,
    )
    for rank, row in enumerate(ranked, start=1):
        row["rank"] = rank

    for row in rows:
        series = row.get("rs20_series") or []
        previous = series[-21] if len(series) >= 21 else None
        row["previous_rs20"] = previous

    previous_ranked = sorted(
        [
            row
            for row in rows
            if row.get("previous_rs20") is not None
        ],
        key=lambda row: row["previous_rs20"],
        reverse=True,
    )
    previous_rank = {
        row["code"]: index for index, row in enumerate(previous_ranked, start=1)
    }
    for row in rows:
        current = row.get("rank")
        previous = previous_rank.get(row["code"])
        row["rank_change"] = (
            previous - current
            if previous is not None and current is not None
            else None
        )

    return rows, previous_rank


def _cross_section_z(rows: list[dict], series_key: str = "rs20_series"):
    dates = []
    if rows:
        dates = rows[0].get("dates") or []
    heat_dates = dates[-HEATMAP_DAYS:]
    first_series = (rows[0].get(series_key) or []) if rows else []
    offset = max(0, len(first_series) - HEATMAP_DAYS)
    values_by_date = []
    for day in range(HEATMAP_DAYS):
        values = []
        for row in rows:
            series = row.get(series_key) or []
            index = offset + day
            value = series[index] if index < len(series) else None
            if value is not None:
                values.append(value)
        if not values:
            values_by_date.append((0.0, 0.0))
            continue
        mean = _mean(values)
        variance = _mean([(value - mean) ** 2 for value in values])
        values_by_date.append((mean, math.sqrt(variance)))

    z_rows = []
    for row in rows:
        series = row.get(series_key) or []
        z_values = []
        for day in range(HEATMAP_DAYS):
            index = offset + day
            value = series[index] if index < len(series) else None
            mean, std = values_by_date[day]
            if value is None or not std:
                z_values.append(None)
            else:
                z_values.append(round((value - mean) / std, 3))
        latest = next(
            (value for value in reversed(z_values) if value is not None), None
        )
        if latest is not None:
            z_rows.append(
                {
                    "code": row["code"],
                    "name": row["name"],
                    "values": z_values,
                    "latest": latest,
                    "rank_change": row.get("rank_change"),
                    "rs20": row.get("rs20"),
                }
            )
    z_rows.sort(key=lambda item: item["latest"], reverse=True)
    return heat_dates, z_rows


def _chunks(values: list[str], size: int = 80):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _build_selected(
    client,
    industry: dict,
    benchmark_bars: list[dict],
) -> dict:
    detail = client.index_constituents(industry["code"])
    constituents = detail.get("item") or []
    code_name = {
        item.get("thscode"): item.get("name") or item.get("ticker") or ""
        for item in constituents
        if item.get("thscode")
    }
    codes = list(code_name.keys())
    snapshot_items = []
    snapshot_timestamp = None
    for chunk in _chunks(codes):
        snapshot = client.stock_snapshot(chunk)
        snapshot_items.extend(snapshot.get("item") or [])
        if snapshot.get("timestamp"):
            snapshot_timestamp = max(
                snapshot_timestamp or 0, int(snapshot["timestamp"])
            )

    stocks = []
    for item in snapshot_items:
        code = item.get("thscode") or ""
        pct = _num(item.get("price_change_ratio_pct"))
        if not code or pct is None:
            continue
        stocks.append(
            {
                "code": to_local_code(code),
                "ticker": item.get("ticker") or "",
                "name": code_name.get(code) or code,
                "pct": pct,
                "last_price": _num(item.get("last_price")),
                "turnover": _num(item.get("turnover")) or 0.0,
            }
        )

    up = sum(1 for item in stocks if item["pct"] > 0)
    down = sum(1 for item in stocks if item["pct"] < 0)
    flat = len(stocks) - up - down
    turnover_total = sum(item["turnover"] for item in stocks)
    equal_weight_pct = _mean([item["pct"] for item in stocks])

    positives = sorted(
        [item for item in stocks if item["pct"] > 0],
        key=lambda item: item["pct"],
        reverse=True,
    )[:6]
    negatives = sorted(
        [item for item in stocks if item["pct"] < 0],
        key=lambda item: item["pct"],
    )[:6]

    bars = _bars(industry.get("_bars") or {})
    bench_map = {item["date"]: item["close"] for item in benchmark_bars}
    dates = [item["date"] for item in bars][-20:]
    industry_series = []
    benchmark_series = []
    if dates:
        base_industry = None
        base_bench = None
        for date in dates:
            bar = next((item for item in bars if item["date"] == date), None)
            bench = bench_map.get(date)
            if not bar or not bench:
                continue
            if base_industry is None:
                base_industry = bar["close"]
                base_bench = bench
            industry_series.append(round(bar["close"] / base_industry * 100, 2))
            benchmark_series.append(round(bench / base_bench * 100, 2))

    return {
        "code": industry["code"],
        "name": industry["name"],
        "rs5": industry.get("rs5"),
        "rs20": industry.get("rs20"),
        "rs60": industry.get("rs60"),
        "volume_pulse": industry.get("volume_pulse"),
        "rank": industry.get("rank"),
        "rank_change": industry.get("rank_change"),
        "dates": dates,
        "industry_series": industry_series,
        "benchmark_series": benchmark_series,
        "constituents": {
            "count": len(stocks),
            "catalog_count": len(constituents),
            "up": up,
            "down": down,
            "flat": flat,
            "equal_weight_pct": equal_weight_pct,
            "turnover_total": turnover_total,
            "top_gainers": positives,
            "top_losers": negatives,
        },
        "data_time_ms": snapshot_timestamp,
    }


def _default_industry(rows: list[dict]) -> dict:
    candidates = [row for row in rows if row.get("rs20") is not None]
    if not candidates:
        return rows[0]
    return max(candidates, key=lambda row: row["rs20"])


def get_industry_matrix(client, thscode: str | None = None) -> dict:
    industries, catalog_timestamp, _ = _list_industries(client)
    if not industries:
        raise ThsFuyaoError(3002, "同花顺行业指数目录为空")

    today = datetime.datetime.now(CN_TZ).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_ms = int((today + datetime.timedelta(days=1)).timestamp() * 1000) - 1
    start_ms = int(
        (today - datetime.timedelta(days=HISTORY_CALENDAR_DAYS)).timestamp()
        * 1000
    )
    benchmark_data = client.index_historical(BENCHMARK_CODE, start_ms, end_ms)
    benchmark_bars = _bars(benchmark_data)
    if len(benchmark_bars) < 25:
        raise ThsFuyaoError(3002, "沪深300历史 K 线不足")

    codes = [item["code"] for item in industries]
    histories, warnings = _fetch_histories(client, codes, start_ms, end_ms)
    rows, _ = _build_industry_rows(industries, histories, benchmark_bars)
    if not rows:
        raise ThsFuyaoError(3002, "行业指数历史 K 线不足，无法计算强度")

    selected = next(
        (row for row in rows if row["code"] == thscode),
        None,
    ) or _default_industry(rows)

    heat_dates, heat_rows = _cross_section_z(rows)
    heat_rows = heat_rows[:TOP_HEATMAP_ROWS]

    strong_count = sum(1 for row in rows if (row.get("rs20") or 0) > 0)
    rising_count = sum(1 for row in rows if (row.get("rs5") or 0) > 0)
    breadth = (rising_count / len(rows)) * 100 if rows else 0
    rotations = [
        abs(row["rank_change"])
        for row in rows
        if row.get("rank_change") is not None
    ]
    rotation = _mean(rotations)

    bubble = []
    for row in rows:
        if row.get("rs5") is None or row.get("rs20") is None:
            continue
        bubble.append(
            {
                "code": row["code"],
                "name": row["name"],
                "rs5": row["rs5"],
                "rs20": row["rs20"],
                "rs60": row["rs60"],
                "volume_pulse": row.get("volume_pulse"),
                "rank": row.get("rank"),
                "rank_change": row.get("rank_change"),
                "return5": row.get("return5"),
                "return20": row.get("return20"),
                "return60": row.get("return60"),
            }
        )

    selected_data = _build_selected(
        client,
        {**selected, "_bars": histories.get(selected["code"]) or {}},
        benchmark_bars,
    )
    # 避免把内部 K 线原样返回
    selected_data.pop("_bars", None)
    for row in rows:
        row.pop("closes", None)
        row.pop("dates", None)
        row.pop("turnovers", None)
        row.pop("rs20_series", None)
        row.pop("previous_rs20", None)

    latest_dates = [row.get("last_date") for row in rows if row.get("last_date")]
    trade_date = max(latest_dates) if latest_dates else ""
    data_time_ms = max(
        [catalog_timestamp or 0, benchmark_data.get("timestamp") or 0]
        + [row.get("last_timestamp") or 0 for row in rows]
        + [selected_data.get("data_time_ms") or 0]
    )

    return {
        "trade_date": trade_date,
        "data_time_ms": data_time_ms,
        "mode": "real",
        "is_simulated": False,
        "stats": {
            "industry_count": len(rows),
            "strong_count": strong_count,
            "rising_count": rising_count,
            "rotation": rotation,
            "breadth": breadth,
        },
        "bubble": bubble,
        "heatmap": {"dates": heat_dates, "rows": heat_rows},
        "industries": [
            {
                "code": row["code"],
                "name": row["name"],
                "rs5": row.get("rs5"),
                "rs20": row.get("rs20"),
                "rs60": row.get("rs60"),
                "rank": row.get("rank"),
                "rank_change": row.get("rank_change"),
            }
            for row in rows
        ],
        "selected": selected_data,
        "warnings": warnings,
        "sources": SOURCE_ENDPOINTS,
        "definitions": {
            "rs": "相对强度 = 行业 N 日收益 - 沪深300 N 日收益，单位为百分点",
            "pulse": "成交额脉冲 = 最近 5 日平均成交额 / 前 20 日平均成交额",
            "rank": "排名变化 = 20 个交易日前 20 日强度排名 - 当前 20 日强度排名",
            "width": "市场宽度 = 5 日相对强度为正的行业数 / 有数据的行业数",
            "heat": "热力带为近 20 日 20 日相对强度的截面 z-score，缺失值留空",
            "selected": "当前成分按等权平均计算涨跌代理，不是指数贡献；成分会随时间变化",
            "window": "行业为同花顺 881xxx 一级行业指数；窗口可切换 5/20/60 日",
        },
    }
