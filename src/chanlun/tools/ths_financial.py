"""
同花顺「单股财务体检」数据组装

取最近 8 期季报的三张表，按 period_end_ms 对齐；最新报告期再取财务指标。
所有金额字段保持原始人民币元，前端统一换算为亿元展示。
季报数值为累计口径，不拆分为单季值。
"""

from chanlun.tools.ths_fuyao import ThsFuyaoError, to_local_code, to_ths_code

FINANCIAL_PERIODS = 8

ABILITY_NAMES = {
    "growth": "成长能力",
    "profitability": "盈利能力",
    "solvency": "偿债能力",
    "operation": "营运能力",
    "cash-flow": "现金流",
}

# index_id -> (中文名, 单位)
INDICATOR_META = {
    # 成长能力
    "calculate_operating_income_yoy_growth_ratio": ("营业收入同比增长率", "%"),
    "operating_income_yoy_growth_ratio": ("营业收入同比增长率", "%"),
    "calculate_operating_profit_yoy_growth_ratio": ("营业利润同比增长率", "%"),
    "operating_profit_yoy_growth_ratio": ("营业利润同比增长率", "%"),
    "calculate_parent_holder_net_profit_yoy_growth_ratio": ("归母净利润同比增长率", "%"),
    "net_profit_yoy_growth_ratio": ("净利润同比增长率", "%"),
    "total_assets_growth_ratio": ("总资产增长率", "%"),
    # 盈利能力
    "sale_gross_margin": ("销售毛利率", "%"),
    "sale_net_interest_ratio": ("销售净利率", "%"),
    "total_assets_net_ratio": ("总资产收益率", "%"),
    "index_deduct_weighted_avg_roe": ("扣非加权净资产收益率", "%"),
    "index_weighted_avg_roe": ("净资产收益率", "%"),
    # 偿债能力
    "current_ratio": ("流动比率", "倍"),
    "quick_ratio": ("速动比率", "倍"),
    "assets_debt_ratio": ("资产负债率", "%"),
    "cash_ratio": ("现金比率", "%"),
    "earned_interest_multiple": ("已获利息倍数", "倍"),
    # 营运能力
    "long_term_debt_equity_ratio": ("长期负债权益比率", ""),
    "total_assets_turnover_ratio": ("总资产周转率", "次"),
    "inventory_turnover_ratio": ("存货周转率", "次"),
    "current_assets_turnover_ratio": ("流动资产周转率", "次"),
    "receive_account_turnover_ratio": ("应收账款周转率", "次"),
    # 现金流
    "cash_operating_index": ("现金营运指数", ""),
    "operating_cash_flow_net_divide_income": ("销售现金比率", "%"),
    "net_profit_cash_content": ("净利润现金含量", "%"),
    "operating_cash_net_yoy_growth_ratio": ("现金流量净额增长率", "%"),
    "cash_meet_invest_ratio": ("现金满足投资比率", ""),
}


def _num(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ms(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ratio(numerator, denominator) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator * 100


def _growth(current, previous) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / abs(previous) * 100


def _period_label(fiscal_year, fiscal_period: str) -> str:
    fiscal_period = (fiscal_period or "").upper()
    if fiscal_period == "FY":
        return f"{fiscal_year}FY"
    return f"{fiscal_year}{fiscal_period}"


def _indicator_report(latest: dict) -> str:
    year = latest.get("fiscal_year")
    fiscal_period = (latest.get("fiscal_period") or "").upper()
    quarter = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "FY": 4}.get(fiscal_period, 4)
    return f"{year}-{quarter}"


def _yi(value) -> str:
    if value is None:
        return "--"
    return f"{value / 100000000:.2f}"


def _pct(value, signed: bool = False) -> str:
    if value is None:
        return "--"
    if signed:
        return f"{value:+.2f}%"
    return f"{value:.2f}%"


def _indicator_value_map(indicators: dict) -> dict[str, float | None]:
    values: dict[str, float | None] = {}
    for ability in (indicators or {}).get("abilities") or []:
        for item in ability.get("indicators") or []:
            index_id = item.get("index_id")
            if index_id:
                values[index_id] = _num(item.get("value"))
    return values


def _indicator_groups(indicators: dict) -> list[dict]:
    groups = []
    for ability in (indicators or {}).get("abilities") or []:
        ability_id = ability.get("ability") or ""
        items = []
        for item in ability.get("indicators") or []:
            index_id = item.get("index_id") or ""
            name, unit = INDICATOR_META.get(index_id, (index_id, ""))
            items.append(
                {
                    "id": index_id,
                    "name": name,
                    "value": item.get("value"),
                    "unit": unit,
                }
            )
        if items:
            groups.append(
                {
                    "ability": ability_id,
                    "name": ABILITY_NAMES.get(ability_id, ability_id),
                    "items": items,
                }
            )
    return groups


def _first_indicator(values: dict[str, float | None], *ids: str) -> float | None:
    for index_id in ids:
        value = values.get(index_id)
        if value is not None:
            return value
    return None


def build_financial_health(
    income: dict,
    balance: dict,
    cash: dict,
    indicators: dict,
    stock: dict,
    report: str | None = None,
    warnings: list[str] | None = None,
) -> dict:
    """把三张表与指标组装成前端看板数据"""
    income_items = [
        item
        for item in (income or {}).get("item") or []
        if item.get("period_end_ms") is not None
    ]
    income_items.sort(key=lambda item: _ms(item.get("period_end_ms")) or 0)
    income_items = income_items[-FINANCIAL_PERIODS:]

    balance_by_period = {
        _ms(item.get("period_end_ms")): item
        for item in (balance or {}).get("item") or []
        if item.get("period_end_ms") is not None
    }
    cash_by_period = {
        _ms(item.get("period_end_ms")): item
        for item in (cash or {}).get("item") or []
        if item.get("period_end_ms") is not None
    }

    periods = []
    for income_item in income_items:
        period_end_ms = _ms(income_item.get("period_end_ms"))
        balance_item = balance_by_period.get(period_end_ms) or {}
        cash_item = cash_by_period.get(period_end_ms) or {}

        revenue = _num(income_item.get("operating_income"))
        net_profit = _num(income_item.get("parent_holder_net_profit"))
        net_profit_total = _num(income_item.get("net_profit"))
        operating_cash_flow = _num(cash_item.get("act_cash_flow_net"))
        total_assets = _num(balance_item.get("assets_total"))
        total_liabilities = _num(balance_item.get("total_debt"))
        equity = _num(balance_item.get("holder_equity_total"))

        periods.append(
            {
                "period": _period_label(
                    income_item.get("fiscal_year"), income_item.get("fiscal_period")
                ),
                "fiscal_year": income_item.get("fiscal_year"),
                "fiscal_period": income_item.get("fiscal_period"),
                "period_end_ms": period_end_ms,
                "report_date_ms": _ms(income_item.get("report_date_ms")),
                "revenue": revenue,
                "net_profit": net_profit,
                "net_profit_total": net_profit_total,
                "operating_cash_flow": operating_cash_flow,
                "total_assets": total_assets,
                "total_liabilities": total_liabilities,
                "equity": equity,
                "cash": _num(balance_item.get("cash")),
                "accounts_receivable": _num(
                    balance_item.get("accounts_receivable")
                ),
                "rd_expense": _num(
                    income_item.get("research_and_development_expenses")
                ),
                "invest_cash_flow": _num(cash_item.get("invest_cash_flow_net")),
                "financing_cash_flow": _num(cash_item.get("financing_cash_flow_net")),
                "debt_ratio": _ratio(total_liabilities, total_assets),
                "net_margin": _ratio(
                    net_profit if net_profit is not None else net_profit_total,
                    revenue,
                ),
                "ocf_to_profit": _ratio(operating_cash_flow, net_profit),
                "ocf_to_revenue": _ratio(operating_cash_flow, revenue),
            }
        )

    if not periods:
        raise ThsFuyaoError(3002, "该标的暂无可用财务数据")

    # 同期对比：同财年同一季度（季报为累计口径，只与上年同期比较）
    by_year_period = {
        (item.get("fiscal_year"), item.get("fiscal_period")): item for item in periods
    }
    for item in periods:
        previous = by_year_period.get(
            (item.get("fiscal_year", 0) - 1, item.get("fiscal_period"))
        )
        if previous is None:
            item["revenue_yoy"] = None
            item["profit_yoy"] = None
            item["ocf_yoy"] = None
            continue
        item["revenue_yoy"] = _growth(item["revenue"], previous["revenue"])
        item["profit_yoy"] = _growth(item["net_profit"], previous["net_profit"])
        item["ocf_yoy"] = _growth(
            item["operating_cash_flow"], previous["operating_cash_flow"]
        )

    latest = periods[-1]
    indicator_values = _indicator_value_map(indicators)

    # 指标接口作为补充和缺失回退
    if latest.get("debt_ratio") is None:
        latest["debt_ratio"] = _first_indicator(
            indicator_values, "assets_debt_ratio"
        )
    if latest.get("net_margin") is None:
        latest["net_margin"] = _first_indicator(
            indicator_values, "sale_net_interest_ratio"
        )
    if latest.get("revenue_yoy") is None:
        latest["revenue_yoy"] = _first_indicator(
            indicator_values,
            "calculate_operating_income_yoy_growth_ratio",
            "operating_income_yoy_growth_ratio",
        )
    if latest.get("profit_yoy") is None:
        latest["profit_yoy"] = _first_indicator(
            indicator_values,
            "calculate_parent_holder_net_profit_yoy_growth_ratio",
            "net_profit_yoy_growth_ratio",
        )

    cards = [
        {
            "key": "revenue",
            "label": "营业收入",
            "value": latest.get("revenue"),
            "is_pct": False,
            "sub": (
                f"同比 {_pct(latest.get('revenue_yoy'), signed=True)}"
                if latest.get("revenue_yoy") is not None
                else "最新报告期累计"
            ),
        },
        {
            "key": "net_profit",
            "label": "归母净利润",
            "value": latest.get("net_profit"),
            "is_pct": False,
            "sub": f"净利率 {_pct(latest.get('net_margin'))}",
        },
        {
            "key": "operating_cash_flow",
            "label": "经营现金流",
            "value": latest.get("operating_cash_flow"),
            "is_pct": False,
            "sub": f"现金含量 {_pct(latest.get('ocf_to_profit'))}",
        },
        {
            "key": "debt_ratio",
            "label": "资产负债率",
            "value": latest.get("debt_ratio"),
            "is_pct": True,
            "sub": f"总资产 {_yi(latest.get('total_assets'))} 亿",
        },
    ]

    summary = []
    if latest.get("revenue") is not None:
        text = f"{latest['period']} 营业收入 {_yi(latest['revenue'])} 亿元"
        if latest.get("revenue_yoy") is not None:
            text += f"，同比 {_pct(latest['revenue_yoy'], signed=True)}"
        summary.append(text + "。")
    if latest.get("net_profit") is not None:
        text = f"归母净利润 {_yi(latest['net_profit'])} 亿元"
        if latest.get("profit_yoy") is not None:
            text += f"，同比 {_pct(latest['profit_yoy'], signed=True)}"
        summary.append(text + "。")
    if (
        latest.get("operating_cash_flow") is not None
        and latest.get("net_profit") is not None
        and latest.get("net_profit") > 0
    ):
        summary.append(
            f"经营现金流 {_yi(latest['operating_cash_flow'])} 亿元，"
            f"为归母净利润的 {_pct(latest.get('ocf_to_profit'))}。"
        )
    if latest.get("debt_ratio") is not None:
        summary.append(
            f"资产负债率 {_pct(latest['debt_ratio'])}，"
            f"总资产 {_yi(latest.get('total_assets'))} 亿元，"
            f"总负债 {_yi(latest.get('total_liabilities'))} 亿元。"
        )
    gross_margin = _first_indicator(indicator_values, "sale_gross_margin")
    roe = _first_indicator(indicator_values, "index_weighted_avg_roe")
    if gross_margin is not None or roe is not None:
        parts = []
        if gross_margin is not None:
            parts.append(f"销售毛利率 {_pct(gross_margin)}")
        if roe is not None:
            parts.append(f"加权 ROE {_pct(roe)}")
        summary.append("，".join(parts) + "。")

    stock = stock or {}
    thscode = stock.get("thscode") or ""
    return {
        "stock": {
            "thscode": thscode,
            "ticker": stock.get("ticker") or "",
            "name": stock.get("name") or thscode,
            "code": to_local_code(thscode) if thscode else "",
            "currency": stock.get("currency") or "CNY",
        },
        "report": {
            "period": latest["period"],
            "report": report or _indicator_report(latest),
            "period_end_ms": latest.get("period_end_ms"),
            "report_date_ms": latest.get("report_date_ms"),
        },
        "latest": latest,
        "periods": periods,
        "cards": cards,
        "summary": summary,
        "indicators": _indicator_groups(indicators),
        "warnings": warnings or [],
        "meta": {
            "currency": "CNY",
            "display_unit": "亿元",
            "period_type": "quarterly",
            "cumulative": True,
            "period_count": len(periods),
            "source": "同花顺金融数据服务",
        },
    }


def _resolve_stock(client, ths_code: str, fallback_name: str | None) -> dict:
    try:
        data = client.search_ticker(ths_code, asset_type="a-share", limit=10)
    except ThsFuyaoError:
        data = {}
    items = (data or {}).get("item") or []
    exact = next(
        (
            item
            for item in items
            if str(item.get("thscode", "")).upper() == ths_code.upper()
        ),
        None,
    )
    item = exact or (items[0] if items else {})
    return {
        "thscode": item.get("thscode") or ths_code,
        "ticker": item.get("ticker") or ths_code.split(".")[0],
        "name": item.get("name") or fallback_name or ths_code,
        "currency": item.get("currency") or "CNY",
    }


def get_financial_health(
    client,
    code: str,
    fallback_name: str | None = None,
) -> dict:
    """调用同花顺财务接口并组装看板数据"""
    ths_code = to_ths_code(code)
    stock = _resolve_stock(client, ths_code, fallback_name)

    income = client.income_statements(ths_code, period="quarterly", limit=8)
    income_items = [
        item
        for item in (income or {}).get("item") or []
        if item.get("period_end_ms") is not None
    ]
    if not income_items:
        raise ThsFuyaoError(3002, "该标的暂无季度财务数据")
    latest_raw = max(
        income_items, key=lambda item: _ms(item.get("period_end_ms")) or 0
    )
    report = _indicator_report(latest_raw)

    warnings = []
    try:
        balance = client.balance_sheets(ths_code, period="quarterly", limit=8)
    except ThsFuyaoError as e:
        balance = {}
        warnings.append(f"资产负债表暂不可用：{e.message}")
    try:
        cash = client.cash_flow_statements(ths_code, period="quarterly", limit=8)
    except ThsFuyaoError as e:
        cash = {}
        warnings.append(f"现金流量表暂不可用：{e.message}")
    try:
        indicators = client.financial_indicators(ths_code, report)
    except ThsFuyaoError:
        indicators = {}

    return build_financial_health(
        income=income,
        balance=balance,
        cash=cash,
        indicators=indicators,
        stock=stock,
        report=report,
        warnings=warnings,
    )
