"""
同花顺特色数据 Web 路由

前端通过 /ths/special/<action> 访问，返回：
    成功：{"ok": True, "data": {...}}
    失败：{"ok": False, "code": 业务码, "msg": 错误信息}
"""

import datetime

from flask import request
from flask_login import login_required

from chanlun.tools.ths_dragon_tiger import get_dragon_tiger_watch
from chanlun.tools.ths_financial import get_financial_health
from chanlun.tools.ths_flow_topology import get_flow_topology
from chanlun.tools.ths_industry_matrix import get_industry_matrix
from chanlun.tools.ths_fuyao import ThsFuyaoError, get_ths_special_data
from chanlun.tools.ths_pulse import get_market_pulse


def _arg(name: str, default: str | None = None) -> str | None:
    value = request.args.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _int_arg(
    name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _bool_arg(name: str) -> bool | None:
    value = _arg(name)
    if value is None:
        return None
    return value.lower() in {"1", "true", "yes"}


def _range_days_arg() -> int | None:
    value = _arg("range_days")
    if value in {"1", "3"}:
        return int(value)
    return None


def _last_days(days: int) -> tuple[str, str]:
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def register_ths_routes(app):

    @app.route("/ths/special/<action>")
    @login_required
    def ths_special(action: str):
        client = get_ths_special_data()
        try:
            if action == "limit_pool":
                kind = _arg("kind", "up")
                kwargs = {
                    "date": _arg("date"),
                    "page": _int_arg("page", 1, 1, 9999),
                    "size": _int_arg("size", 200, 1, 200),
                }
                if kind == "up":
                    data = client.limit_up_pool(**kwargs)
                elif kind == "down":
                    data = client.limit_down_pool(**kwargs)
                elif kind == "break":
                    data = client.limit_break_pool(**kwargs)
                else:
                    raise ThsFuyaoError(1002, "未知的涨跌停池类型")
                data = dict(data)
                data["trade_date"] = kwargs["date"] or client.latest_trade_date()

            elif action == "ladder":
                data = client.limit_up_ladder()

            elif action == "hot":
                kind = _arg("kind", "hot")
                period = _arg("period", "day")
                limit = _int_arg("limit", 100, 1, 100)
                if kind == "hot":
                    data = client.hot_stock_list(period=period, limit=limit)
                elif kind == "skyrocket":
                    data = client.skyrocket_list(period=period, limit=limit)
                else:
                    raise ThsFuyaoError(1002, "未知的热榜类型")

            elif action == "hot_history":
                data = client.hot_stock_list_history(date=_arg("date"))

            elif action == "hot_rank":
                code = _arg("code")
                if not code:
                    raise ThsFuyaoError(1001, "缺少股票代码")
                start_date, end_date = _last_days(30)
                data = client.hot_stock_rank_trend(
                    code=code,
                    start_date=_arg("start_date", start_date),
                    end_date=_arg("end_date", end_date),
                )

            elif action == "anomaly":
                data = client.anomaly_analysis_list(tag_codes=_arg("tags"))

            elif action == "anomaly_stock":
                codes = _arg("codes")
                if not codes:
                    raise ThsFuyaoError(1001, "缺少股票代码")
                data = client.anomaly_analysis_stock(codes)

            elif action == "dragon_tiger":
                data = client.dragon_tiger_list(
                    board_type=_arg("board_type", "all"),
                    date=_arg("date"),
                )

            elif action == "limit_statistics":
                data = client.limit_up_statistics(date=_arg("date"))

            elif action == "institution":
                code = _arg("code")
                if not code:
                    raise ThsFuyaoError(1001, "缺少股票代码")
                data = client.dragon_tiger_stock_institution(
                    code=code,
                    date=_arg("date"),
                    range_days=_range_days_arg(),
                )

            elif action == "department_classic":
                department_id = _arg("id")
                if not department_id:
                    raise ThsFuyaoError(1001, "缺少营业部 ID")
                data = client.dragon_tiger_department_classic_record(department_id)

            elif action == "department_history":
                department_id = _arg("id")
                if not department_id:
                    raise ThsFuyaoError(1001, "缺少营业部 ID")
                data = client.dragon_tiger_department_history(
                    department_id=department_id,
                    page=_int_arg("page", 1, 1, 9999),
                    size=_int_arg("size", 30, 1, 30),
                )

            elif action == "abnormal_monitor":
                data = client.abnormal_monitor_list(
                    state=_arg("state", "current"),
                    page=_int_arg("page", 1, 1, 9999),
                    size=_int_arg("size", 30, 1, 30),
                )

            elif action == "auction":
                data = client.auction_stock_opportunities(
                    date=_arg("date"),
                    warn_types=_arg("warn_types"),
                    market=_arg("market"),
                    exclude_st=_bool_arg("exclude_st"),
                    codes=_arg("codes"),
                    page=_int_arg("page", 1, 1, 9999),
                    size=_int_arg("size", 20, 1, 20),
                )

            elif action == "auction_dates":
                data = client.auction_opportunity_dates()

            elif action == "financial_health":
                code = _arg("code")
                if not code:
                    raise ThsFuyaoError(1001, "缺少股票代码")
                data = get_financial_health(
                    client,
                    code,
                    fallback_name=_arg("name"),
                )

            elif action == "market_pulse":
                data = get_market_pulse(client, date=_arg("date"))

            elif action == "dragon_tiger_watch":
                data = get_dragon_tiger_watch(client)

            elif action == "flow_topology":
                data = get_flow_topology(client)

            elif action == "industry_matrix":
                data = get_industry_matrix(client, thscode=_arg("thscode"))

            else:
                raise ThsFuyaoError(1404, f"未知的同花顺特色数据接口：{action}")

        except ThsFuyaoError as e:
            return {"ok": False, "code": e.code, "msg": e.message}
        except Exception:
            app.logger.exception("同花顺特色数据请求失败：%s", action)
            return {"ok": False, "code": -1, "msg": "同花顺数据请求失败，请稍后重试"}

        return {"ok": True, "data": data}
