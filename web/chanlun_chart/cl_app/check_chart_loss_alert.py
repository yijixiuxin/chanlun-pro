import json
import traceback
from typing import Dict, List, Optional, Tuple

from chanlun.base import Market
from chanlun.db import TableByTVCharts, db
from chanlun.exchange import get_exchange
from chanlun.fun import get_logger
from chanlun.utils import send_fs_msg

logger = get_logger("check_chart_loss_alert.log")

"""
根据前端tv图表设置的水平止损线，进行监控与提醒
"""

# symbol 前缀 -> Market 枚举 映射
SYMBOL_MARKET_MAP: Dict[str, Market] = {
    "US": Market.US,
    "A": Market.A,
    "HK": Market.HK,
    "FUTURES": Market.FUTURES,
    "FX": Market.FX,
    "CURRENCY": Market.CURRENCY,
    "CURRENCY_SPOT": Market.CURRENCY_SPOT,
    "NY_FUTURES": Market.NY_FUTURES,
}


def parse_symbol(symbol: str) -> Optional[Tuple[Market, str]]:
    """
    解析 symbol 字符串，格式如 "US:AMZN"、"A:SH.600519"
    返回 (Market, code) 或 None
    """
    if ":" not in symbol:
        logger.warning(f"Invalid symbol format (missing ':'): {symbol}")
        return None
    parts = symbol.split(":", 1)
    market_key = parts[0].strip()
    code = parts[1].strip()
    if not market_key or not code:
        logger.warning(f"Invalid symbol format (empty market or code): {symbol}")
        return None
    market = SYMBOL_MARKET_MAP.get(market_key)
    if market is None:
        logger.warning(f"Unknown market in symbol: {market_key}")
        return None
    return market, code


def get_stop_loss_lines() -> List[dict]:
    """
    从数据库中读取所有图表，解析出止损线信息。
    返回止损线信息列表，每条包含 market, code, stop_price, chart_name, direction 等字段。
    direction: 'long' 表示做多止损线，'short' 表示做空止损线。
    """
    # 止损线文本 -> 方向 映射
    STOP_LOSS_TEXT_MAP = {
        "止损线": "long",
        "做空止损线": "short",
    }

    results: List[dict] = []
    try:
        with db.Session() as session:
            charts: List[TableByTVCharts] = session.query(TableByTVCharts).all()
    except Exception as e:
        logger.error(f"Failed to query charts from database: {e}")
        return results

    for chart in charts:
        try:
            # 第一层 JSON 解析 content 字段
            content_data = json.loads(chart.content)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse chart content (id={chart.id}): {e}")
            continue

        # 第二层 JSON 解析 content 中的 content 属性
        inner_content = content_data.get("content")
        if not inner_content:
            continue
        try:
            inner_data = json.loads(inner_content)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse inner content (id={chart.id}): {e}")
            continue

        # 导航到 charts[0].panes[0].sources
        charts_list = inner_data.get("charts")
        if (
            not charts_list
            or not isinstance(charts_list, list)
            or len(charts_list) == 0
        ):
            continue
        panes = charts_list[0].get("panes")
        if not panes or not isinstance(panes, list) or len(panes) == 0:
            continue
        sources = panes[0].get("sources")
        if not sources or not isinstance(sources, list):
            continue

        # 遍历 sources，找到止损线
        for source in sources:
            if source.get("type") != "LineToolHorzLine":
                continue
            state = source.get("state")
            if not state:
                continue

            line_text = state.get("text", "")
            direction = STOP_LOSS_TEXT_MAP.get(line_text)
            if direction is None:
                continue

            # 获取 symbol
            symbol = state.get("symbol", "")
            if not symbol:
                logger.warning(f"Chart id={chart.id} has stop-loss line without symbol")
                continue

            parsed = parse_symbol(symbol)
            if parsed is None:
                continue
            market, code = parsed

            # 获取止损价格 points[0].price
            points = source.get("points")
            if not points or not isinstance(points, list) or len(points) == 0:
                logger.warning(f"Chart id={chart.id} stop-loss line has no points")
                continue
            stop_price = points[0].get("price")
            if stop_price is None:
                continue

            results.append(
                {
                    "chart_id": chart.id,
                    "chart_name": chart.name,
                    "market": market,
                    "code": code,
                    "stop_price": float(stop_price),
                    "symbol": symbol,
                    "direction": direction,
                }
            )
            # logger.info(
            #     f"Found stop-loss line [{direction}]: {symbol} stop_price={stop_price} (chart: {chart.name})"
            # )

    return results


def check_and_alert():
    """主检查逻辑：获取止损线，比较行情，发送报警。"""
    # logger.info("Starting stop-loss check cycle...")

    stop_loss_lines = get_stop_loss_lines()
    if not stop_loss_lines:
        # logger.info("No stop-loss lines found, skipping check")
        return

    # 按 market 分组，同一市场的代码批量获取 ticks
    market_codes: Dict[Market, List[dict]] = {}
    for sl in stop_loss_lines:
        market_codes.setdefault(sl["market"], []).append(sl)

    alert_msgs: Dict[str, List[str]] = {}  # market_value -> list of alert messages

    for market, sl_list in market_codes.items():
        codes = [sl["code"] for sl in sl_list]
        try:
            ex = get_exchange(market)
            if ex.now_trading() is False:  # 非交易时间不执行
                continue
            ticks = ex.ticks(codes)
        except Exception as e:
            logger.error(f"Failed to get ticks for market {market}: {e}")
            logger.error(traceback.format_exc())
            continue

        for sl in sl_list:
            code = sl["code"]
            stop_price = sl["stop_price"]
            direction = sl["direction"]
            stock = ex.stock_info(code)
            tick = ticks.get(code)
            if tick is None:
                logger.warning(f"No tick data for {code} in market {market}")
                continue

            current_price = tick.last
            if current_price <= 0:
                logger.warning(f"Invalid current price for {code}: {current_price}")
                continue

            # 做多止损线: 当前价 < 止损价 触发报警
            # 做空止损线: 当前价 > 止损价 触发报警
            triggered = False
            if direction == "long" and current_price < stop_price:
                triggered = True
                loss_pct = (stop_price - current_price) / stop_price * 100
                alert_prefix = "【做多止损报警】"
            elif direction == "short" and current_price > stop_price:
                triggered = True
                loss_pct = (current_price - stop_price) / stop_price * 100
                alert_prefix = "【做空止损报警】"

            if triggered:
                msg = (
                    f"{alert_prefix}{sl['symbol']}:{stock['name']}\n"
                    f"止损价格: {stop_price:.2f}\n"
                    f"当前价格: {current_price:.2f}\n"
                    f"偏离幅度: {loss_pct:.2f}%\n"
                    f"图表: {sl['chart_name']}"
                )
                market_value = market.value
                alert_msgs.setdefault(market_value, []).append(msg)
                logger.info(
                    f"ALERT [{direction}]: {sl['symbol']} current={current_price} stop={stop_price}"
                )

    # 发送飞书报警
    for market_value, msgs in alert_msgs.items():
        try:
            send_fs_msg("alert", "止损线报警", msgs)
            logger.info(f"Sent {len(msgs)} alerts to Feishu for market {market_value}")
        except Exception as e:
            logger.error(
                f"Failed to send Feishu message for market {market_value}: {e}"
            )
            logger.error(traceback.format_exc())

    # logger.info("Stop-loss check cycle completed")


if __name__ == '__main__':
    print(get_stop_loss_lines())