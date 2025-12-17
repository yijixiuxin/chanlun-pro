from datetime import timedelta, datetime, time
from typing import Dict, List, Union
from decimal import Decimal
import pandas as pd
import pytz

from longport.openapi import (
    Config, QuoteContext, TradeContext, Market, Period,
    AdjustType, OrderSide, OrderType, TimeInForceType,
    SecurityListCategory, TradeSessions
)
from chanlun import fun
from chanlun.exchange import Exchange
from chanlun.exchange.exchange import Tick
from chanlun.fun import str_to_datetime
from chanlun.tools.log_util import LogUtil


@fun.singleton
class ExchangeChangQiao(Exchange):
    """
    长桥交易所实现 (LongPort)
    """

    def __init__(self):
        # 初始化配置和上下文
        self.config = Config.from_env()
        self.quote_ctx = QuoteContext(self.config)
        self.trade_ctx = TradeContext(self.config)

        # 缓存与常量
        self.stock_list_cache = None
        self.tz_sh = pytz.timezone("Asia/Shanghai")
        self.tz_ny = pytz.timezone("America/New_York")

        # 默认回看周期配置
        self.LOOKBACK_PERIODS = {
            "1m": timedelta(days=5),
            "5m": timedelta(days=15),
            "15m": timedelta(days=30),
            "30m": timedelta(days=60),
            "60m": timedelta(days=90),
            "d": timedelta(days=365 * 2),
            "w": timedelta(days=365 * 5),
            "m": timedelta(days=365 * 10),
        }

        # 周期映射
        self.PERIOD_MAP = {
            "1m": Period.Min_1, "5m": Period.Min_5, "15m": Period.Min_15,
            "30m": Period.Min_30, "60m": Period.Min_60, "d": Period.Day,
            "w": Period.Week, "m": Period.Month,
        }

    def default_code(self) -> str:
        return "TSLA.US"

    def support_frequencys(self) -> dict:
        return {
            "1m": "1 Min", "5m": "5 Min", "15m": "15 Min", "30m": "30 Min",
            "60m": "60 Min", "d": "Day", "w": "Week", "m": "Month",
        }

    def all_stocks(self):
        """
        获取支持的所有股票列表 (带缓存)
        """
        if self.stock_list_cache:
            return self.stock_list_cache

        try:
            # 获取美股隔夜支持列表作为基础列表
            resp = self.quote_ctx.security_list(Market.US, SecurityListCategory.Overnight)
            self.stock_list_cache = [{"code": info.symbol, "name": info.name_en} for info in resp]
            return self.stock_list_cache
        except Exception as e:
            LogUtil.error(f"长桥获取股票列表失败: {e}")
            return []

    def now_trading(self):
        """
        判断当前是否为交易时间 (美股)
        """
        try:
            now_et = datetime.now(self.tz_ny).time()
            sessions = self.quote_ctx.trading_session()

            for session in sessions:
                if session.market != Market.US:
                    continue

                # 检查所有交易时段
                for trade_info in session.trade_sessions:
                    start = trade_info.begin_time
                    end = trade_info.end_time

                    if not (isinstance(start, time) and isinstance(end, time)):
                        continue

                    # 交易时段判断 (左闭右开)
                    if start <= end:
                        if start <= now_et < end:
                            return True
                    else:
                        # 跨午夜情况
                        if now_et >= start or now_et < end:
                            return True
            return False

        except Exception as e:
            LogUtil.error(f"判断交易时间异常: {e}")
            return False

    def klines(self, code: str, frequency: str, start_date: str = None, end_date: str = None,
               args=None) -> pd.DataFrame:
        """
        获取 K 线数据 (分页获取并合并)
        """
        if frequency not in self.PERIOD_MAP:
            # 简单处理：不支持的周期直接返回空，或者可以像原代码一样递归调用 1m 进行 resample
            return pd.DataFrame()

        # 1. 时间处理
        end_dt = str_to_datetime(end_date) if end_date else datetime.now(self.tz_sh)
        if end_dt.tzinfo is None:
            end_dt = self.tz_sh.localize(end_dt)

        if start_date:
            start_dt = str_to_datetime(start_date)
            if start_dt.tzinfo is None:
                start_dt = self.tz_sh.localize(start_dt)
        else:
            start_dt = end_dt - self.LOOKBACK_PERIODS.get(frequency, timedelta(days=30))

        # 2. API 参数准备
        period = self.PERIOD_MAP[frequency]
        adjust = AdjustType.ForwardAdjust

        all_candles = []
        current_cursor = end_dt

        try:
            # 3. 分页循环获取
            while True:
                candlesticks = self.quote_ctx.history_candlesticks_by_offset(
                    symbol=code,
                    period=period,
                    adjust_type=adjust,
                    forward=False,  # 向前(历史)查找
                    count=1000,
                    time=current_cursor,
                    trade_sessions=TradeSessions.Intraday
                )

                if not candlesticks:
                    break

                # 转换数据
                data_list = []
                min_ts = None

                for c in candlesticks:
                    # 转换为带时区的 datetime
                    c_dt = pd.Timestamp(c.timestamp).tz_convert(self.tz_sh)
                    if min_ts is None or c_dt < min_ts:
                        min_ts = c_dt

                    if c_dt >= start_dt:
                        data_list.append({
                            "date": c_dt,
                            "code": code,
                            "open": float(c.open),
                            "high": float(c.high),
                            "low": float(c.low),
                            "close": float(c.close),
                            "volume": float(c.volume),
                            "frequency": frequency
                        })

                # 添加到总列表 (注意：API返回通常是时间倒序或正序，这里我们收集后统一排序)
                if data_list:
                    all_candles.extend(data_list)

                # 4. 终止条件判断
                # 如果获取的数据中最老的时间已经小于 start_dt，说明数据够了
                if min_ts and min_ts < start_dt:
                    break

                # 如果返回数量不足 1000，说明没有更多历史数据了
                if len(candlesticks) < 1000:
                    break

                # 更新游标为当前批次的最早时间，用于下一次查询
                current_cursor = min_ts

            if not all_candles:
                return pd.DataFrame()

            # 5. 构建 DataFrame
            df = pd.DataFrame(all_candles)
            df = df.sort_values(by="date").reset_index(drop=True)

            # 过滤不需要的列并返回
            return df[["date", "frequency", "code", "open", "high", "low", "close", "volume"]]

        except Exception as e:
            LogUtil.error(f"获取K线数据异常 {code}: {e}")
            return pd.DataFrame()

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取 Tick 数据
        """
        try:
            quotes = self.quote_ctx.quote(codes)
            res = {}
            for q in quotes:
                last_done = float(q.last_done)
                prev_close = float(q.prev_close)

                # 计算涨跌幅
                rate = 0.0
                if prev_close > 0:
                    rate = round(((last_done - prev_close) / prev_close) * 100, 2)

                res[q.symbol] = Tick(
                    code=q.symbol,
                    last=last_done,
                    buy1=0.0,  # 长桥Quote可能不直接包含盘口，需深度行情接口，此处置0
                    sell1=0.0,
                    high=float(q.high),
                    low=float(q.low),
                    open=float(q.open),
                    volume=float(q.volume),
                    rate=rate
                )
            return res
        except Exception as e:
            LogUtil.error(f"获取Ticks异常: {e}")
            return {}

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票静态信息
        """
        try:
            # 自动补全 .US 后缀
            symbol = code if code.endswith('.US') else f"{code}.US"
            infos = self.quote_ctx.static_info([symbol])

            if not infos:
                return None

            info = infos[0]
            return {
                "code": info.symbol,
                "name": info.name_cn,
                "exchange": info.exchange,
                "currency": info.currency,
                "lot_size": info.lot_size,
                "total_shares": info.total_shares,
                "circulating_shares": info.circulating_shares,
            }
        except Exception as e:
            LogUtil.error(f"获取股票信息异常 {code}: {e}")
            return None

    def stock_owner_plate(self, code: str):
        # 暂不支持
        return None

    def plate_stocks(self, code: str):
        # 暂不支持
        return None

    def balance(self):
        """
        获取账户资金
        """
        try:
            balances = self.trade_ctx.account_balance()
            if not balances:
                return {}
            # 默认取第一个币种，通常是 USD
            b = balances[0]
            return {
                "total_cash": float(b.total_cash),
                "max_finance_amount": float(b.max_finance_amount),
                "currency": b.currency,
                "net_assets": float(b.total_cash)  # 简化处理，可视情况取 total_assets
            }
        except Exception as e:
            LogUtil.error(f"获取账户资金异常: {e}")
            return {}

    def positions(self, code: str = ""):
        """
        获取持仓信息
        """
        try:
            resp = self.trade_ctx.stock_positions()
            positions = []
            for channel in resp.channels:
                for p in channel.positions:
                    pos_data = {
                        "code": p.symbol,
                        "qty": float(p.quantity),
                        "can_sell_qty": float(p.available_quantity),
                        "cost_price": float(p.cost_price),
                        "profit": float(p.quantity) * (float(p.last_done) - float(p.cost_price)) if p.last_done else 0.0
                    }
                    if code and p.symbol == code:
                        return [pos_data]
                    positions.append(pos_data)

            return positions
        except Exception as e:
            LogUtil.error(f"获取持仓异常: {e}")
            return []

    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单接口
        """
        try:
            side = OrderSide.Buy if o_type.lower() == "buy" else OrderSide.Sell
            order_type = OrderType.MO  # 市价单
            price = None

            if args and "price" in args:
                price = Decimal(str(args["price"]))
                order_type = OrderType.LO  # 限价单

            qty = Decimal(str(amount))

            resp = self.trade_ctx.submit_order(
                symbol=code,
                order_type=order_type,
                side=side,
                submitted_quantity=qty,
                time_in_force=TimeInForceType.Day,
                submitted_price=price,
            )
            return resp.order_id
        except Exception as e:
            LogUtil.error(f"下单异常 {code} {o_type}: {e}")
            return None