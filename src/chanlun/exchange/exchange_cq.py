from typing import Dict, List, Union
import pandas as pd
import pytz
from decimal import Decimal

from longport.openapi import Config, QuoteContext, TradeContext, Market, TradeSession, Period, \
    AdjustType, OrderSide, OrderType, TimeInForceType

from chanlun import fun
from chanlun.exchange import Exchange
from chanlun.exchange.exchange import convert_stock_kline_frequency, Tick
from chanlun.fun import str_to_datetime

# 统一时区设置
__tz = pytz.timezone("Asia/Shanghai")

@fun.singleton
class ExchangeChangQiao(Exchange):
    """
    长桥交易所实现
    """

    def __init__(self):
        self.config = Config.from_env()
        self.quote_ctx = QuoteContext(self.config)
        self.trade_ctx = TradeContext(self.config)

    def default_code(self) -> str:
        """
        返回WEB默认展示的代码
        """
        return "TSLA"

    def support_frequencys(self) -> dict:
        """
        返回交易所支持的周期对照关系
        内部使用代码 ： WEB端展示名称
        """
        return {
            "1m": "1 Min",
            "5m": "5 Min",
            "15m": "15 Min",
            "30m": "30 Min",
            "60m": "60 Min",
            "d": "Day",
            "w": "Week",
            "m": "Month",
        }

    def all_stocks(self):
        """
        获取支持的所有股票列表
        """
        # 获取 CN 市场所有股票（可扩展到其他市场）
        try:
            resp = self.quote_ctx.security_list(Market.US)
            return [{"code": info.symbol, "name": info.name_cn} for info in resp]
        except Exception as e:
            print(f"Error in all_stocks: {e}")
            return []

    def now_trading(self):
        """
        返回当前是否可交易
        """
        sessions = self.quote_ctx.trading_session()
        for session in sessions:
            if session.market == Market.US and session.trade_sessions == TradeSession.Intraday:
                return True
        return False

    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        """
        获取 Kline 线
        """
        period_map = {
            "1m": Period.Min_1,
            "5m": Period.Min_5,
            "15m": Period.Min_15,
            "30m": Period.Min_30,
            "60m": Period.Min_60,
            "d": Period.Day,
            "w": Period.Week,
            "m": Period.Month,
        }
        if frequency not in period_map:
            # 使用基础周期转换
            base_klines = self.klines(code, "1m", start_date, end_date, args)
            if base_klines is None or base_klines.empty:
                return None
            return convert_stock_kline_frequency(base_klines, frequency)

        period = period_map[frequency]
        adjust = AdjustType.NoAdjust
        if args and "adjust" in args:
            if args["adjust"] == "qfq":
                adjust = AdjustType.ForwardAdjust
            elif args["adjust"] == "hfq":
                adjust = AdjustType.NoAdjust  # Assuming Backward exists; adjust if needed

        start = str_to_datetime(start_date) if start_date else None
        end = str_to_datetime(end_date) if end_date else None
        try:
            candlesticks = self.quote_ctx.history_candlesticks_by_date(symbol=code, period=period, adjust_type=adjust, start=start, end=end)

            if not candlesticks:
                return None

            data = {
                "date": [c.timestamp for c in candlesticks],
                "code": [code] * len(candlesticks),
                "open": [float(c.open) for c in candlesticks],
                "high": [float(c.high) for c in candlesticks],
                "low": [float(c.low) for c in candlesticks],
                "close": [float(c.close) for c in candlesticks],
                "volume": [c.volume for c in candlesticks],
                "frequency": [frequency] * len(candlesticks),
            }
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"], unit='s').dt.tz_localize('UTC').dt.tz_convert("UTC")
            return df[["date", "frequency", "code", "open", "high", "low", "close", "volume"]]
        except Exception as e:
            print(f"Error in klines: {e}")
            return None

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取股票列表的 Tick 信息
        """
        try:
            quotes = self.quote_ctx.quote(codes)
            res = {}
            for q in quotes:
                res[q.symbol] = Tick(
                    code=q.symbol,
                    last=float(q.last_done),
                    high=float(q.high),
                    low=float(q.low),
                    open=float(q.open),
                    volume=q.volume
                )
            return res
        except Exception as e:
            print(f"Error in ticks: {e}")
            return {}

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票的基本信息
        """
        try:
            symbol = code + '.US'
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
            print(f"Error in stock_info: {e}")
            return None

    def stock_owner_plate(self, code: str):
        """
        股票所属板块信息
        """
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        """
        获取板块股票列表信息
        """
        raise Exception("交易所不支持")

    def balance(self):
        """
        账户资产信息
        """
        try:
            balances = self.trade_ctx.account_balance()
            if not balances:
                return {}
            b = balances[0]  # Assume first currency
            return {
                "total_cash": float(b.total_cash),
                "max_finance_amount": float(b.max_finance_amount),
                "currency": b.currency,
            }
        except Exception as e:
            print(f"Error in balance: {e}")
            return {}

    def positions(self, code: str = ""):
        """
        当前账户持仓信息
        """
        try:
            resp = self.trade_ctx.stock_positions()
            positions = []
            for channel in resp.channels:
                for p in channel.positions:
                    pos = {
                        "code": p.symbol,
                        "qty": p.quantity,
                        "can_sell_qty": p.available_quantity,
                        "cost_price": float(p.cost_price),
                    }
                    positions.append(pos)
            if code:
                positions = [p for p in positions if p["code"] == code]
            return positions
        except Exception as e:
            print(f"Error in positions: {e}")
            return []

    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单接口
        """
        side = OrderSide.Buy if o_type.lower() == "buy" else OrderSide.Sell
        order_type = OrderType.MO  # Default market order
        price = None
        if args and "price" in args:
            price = Decimal(str(args["price"]))
            order_type = OrderType.LO  # Limit order
        qty = Decimal(str(amount))
        try:
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
            print(f"Error in order: {e}")
            return None