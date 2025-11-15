from datetime import timedelta, datetime
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
    ) -> pd.DataFrame:
        """
        获取 Kline 线
        """

        # 0. 定义时区
        tz = pytz.timezone("Asia/Shanghai")

        # 1. 定义默认回看周期
        DEFAULT_LOOKBACK = {
            "1m": timedelta(days=30),
            "5m": timedelta(days=60),
            "15m": timedelta(days=90),
            "30m": timedelta(days=120),
            "60m": timedelta(days=365),
            "d": timedelta(days=365 * 2),
            "w": timedelta(days=365 * 10),
            "m": timedelta(days=365 * 20),
        }

        # 2. 确定最终的开始和结束 datetime 对象 (带时区)

        # 默认结束时间为 "现在" (带时区)
        end_dt = str_to_datetime(end_date) if end_date else datetime.now(tz)
        if end_dt.tzinfo is None or end_dt.tzinfo.utcoffset(end_dt) is None:
            end_dt = tz.localize(end_dt)  # 确保时区
        else:
            end_dt = end_dt.astimezone(tz)

        start_dt = None
        if start_date:
            start_dt = str_to_datetime(start_date)
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
        else:
            lookback_delta = DEFAULT_LOOKBACK.get(frequency)
            if lookback_delta:
                start_dt = end_dt - lookback_delta
        # period_map 假设已定义
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

        # 3. 处理自定义周期 (resampling)
        if frequency not in period_map:
            base_start_dt = start_dt
            if not base_start_dt:
                # 如果是自定义周期且没有 start_date, 递归时会使用 "1m" 的默认回看
                base_start_str = None
            else:
                base_start_str = base_start_dt.isoformat()

            end_str = end_dt.isoformat()  # end_dt 始终有值

            print(f"Custom frequency '{frequency}'. Fetching '1m' base data...")
            base_klines = self.klines(code, "1m", base_start_str, end_str, args)

            if base_klines is None or base_klines.empty:
                print("No base data found for resampling.")
                return pd.DataFrame()

            print("Skipping resampling for this example.")
            return base_klines  # 仅为示例

        period = period_map[frequency]
        adjust = AdjustType.NoAdjust
        if args and "adjust" in args:
            if args["adjust"] == "qfq":
                adjust = AdjustType.ForwardAdjust

        # *** 5. API 调用 (使用 offset 进行分页) ***
        try:
            # 使用字典去重
            all_candles_dict = {}
            current_page_end_dt = end_dt  # 分页 "光标"

            # 安全中断，防止无限循环
            MAX_LOOPS = 100
            loop_count = 0

            print(f"Fetching data for {code} ({frequency}). Range: {start_dt} -> {end_dt}")

            while loop_count < MAX_LOOPS:
                loop_count += 1
                print(f"--- Pagination loop {loop_count}: Fetching 1000 bars *before* {current_page_end_dt} ---")

                # *** 使用 history_candlesticks_by_offset ***
                candlesticks = self.quote_ctx.history_candlesticks_by_offset(
                    symbol=code,
                    period=period,
                    adjust_type=adjust,
                    forward=False,  # <-- 核心：获取之前的数据 (renamed from is_next)
                    count=1000,  # <-- 核心：每次获取 1000 条
                    time=current_page_end_dt  # <-- 核心：分页光标 (renamed from end_time)
                )

                if not candlesticks:
                    print("API returned no data for this page. Breaking loop.")
                    break

                print(f"API returned {len(candlesticks)} items.")

                # 获取这批数据中最早的 K 线时间
                try:
                    oldest_candle_ts_str = candlesticks[0].timestamp
                    # 必须转换为带时区的 datetime 对象
                    oldest_candle_dt = pd.to_datetime(oldest_candle_ts_str).to_pydatetime()
                    if oldest_candle_dt.tzinfo is None:
                        oldest_candle_dt = tz.localize(oldest_candle_dt)
                    else:
                        oldest_candle_dt = oldest_candle_dt.astimezone(tz)
                except Exception as e:
                    break

                # 检查这批数据是否穿过了我们的 start_dt
                if oldest_candle_dt < start_dt:
                    # 是的，这是最后一批需要处理的数据
                    final_batch = []
                    for c in candlesticks:
                        c_dt = pd.to_datetime(c.timestamp).to_pydatetime()
                        if c_dt.tzinfo is None:
                            c_dt = tz.localize(c_dt)
                        else:
                            c_dt = c_dt.astimezone(tz)

                        # 只保留大于等于 start_dt 的数据
                        if c_dt >= start_dt:
                            final_batch.append(c)

                    # 添加到字典并终止
                    for c in final_batch:
                        all_candles_dict[c.timestamp] = c
                    print(f"Reached start_dt ({start_dt}). Added {len(final_batch)} final items. Breaking loop.")
                    break
                else:
                    # 否，这整批数据都在我们的范围内
                    for c in candlesticks:
                        all_candles_dict[c.timestamp] = c

                # 为下一次循环设置分页光标
                current_page_end_dt = oldest_candle_dt
                print(f"Next page will fetch before: {current_page_end_dt}")

                # 如果 API 返回的K线少于 1000, 说明已到历史尽头
                if len(candlesticks) < 1000:
                    print(f"API returned {len(candlesticks)} items (< 1000). Assuming end of history. Breaking loop.")
                    break

            if loop_count == MAX_LOOPS:
                print(f"Warning: klines fetch for {code} reached MAX_LOOPS ({MAX_LOOPS}). Data might be incomplete.")

            # 从字典中获取所有 K 线
            all_candlesticks = list(all_candles_dict.values())

            if not all_candlesticks:
                print("No candlesticks fetched.")
                return pd.DataFrame()

            # 6. 构建 DataFrame
            data = {
                "date": [c.timestamp for c in all_candlesticks],
                "code": [code] * len(all_candlesticks),
                "open": [float(c.open) for c in all_candlesticks],
                "high": [float(c.high) for c in all_candlesticks],
                "low": [float(c.low) for c in all_candlesticks],
                "close": [float(c.close) for c in all_candlesticks],
                "volume": [c.volume for c in all_candlesticks],
                "frequency": [frequency] * len(all_candlesticks),
            }
            df = pd.DataFrame(data)

            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize("Asia/Shanghai")

            df = df.sort_values(by="date")

            return df[["date", "frequency", "code", "open", "high", "low", "close", "volume"]]

        except Exception as e:
            print(f"Error in klines: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

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