import time
import functools
from datetime import timedelta, datetime, time as datetime_time
from typing import Dict, List, Union
import pandas as pd
import pytz
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

# Longport SDK imports
from longport.openapi import Config, QuoteContext, TradeContext, Market, Period, \
    AdjustType, OrderSide, OrderType, TimeInForceType, SecurityListCategory, TradeSessions

# Chanlun SDK imports
from chanlun import fun
from chanlun.exchange import Exchange
from chanlun.exchange.exchange import Tick
from chanlun.fun import str_to_datetime
from chanlun.tools.log_util import LogUtil

# 统一时区设置
__tz = pytz.timezone("Asia/Shanghai")


# === 性能监控装饰器 ===
def time_logger(func):
    """
    用于监控函数执行耗时的装饰器
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_ts = time.time()
        # LogUtil.info(f"===> Start: {func.__name__}") # 可根据需要开启
        result = func(*args, **kwargs)
        end_ts = time.time()
        duration = end_ts - start_ts
        LogUtil.info(f"<=== Finish: {func.__name__}, Duration: {duration:.4f}s")
        return result

    return wrapper


@fun.singleton
class ExchangeChangQiao(Exchange):
    """
    长桥交易所实现 - 高性能并发优化版
    """

    def __init__(self):
        self.config = Config.from_env()
        self.quote_ctx = QuoteContext(self.config)
        self.trade_ctx = TradeContext(self.config)
        self.stock_list_cache = None

        # 线程池配置
        # 建议设置在 8-16 之间。过高可能导致 API 触发流控限制 (Rate Limit)
        self.executor = ThreadPoolExecutor(max_workers=16)

    def default_code(self) -> str:
        return "TSLA.US"

    def support_frequencys(self) -> dict:
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
        if self.stock_list_cache is not None:
            return self.stock_list_cache
        try:
            resp = self.quote_ctx.security_list(Market.US, SecurityListCategory.Overnight)
            self.stock_list_cache = [{"code": info.symbol, "name": info.name_en} for info in resp]
            return self.stock_list_cache
        except Exception as e:
            LogUtil.info(f"Error in all_stocks: {e}")
            return []

    def now_trading(self):
        """
        判断当前是否交易时间
        """
        if pytz is None:
            return False

        try:
            et_timezone = pytz.timezone('America/New_York')
            now_et = datetime.now(et_timezone).time()
            sessions = self.quote_ctx.trading_session()

            for session in sessions:
                if session.market == Market.US:
                    for trade_info in session.trade_sessions:
                        try:
                            start_time = trade_info.begin_time
                            end_time = trade_info.end_time

                            if not isinstance(start_time, datetime_time) or not isinstance(end_time, datetime_time):
                                continue

                            if start_time > end_time:
                                if now_et >= start_time or now_et < end_time:
                                    return True
                            else:
                                if start_time <= now_et and now_et < end_time:
                                    return True
                        except AttributeError:
                            continue
                    return False
            return False
        except Exception as e:
            LogUtil.info(f"Error checking trading session: {e}")
            return False

    def _fetch_segment_data(self, code, period, adjust, end_dt, start_dt_limit):
        """
        [内部并发任务] 获取特定时间片段的数据
        策略：从 end_dt 向前获取，直到遇到 start_dt_limit 或数据耗尽
        """
        segment_candles = []
        current_cursor = end_dt
        retry_count = 0
        max_retries = 3

        # 确保 limit 是带时区的
        tz = pytz.timezone("Asia/Shanghai")
        if start_dt_limit.tzinfo is None:
            start_dt_limit = tz.localize(start_dt_limit)

        while True:
            try:
                # 核心 API 调用
                candlesticks = self.quote_ctx.history_candlesticks_by_offset(
                    symbol=code,
                    period=period,
                    adjust_type=adjust,
                    forward=False,  # 向前追溯
                    count=1000,  # 单次最大条数
                    time=current_cursor,
                    trade_sessions=TradeSessions.Intraday
                )
                retry_count = 0  # 成功重置计数
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    LogUtil.error(f"API Retry failed for segment {end_dt}: {e}")
                    break
                time.sleep(0.5 * retry_count)  # 退避重试
                continue

            if not candlesticks:
                break

            # 获取这批数据中最老的一条时间，用于更新游标
            oldest_candle = candlesticks[0]

            # --- [修复] 统一转为带时区的 datetime 进行比较 ---
            try:
                ts = oldest_candle.timestamp
                if isinstance(ts, (int, float)):
                    # Unix 时间戳 (秒) -> UTC -> 转上海
                    oldest_dt = pd.to_datetime(ts, unit='s', utc=True).dt.tz_convert(tz).to_pydatetime()
                else:
                    # Datetime 对象
                    oldest_dt = pd.to_datetime(ts).to_pydatetime()
                    if oldest_dt.tzinfo is None:
                        # 如果是 Naive，假设它已经是上海时间，贴上标签
                        oldest_dt = tz.localize(oldest_dt)
                    else:
                        oldest_dt = oldest_dt.astimezone(tz)
            except Exception as e:
                LogUtil.error(f"Time parse error in segment fetch: {e}")
                break
            # -------------------------------------------

            segment_candles.extend(candlesticks)

            # 更新游标
            current_cursor = oldest_dt

            if len(candlesticks) < 1000:
                break

            if oldest_dt < start_dt_limit:
                break

        return segment_candles

    @time_logger  # 开启耗时监控
    def klines(
            self,
            code: str,
            frequency: str,
            start_date: str = None,
            end_date: str = None,
            args=None,
    ) -> pd.DataFrame:
        """
        获取 Kline 线 (并发优化版)
        """
        tz = pytz.timezone("Asia/Shanghai")

        # 1. 默认回看周期配置
        DEFAULT_LOOKBACK = {
            "1m": timedelta(days=30),
            "5m": timedelta(days=60),
            "15m": timedelta(days=90),
            "30m": timedelta(days=180),
            "60m": timedelta(days=365),
            "d": timedelta(days=365 * 5),
            "w": timedelta(days=365 * 5),
            "m": timedelta(days=365 * 10),
        }

        # 2. 时间标准化处理
        # 标记：是否是“历史查询”模式（即用户是否指定了截止日期）
        is_history_query = end_date is not None

        now_dt = datetime.now(tz)

        if is_history_query:
            # 用户指定了结束时间，严格按照用户时间查询
            end_dt = str_to_datetime(end_date)
            if end_dt.tzinfo is None:
                end_dt = tz.localize(end_dt)
            else:
                end_dt = end_dt.astimezone(tz)
            # API 请求游标
            api_cursor_dt = end_dt
        else:
            # 用户查最新：end_dt 设为当前时间
            end_dt = now_dt
            # API 请求游标：往后推 5 分钟，确保能囊括服务器端刚刚生成的最新 K 线
            # 解决本地时间落后服务器几秒导致拿不到最新数据的问题
            api_cursor_dt = now_dt + timedelta(minutes=5)

        if start_date:
            start_dt = str_to_datetime(start_date)
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
        else:
            lookback = DEFAULT_LOOKBACK.get(frequency, timedelta(days=30))
            start_dt = end_dt - lookback

        # 3. 周期映射
        period_map = {
            "1m": Period.Min_1, "5m": Period.Min_5, "15m": Period.Min_15,
            "30m": Period.Min_30, "60m": Period.Min_60, "d": Period.Day,
            "w": Period.Week, "m": Period.Month,
        }

        if frequency not in period_map:
            base_start = start_dt.isoformat() if start_dt else None
            return self.klines(code, "1m", base_start, end_date, args)

        period = period_map[frequency]
        adjust = AdjustType.ForwardAdjust

        # 5. 并发分片策略
        if frequency == '1m':
            chunk_days = 5
        elif frequency == '5m':
            chunk_days = 15
        elif frequency in ['15m', '30m', '60m']:
            chunk_days = 60
        else:
            chunk_days = 3650

        tasks = []
        curr_end = api_cursor_dt  # 使用带 Buffer 的游标

        while curr_end > start_dt:
            curr_start = curr_end - timedelta(days=chunk_days)
            if curr_start < start_dt:
                curr_start = start_dt

            tasks.append(self.executor.submit(
                self._fetch_segment_data,
                code, period, adjust, curr_end, curr_start
            ))
            curr_end = curr_start
            if curr_end <= start_dt:
                break

        # 6. 收集结果
        all_candles = []
        for future in as_completed(tasks):
            try:
                res = future.result()
                if res:
                    all_candles.extend(res)
            except Exception as e:
                LogUtil.error(f"Kline segment fetch error: {e}")

        if not all_candles:
            return pd.DataFrame()

        # *** 7. 向量化构建 DataFrame (核心修复) ***
        try:
            unique_candles = {c.timestamp: c for c in all_candles}.values()
            if not unique_candles:
                return pd.DataFrame()

            data = [
                (c.timestamp, float(c.open), float(c.high), float(c.low), float(c.close), float(c.volume))
                for c in unique_candles
            ]

            df = pd.DataFrame(data, columns=["date", "open", "high", "low", "close", "volume"])

            # === [智能时间处理逻辑] ===
            if len(data) > 0:
                first_ts = data[0][0]

                # 情况 A: 必须明确区分 Unix Timestamp 和 Datetime Object
                if isinstance(first_ts, (int, float)):
                    # Unix Timestamp: 必须指定 unit='s' 和 utc=True，然后转上海
                    df["date"] = pd.to_datetime(df["date"], unit='s', utc=True)
                    df["date"] = df["date"].dt.tz_convert("Asia/Shanghai")
                else:
                    # 情况 B: 已经是 Datetime 对象 (你现在的场景)
                    df["date"] = pd.to_datetime(df["date"])

                    if df["date"].dt.tz is None:
                        # 关键修复：
                        # 如果是 Naive 时间，且数值已经是本地时间 (如 00:07)，
                        # 使用 tz_localize 仅仅加上时区标签，不要转换数值。
                        df["date"] = df["date"].dt.tz_localize("Asia/Shanghai")
                    else:
                        # 如果自带时区，则转换为上海时间
                        df["date"] = df["date"].dt.tz_convert("Asia/Shanghai")
            # ==========================

            # === [智能过滤逻辑] ===
            # 1. 下界过滤：必须大于等于 start_dt (保留)
            mask = df["date"] >= start_dt

            # 2. 上界过滤：仅当用户查历史 (指定了 end_date) 时才严格过滤
            # 查实时增量时，不做 <= end_dt 过滤，避免因本地时钟微小差异丢掉最新 K 线
            if is_history_query:
                mask = mask & (df["date"] <= end_dt)

            df = df.loc[mask]

            # 补充字段并排序
            df["code"] = code
            df["frequency"] = frequency
            df = df.sort_values(by="date").reset_index(drop=True)

            return df[["date", "frequency", "code", "open", "high", "low", "close", "volume"]]

        except Exception as e:
            LogUtil.error(f"DataFrame construction error: {e}")
            return pd.DataFrame()

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取 Ticks
        """
        try:
            quotes = self.quote_ctx.quote(codes)
            res = {}
            for q in quotes:
                last_done = float(q.last_done)
                prev_close = float(q.prev_close)

                cal_rate = 0.0
                if prev_close > 0:
                    cal_rate = round(((last_done - prev_close) / prev_close) * 100, 2)

                res[q.symbol] = Tick(
                    code=q.symbol,
                    last=last_done,
                    buy1=0.0,
                    sell1=0.0,
                    high=float(q.high),
                    low=float(q.low),
                    open=float(q.open),
                    volume=float(q.volume),
                    rate=cal_rate
                )
            return res
        except Exception as e:
            LogUtil.error(f"Error in ticks: {e}")
            return {}

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票基础信息
        """
        try:
            # 兼容处理
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
            LogUtil.info(f"Error in stock_info: {e}")
            return None

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")

    def balance(self):
        """
        获取账户余额
        """
        try:
            balances = self.trade_ctx.account_balance()
            if not balances:
                return {}
            b = balances[0]
            return {
                "total_cash": float(b.total_cash),
                "max_finance_amount": float(b.max_finance_amount),
                "currency": b.currency,
            }
        except Exception as e:
            LogUtil.info(f"Error in balance: {e}")
            return {}

    def positions(self, code: str = ""):
        """
        获取持仓
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
            LogUtil.info(f"Error in positions: {e}")
            return []

    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单
        """
        side = OrderSide.Buy if o_type.lower() == "buy" else OrderSide.Sell
        order_type = OrderType.MO
        price = None

        if args and "price" in args:
            price = Decimal(str(args["price"]))
            order_type = OrderType.LO

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
            LogUtil.info(f"Error in order: {e}")
            return None