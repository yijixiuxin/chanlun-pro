import os
import time
import atexit
import weakref
import threading
import functools
from datetime import timedelta, datetime, time as datetime_time
from typing import Dict, List, Union
import pandas as pd
import pytz
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from longbridge.openapi import Config, QuoteContext, TradeContext, Market, Period, \
    AdjustType, OrderSide, OrderType, TimeInForceType, SecurityListCategory, TradeSessions, OpenApiException

# Chanlun SDK imports
from chanlun import fun
from chanlun.exchange import Exchange
from chanlun.exchange.exchange import Tick
from chanlun.fun import str_to_datetime
from chanlun.tools.log_util import LogUtil

# 统一时区设置
__tz = pytz.timezone("Asia/Shanghai")


def _get_env(new_key: str, legacy_key: str = None):
    # 优先读取长桥当前环境变量；如果用户还在沿用旧的 LONGPORT_*，这里继续兼容。
    return os.getenv(new_key) or (os.getenv(legacy_key) if legacy_key else None)


def _get_env_bool(new_key: str, legacy_key: str = None, default: bool = False) -> bool:
    # SDK 的布尔配置通常通过环境变量传入，这里统一做一次字符串到 bool 的转换。
    value = _get_env(new_key, legacy_key)
    if value is None:
        return default
    return value.lower() == "true"


def _build_longbridge_config() -> Config:
    """
    优先使用长桥新环境变量，兼容旧 LONGPORT_* 配置与新旧 SDK 初始化方法。
    """
    app_key = _get_env("LONGBRIDGE_APP_KEY", "LONGPORT_APP_KEY")
    app_secret = _get_env("LONGBRIDGE_APP_SECRET", "LONGPORT_APP_SECRET")
    access_token = _get_env("LONGBRIDGE_ACCESS_TOKEN", "LONGPORT_ACCESS_TOKEN")

    http_url = _get_env("LONGBRIDGE_HTTP_URL", "LONGPORT_HTTP_URL")
    quote_ws_url = _get_env("LONGBRIDGE_QUOTE_WS_URL", "LONGPORT_QUOTE_WS_URL")
    trade_ws_url = _get_env("LONGBRIDGE_TRADE_WS_URL", "LONGPORT_TRADE_WS_URL")
    enable_overnight = _get_env_bool("LONGBRIDGE_ENABLE_OVERNIGHT", "LONGPORT_ENABLE_OVERNIGHT", False)
    enable_print_quote_packages = _get_env_bool(
        "LONGBRIDGE_PRINT_QUOTE_PACKAGES", "LONGPORT_PRINT_QUOTE_PACKAGES", True
    )
    log_path = _get_env("LONGBRIDGE_LOG_PATH", "LONGPORT_LOG_PATH")

    # 新版 SDK 优先走 from_apikey；这样可以显式传入 host / ws / 其他开关，避免不同版本行为差异。
    if hasattr(Config, "from_apikey") and app_key and app_secret and access_token:
        return Config.from_apikey(
            app_key,
            app_secret,
            access_token,
            http_url=http_url,
            quote_ws_url=quote_ws_url,
            trade_ws_url=trade_ws_url,
            enable_overnight=enable_overnight,
            enable_print_quote_packages=enable_print_quote_packages,
            log_path=log_path,
        )

    # 某些版本只提供 from_apikey_env，会自行从环境变量读取凭证与接入点。
    if hasattr(Config, "from_apikey_env"):
        return Config.from_apikey_env()

    # 再兼容更老的 SDK 版本；如果没有这个方法，会继续走最下方的构造函数初始化。
    if hasattr(Config, "from_env"):
        return Config.from_env()

    # 兜底直接实例化，兼容不提供类方法的 SDK 版本。
    return Config(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        http_url=http_url,
        quote_ws_url=quote_ws_url,
        trade_ws_url=trade_ws_url,
        enable_overnight=enable_overnight,
        enable_print_quote_packages=enable_print_quote_packages,
        log_path=log_path,
    )


class RateLimiter:
    """
    简单令牌桶限流器 (Simple Token Bucket)
    """
    def __init__(self, calls: int, period: float):
        self.calls = calls
        self.period = period
        self.timestamps = []
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            while True:
                now = time.time()
                self.timestamps = [t for t in self.timestamps if now - t < self.period]
                if len(self.timestamps) < self.calls:
                    self.timestamps.append(now)
                    return
                wait_time = self.timestamps[0] + self.period - now
                if wait_time > 0:
                    time.sleep(wait_time)


def is_retryable_exception(exception):
    """
    判断异常是否需要重试
    """
    if isinstance(exception, OpenApiException):
        # 301600: out of minute kline begin date - 数据超限，不重试
        # 使用 getattr 避免 LSP 或版本差异导致属性不存在报错
        code = getattr(exception, 'code', None)
        if code == 301600:
            return False
    return True


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
        # 这里只构建配置对象，不在构造时立刻连网。
        # 真正访问长桥时再懒加载 QuoteContext / TradeContext，避免应用启动阶段因网络问题直接失败。
        self.config = _build_longbridge_config()
        self._quote_context = None
        self._trade_context = None
        # 历史 bug：原来用单一字段 stock_list_cache 缓存 all_stocks 结果，且 all_stocks 写死
        # 只查 Market.US 的列表。配合 @fun.singleton（A/HK/US 共享同一实例），结果是任何
        # market 调 all_stocks 都返回同一份美股数据，导致 web 搜索框 A 股/港股全是美股标的。
        # 改为按 market 维度分桶缓存，并支持显式传入 market 区分。
        self.stock_list_cache: Dict[str, List[Dict]] = {}
        # 由 get_exchange 在创建实例后注入，标识"当前调用来自哪个 market"。
        # 不传时 all_stocks 兜底走 US（保留原行为，避免外部裸调用 ExchangeChangQiao() 后行为突变）。
        self.default_market: str = "us"

        # 线程池配置
        # 建议设置在 8-16 之间。过高可能导致 API 触发流控限制 (Rate Limit)
        self.executor = ThreadPoolExecutor(max_workers=16)
        # 初始化限流器 (3 QPS)。长桥行情接口默认配额很紧张（约 10 QPS），
        # 这里保守用 3 QPS 留出突发余量，避免触发 301600。如确认账号配额更高可调大。
        self.rate_limiter = RateLimiter(calls=3, period=1.0)

        # G8：进程退出时显式关闭线程池，避免长桥 SDK 的 quote/trade 网络上下文
        # 在 daemon 线程里被强行打断造成日志噪音/连接半关。
        # 用 weakref 避免闭包持有 self 阻碍单例 GC（虽然 fun.singleton 通常永生，
        # 但保持 weakref 风格更安全）。
        _self_ref = weakref.ref(self)

        def _shutdown_on_exit():
            inst = _self_ref()
            if inst is None:
                return
            try:
                inst.executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

        atexit.register(_shutdown_on_exit)

    def _quote_ctx(self) -> QuoteContext:
        # QuoteContext 建立时会初始化行情连接，这里按需创建并复用单例连接。
        if self._quote_context is None:
            self._quote_context = QuoteContext(self.config)
        return self._quote_context

    def _trade_ctx(self) -> TradeContext:
        # TradeContext 同样使用懒加载，避免只查行情时也初始化交易连接。
        if self._trade_context is None:
            self._trade_context = TradeContext(self.config)
        return self._trade_context

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

    # 长桥 OpenAPI 实测结论（2026-04 验证）：
    #   - SecurityListCategory 枚举只有 Overnight 一个值；
    #   - security_list 仅 (Market.US, Overnight) 组合可用，其它市场 / 不传 category 都返回 param_error；
    #   - QuoteContext 没有任何"按市场拉全量列表"的能力（watchlist 是用户自选股、static_info 必须先知道 code）。
    # 因此港股 / A 股的全量列表只能从外部数据源拿。这里采用 lazy fallback：
    #   - hk → 复用 ExchangeTDXHK.all_stocks()（通达信港股）
    #   - a  → 复用 ExchangeTDX.all_stocks()（通达信 A 股）
    # K 线 / Tick / 下单等其它接口仍然完全走长桥，不受影响。
    _LB_MARKET_MAP = {
        "us": Market.US,
    }

    def _fallback_all_stocks(self, market_key: str) -> List[Dict]:
        """当长桥 SDK 不支持时，借用项目里其它 exchange 实现拿全量列表。

        ``ExchangeTDXHK`` / ``ExchangeTDX`` 在内部都有自己的缓存（``g_all_stocks``）和
        服务器选择逻辑，这里 lazy import 避免引入循环依赖、也避免在长桥单进程启动时
        无谓地建立通达信连接。

        失败容错：检测 ``init_failed`` 标志, 通达信连接坏了就快速返回空, 不再卡 30s。
        """
        try:
            if market_key == "hk":
                from chanlun.exchange.exchange_tdx_hk import ExchangeTDXHK
                ex = ExchangeTDXHK()
                if getattr(ex, "init_failed", False):
                    LogUtil.info(
                        "all_stocks fallback: ExchangeTDXHK init_failed, return []"
                    )
                    return []
                return ex.all_stocks() or []
            if market_key == "a":
                from chanlun.exchange.exchange_tdx import ExchangeTDX
                ex = ExchangeTDX()
                if getattr(ex, "init_failed", False):
                    LogUtil.info(
                        "all_stocks fallback: ExchangeTDX init_failed, return []"
                    )
                    return []
                return ex.all_stocks() or []
        except Exception as e:
            LogUtil.info(
                f"all_stocks fallback failed: market={market_key!r}, err={e}"
            )
        return []

    def all_stocks(self, market: str = None):
        """获取指定 market 的全量 symbol 列表。

        :param market: 可选；项目内的 market 标识（"a"/"hk"/"us" 等）。不传则使用
            ``self.default_market``（由 ``get_exchange`` 注入）。这是为了兼容 ``Exchange``
            基类无参签名，同时避免历史 bug——同一个长桥单例被三个市场共用，原实现写死
            ``Market.US`` 导致 A 股/港股的 ``all_stocks`` 都返回美股列表。

        缓存策略：按 market 分桶，避免互相污染。
        """
        market_key = (market or self.default_market or "us").lower()

        cached = self.stock_list_cache.get(market_key)
        if cached is not None:
            return cached

        try:
            stocks: List[Dict] = []
            lb_market = self._LB_MARKET_MAP.get(market_key)
            if lb_market is not None:
                # 美股：长桥唯一支持 security_list 的市场，保留原 Overnight 行为。
                resp = self._quote_ctx().security_list(
                    lb_market, SecurityListCategory.Overnight
                )
                stocks = [
                    {"code": info.symbol, "name": getattr(info, "name_cn", None) or info.name_en}
                    for info in resp
                ]
            else:
                # 港股 / A 股 / 外汇：长桥不提供全量列表，借助通达信 exchange 拿。
                stocks = self._fallback_all_stocks(market_key)
                LogUtil.info(
                    f"all_stocks: market={market_key!r} 通过 fallback 获取到 {len(stocks)} 条"
                )

            self.stock_list_cache[market_key] = stocks
            return stocks
        except Exception as e:
            LogUtil.info(f"Error in all_stocks(market={market_key!r}): {e}")
            # 出错时也尝试 fallback，避免单点失败导致搜索完全不可用。
            if market_key not in self._LB_MARKET_MAP:
                return self._fallback_all_stocks(market_key)
            return []

    def now_trading(self):
        """
        判断当前是否交易时间
        """
        if pytz is None:
            return False

        def _do_check():
            try:
                et_timezone = pytz.timezone('America/New_York')
                now_et = datetime.now(et_timezone).time()
                sessions = self._quote_ctx().trading_session()

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

        # Add timeout wrapper to prevent blocking the main thread
        try:
            future = self.executor.submit(_do_check)
            return future.result(timeout=2.0)
        except Exception as e:
            LogUtil.error(f"now_trading timeout or error: {e}")
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception(is_retryable_exception))
    def _fetch_candlesticks_api(self, symbol, period, adjust_type, count, time_cursor, trade_sessions):
        """
        带限流和重试的 API 调用
        """
        self.rate_limiter.wait()
        return self._quote_ctx().history_candlesticks_by_offset(
            symbol=symbol,
            period=period,
            adjust_type=adjust_type,
            forward=False,  # 向前追溯
            count=count,
            time=time_cursor,
            trade_sessions=trade_sessions
        )

    def _fetch_segment_data(self, code, period, adjust, end_dt, start_dt_limit):
        """
        [内部并发任务] 获取特定时间片段的数据
        策略：从 end_dt 向前获取，直到遇到 start_dt_limit 或数据耗尽
        """
        segment_candles = []
        current_cursor = end_dt

        # 确保 limit 是带时区的
        tz = pytz.timezone("Asia/Shanghai")
        if start_dt_limit.tzinfo is None:
            start_dt_limit = tz.localize(start_dt_limit)

        while True:
            try:
                # 核心 API 调用
                candlesticks = self._fetch_candlesticks_api(
                    symbol=code,
                    period=period,
                    adjust_type=adjust,
                    count=1000,
                    time_cursor=current_cursor,
                    trade_sessions=TradeSessions.Intraday
                )
            except Exception as e:
                # tenacity 重试失败或遇到不可重试异常
                err_code = getattr(e, 'code', None)
                if isinstance(e, OpenApiException) and err_code == 301600:
                    LogUtil.warning(f"Data limit reached (301600) for {code}, stopping segment.")
                else:
                    LogUtil.error(f"API Retry failed for segment {end_dt}: {e}")
                break

            if not candlesticks:
                break

            # 获取这批数据中最老的一条时间，用于更新游标
            oldest_candle = candlesticks[0]

            # --- [修复] 统一转为带时区的 datetime 进行比较 ---
            # 注意：pd.to_datetime(标量) 返回 Timestamp（不是 Series），没有 .dt 访问器，
            # 直接 .dt.tz_convert 会抛 AttributeError，这里使用 pd.Timestamp 构造。
            try:
                ts = oldest_candle.timestamp
                if isinstance(ts, (int, float)):
                    # Unix 时间戳 (秒) -> UTC -> 转上海
                    oldest_dt = pd.Timestamp(ts, unit='s', tz='UTC').tz_convert(tz).to_pydatetime()
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
            "5m": timedelta(days=90),
            "15m": timedelta(days=180),
            "30m": timedelta(days=365),
            "60m": timedelta(days=365 * 2),
            "d": timedelta(days=365 * 3),
            "w": timedelta(days=365 * 10),
            "m": timedelta(days=365 * 30),
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

        # 限制最大回看范围：不允许请求超过 DEFAULT_LOOKBACK 的历史数据
        max_lookback = DEFAULT_LOOKBACK.get(frequency, timedelta(days=30))
        earliest_allowed = now_dt - max_lookback
        if start_dt < earliest_allowed:
            start_dt = earliest_allowed
        if start_dt >= end_dt:
            return pd.DataFrame()

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
        def _do_ticks():
            try:
                quotes = self._quote_ctx().quote(codes)
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

        # Add timeout wrapper to prevent blocking the main thread
        try:
            future = self.executor.submit(_do_ticks)
            return future.result(timeout=2.0)
        except Exception as e:
            LogUtil.error(f"ticks timeout or error: {e}")
            return {}

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票基础信息
        """
        try:
            # 兼容处理
            symbol = code if code.endswith('.US') else f"{code}.US"
            infos = self._quote_ctx().static_info([symbol])
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
            balances = self._trade_ctx().account_balance()
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
            resp = self._trade_ctx().stock_positions()
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
            resp = self._trade_ctx().submit_order(
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