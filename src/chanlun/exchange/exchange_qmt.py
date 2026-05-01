import datetime
import threading
import time
from typing import Dict, List, Union
from datetime import timedelta
import pandas as pd
import pytz
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random

from chanlun import fun
from chanlun.exchange.exchange import Exchange, Tick, convert_stock_kline_frequency
from chanlun.tools.log_util import LogUtil
from xtquant import xtdata

"""
QMT 沪深行情
"""


# xtquant 的 native 客户端不是线程安全的：多线程并发调用 download_history_data /
# get_market_data / get_full_tick / get_instrument_detail 等接口时，其内部 BSON
# 序列化层可能触发 `Assertion failed: u < 1000000, file ...\bson\src\bsonobj.cpp`
# 断言，进而以 0xC0000409 (STATUS_STACK_BUFFER_OVERRUN) 强制终止整个 Python 进程，
# Python 层无法 try/except 捕获。这里用进程级全局可重入锁把所有对 xtdata 的调用
# 串行化，作为防御性兜底，避免多线程并发触发崩溃。
_XTDATA_NATIVE_LOCK = threading.RLock()


class ExchangeQMT(Exchange):
    def __init__(self):
        xtdata.enable_hello = False

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        # H1: g_all_stocks 必须为实例属性，且并发构建期间用 Lock 保护，
        # 避免多线程同时进入 all_stocks() 时各自跑一次 5500 只股票全量扫描，
        # 也避免类属性被多实例共享导致的状态穿透。
        self.g_all_stocks: list = []
        self._all_stocks_lock = threading.Lock()

        # 1. 读取数据的周期映射 (用于 get_market_data)
        # 注意：移除 "y": "1y"，xtquant 实际不支持年线 period，传入会被 native 层拒绝甚至触发 BSON 异常。
        self.frequency_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "60m": "1h",
            "d": "1d",
            "w": "1w",
            "m": "1mon",
        }

        # 2. 新增：下载数据的周期映射 (用于 download_history_data)
        # QMT 下载接口通常只支持基础周期：1m, 5m, 1d (Tick)
        # 逻辑：日线及以上下载 1d，分钟线下载 1m 或 5m
        # 注意：为了数据精确性和合成的灵活性，建议 1m, 5m, 15m, 30m, 60m 都下载基础的 1m 或 5m 数据
        # 这里为了效率，5m及倍数周期下载 5m，1m 下载 1m
        self.download_frequency_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "5m",  # 15m 是 5m 的倍数，下载 5m 数据即可合成
            "30m": "5m",  # 30m 是 5m 的倍数，下载 5m 数据即可合成
            "60m": "5m",  # 60m 是 5m 的倍数，下载 5m 数据即可合成
            "d": "1d",
            "w": "1d",
            "m": "1d",
        }

        # 默认回看时长：原本 60m=8年 / d=10年 / 1m=90天 等过长，会让 xtquant 一次性
        # 拉取数十万根 K 线，BSON payload 极大，是触发 `u < 1000000` 断言的主因。
        # 这里大幅缩短到合理值：分钟线最多 90 天，日线 3 年，周线 10 年，月线 30 年。
        # 上层（如 tv_history）通常一次只要 300 根 K 线，进一步用 args["limit"] 短路。
        self.DEFAULT_LOOKBACK = {
            "1m": timedelta(days=30),
            "5m": timedelta(days=90),
            "15m": timedelta(days=180),
            "30m": timedelta(days=365),
            "60m": timedelta(days=365 * 2),
            "d": timedelta(days=365 * 3),
            "w": timedelta(days=365 * 10),
            "m": timedelta(days=365 * 30),
        }

    def code_to_tdx(self, code: str):
        _c = code.split(".")
        if len(_c[0]) == 6:
            return _c[1] + "." + _c[0]
        else:
            return _c[0] + "." + _c[1]

    def code_to_qmt(self, code: str):
        _c = code.split(".")
        if len(_c[0]) == 6:
            return _c[0] + "." + _c[1]
        else:
            return _c[1] + "." + _c[0]

    def default_code(self):
        return "SH.000001"

    def support_frequencys(self):
        return self.frequency_map

    def all_stocks(self):
        # H1: 双检锁（double-checked locking）防止并发线程都进入构建临界区。
        # 已就绪后所有读者无锁直接返回，零开销。
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        with self._all_stocks_lock:
            # 拿到锁后再查一次：可能已经有别的线程构建完了。
            if len(self.g_all_stocks) > 0:
                return self.g_all_stocks

            # G5：黑名单改为 set，避免 5500+ 次 list 线性查找（O(n) → O(1)）
            black_codes = {
                "SZ.399290", "SZ.399289", "SZ.399302", "SZ.399298", "SZ.399481",
                "SZ.399299", "SZ.399301", "SH.000013", "SH.000022", "SH.000116",
                "SH.000061", "SH.000101", "SH.000012", "SZ.988201", "SZ.980068",
                "SZ.980001", "SZ.980023",
            }

            # G5 性能优化：
        # - 把全市场扫描放在一把大锁内，避免 5500+ 次抢锁/释放锁的开销，
        #   同时规避 xtdata 多线程穿插调用触发 BSON 断言。RLock 可重入。
        # - 原实现每只股票要走 stock_info() → 内部再调 get_instrument_detail，
        #   总计 2 次 native 调用 + 一次 code_to_qmt(code_to_tdx(_c)) 来回转换。
        #   这里直接 inline 拿 detail，单只股票降为 1 次 native 调用，整体减半。
            # - 黑名单改为循环内提前过滤，省一次列表二次遍历。
            with _XTDATA_NATIVE_LOCK:
                ticks = xtdata.get_full_tick(["SH", "SZ", "BJ"])
                tick_codes = list(ticks.keys())

                all_stocks = []
                for _c in tick_codes:
                    _stock_type: dict = xtdata.get_instrument_type(_c)
                    if not (_stock_type.get("stock") or _stock_type.get("etf") or _stock_type.get("index")):
                        continue

                    tdx_code = self.code_to_tdx(_c)
                    if tdx_code in black_codes:
                        continue

                    try:
                        stock_detail = xtdata.get_instrument_detail(_c, False)
                    except Exception as _e:
                        LogUtil.warning(
                            f"[ExchangeQMT.all_stocks] get_instrument_detail failed code={_c} err={_e}"
                        )
                        continue
                    if not stock_detail:
                        continue

                    all_stocks.append({
                        "code": tdx_code,
                        "name": stock_detail["InstrumentName"],
                        "precision": fun.reverse_decimal_to_power_of_ten(stock_detail["PriceTick"]),
                    })

            self.g_all_stocks = all_stocks
            return self.g_all_stocks

    def get_start_date_by_frequency(self, frequency: str, req_counts: int = None) -> str:
        """
        根据周期获取默认起始日期。
        如果上层指定了 req_counts（用户实际只要这么多根 K 线），会按周期估算
        一个紧凑的回看窗口，避免无谓地拉超长历史导致 BSON payload 过大。
        """
        now = datetime.datetime.now()
        delta = self.DEFAULT_LOOKBACK.get(frequency, timedelta(days=365))

        # 当调用方指定了请求 K 线数量时，按周期估算一个紧凑的窗口（带 3 倍冗余）。
        # 这样典型场景（300 根 1m）只需要 ~3 天历史，而不是 30 天。
        if req_counts is not None and req_counts > 0:
            minutes_per_bar = {
                "1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60,
                "d": 60 * 24, "w": 60 * 24 * 7, "m": 60 * 24 * 30,
            }
            est_minutes = minutes_per_bar.get(frequency, 60) * req_counts * 3
            est_delta = timedelta(minutes=est_minutes)
            # 取估算窗口和默认窗口中较小的
            if est_delta < delta:
                delta = est_delta

        start_date = now - delta
        return start_date.strftime("%Y%m%d")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random(min=0.1, max=1)
    )
    def klines(
            self,
            code: str,
            frequency: str,
            start_date: str = None,
            end_date: str = None,
            args=None,
    ) -> pd.DataFrame:
        empty_df = pd.DataFrame(columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume'])

        # 1. 确定 读取周期 (Read Period) 和 下载周期 (Download Period)
        qmt_read_period = self.frequency_map.get(frequency, "1m")
        qmt_code = self.code_to_qmt(code)

        # 核心修改：根据请求周期，选择合适的“基础周期”进行下载
        # 如果请求是 30m，我们下载 5m 数据（因为 5m 是 30m 的因子，且 QMT 支持 5m 下载）
        # 如果请求是 1m，下载 1m
        # 如果请求是 d，下载 1d
        qmt_download_period = self.download_frequency_map.get(frequency)

        # 兜底逻辑：如果映射里没有，按日内/日线区分
        if qmt_download_period is None:
            if frequency in ["1m", "5m", "15m", "30m", "60m"]:
                qmt_download_period = "1m"  # 分钟线兜底下载 1m，最稳妥但数据量大
            else:
                qmt_download_period = "1d"

        # 2. 确定时间范围
        # 上层若传了 args["req_counts"]（实际只要 N 根 K 线），用它来收紧默认回看窗口，
        # 避免一次性拉超长历史触发 xtquant BSON 断言。
        req_counts = args.get("req_counts") if args else None
        if start_date:
            query_start = start_date.replace("-", "").replace(" ", "").replace(":", "")
        else:
            query_start = self.get_start_date_by_frequency(frequency, req_counts=req_counts)

        dividend_type = args.get("dividend_type", "front") if args else "front"

        # 3. 智能增量下载 + 4. 获取数据
        # 用全局锁串行化所有 xtdata 调用，规避 native 层 BSON 断言崩溃。
        # 关键点：
        # - 把 download + get_market_data 一起包住，避免两调用之间被其它线程
        #   插入新的 native 请求（半包/状态错乱也是触发崩溃的诱因之一）。
        # - download 改用 incrementally=True，避免每次都全量下载导致 BSON
        #   payload 过大触发 `u < 1000000` 断言。
        field_list = ["time", "open", "high", "low", "close", "volume"]
        with _XTDATA_NATIVE_LOCK:
            try:
                xtdata.download_history_data(
                    stock_code=qmt_code,
                    period=qmt_download_period,
                    start_time=query_start,
                    end_time="",
                    incrementally=True,
                )
                raw_data = xtdata.get_market_data(
                    field_list=field_list,
                    stock_list=[qmt_code],
                    period=qmt_read_period,
                    start_time=query_start,
                    end_time="",
                    count=-1,
                    dividend_type=dividend_type,
                    fill_data=False,
                )
            except Exception as e:
                # native 层抛出的普通异常仍然走外层 retry；
                # 注意：BSON 断言导致的 0xC0000409 进程崩溃 Python 无法捕获，
                # 那种情况只能靠外部进程守护（如 NSSM）拉起。
                LogUtil.warning(
                    f"[ExchangeQMT.klines] xtdata call failed code={qmt_code} "
                    f"freq={frequency} read={qmt_read_period} dl={qmt_download_period} err={e}"
                )
                raise

        # 数据完整性检查
        # 注意：xtdata.get_market_data 返回的字段值，老版本是 pd.DataFrame（有 .empty），
        # 新版本可能是 np.ndarray（没有 .empty，但有 .size）。这里两种都兼容。
        if not raw_data:
            return empty_df
        time_col = raw_data.get("time")
        if time_col is None:
            return empty_df
        try:
            if hasattr(time_col, "empty"):
                if time_col.empty:
                    return empty_df
            elif hasattr(time_col, "size"):
                if time_col.size == 0:
                    return empty_df
            elif len(time_col) == 0:
                return empty_df
        except Exception as e:
            LogUtil.warning(
                f"[exchange_qmt._build_empty_df] check time_col failed code={code} freq={frequency}: {e}"
            )
            return empty_df

        # 5. 极速构建 DataFrame
        try:
            data_dict = {
                "date": raw_data["time"].values[0],
                "open": raw_data["open"].values[0],
                "high": raw_data["high"].values[0],
                "low": raw_data["low"].values[0],
                "close": raw_data["close"].values[0],
                "volume": raw_data["volume"].values[0]
            }
        except (IndexError, KeyError, AttributeError, ValueError):
            return empty_df

        klines_df = pd.DataFrame(data_dict)

        if klines_df.empty:
            return empty_df

        # 6. 时间列处理
        try:
            klines_df["date"] = pd.to_datetime(klines_df["date"], unit="ms", utc=True)
            klines_df["date"] = klines_df["date"].dt.tz_convert(self.tz)

            if frequency in ["d", "w", "m", "y"]:
                klines_df["date"] = klines_df["date"].dt.normalize() + pd.Timedelta(hours=15)
        except Exception as e:
            LogUtil.warning(
                f"[exchange_qmt] tz convert failed code={code} freq={frequency}: {e}"
            )
            return empty_df

        klines_df["code"] = code

        # 7. 整理格式
        klines_df = klines_df[["code", "date", "open", "high", "low", "close", "volume"]]
        cols_to_float = ["open", "high", "low", "close", "volume"]
        klines_df[cols_to_float] = klines_df[cols_to_float].astype(float)

        # 8. 非原生周期重采样 (如需)
        if frequency not in self.frequency_map:
            klines_df = convert_stock_kline_frequency(klines_df, frequency)

        # 截断数据
        if args and "req_counts" in args:
            req_counts = args["req_counts"]
            if len(klines_df) > req_counts:
                klines_df = klines_df.iloc[-req_counts:]

        return klines_df

    def stock_info(self, code: str) -> Union[Dict, None]:
        qmt_code = self.code_to_qmt(code)
        with _XTDATA_NATIVE_LOCK:
            stock_detail = xtdata.get_instrument_detail(qmt_code, False)
        return {
            "code": code,
            "name": stock_detail["InstrumentName"],
            "precision": fun.reverse_decimal_to_power_of_ten(stock_detail["PriceTick"]),
        }

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取 tick 信息
        """
        ticks = {}
        if len(codes) == 0:
            return ticks
        with _XTDATA_NATIVE_LOCK:
            qmt_ticks = xtdata.get_full_tick([self.code_to_qmt(_c) for _c in codes])
        for _c, _t in qmt_ticks.items():
            ticks[self.code_to_tdx(_c)] = Tick(
                code=self.code_to_tdx(_c),
                last=_t["lastPrice"],
                buy1=_t["bidPrice"][0],
                sell1=_t["askPrice"][0],
                high=_t["high"],
                low=_t["low"],
                open=_t["open"],
                volume=_t["volume"],
                rate=(_t["lastPrice"] - _t["lastClose"]) / _t["lastClose"] * 100,
            )

        return ticks

    def all_ticks(self) -> Dict[str, Tick]:
        ticks = {}
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]
        with _XTDATA_NATIVE_LOCK:
            qmt_ticks = xtdata.get_full_tick(["SH", "SZ", "BJ"])
        for _c, _t in qmt_ticks.items():
            _tdx_code = self.code_to_tdx(_c)
            if _tdx_code not in all_codes:
                continue
            ticks[_tdx_code] = Tick(
                code=_tdx_code,
                last=_t["lastPrice"],
                buy1=_t["bidPrice"][0],
                sell1=_t["askPrice"][0],
                high=_t["high"],
                low=_t["low"],
                open=_t["open"],
                volume=_t["volume"],
                rate=(
                    (_t["lastPrice"] - _t["lastClose"]) / _t["lastClose"] * 100
                    if _t["lastClose"] != 0
                    else 0
                ),
            )

        return ticks

    def get_divid_factors(self, stock_code: str) -> pd.DataFrame:
        """
        获取股票除权除息信息
        """
        with _XTDATA_NATIVE_LOCK:
            df = xtdata.get_divid_factors(self.code_to_qmt(stock_code))
        if df is None or df.empty:
            return None
        df.loc[:, "stock_code"] = stock_code
        df["divid_date"] = pd.to_datetime(df["time"] / 1000, unit="s")
        return df

    def subscribe_all_ticks(
        self, callback, market_list: List[str] = ["SH", "SZ", "BJ"]
    ):
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in all_codes:
                    continue
                callback(_tdx_code, _tick)

        xtdata.subscribe_whole_quote(market_list, on_tick)
        xtdata.run()

    def subscribe_stocks_quotes(self, codes: List[str], callback):
        """
        订阅股票行情
        """
        qmt_codes = [self.code_to_qmt(_c) for _c in codes]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in codes:
                    continue
                callback(_tdx_code, _tick)

        xtdata.subscribe_whole_quote(qmt_codes, on_tick)
        xtdata.run()

    def now_trading(self):
        """
        返回当前是否是交易时间。

        H10 修正：
        - 用 self.tz（Asia/Shanghai）替换 datetime.now() 裸调用，避免服务器
          所在时区不在 +8 时整体判错。
        - 增加集合竞价时段 09:15-09:25：A 股该时段是有 tick 数据的，
          上游若用 now_trading 决定是否拉 tick / 订阅，会漏掉早盘竞价数据。

        交易时段（含集合竞价）：
            周一至周五
            09:15-09:25 集合竞价
            09:30-11:30 上午连续竞价
            13:00-15:00 下午连续竞价
        """
        now_dt = datetime.datetime.now(self.tz)
        if now_dt.weekday() in [5, 6]:  # 周六日不交易
            return False
        hour = now_dt.hour
        minute = now_dt.minute

        # 集合竞价 09:15-09:25
        if hour == 9 and 15 <= minute < 25:
            return True
        # 上午连续竞价 09:30-09:59
        if hour == 9 and minute >= 30:
            return True
        # 上午 10:00-10:59
        if hour == 10:
            return True
        # 上午 11:00-11:29
        if hour == 11 and minute < 30:
            return True
        # 下午 13:00-14:59
        if hour in (13, 14):
            return True
        return False

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")

    def balance(self):
        raise Exception("QMT 交易功能在 trader 目录实现")

    def positions(self, code: str = ""):
        raise Exception("QMT 交易功能在 trader 目录实现")

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise Exception("QMT 交易功能在 trader 目录实现")


if __name__ == "__main__":
    ex = ExchangeQMT()

    # stocks = ex.all_stocks()
    # stock_maps = {}
    # for _s in stocks:
    #     stock_maps[_s["code"][0:5]] = _s
    # for _t, _s in stock_maps.items():
    #     print(_t, _s)
    # print(len(stocks))

    klines = ex.klines(
        "SZ.002645",
        "1m",
    )

    # 2026-02-04 日期的 volume 累加
    klines["ddd"] = klines["date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    volume = klines[klines["ddd"] == "2026-02-04"]["volume"].sum()

    print(klines[klines["ddd"] == "2026-02-04"])
    print(volume)

    # stock = ex.stock_info("SH.000001")
    # print(stock)

    # df = ex.get_divid_factors("SH.600519")

    # print(df)

    # def on_klines(_qmt_code, tick):
    #     if _qmt_code != "600519.SH":
    #         return
    #     print(
    #         _qmt_code,
    #         "最新价格",
    #         tick["lastPrice"],
    #         " 时间：",
    #         fun.timeint_to_datetime(int(tick["time"] / 1000)),
    #     )
    #     _tdx_code = ex.code_to_tdx(_qmt_code)
    #     print(tick)
    #     # for _f in ["1m", "5m", "d"]:
    #     #     print(f"周期：{_f}")
    #     #     klines_df = ex.klines(_tdx_code, _f, args={"req_counts": 2})

    #     #     print(klines_df)
    #     print("-" * 20)

    # ex.subscribe_all_ticks(on_klines)

    # ticks = ex.all_ticks()
    # print(len(ticks))