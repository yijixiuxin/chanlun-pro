import datetime
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


class ExchangeQMT(Exchange):
    g_all_stocks = []

    def __init__(self):
        xtdata.enable_hello = False

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        # 1. 优化：利用 QMT 原生支持的周期，减少本地 Resample 的开销
        self.frequency_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "60m": "1h",  # 优化：直接使用 1h
            "d": "1d",
            "w": "1w",
            "m": "1mon",
            "y": "1y",
        }

        # 2. 你的默认回看周期配置
        self.DEFAULT_LOOKBACK = {
            "1m": timedelta(days=30),
            "5m": timedelta(days=150),
            "15m": timedelta(days=90),
            "30m": timedelta(days=750),
            "60m": timedelta(days=365),
            "d": timedelta(days=365 * 10),
            "w": timedelta(days=365 * 20),
            "m": timedelta(days=365 * 30),
        }

    def code_to_tdx(self, code: str):
        """
        兼容之前的通达信格式,和 qmt 代码格式进行转换
        """
        _c = code.split(".")
        if len(_c[0]) == 6:
            return _c[1] + "." + _c[0]
        else:
            return _c[0] + "." + _c[1]

    def code_to_qmt(self, code: str):
        """
        兼容之前的通达信格式,和 qmt 代码格式进行转换
        """
        _c = code.split(".")
        if len(_c[0]) == 6:
            return _c[0] + "." + _c[1]
        else:
            return _c[1] + "." + _c[0]

    def default_code(self):
        return "SH.000001"

    def support_frequencys(self):
        return {
            "y": "1y",
            "m": "1mon",
            "w": "1w",
            "d": "1d",
            "60m": "1h",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }

    def all_stocks(self):
        """
        获取所有股票代码
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        # 黑名单 code
        black_codes = [
            "SZ.399290",
            "SZ.399289",
            "SZ.399302",
            "SZ.399298",
            "SZ.399481",
            "SZ.399299",
            "SZ.399301",
            "SH.000013",
            "SH.000022",
            "SH.000116",
            "SH.000061",
            "SH.000101",
            "SH.000012",
            "SZ.988201",
            "SZ.980068",
            "SZ.980001",
            "SZ.980023",
        ]

        ticks = xtdata.get_full_tick(["SH", "SZ", "BJ"])
        tick_codes = list(ticks.keys())

        all_stocks = []
        for _c in tick_codes:
            _stock_type: dict = xtdata.get_instrument_type(_c)
            if (
                _stock_type.get("stock")
                or _stock_type.get("etf")
                or _stock_type.get("index")
            ):
                pass
            else:
                continue
            _stock = self.stock_info(self.code_to_tdx(_c))
            all_stocks.append(_stock)

        all_stocks = [_s for _s in all_stocks if _s["code"] not in black_codes]

        self.g_all_stocks = all_stocks

        # print(f"股票共获取数量：{len(self.g_all_stocks)}")
        # print(f"耗时：{time.time() - s_time}")

        # print(f"股票共获取数量：{len(self.g_all_stocks)}")
        return self.g_all_stocks

    def get_start_date_by_frequency(self, frequency: str) -> str:
        """根据频率和配置计算所需的起始时间字符串 (YYYYMMDD)"""
        now = datetime.datetime.now()
        delta = self.DEFAULT_LOOKBACK.get(frequency, timedelta(days=365))
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

        # 定义标准的空返回值，包含列名，防止下游 KeyError
        empty_df = pd.DataFrame(columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume'])

        # 1. 确定 QMT 周期
        qmt_period = self.frequency_map.get(frequency, "1m")
        qmt_code = self.code_to_qmt(code)

        # 2. 确定时间范围
        if start_date:
            query_start = start_date.replace("-", "").replace(" ", "").replace(":", "")
        else:
            query_start = self.get_start_date_by_frequency(frequency)

        # 处理复权
        dividend_type = args.get("dividend_type", "front") if args else "front"

        # 3. 智能增量下载
        xtdata.download_history_data(
            stock_code=qmt_code,
            period=qmt_period,
            start_time=query_start,
            end_time="",
            incrementally=True
        )

        # 4. 获取数据
        field_list = ["time", "open", "high", "low", "close", "volume"]
        raw_data = xtdata.get_market_data(
            field_list=field_list,
            stock_list=[qmt_code],
            period=qmt_period,
            start_time=query_start,
            end_time="",
            count=-1,
            dividend_type=dividend_type,
            fill_data=False,
        )

        # 数据完整性检查
        # 如果 raw_data 为空，或者 time 字段为空/None
        if not raw_data or raw_data.get("time") is None or raw_data["time"].empty:
            return empty_df

        # 5. 极速构建 DataFrame
        try:
            # 使用 .values[0] 提取第一只股票的数据数组，解决 KeyError: 0
            data_dict = {
                "date": raw_data["time"].values[0],
                "open": raw_data["open"].values[0],
                "high": raw_data["high"].values[0],
                "low": raw_data["low"].values[0],
                "close": raw_data["close"].values[0],
                "volume": raw_data["volume"].values[0]
            }
        except (IndexError, KeyError, AttributeError, ValueError):
            # 捕获所有可能的解析错误，返回空DF
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
        except Exception:
            # 如果时间转换失败
            return empty_df

        # 添加 code 列
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
        """
        获取股票名称
        """
        qmt_code = self.code_to_qmt(code)
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
        df = xtdata.get_divid_factors(self.code_to_qmt(stock_code))
        if df is None or df.empty:
            return None
        df.loc[:, "stock_code"] = stock_code
        df["divid_date"] = pd.to_datetime(df["time"] / 1000, unit="s")
        return df

    def subscribe_all_ticks(self, callback):
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in all_codes:
                    continue
                callback(_tdx_code, _tick)

        xtdata.subscribe_whole_quote(["SH", "SZ", "BJ"], on_tick)
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
        返回当前是否是交易时间
        周一至周五，09:30-11:30 13:00-15:00
        """
        now_dt = datetime.datetime.now()
        if now_dt.weekday() in [5, 6]:  # 周六日不交易
            return False
        hour = now_dt.hour
        minute = now_dt.minute
        if hour == 9 and minute >= 30:
            return True
        if hour in [10, 13, 14]:
            return True
        if hour == 11 and minute < 30:
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
        "SH.688045",
        "1m",
    )
    print(klines)

    # coding:utf-8
    # import time
    # from xtquant import xtdata
    #
    # code = "000001.SH"
    # # 下载历史数据 下载接口本身不返回数据
    # xtdata.download_history_data(code, period='1m', start_time='20251101', end_time='20251106')
    #
    # data = xtdata.get_market_data([], [code], period='1m', start_time='20251101', end_time='20251106')
    # print('一次性取数据', data)
    #
    # do_subscribe_quote(code_list, period)  # 设置订阅参数，使gmd_ex取到最新行情
    # count = -1  # 设置count参数，使gmd_ex返回全部数据
    # data3 = xtdata.get_market_data_ex([], code_list, period=period, start_time=start_date, end_time=end_date,
    #                                   count=-1)  # count 设置为1，使返回值只包含最新行情

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
