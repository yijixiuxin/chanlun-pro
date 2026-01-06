import datetime
import time
from typing import Dict, List, Union

import pandas as pd
import pytz
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random

from chanlun import fun
from chanlun.exchange.exchange import Exchange, Tick, convert_stock_kline_frequency
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=5),
        retry=retry_if_result(lambda _r: _r is None),
    )
    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        frequency_map = {
            "y": "1d",
            "m": "1d",
            "w": "1d",
            "d": "1d",
            "60m": "5m",
            "30m": "5m",
            "15m": "5m",
            "5m": "5m",
            "3m": "1m",
            "1m": "1m",
        }
        # TODO 多数据，自定义周去转换消耗时间比较长，可修改为增量替换
        frequency_count = {
            "y": -1,
            "m": 8000 * 20,
            "w": 8000 * 5,
            "d": 8000,
            "60m": 8000 * 12,
            "30m": 8000 * 6,
            "15m": 8000 * 3,
            "5m": 8000,
            "3m": 8000 * 3,
            "1m": 8000,
        }
        # 复权方式
        dividend_type = "front"
        if args is not None and "dividend_type" in args:
            dividend_type = args["dividend_type"]

        # 指定请求数据条数
        req_counts = frequency_count[frequency]
        if args is not None and "req_counts" in args:
            req_counts = args["req_counts"]
        if start_date:
            req_counts = -1

        qmt_code = self.code_to_qmt(code)

        # 首先检查当前是否有数据
        kline_exists = xtdata.get_market_data(
            field_list=[],
            stock_list=[qmt_code],
            period=frequency_map[frequency],
            start_time="",
            end_time="",
            count=1,
            dividend_type=dividend_type,
            fill_data=False,
        )
        if kline_exists is None or kline_exists["time"].empty:
            # 如果没有数据，则全量下载
            s_time = time.time()
            # 根据周期，决定下载的时间起始日期
            if frequency in ["1m", "3m"]:
                download_start_date = fun.datetime_to_str(
                    datetime.datetime.now() - datetime.timedelta(days=180), "%Y%m%d"
                )
            elif frequency in ["5m", "15m", "30m", "60m"]:
                download_start_date = fun.datetime_to_str(
                    datetime.datetime.now() - datetime.timedelta(days=2880), "%Y%m%d"
                )
            else:
                download_start_date = ""
            if args is not None and "download_start_date" in args:
                download_start_date = args["download_start_date"]

            xtdata.download_history_data(
                qmt_code,
                frequency_map[frequency],
                start_time=download_start_date,
                end_time="",
                incrementally=True,
            )
            # print(f"{code}-{frequency} 全量下载历史数据耗时：{time.time() - s_time}")
        else:
            # 增量下载
            # s_time = time.time()
            xtdata.download_history_data(
                qmt_code,
                frequency_map[frequency],
                start_time="",
                end_time="",
                incrementally=True,
            )
            # print(f"{code}-{frequency} 增量下载历史数据耗时：{time.time() - s_time}")

        # s_time = time.time()
        qmt_klines = xtdata.get_market_data(
            field_list=[],
            stock_list=[qmt_code],
            period=frequency_map[frequency],
            start_time=start_date.replace("-", "") if start_date else "",
            end_time="",
            count=req_counts,
            dividend_type=dividend_type,
            fill_data=False,
        )
        # print(f"{code}-{frequency} 获取历史数据耗时：{time.time() - s_time}")

        s_time = time.time()
        klines_df = pd.DataFrame(
            {key: value.values[0] for key, value in qmt_klines.items()}
        )
        klines_df["code"] = code

        klines_df["date"] = pd.to_datetime(
            klines_df["time"], unit="ms", utc=True
        ).dt.tz_convert(self.tz)
        klines_df = klines_df[
            ["code", "date", "open", "high", "low", "close", "volume"]
        ]
        # 将 open high low close volume 转换为 float
        klines_df[["open", "high", "low", "close", "volume"]] = klines_df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)
        klines_df = klines_df.sort_values("date")

        # 如果日线，小时设置为15点
        if frequency in ["d", "w", "m", "y"]:
            klines_df["date"] = klines_df["date"].apply(lambda x: x.replace(hour=15))

        # print(f"{code}-{frequency} 获取历史数据转换耗时：{time.time() - s_time}")

        if frequency not in ["d", "5m", "1m"]:
            # s_time = time.time()
            klines_df = convert_stock_kline_frequency(klines_df, frequency)
            # print(f"{code}-{frequency} 转换历史周期数据耗时：{time.time() - s_time}")

        # print(klines_df.tail(2))
        if req_counts > 0:
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

    def subscribe_all_ticks(self, callback, market_list: List[str] = ["SH", "SZ", "BJ"]):
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]
        print(len(all_codes))

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

    # klines = ex.klines(
    #     "SH.000001",
    #     "d",
    # )
    # print(klines)

    # stock = ex.stock_info("SH.000001")
    # print(stock)

    # df = ex.get_divid_factors("SH.600519")

    # print(df)

    def on_klines(_qmt_code, tick):
        if _qmt_code != "600519.SH":
            return
        print(
            _qmt_code,
            "最新价格",
            tick["lastPrice"],
            " 时间：",
            fun.timeint_to_datetime(int(tick["time"] / 1000)),
        )
        _tdx_code = ex.code_to_tdx(_qmt_code)
        print(tick)
        # for _f in ["1m", "5m", "d"]:
        #     print(f"周期：{_f}")
        #     klines_df = ex.klines(_tdx_code, _f, args={"req_counts": 2})

        #     print(klines_df)
        print("-" * 20)

    ex.subscribe_all_ticks(on_klines)

    # ticks = ex.all_ticks()
    # print(len(ticks))
