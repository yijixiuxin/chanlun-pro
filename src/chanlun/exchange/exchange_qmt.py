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

        # 1. 读取数据的周期映射 (用于 get_market_data)
        self.frequency_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "60m": "1h",
            "d": "1d",
            "w": "1w",
            "m": "1mon",
            "y": "1y",
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
            "y": "1d",
        }

        self.DEFAULT_LOOKBACK = {
            "1m": timedelta(days=30),
            "5m": timedelta(days=150),
            "15m": timedelta(days=90),
            "30m": timedelta(days=365 * 5),
            "60m": timedelta(days=365 * 8),
            "d": timedelta(days=365 * 10),
            "w": timedelta(days=365 * 20),
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
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        black_codes = [
            "SZ.399290", "SZ.399289", "SZ.399302", "SZ.399298", "SZ.399481",
            "SZ.399299", "SZ.399301", "SH.000013", "SH.000022", "SH.000116",
            "SH.000061", "SH.000101", "SH.000012", "SZ.988201", "SZ.980068",
            "SZ.980001", "SZ.980023",
        ]

        ticks = xtdata.get_full_tick(["SH", "SZ", "BJ"])
        tick_codes = list(ticks.keys())

        all_stocks = []
        for _c in tick_codes:
            _stock_type: dict = xtdata.get_instrument_type(_c)
            if _stock_type.get("stock") or _stock_type.get("etf") or _stock_type.get("index"):
                pass
            else:
                continue
            _stock = self.stock_info(self.code_to_tdx(_c))
            all_stocks.append(_stock)

        all_stocks = [_s for _s in all_stocks if _s["code"] not in black_codes]
        self.g_all_stocks = all_stocks
        return self.g_all_stocks

    def get_start_date_by_frequency(self, frequency: str) -> str:
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
        if start_date:
            query_start = start_date.replace("-", "").replace(" ", "").replace(":", "")
        else:
            query_start = self.get_start_date_by_frequency(frequency)

        dividend_type = args.get("dividend_type", "front") if args else "front"

        # 3. 智能增量下载
        xtdata.download_history_data(
            stock_code=qmt_code,
            period=qmt_download_period,
            start_time=query_start,
            end_time="",
            incrementally=False
        )

        # 4. 获取数据
        field_list = ["time", "open", "high", "low", "close", "volume"]
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

        # 数据完整性检查
        if not raw_data or raw_data.get("time") is None or raw_data["time"].empty:
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
        except Exception:
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