import datetime
from typing import Dict, List, Union

import pandas as pd
import pytz
from tzlocal import get_localzone

from chanlun import fun
from chanlun.base import Market
from chanlun.db import db
from chanlun.exchange.exchange import (
    Exchange,
    Tick,
    convert_currency_kline_frequency,
    convert_futures_kline_frequency,
    convert_stock_kline_frequency,
    convert_us_kline_frequency,
)


class ExchangeDB(Exchange):
    """
    数据库行情
    """

    def __init__(self, market):
        """
        :param market: 市场 a A股市场 hk 香港市场 us 美股市场 currency 数字货币市场  futures 期货市场
        """
        self.market = market
        self.exchange = None
        self.online_ex = None

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")
        if self.market == "us":
            self.tz = pytz.timezone("US/Eastern")
        if self.market in ["currency", "currency_spot"]:
            self.tz = pytz.timezone(str(get_localzone()))

    def default_code(self):
        if self.market == Market.A.value:
            return "SH.000001"
        elif self.market == Market.HK.value:
            return "HK.00700"
        elif self.market == Market.FUTURES.value:
            return "KQ.m@SHFE.rb"
        elif self.market == Market.NY_FUTURES.value:
            return "CO.GC00W"
        elif self.market == Market.US.value:
            return "AAPL"
        elif self.market == Market.CURRENCY.value:
            return "BTC/USDT"
        elif self.market == Market.CURRENCY_SPOT.value:
            return "BTC/USDT"
        return ""

    def support_frequencys(self):
        if self.market == Market.A.value:
            return {
                "y": "Y",
                "m": "M",
                "w": "W",
                "d": "D",
                "120m": "120m",
                "60m": "60m",
                "30m": "30m",
                "15m": "15m",
                "10m": "10m",
                "5m": "5m",
            }
        elif self.market == Market.HK.value:
            return {
                "y": "Y",
                "q": "Q",
                "m": "M",
                "w": "W",
                "d": "D",
                "60m": "60m",
                "30m": "30m",
                "15m": "15m",
                "5m": "5m",
            }
        elif self.market == Market.FUTURES.value:
            return {
                "w": "W",
                "d": "D",
                "120m": "2H",
                "60m": "1H",
                "30m": "30m",
                "15m": "15m",
                "10m": "10m",
                "5m": "5m",
                "1m": "1m",
            }
        elif self.market == Market.NY_FUTURES.value:
            return {
                "w": "W",
                "d": "D",
                "120m": "2H",
                "60m": "1H",
                "30m": "30m",
                "15m": "15m",
                "10m": "10m",
                "5m": "5m",
                "1m": "1m",
            }
        elif self.market == Market.US.value:
            return {
                "w": "Week",
                "d": "Day",
                "60m": "60m",
                "30m": "30m",
                "10m": "10m",
                "15m": "15m",
                "5m": "5m",
            }
        elif self.market == Market.CURRENCY.value:
            return {
                "w": "Week",
                "d": "Day",
                "4h": "4H",
                "60m": "1H",
                "30m": "30m",
                "15m": "15m",
                "10m": "5m",
                "5m": "5m",
                "3m": "3m",
                "2m": "2m",
                "1m": "1m",
            }
        elif self.market == Market.CURRENCY_SPOT.value:
            return {
                "w": "Week",
                "d": "Day",
                "4h": "4H",
                "60m": "1H",
                "30m": "30m",
                "15m": "15m",
                "10m": "5m",
                "5m": "5m",
                "3m": "3m",
                "2m": "2m",
                "1m": "1m",
            }
        return {"d": "D", "30m": "30m"}

    def query_last_datetime(self, code, frequency) -> Union[None, str]:
        """
        查询交易对儿最后更新时间
        :param frequency:
        :param code:
        :return:
        """
        return db.klines_last_datetime(self.market, code, frequency)

    def insert_klines(self, code, frequency, klines):
        """
        批量添加交易对儿Kline数据
        :param code:
        :param frequency
        :param klines:
        :return:
        """
        db.klines_insert(self.market, code, frequency, klines)
        return True

    def del_klines(self, code, frequency, _datetime: datetime.datetime):
        """
        删除一条记录
        """
        db.klines_delete(self.market, code, frequency, _datetime)
        return

    def del_klines_by_code(self, code):
        db.klines_delete(self.market, code)
        return

    def del_klines_by_code_freq(self, code, freq):
        db.klines_delete(self.market, code, frequency=freq)
        return

    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        if args is None:
            args = {}

        limit = 5000
        if "limit" in args.keys():
            limit = args["limit"]
        order = "desc"
        if "order" in args.keys():
            order = args["order"]

        if start_date is not None and end_date is not None and "limit" not in args:
            limit = None
        if start_date is not None:
            start_date = fun.str_to_datetime(start_date)
        if end_date is not None:
            end_date = fun.str_to_datetime(end_date)
        klines = db.klines_query(
            self.market, code, frequency, start_date, end_date, limit, order
        )
        kline_pd = []
        for _k in klines:
            _kline = {
                "code": _k.code,
                "date": _k.dt,
                "open": _k.o,
                "high": _k.h,
                "low": _k.l,
                "close": _k.c,
                "volume": _k.v,
            }
            if self.market == Market.FUTURES.value:
                _kline["position"] = _k.p
            kline_pd.append(_kline)
        if len(kline_pd) == 0:
            kline_pd = pd.DataFrame(
                [], columns=["date", "code", "high", "low", "open", "close", "volume"]
            )
            return kline_pd

        kline_pd = pd.DataFrame(kline_pd)
        kline_pd["code"] = code
        kline_pd["date"] = pd.to_datetime(kline_pd["date"]).dt.tz_localize(
            self.tz
        )  # .map(lambda d: d.to_pydatetime())
        kline_pd["date"] = kline_pd["date"].apply(self.__convert_date)
        kline_pd.sort_values(by="date", inplace=True)
        kline_pd = kline_pd.reset_index(drop=True)

        return kline_pd

    def __convert_date(self, dt: datetime.datetime):
        """
        统一各个市场的时间格式
        TODO 需要根据自己数据源的数据格式进行调整
        TODO 将日及以上周期（大多数这类的时间都是 0点0分），修改为交易日结束或开始时间（根据日期是前对其还是后对其来决定是开盘时间还是收盘时间）
        """
        if self.market == Market.A.value:
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=15, minute=0)
        if self.market == Market.HK.value:
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=16, minute=0)
        if self.market == Market.FUTURES.value:
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=9, minute=0)
        if self.market == Market.US.value:
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=9, minute=30)
        return dt

    def convert_kline_frequency(self, klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
        """
        转换K线周期
        """
        if (
            self.market == Market.CURRENCY.value
            or self.market == Market.CURRENCY_SPOT.value
        ):
            return convert_currency_kline_frequency(klines, to_f)
        elif self.market == Market.FUTURES.value:
            return convert_futures_kline_frequency(klines, to_f)
        elif self.market == Market.US.value:
            return convert_us_kline_frequency(klines, to_f)
        else:
            return convert_stock_kline_frequency(klines, to_f)

    def all_stocks(self):
        return []

    def now_trading(self):
        pass

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        ticks = {}
        for _code in codes:
            klines = self.klines(_code, "d", args={"limit": 1})
            if len(klines) == 0:
                continue
            ticks[_code] = Tick(
                _code,
                klines.iloc[-1]["close"],
                klines.iloc[-1]["close"],
                klines.iloc[-1]["close"],
                klines.iloc[-1]["high"],
                klines.iloc[-1]["low"],
                klines.iloc[-1]["open"],
                klines.iloc[-1]["volume"],
                0,
            )
        return ticks

    def stock_info(self, code: str) -> Dict:
        return {
            "code": code,
            "name": code,
        }

    def stock_owner_plate(self, code: str):
        pass

    def plate_stocks(self, code: str):
        pass

    def balance(self):
        pass

    def positions(self, code: str = ""):
        pass

    def order(self, code: str, o_type: str, amount: float, args=None):
        pass


if __name__ == "__main__":
    ex = ExchangeDB(Market.CURRENCY_SPOT.value)
    # ticks = ex.ticks(['SHSE.000001'])
    # print(ticks)

    # ex.del_klines_by_code_freq("BTC/USDT", "4h")

    klines = ex.klines(
        "BTC/USDT",
        "4h",
        # start_date="2023-12-01 00:00:00",
        args={"limit": 10000},
    )
    print(len(klines))
    print(klines.head(5))
    print(klines.tail(5))
