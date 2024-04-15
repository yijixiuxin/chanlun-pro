"""
US Polygon 行情接口
"""

import datetime as dt
import os
from typing import Union

from polygon.rest import RESTClient
from tenacity import retry_if_result, wait_random, stop_after_attempt, retry

from chanlun import config
from chanlun import fun
from chanlun.exchange.exchange import *


@fun.singleton
class ExchangePolygon(Exchange):
    """
    美股 Polygon 行情服务
    """

    g_all_stocks = []

    def __init__(self):
        super().__init__()

        self.client = RESTClient(config.POLYGON_APIKEY)

        self.trade_days = None

        # 设置时区
        self.tz = pytz.timezone("US/Eastern")

    def default_code(self):
        return "AAPL"

    def support_frequencys(self):
        return {
            "y": "Year",
            "q": "Quarter",
            "m": "Month",
            "w": "Week",
            "d": "Day",
            "120m": "2H",
            "60m": "1H",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }

    def all_stocks(self):
        """
        使用 Polygono 的方式获取所有股票代码
        美股获取所有标的时间比较长，直接从 json 文件中获取
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks
        stocks = pd.read_csv(
            os.path.split(os.path.realpath(__file__))[0] + "/us_symbols.csv"
        )
        __all_stocks = []
        for s in stocks.iterrows():
            __all_stocks.append({"code": s[1]["code"], "name": s[1]["name"]})
        self.g_all_stocks = __all_stocks
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
        if args is None:
            args = {}
        frequency_map = {
            "y": "year",
            "q": "quarter",
            "m": "month",
            "w": "week",
            "d": "day",
            "120m": "hour",
            "60m": "hour",
            "30m": "minute",
            "15m": "minute",
            "5m": "minute",
            "1m": "minute",
        }

        frequency_mult = {
            "y": 1,
            "q": 1,
            "m": 1,
            "w": 1,
            "d": 1,
            "120m": 2,
            "60m": 1,
            "30m": 30,
            "15m": 15,
            "5m": 5,
            "1m": 1,
        }

        try:
            if end_date is None:
                end_date = datetime.datetime.now()
                end_date = end_date + dt.timedelta(days=1)
                end_date = fun.str_to_datetime(
                    fun.datetime_to_str(end_date, "%Y-%m-%d"), "%Y-%m-%d"
                )
            else:
                if len(end_date) == 10:
                    end_date = fun.str_to_datetime(end_date, "%Y-%m-%d")
                else:
                    end_date = fun.str_to_datetime(end_date)
            if start_date is None:
                if frequency == "1m":
                    start_date = end_date - dt.timedelta(days=15)
                elif frequency == "5m":
                    start_date = end_date - dt.timedelta(days=15)
                elif frequency == "30m":
                    start_date = end_date - dt.timedelta(days=75)
                elif frequency == "60m":
                    start_date = end_date - dt.timedelta(days=150)
                elif frequency == "120m":
                    start_date = end_date - dt.timedelta(days=150)
                elif frequency == "d":
                    start_date = end_date - dt.timedelta(days=5000)
                elif frequency == "w":
                    start_date = end_date - dt.timedelta(days=7800)
                elif frequency == "y":
                    start_date = end_date - dt.timedelta(days=15000)
            else:
                if len(end_date) == 10:
                    start_date = fun.str_to_datetime(start_date, "%Y-%m-%d")
                else:
                    start_date = fun.str_to_datetime(start_date)

            resp = self.client.get_aggs(
                code.upper(),
                frequency_mult[frequency],
                frequency_map[frequency],
                start_date,
                end_date,
                limit=50000,
            )
            klines_df = []
            for r in resp:
                klines_df.append(
                    {
                        "code": code.upper(),
                        "date": fun.timeint_to_datetime(r.timestamp / 1000).astimezone(
                            self.tz
                        ),
                        "open": r.open,
                        "close": r.close,
                        "high": r.high,
                        "low": r.low,
                        "volume": r.volume,
                    }
                )
            klines_df = pd.DataFrame(klines_df)
            klines_df.sort_values("date", inplace=True)
            if frequency in ["y", "q", "m", "w", "d"]:
                klines_df["date"] = klines_df["date"].apply(
                    lambda _d: _d.replace(hour=9, minute=30)
                )
            return klines_df
        except Exception as e:
            print("polygon.io 获取行情异常 %s Exception ：%s" % (code, str(e)))

        return None

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票名称
        """
        all_stocks = self.all_stocks()
        for s in all_stocks:
            if s["code"].upper() == code.upper():
                return s
        return None

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        使用富途的接口获取行情Tick数据
        """
        # ticks = {}
        # for _c in codes:
        #     _t = self.client.get_daily_open_close_agg(_c)
        #     ticks[_c] = Tick(
        #         code=_c, last=_t.close, buy1=_t.close, sell1=_t.close,
        #         high=_t.high, low=_t.low, open=_t.open, volume=_t.volume,
        #         rate=_t.
        #     )
        raise Exception("交易所不支持")

    def now_trading(self):
        """
        返回当前是否是交易时间
        """
        resp = self.client.get_market_status()
        if resp.market != "closed":
            return True
        return False

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")

    def balance(self):
        raise Exception("交易所不支持")

    def positions(self, code: str = ""):
        raise Exception("交易所不支持")

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise Exception("交易所不支持")


if __name__ == "__main__":
    ex = ExchangePolygon()

    # is_trading = ex.now_trading()
    # print(is_trading)

    # klines = ex.klines(ex.default_code(), "30m")
    # print(klines.tail(50))

    # ticks = ex.ticks([ex.default_code()])
    # print(ticks)

    tickers = ex.client.list_tickers(
        type="CS", market="stocks", active=True, limit=1000
    )
    stocks = []
    for t in tickers:
        stocks.append(
            {
                "ticker": t.ticker,
                "exchange": t.primary_exchange,
                "name": t.name,
                "currency": t.currency_name,
                "last_updated": t.last_updated_utc,
            }
        )

    print(stocks)
    print(len(stocks))
