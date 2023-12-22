import datetime as dt
import os

from alpaca.data import StockBarsRequest, StockSnapshotRequest, DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

import datetime as dt
from chanlun import config
from chanlun import fun
from chanlun.exchange.exchange import *

g_all_stocks = []


@fun.singleton
class ExchangeAlpaca(Exchange):
    """
    TODO 年久失修，使用前请自行修改测试
    """

    def __init__(self):
        super().__init__()

        self.client = StockHistoricalDataClient(
            api_key=config.ALPACA_APIKEY, secret_key=config.ALPACA_SECRET
        )

        # 设置时区
        self.tz = pytz.timezone("US/Eastern")

        # is vip 如果是付费的，可以查询最新的数据，否则只能查询历史
        self.is_vip = False

    def default_code(self):
        return "AAPL"

    def support_frequencys(self):
        return {
            "m": "Month",
            "w": "Week",
            "d": "Day",
            "60m": "1H",
            "30m": "30m",
            "10m": "10m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }

    def all_stocks(self):
        """
        获取所有股票代码
        """
        global g_all_stocks
        if len(g_all_stocks) > 0:
            return g_all_stocks
        stocks = pd.read_csv(
            os.path.split(os.path.realpath(__file__))[0] + "/us_symbols.csv"
        )
        for s in stocks.iterrows():
            g_all_stocks.append({"code": s[1]["code"], "name": s[1]["name"]})
        return g_all_stocks

        # 以下是从网络获取
        # if len(g_all_stocks) > 0:
        #     return g_all_stocks
        # g_all_stocks = rd.get_ex('us_stocks_all')
        # if g_all_stocks is not None:
        #     return g_all_stocks
        # g_all_stocks = []
        #
        # g_all_stocks = [el.symbol for el in self.api.list_assets(status='active', asset_class='us_equity')]
        # if len(g_all_stocks) > 0:
        #     rd.save_ex('us_stocks_all', 24 * 60 * 60, g_all_stocks)
        #
        # return g_all_stocks

    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> [pd.DataFrame, None]:
        if args is None:
            args = {}
        frequency_map = {
            "m": TimeFrame.Month,
            "w": TimeFrame.Week,
            "d": TimeFrame.Day,
            "60m": TimeFrame.Hour,
            "30m": TimeFrame(30, TimeFrameUnit.Minute),
            "10m": TimeFrame(10, TimeFrameUnit.Minute),
            "15m": TimeFrame(15, TimeFrameUnit.Minute),
            "5m": TimeFrame(5, TimeFrameUnit.Minute),
            "1m": TimeFrame(1, TimeFrameUnit.Minute),
        }
        timeframe = frequency_map[frequency]
        try:
            if end_date is None:
                end_date = datetime.datetime.now()
                end_date = (
                    end_date + dt.timedelta(days=1)
                    if self.is_vip
                    else end_date - dt.timedelta(days=1)
                )
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
            req = StockBarsRequest(
                symbol_or_symbols=code.upper(),
                timeframe=timeframe,
                start=start_date,
                end=end_date,
                limit=5000,
            )
            bars = self.client.get_stock_bars(req)
            klines = []
            for _b in bars.data[code.upper()]:
                klines.append(
                    {
                        "code": code,
                        "date": _b.timestamp,
                        "open": _b.open,
                        "close": _b.close,
                        "high": _b.high,
                        "low": _b.low,
                        "volume": _b.volume,
                    }
                )
            klines = pd.DataFrame(klines)
            return klines
        except Exception as e:
            print(f"alpaca 获取行情异常 {code} Exception ：{str(e)}")
        return None

    def stock_info(self, code: str) -> [Dict, None]:
        """
        获取股票名称，避免网络 api 请求，从 all_stocks 中获取
        """
        stocks = self.all_stocks()
        return next((s for s in stocks if s["code"] == code.upper()), None)

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取行情Tick数据
        """
        code_ticks = {}
        req = StockSnapshotRequest(symbol_or_symbols=codes, feed=DataFeed.IEX)
        res = self.client.get_stock_snapshot(req)
        for _c, _t in res.items():
            code_ticks[_c] = Tick(
                code=_c,
                last=_t.latest_trade.price,
                buy1=_t.latest_quote.bid_price,
                sell1=_t.latest_quote.ask_price,
                high=_t.daily_bar.high,
                low=_t.daily_bar.low,
                open=_t.daily_bar.open,
                volume=_t.daily_bar.volume,
                rate=round(
                    (_t.daily_bar.close - _t.previous_daily_bar.close)
                    / _t.previous_daily_bar.close
                    * 100,
                    2,
                ),
            )
        return code_ticks

    def now_trading(self):
        """
        返回当前是否是交易时间
        """
        tz = pytz.timezone("US/Eastern")
        now = datetime.datetime.now(tz)
        weekday = now.weekday()
        hour = now.hour
        minute = now.minute
        if weekday in [0, 1, 2, 3, 4] and (
            (10 <= hour < 16) or (hour == 9 and minute >= 30)
        ):
            return True
        return False

    @staticmethod
    def __convert_date(_dt):
        _dt = fun.datetime_to_str(_dt, "%Y-%m-%d")
        return fun.str_to_datetime(_dt, "%Y-%m-%d")

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
    ex = ExchangeAlpaca()

    # klines = ex.klines(ex.default_code(), '30m')
    # print(klines.tail())

    ticks = ex.ticks([ex.default_code()])
    print(ticks)
