import ccxt
import pymysql.err
import pytz

from chanlun import config
from chanlun.exchange.exchange import *
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun import fun

g_all_stocks = []


@fun.singleton
class ExchangeZB(Exchange):
    def __init__(self):
        params = {}
        if config.PROXY_HOST != "":
            params["proxies"] = {
                "https": f"http://{config.PROXY_HOST}:{config.PROXY_PORT}",
                "http": f"http://{config.PROXY_HOST}:{config.PROXY_PORT}",
            }

        if config.ZB_APIKEY != "":
            params["apiKey"] = config.ZB_APIKEY
            params["secret"] = config.ZB_SECRET

        if config.ZB_APIKEY != "":
            params["apiKey"] = config.ZB_APIKEY
            params["secret"] = config.ZB_SECRET

        params["verify"] = False

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        self.exchange = ccxt.zb(params)

        self.db_exchange = ExchangeDB("currency")

    def default_code(self):
        return "BTC/USDT"

    def support_frequencys(self):
        return {
            "w": "W",
            "d": "D",
            "4h": "4H",
            "60m": "1H",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }

    def all_stocks(self):
        global g_all_stocks
        if len(g_all_stocks) > 0:
            return g_all_stocks

        markets = self.exchange.load_markets()
        for s in markets:
            if "/" in s:
                g_all_stocks.append({"code": s, "name": s})

        return g_all_stocks

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
        try:
            if start_date is not None or end_date is not None:
                online_klines = self._online_klines(
                    code, frequency, start_date, end_date, args
                )
                self.db_exchange.insert_klines(code, frequency, online_klines)
                return online_klines

            db_klines = self.db_exchange.klines(code, frequency)
            if len(db_klines) == 0:
                online_klines = self._online_klines(
                    code, frequency, start_date, end_date, args
                )
                self.db_exchange.insert_klines(code, frequency, online_klines)
                return online_klines

            last_datetime = db_klines.iloc[-1]["date"].strftime("%Y-%m-%d %H:%M:%S")
            online_klines = self._online_klines(
                code, frequency, start_date=last_datetime
            )
            self.db_exchange.insert_klines(code, frequency, online_klines)
            if len(online_klines) == 1000:
                online_klines = self._online_klines(
                    code, frequency, start_date, end_date, args
                )
                self.db_exchange.insert_klines(code, frequency, online_klines)
                return online_klines

            klines = pd.concat([db_klines, online_klines], ignore_index=True)
            klines.drop_duplicates(subset=["date"], keep="last", inplace=True)
            return klines[-1000::]
        except Exception as e:
            print(e)

        return []

    def _online_klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> [pd.DataFrame, None]:
        # 1m  3m  5m  15m  30m  1h  2h  4h  6h  8h  12h  1d  3d  1w  1M
        if args is None:
            args = {}
        frequency_map = {
            "w": "1w",
            "d": "1d",
            "4h": "4h",
            "60m": "1h",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }
        if frequency not in frequency_map.keys():
            raise Exception(f"不支持的周期: {frequency}")

        if start_date is not None:
            start_date = (
                int(
                    datetime.datetime.timestamp(
                        datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                    )
                )
                * 1000
            )
        if end_date is not None:
            end_date = (
                int(
                    datetime.datetime.timestamp(
                        datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                    )
                )
                * 1000
            )

        kline = self.exchange.fetch_ohlcv(
            symbol=code,
            timeframe=frequency_map[frequency],
            limit=1000,
            params={"startTime": start_date, "endTime": end_date},
        )
        kline_pd = pd.DataFrame(
            kline, columns=["date", "open", "high", "low", "close", "volume"]
        )
        kline_pd["code"] = code
        kline_pd["date"] = kline_pd["date"].apply(
            lambda x: datetime.datetime.fromtimestamp(x / 1e3)
        )
        kline_pd["date"] = kline_pd["date"].dt.tz_localize(self.tz)
        # kline_pd['date'] = pd.to_datetime(kline_pd['date'].values / 1000, unit='s', utc=True).tz_convert(
        # 'Asia/Shanghai')
        return kline_pd[["code", "date", "open", "close", "high", "low", "volume"]]

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        res_ticks = {}
        for code in codes:
            try:
                _t = self.exchange.fetch_ticker(code)
                res_ticks[code] = Tick(
                    code=code,
                    last=_t["last"],
                    buy1=_t["bid"],
                    sell1=_t["ask"],
                    high=_t["high"],
                    low=_t["low"],
                    open=_t["open"],
                    volume=_t["quoteVolume"],
                    rate=_t["riseRate"],
                )
            except Exception as e:
                print(f"{code} 获取 tick 异常 {e}")
        return res_ticks

    def now_trading(self):
        return True

    def stock_info(self, code: str) -> [Dict, None]:
        return {"code": code, "name": code}

    def balance(self):
        return self.exchange.fetch_balance()

    def positions(self, code: str = ""):
        raise Exception("现货没有持仓，根据当前资产判断持有那些币")

    # 撤销所有挂单
    def cancel_all_order(self, code):
        orders = self.exchange.fetch_open_orders(code)
        for _o in orders:
            self.exchange.cancel_order(_o["id"], code)
        return True

    def order(self, code: str, o_type: str, amount: float, args=None):
        trade_maps = {
            "open_long": {"side": "BUY", "positionSide": "LONG"},
            "open_short": {"side": "SELL", "positionSide": "SHORT"},
            "close_long": {"side": "SELL", "positionSide": "LONG"},
            "close_short": {"side": "BUY", "positionSide": "SHORT"},
        }
        if o_type == "open_long":
            order = self.exchange.create_market_buy_order(code, amount)
            return order
        if o_type == "close_long":
            order = self.exchange.create_market_sell_order(code, amount)
            return order
        raise Exception("现货不支持做空")

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")


if __name__ == "__main__":
    ex = ExchangeZB()

    stocks = ex.all_stocks()
    print(stocks)

    klines = ex.klines(ex.default_code(), "30m")
    print(klines.tail())
