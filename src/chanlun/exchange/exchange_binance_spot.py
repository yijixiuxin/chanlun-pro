from typing import Union

import ccxt
import pytz
from tenacity import retry, stop_after_attempt, wait_random, retry_if_result


from chanlun import config, fun
from chanlun.utils import config_get_proxy
from chanlun.base import Market
from chanlun.exchange.exchange import *
from chanlun.exchange.exchange_db import ExchangeDB


@fun.singleton
class ExchangeBinanceSpot(Exchange):
    """
    数字货币交易所接口(现货交易)
    """

    g_all_stocks = []

    def __init__(self):
        params = {}

        proxy = config_get_proxy()
        # print(proxy)

        # 设置是否使用代理
        if proxy != None and proxy["host"] != "":
            params["proxies"] = {
                "https": f"http://{proxy['host']}:{proxy['port']}",
                "http": f"http://{proxy['host']}:{proxy['port']}",
            }

        # 设置是否设置交易 api
        if config.BINANCE_APIKEY != "":
            params["apiKey"] = config.BINANCE_APIKEY
            params["secret"] = config.BINANCE_SECRET

        self.exchange = ccxt.binance(params)
        # self.exchange = ccxt.htx(params)

        self.db_exchange = ExchangeDB(Market.CURRENCY_SPOT.value)

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

    def default_code(self):
        return "BTC/USDT"

    def support_frequencys(self):
        return {
            "w": "Week",
            "d": "Day",
            "12h": "12H",
            "8h": "8H",
            "6h": "6H",
            "4h": "4H",
            "3h": "3H",
            "60m": "1H",
            "30m": "30m",
            "15m": "15m",
            "10m": "10m",
            "5m": "5m",
            "3m": "3m",
            "2m": "2m",
            "1m": "1m",
        }

    def now_trading(self):
        """
        返回交易时间，数字货币 24 小时可交易
        """
        return True

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        数字货币全部返回 code 值
        """
        return {"code": code, "name": code}

    def all_stocks(self):
        """
        返回所有交易对儿
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        markets = self.exchange.load_markets()
        __all_stocks = []
        for _, s in markets.items():
            if s["quote"] == "USDT":
                __all_stocks.append(
                    {
                        "code": s["base"] + "/" + s["quote"],
                        "name": s["base"] + "/" + s["quote"],
                    }
                )
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
        """
        返回 k 线数据
        优先从数据库中获取，在进行 api 请求，合并数据，并更新数据库，之后返回k线行情
        可以减少网络请求，优化 vpn 使用流量
        """
        if args is None:
            args = {}

        if "use_online" in args.keys() and args["use_online"]:
            # 个别情况需要直接调用交易所结果，不需要通过数据库
            return self.online_klines(code, frequency, start_date, end_date, args)

        try:
            if start_date is not None or end_date is not None:
                online_klines = self.online_klines(
                    code, frequency, start_date, end_date, args
                )
                self.db_exchange.insert_klines(code, frequency, online_klines)
                return online_klines
            else:
                # 查询数据库，如果数据库为0，api查询并插入数据库
                db_klines = self.db_exchange.klines(
                    code, frequency, args={"limit": 10000}
                )
                if len(db_klines) == 0:
                    online_klines = self.online_klines(
                        code, frequency, start_date, end_date, args
                    )
                    self.db_exchange.insert_klines(code, frequency, online_klines)
                    return online_klines

                while True:
                    # 根据数据库中的最后时间，调用api进行返回数据
                    last_datetime = db_klines.iloc[-2]["date"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    online_klines = self.online_klines(
                        code, frequency, start_date=last_datetime
                    )
                    self.db_exchange.insert_klines(code, frequency, online_klines)
                    if (
                        len(online_klines) == 1500
                    ):  # 如果api获取的还是 1500，则说明没有跟新到最新的数据，则继续调用
                        db_klines = self.db_exchange.klines(
                            code, frequency, args={"limit": 10000}
                        )
                    else:
                        break

                klines = pd.concat([db_klines, online_klines], ignore_index=True)
                klines.drop_duplicates(subset=["date"], keep="last", inplace=True)
                return klines[-10000::]
        except Exception as e:
            print(f"{code} - {frequency} Error : {e}")
            # print(traceback.format_exc())
            # exit()

        return None

    def online_klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        """
        api 接口请求行情数据
        """
        # 1m  3m  5m  15m  30m  1h  2h  4h  6h  8h  12h  1d  3d  1w  1M
        if args is None:
            args = {}
        frequency_map = {
            "w": "1w",
            "d": "1d",
            "12h": "12h",
            "8h": "8h",
            "6h": "6h",
            "4h": "4h",
            "3h": "1h",
            "60m": "1h",
            "30m": "30m",
            "15m": "15m",
            "10m": "5m",
            "5m": "5m",
            "3m": "3m",
            "2m": "1m",
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
        params = {}
        if start_date is not None:
            params["startTime"] = start_date
        if end_date is not None:
            params["endTime"] = end_date

        kline = self.exchange.fetch_ohlcv(
            symbol=code,
            timeframe=frequency_map[frequency],
            limit=1000,
            params=params,
        )
        kline_pd = pd.DataFrame(
            kline, columns=["date", "open", "high", "low", "close", "volume"]
        )
        # kline_pd.loc[:, 'code'] = code
        # kline_pd.loc[:, 'date'] = kline_pd['date'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e3))
        kline_pd["code"] = code
        kline_pd["date"] = kline_pd["date"].apply(
            lambda x: datetime.datetime.fromtimestamp(x / 1e3).astimezone(self.tz)
        )
        kline_pd = kline_pd[["code", "date", "open", "close", "high", "low", "volume"]]
        # 自定义级别，需要进行转换
        if frequency in ["10m", "2m", "3h"] and len(kline_pd) > 0:
            kline_pd = convert_currency_kline_frequency(kline_pd, frequency)
        return kline_pd

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        res_ticks = {}
        for code in codes:
            try:
                _t = self.exchange.fetch_ticker(code)
                res_ticks[code] = Tick(
                    code=code,
                    last=_t["last"],
                    buy1=_t["last"],
                    sell1=_t["last"],
                    high=_t["high"],
                    low=_t["low"],
                    open=_t["open"],
                    volume=_t["quoteVolume"],
                    rate=_t["percentage"],
                )
            except Exception as e:
                print(f"{code} 获取 tick 异常 {e}")
        return res_ticks

    def ticker24HrRank(self, num=20):
        """
        获取24小时交易量排行前的交易对儿
        TODO 废弃了，不用了
        :param num:
        :return:
        """
        tickers = self.exchange.fapiPublicGetTicker24hr()
        tickers = sorted(tickers, key=lambda d: float(d["quoteVolume"]), reverse=True)
        codes = []
        for t in tickers:
            if t["symbol"][-4:] == "USDT":
                codes.append(t["symbol"][:-4] + "/" + t["symbol"][-4:])
            if len(codes) >= num:
                break
        return codes

    def balance(self):
        raise RuntimeWarning("交易接口未实现")

    def positions(self, code: str = ""):
        raise RuntimeWarning("交易接口未实现")

    # 撤销所有挂单
    def cancel_all_order(self, code):
        raise RuntimeWarning("交易接口未实现")

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise RuntimeWarning("交易接口未实现")

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")


if __name__ == "__main__":

    ex = ExchangeBinanceSpot()

    klines = ex.klines("BTC/USDT", "d")
    print(klines)
