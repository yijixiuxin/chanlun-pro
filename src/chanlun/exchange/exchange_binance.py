import datetime
from typing import Dict, List, Union

import ccxt
import pandas as pd
import pytz
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random
from tzlocal import get_localzone

from chanlun import config, fun
from chanlun.exchange.exchange import Exchange, Tick, convert_currency_kline_frequency
from chanlun.exchange.exchange_db import ExchangeDB
from chanlun.utils import config_get_proxy


@fun.singleton
class ExchangeBinance(Exchange):
    """
    数字货币交易所接口
    """

    g_all_stocks = []

    def __init__(self):
        params = {}

        proxy = config_get_proxy()
        # print(proxy)

        # 设置是否使用代理
        if proxy["host"] != "":
            params["proxies"] = {
                "https": f"http://{proxy['host']}:{proxy['port']}",
                "http": f"http://{proxy['host']}:{proxy['port']}",
            }

        # 设置是否设置交易 api
        if config.BINANCE_APIKEY != "":
            params["apiKey"] = config.BINANCE_APIKEY
            params["secret"] = config.BINANCE_SECRET

        self.exchange = ccxt.binanceusdm(params)

        self.db_exchange = ExchangeDB("currency")

        # 设置时区
        # self.tz = pytz.timezone("Asia/Shanghai")
        self.tz = pytz.timezone(str(get_localzone()))

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
        all_stocks = self.all_stocks()
        for _s in all_stocks:
            if _s["code"] == code:
                return _s

    def all_stocks(self):
        """
        返回所有交易对儿
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        markets = self.exchange.load_markets(reload=True)
        __all_stocks = []
        for _, s in markets.items():
            if s["active"] and s["quote"] == "USDT":
                __all_stocks.append(
                    {
                        "code": s["base"] + "/" + s["quote"],
                        "name": s["base"] + "/" + s["quote"],
                        "precision": fun.reverse_decimal_to_power_of_ten(
                            s["precision"]["price"]
                        ),
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
            # 查询数据库，如果数据库为0，api查询并插入数据库
            db_klines = self.db_exchange.klines(code, frequency, args={"limit": 10000})
            if len(db_klines) == 0:
                online_klines = self.increment_klines_by_online(
                    code, frequency, start_date=None
                )
                self.db_exchange.insert_klines(code, frequency, online_klines)
                return online_klines
            else:
                # 根据数据库中的最后时间，调用api进行返回数据
                last_datetime = db_klines.iloc[-2]["date"].strftime("%Y-%m-%d %H:%M:%S")
                online_klines = self.increment_klines_by_online(
                    code, frequency, start_date=last_datetime
                )
                self.db_exchange.insert_klines(code, frequency, online_klines)
            klines = pd.concat([db_klines, online_klines], ignore_index=True)
            klines.drop_duplicates(subset=["date"], keep="last", inplace=True)
            klines = klines.sort_values(by="date", ascending=True)
            return klines[-10000::]
        except Exception as e:
            print(f"{code} - {frequency} Error : {e}")
            # print(traceback.format_exc())
            # exit()

        return None

    def increment_klines_by_online(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        """
        增量 API 接口请求行情数据

        Args:
            code: 交易对代码
            frequency: K线周期
            start_date: 开始日期，格式为 "YYYY-MM-DD HH:MM:SS"
            end_date: 结束日期，格式为 "YYYY-MM-DD HH:MM:SS"
            args: 额外参数

        Returns:
            pd.DataFrame: 包含K线数据的DataFrame，如果出错则返回None

        说明:
            - 如果start_date为空，则从最新数据往前获取，直到获取10000根或返回不足1000根
            - 如果start_date有值，则从该时间点开始往后获取，直到获取到最新数据
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

        # 转换时间戳
        start_timestamp = None

        if start_date is not None:
            start_timestamp = (
                int(
                    datetime.datetime.timestamp(
                        datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                    )
                )
                * 1000
            )

        # 存储所有获取的K线数据
        all_klines = []
        target_count = 10000  # 目标K线数量

        if start_date is None:
            # 从最新数据往前获取
            current_end = None  # 初始为None表示获取最新数据
            while len(all_klines) < target_count:
                params = {}
                if current_end is not None:
                    params["endTime"] = current_end
                # 获取K线数据
                kline = self.exchange.fetch_ohlcv(
                    symbol=code,
                    timeframe=frequency_map[frequency],
                    limit=1000,
                    params=params,
                )
                # 如果返回的数据少于1000条，说明已经没有更多历史数据了
                if len(kline) < 1000:
                    all_klines = kline + all_klines
                    break

                # 更新结束时间为当前批次的第一条记录的时间（最早的时间）
                current_end = kline[0][0]

                # 将当前批次添加到结果中（注意顺序）
                all_klines = kline + all_klines

                # 如果已经获取足够多的数据，就停止
                if len(all_klines) >= target_count:
                    break
        else:
            # 从指定的开始时间往后获取，直到最新数据
            current_start = start_timestamp

            while True:
                params = {"startTime": current_start}

                # 获取K线数据
                kline = self.exchange.fetch_ohlcv(
                    symbol=code,
                    timeframe=frequency_map[frequency],
                    limit=1000,
                    params=params,
                )

                # 如果返回的数据少于1000条，说明已经到达最新数据
                if len(kline) < 1000:
                    all_klines.extend(kline)
                    break

                all_klines.extend(kline)

                # 更新开始时间为当前批次的最后一条记录的时间（最新的时间）
                current_start = kline[-1][0]

        # 如果没有获取到数据，返回None
        if len(all_klines) == 0:
            return None

        # 转换为DataFrame
        kline_pd = pd.DataFrame(
            all_klines, columns=["date", "open", "high", "low", "close", "volume"]
        )
        kline_pd["code"] = code
        kline_pd["date"] = kline_pd["date"].apply(
            lambda x: datetime.datetime.fromtimestamp(x / 1e3).astimezone(self.tz)
        )
        kline_pd = kline_pd[["code", "date", "open", "close", "high", "low", "volume"]]
        kline_pd.drop_duplicates(subset=["date"], keep="last", inplace=True)

        # 自定义级别，需要进行转换
        if frequency in ["10m", "2m", "3h"] and len(kline_pd) > 0:
            kline_pd = convert_currency_kline_frequency(kline_pd, frequency)

        return kline_pd

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
        _ts = self.exchange.fetch_tickers(codes)
        for _s, _t in _ts.items():
            if _t["last"] is None:
                continue
            _c = _s.split(":")[0]
            res_ticks[_c] = Tick(
                code=_c,
                last=_t["last"],
                buy1=_t["last"],
                sell1=_t["last"],
                high=_t["high"],
                low=_t["low"],
                open=_t["open"],
                volume=_t["quoteVolume"],
                rate=_t["percentage"],
            )

        return res_ticks

    def balance(self):
        b = self.exchange.fetch_balance()
        balances = {
            "total": b["USDT"]["total"],
            "free": b["USDT"]["free"],
            "used": b["USDT"]["used"],
            "profit": b["info"]["totalUnrealizedProfit"],
        }
        for asset in b["info"]["assets"]:
            balances[asset["asset"]] = {
                "total": asset["availableBalance"],
                "profit": asset["unrealizedProfit"],
            }
        return balances

    def positions(self, code: str = ""):
        try:
            position = self.exchange.fetch_positions(
                symbols=[code] if code != "" else None
            )
        except Exception as e:
            if "precision" in str(e):
                self.__init__()
                position = self.exchange.fetch_positions(
                    symbols=[code] if code != "" else None
                )
            else:
                raise e
        """
        symbol 标的
        entryPrice 价格
        contracts 持仓数量
        side 方向 long short
        leverage 杠杠倍数
        unrealizedPnl 未实现盈亏
        initialMargin 占用保证金
        percentage 盈亏百分比
        """
        # 替换其中的 symbol ，去除后面的 :USDT
        res_poss = []
        for p in position:
            if p["entryPrice"] != 0.0:
                p["symbol"] = p["symbol"].replace(":USDT", "")
                res_poss.append(p)
        return res_poss

    # 撤销所有挂单
    def cancel_all_order(self, code):
        self.exchange.cancel_all_orders(symbol=code)
        return True

    def order(self, code: str, o_type: str, amount: float, args=None):
        trade_maps = {
            "open_long": {"side": "BUY", "positionSide": "LONG"},
            "open_short": {"side": "SELL", "positionSide": "SHORT"},
            "close_long": {"side": "SELL", "positionSide": "LONG"},
            "close_short": {"side": "BUY", "positionSide": "SHORT"},
        }
        if "open" in o_type:
            self.exchange.set_leverage(args["leverage"], symbol=code)
        return self.exchange.create_order(
            symbol=code,
            type="MARKET",
            side=trade_maps[o_type]["side"],
            amount=amount,
            params={"positionSide": trade_maps[o_type]["positionSide"]},
        )

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")


if __name__ == "__main__":
    ex = ExchangeBinance()

    stocks = ex.all_stocks()
    print(len(stocks))
    print(stocks[0])

    # klines = ex.klines("BTC/USDT", "1m")
    # print(klines)

    # ticks = ex.ticks(["BTC/USDT", "ETH/USDT"])
    # for _c, _t in ticks.items():
    #     print(
    #         _c, _t.last, _t.buy1, _t.sell1, _t.high, _t.low, _t.open, _t.volume, _t.rate
    #     )

    # zx = zixuan.ZiXuan("currency")
    # zx_group = "选股"
    # run_codes = zx.zx_stocks("策略代码")
    # run_codes = [_s["code"] for _s in run_codes]
    # error_codes = []
    # for code in run_codes:
    #     try:
    #         klines = ex.klines(code, "60m")
    #         print(code)
    #         print(klines.tail())
    #         print(len(klines))
    #     except Exception as e:
    #         print(f"ERROR {code}")
    #         error_codes.append(code)

    # print("Error codes : ", error_codes)

    # balance = ex.balance()
    # print(balance)
