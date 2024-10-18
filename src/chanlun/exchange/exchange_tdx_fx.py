import time
import traceback
from typing import Union

from pytdx.errors import TdxConnectionError
from pytdx.exhq import TdxExHq_API
from pytdx.util import best_ip
from tenacity import retry, stop_after_attempt, wait_random, retry_if_result

from chanlun import fun
from chanlun.db import db
from chanlun.exchange.exchange import *
from chanlun.file_db import FileCacheDB


@fun.singleton
class ExchangeTDXFX(Exchange):
    """
    通达信外汇行情接口
    """

    g_all_stocks = []

    def __init__(self):
        # super().__init__()

        # 选择最优的服务器，并保存到 cache 中
        self.connect_info = db.cache_get("tdxex_connect_ip")
        if self.connect_info is None:
            self.connect_info = self.reset_tdx_ip()
            # print(f"TDXEX 最优服务器：{self.connect_info}")

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        # 文件缓存
        self.fdb = FileCacheDB()

    def reset_tdx_ip(self):
        """
        重新选择tdx最优服务器
        """
        connect_info = best_ip.select_best_ip("future")
        connect_info = {"ip": connect_info["ip"], "port": int(connect_info["port"])}
        db.cache_set("tdxex_connect_ip", connect_info)
        self.connect_info = connect_info
        return connect_info

    def default_code(self):
        return "AUDUSD"

    def support_frequencys(self):
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
            "1m": "1m",
        }

    def all_stocks(self):
        """
        使用 通达信的方式获取所有股票代码
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        __all_stocks = []
        client = TdxExHq_API(raise_exception=True, auto_retry=True)
        with client.connect(self.connect_info["ip"], self.connect_info["port"]):
            start_i = 0
            count = 1000
            while True:
                instruments = client.get_instrument_info(start_i, count)
                for _i in instruments:
                    if _i["category"] == 4 and _i["market"] in [10, 11]:
                        __all_stocks.append(
                            {
                                "code": f"{_i['market']}.{_i['code']}",
                                "name": _i["name"],
                            }
                        )
                start_i += count
                if len(instruments) < count:
                    break

        self.g_all_stocks = __all_stocks
        # print(f"期货获取数量：{len(self.g_all_stocks)}")

        return self.g_all_stocks

    def to_tdx_code(self, code):
        """
        转换为 tdx 对应的代码
        """
        code_infos = code.split(".")
        return code_infos[0], code_infos[1]

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
        通达信，不支持按照时间查找
        """
        if args is None:
            args = {}
        if "pages" not in args.keys():
            args["pages"] = 8
        else:
            args["pages"] = int(args["pages"])

        frequency_map = {
            "y": 11,
            "q": 10,
            "m": 6,
            "w": 5,
            "d": 9,
            "60m": 3,
            "30m": 2,
            "15m": 1,
            "5m": 0,
            "1m": 8,
        }
        market, tdx_code = self.to_tdx_code(code)
        if market is None or start_date is not None or end_date is not None:
            print("不支持的调用参数")
            return None

        # _time_s = time.time()
        try:
            client = TdxExHq_API(raise_exception=True, auto_retry=True)
            with client.connect(self.connect_info["ip"], self.connect_info["port"]):
                klines: pd.DataFrame = self.fdb.get_tdx_klines(code, frequency)
                if klines is None:
                    # 获取 8*800 = 6400 条数据
                    klines = pd.concat(
                        [
                            client.to_df(
                                client.get_instrument_bars(
                                    frequency_map[frequency],
                                    market,
                                    tdx_code,
                                    (i - 1) * 800,
                                    800,
                                )
                            )
                            for i in range(1, args["pages"] + 1)
                        ],
                        axis=0,
                        sort=False,
                    )
                    if len(klines) == 0:
                        return pd.DataFrame([])
                    klines["datetime"] = klines["datetime"].apply(self.fix_yp_date)
                    klines.loc[:, "date"] = pd.to_datetime(klines["datetime"])
                    klines.sort_values("date", inplace=True)
                else:
                    for i in range(1, args["pages"] + 1):
                        # print(f'{code} 使用缓存，更新获取第 {i} 页')
                        _ks = client.to_df(
                            client.get_instrument_bars(
                                frequency_map[frequency],
                                market,
                                tdx_code,
                                (i - 1) * 800,
                                800,
                            )
                        )
                        _ks["datetime"] = _ks["datetime"].apply(self.fix_yp_date)
                        _ks.loc[:, "date"] = pd.to_datetime(_ks["datetime"])
                        _ks.sort_values("date", inplace=True)
                        new_start_dt = _ks.iloc[0]["date"]
                        old_end_dt = klines.iloc[-1]["date"]
                        klines = pd.concat([klines, _ks], ignore_index=True)
                        # 如果请求的第一个时间大于缓存的最后一个时间，退出
                        if old_end_dt >= new_start_dt:
                            break

            # 删除重复数据
            klines = klines.drop_duplicates(["date"], keep="last").sort_values("date")
            self.fdb.save_tdx_klines(code, frequency, klines)

            klines.loc[:, "code"] = code
            klines.loc[:, "volume"] = klines["trade"]
            klines.loc[:, "date"] = pd.to_datetime(klines["datetime"]).dt.tz_localize(
                self.tz
            )

            # 将 volume 转换成 float类型
            klines[["volume"]] = klines[["volume"]].astype(float)

            return klines[["code", "date", "open", "close", "high", "low", "volume"]]
        except TdxConnectionError:
            self.reset_tdx_ip()
        except Exception as e:
            print(f"获取行情异常 {code} Exception ：{str(e)}")
            traceback.print_exc()
        finally:
            pass
            # print(f'请求行情用时：{time.time() - _s_time}')
        return None

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票名称
        """
        all_stock = self.all_stocks()
        stock = [_s for _s in all_stock if _s["code"] == code]
        if not stock:
            return None
        return {"code": stock[0]["code"], "name": stock[0]["name"]}

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        如果可以使用 富途 的接口，就用 富途的，否则就用 日线的 K线计算
        使用 富途 的接口会很快，日线则很慢
        获取日线的k线，并返回最后一根k线的数据
        """
        ticks = {}
        client = TdxExHq_API(raise_exception=True, auto_retry=True)
        with client.connect(self.connect_info["ip"], self.connect_info["port"]):
            for _code in codes:
                _market, _tdx_code = self.to_tdx_code(_code)
                if _market is None:
                    continue
                _quote = client.get_instrument_quote(_market, _tdx_code)
                # [OrderedDict([('market', 1), ('code', 'FG2305'), ('pre_close', 1546.0), ('open', 1548.0),
                # ('high', 1558.0), ('low', 1536.0), ('price', 1543.0), ('kaicang', 341886), ('zongliang', 367292),
                # ('xianliang', 1), ('neipan', 192905), ('waipan', 174387), ('chicang', 993096), ('bid1', 1543.0),
                # ('bid2', 0.0), ('bid3', 0.0), ('bid4', 0.0), ('bid5', 0.0), ('bid_vol1', 903), ('bid_vol2', 0),
                # ('bid_vol3', 0), ('bid_vol4', 0), ('bid_vol5', 0), ('ask1', 1544.0), ('ask2', 0.0), ('ask3', 0.0),
                # ('ask4', 0.0), ('ask5', 0.0), ('ask_vol1', 512), ('ask_vol2', 0), ('ask_vol3', 0), ('ask_vol4', 0),
                # ('ask_vol5', 0)])]
                if len(_quote) > 0:
                    _quote = _quote[0]
                    ticks[_code] = Tick(
                        code=_code,
                        last=_quote["price"],
                        buy1=_quote["bid1"],
                        sell1=_quote["ask1"],
                        low=_quote["low"],
                        high=_quote["high"],
                        volume=_quote["zongliang"],
                        open=_quote["open"],
                        rate=(
                            round(
                                (_quote["price"] - _quote["pre_close"])
                                / _quote["price"]
                                * 100,
                                2,
                            )
                            if _quote["price"] > 0
                            else 0
                        ),
                    )
        return ticks

    def now_trading(self):
        """
        返回当前是否是交易时间
        """
        return True

    def balance(self):
        raise Exception("交易所不支持")

    def positions(self, code: str = ""):
        raise Exception("交易所不支持")

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise Exception("交易所不支持")

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")


if __name__ == "__main__":
    ex = ExchangeTDXFX()
    # stocks = ex.all_stocks()
    # print(len(stocks))
    # for s in stocks:
    #     if '原油' in s["name"]:
    #         print(s)

    # print(ex.to_tdx_code('QS.ZN2306'))
    #
    klines = ex.klines("10.AUDUSD", "d")
    print(len(klines))
    print(klines.tail(60))
