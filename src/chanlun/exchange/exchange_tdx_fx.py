import time
import traceback
from typing import Dict, List, Union

import pandas as pd
import pytz
from pytdx.errors import TdxConnectionError
from pytdx.exhq import TdxExHq_API
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random

from chanlun import fun
from chanlun.base import Market
from chanlun.db import db
from chanlun.exchange.exchange import Exchange, Tick
from chanlun.file_db import FileCacheDB
from chanlun.tools import tdx_best_ip as best_ip


@fun.singleton
class ExchangeTDXFX(Exchange):
    """
    通达信外汇行情接口
    """

    g_all_stocks = []

    def __init__(self):
        # super().__init__()

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        # 文件缓存
        self.fdb = FileCacheDB()

        try:
            # 选择最优的服务器，并保存到 cache 中
            self.connect_info = db.cache_get("tdxex_connect_ip")
            if self.connect_info is None:
                self.connect_info = self.reset_tdx_ip()
                # print(f"TDXEX 最优服务器：{self.connect_info}")

            # 初始化，映射交易所代码
            self.market_maps = {}
            while True:
                try:
                    client = TdxExHq_API(
                        multithread=True, raise_exception=True, auto_retry=True
                    )
                    with client.connect(
                        self.connect_info["ip"], self.connect_info["port"]
                    ):
                        all_markets = client.get_markets()
                        for _m in all_markets:
                            if _m["category"] == 4:
                                self.market_maps[_m["short_name"]] = {
                                    "market": _m["market"],
                                    "category": _m["category"],
                                    "name": _m["name"],
                                }
                    break
                except TdxConnectionError:
                    self.reset_tdx_ip()
        except Exception:
            print(traceback.format_exc())
            print("通达信 外汇行情接口初始化失败，外汇行情不可用")

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
        return "FX.USDEUR"

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
            "10m": "10m",
            "5m": "5m",
            "1m": "1m",
        }

    def all_stocks(self):
        """
        使用 通达信的方式获取所有外汇代码
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        __all_stocks = []
        client = TdxExHq_API(multithread=True, raise_exception=True, auto_retry=True)
        with client.connect(self.connect_info["ip"], self.connect_info["port"]):
            start_i = 0
            count = 1000
            market_map_short_names = {
                _m_i["market"]: _m_s for _m_s, _m_i in self.market_maps.items()
            }
            while True:
                instruments = client.get_instrument_info(start_i, count)
                for _i in instruments:
                    if (
                        _i["category"] != 4
                        or _i["market"] not in market_map_short_names.keys()
                    ):
                        continue

                    __all_stocks.append(
                        {
                            "code": f"{market_map_short_names[_i['market']]}.{_i['code']}",
                            "name": _i["name"],
                        }
                    )
                start_i += count
                if len(instruments) < count:
                    break

        self.g_all_stocks = __all_stocks
        # print(f"获取数量：{len(self.g_all_stocks)}")

        return self.g_all_stocks

    def to_tdx_code(self, code):
        """
        转换为 tdx 对应的代码
        """
        code_str = str(code)
        code_infos = code_str.split(".")
        market_info = self.market_maps[code_infos[0]]
        return market_info["market"], code_infos[1]

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
            args["pages"] = 10
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
            "10m": 0,
            "5m": 0,
            "1m": 8,
        }
        market, tdx_code = self.to_tdx_code(code)
        if market is None or start_date is not None or end_date is not None:
            print("不支持的调用参数")
            return None

        _s_time = time.time()
        try:
            client = TdxExHq_API(
                multithread=True, raise_exception=True, auto_retry=True
            )
            with client.connect(self.connect_info["ip"], self.connect_info["port"]):
                klines: pd.DataFrame = self.fdb.get_tdx_klines(
                    Market.FX.value, code, frequency
                )
                if klines is None:
                    # 获取 8*700 = 5600 条数据
                    klines = pd.concat(
                        [
                            client.to_df(
                                client.get_instrument_bars(
                                    frequency_map[frequency],
                                    market,
                                    tdx_code,
                                    (i - 1) * 700,
                                    700,
                                )
                            )
                            for i in range(1, args["pages"] + 1)
                        ],
                        axis=0,
                        sort=False,
                    )
                    if len(klines) == 0:
                        return pd.DataFrame([])
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
                                (i - 1) * 700,
                                700,
                            )
                        )
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
            self.fdb.save_tdx_klines(Market.FX.value, code, frequency, klines)

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
        获取标的名称
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
        client = TdxExHq_API(multithread=True, raise_exception=True, auto_retry=True)
        with client.connect(self.connect_info["ip"], self.connect_info["port"]):
            for _code in codes:
                _market, _tdx_code = self.to_tdx_code(_code)
                if _market is None:
                    continue
                _quote = client.get_instrument_quote(_market, _tdx_code)
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
    stocks = ex.all_stocks()
    print(len(stocks))
    print(stocks)
    # print(ex.market_maps)

    klines = ex.klines("FX.GBPEUR", "1m", args={"pages": 10})
    print(len(klines))
    print(klines)
