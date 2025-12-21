import datetime
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
from chanlun.exchange.exchange import (
    Exchange,
    Tick,
    convert_tdx_futures_kline_frequency,
)
from chanlun.file_db import FileCacheDB
from chanlun.tools import tdx_best_ip as best_ip


@fun.singleton
class ExchangeTDXFutures(Exchange):
    """
    通达信期货行情接口
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
            # self.connect_info = None
            if self.connect_info is None:
                self.connect_info = self.reset_tdx_ip()
                # print(f"TDXEX 最优服务器：{self.connect_info}")

            # 初始化，映射交易所代码
            self.market_maps = {}
            while True:
                try:
                    client = TdxExHq_API(raise_exception=True, auto_retry=True)
                    with client.connect(
                        self.connect_info["ip"], self.connect_info["port"]
                    ):
                        all_markets = client.get_markets()
                        for _m in all_markets:
                            if _m["category"] == 3 and _m["market"] in [
                                23,  # PR 香港金融期货
                                28,  # QZ 郑州商品
                                29,  # QD 大连商品
                                30,  # QS 上海期货
                                42,  # TI 商品指数
                                47,  # CZ 中金所期货
                                66,  # QG 广州期货
                            ]:
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
            print("通达信 期货行情接口初始化失败，期货行情不可用")

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
        return "QS.RBL8"

    def support_frequencys(self):
        return {
            "m": "M",
            "w": "W",
            "d": "D",
            "60m": "60m",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "3m": "3m",
            "2m": "2m",
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
            market_map_short_names = {
                _m_i["market"]: _m_s for _m_s, _m_i in self.market_maps.items()
            }
            while True:
                instruments = client.get_instrument_info(start_i, count)
                for _i in instruments:
                    if (
                        _i["category"] != 3
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
        # print(f"期货获取数量：{len(self.g_all_stocks)}")

        return self.g_all_stocks

    def to_tdx_code(self, code):
        """
        转换为 tdx 对应的代码
        """
        code_infos = code.split(".")
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
            "3m": 8,
            "2m": 8,
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
                klines: pd.DataFrame = self.fdb.get_tdx_klines(
                    Market.FUTURES.value, code, frequency
                )
                if klines is None or len(klines) == 0:
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
                    klines["fix_datetime"] = klines["datetime"].apply(
                        lambda _dt: self.fix_yp_date(code, _dt)
                    )
                    klines.loc[:, "date"] = pd.to_datetime(klines["fix_datetime"])
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
                        _ks["fix_datetime"] = _ks["datetime"].apply(
                            lambda _dt: self.fix_yp_date(code, _dt)
                        )
                        _ks.loc[:, "date"] = pd.to_datetime(_ks["fix_datetime"])
                        _ks.sort_values("date", inplace=True)
                        new_start_dt = _ks.iloc[0]["date"]
                        old_end_dt = klines.iloc[-1]["date"]
                        klines = pd.concat([klines, _ks], ignore_index=True)
                        # 如果请求的第一个时间大于缓存的最后一个时间，退出
                        if old_end_dt >= new_start_dt:
                            break

            # 删除重复数据
            klines = klines.drop_duplicates(["date"], keep="last").sort_values("date")
            self.fdb.save_tdx_klines(
                Market.FUTURES.value, f"v1_{code}", frequency, klines
            )

            klines.loc[:, "code"] = code
            klines.loc[:, "volume"] = klines["trade"]
            klines.loc[:, "date"] = pd.to_datetime(klines["date"]).dt.tz_localize(
                self.tz
            )
            klines.sort_values("date", inplace=True)

            if frequency in {"y", "q", "m", "w", "d"}:
                klines["date"] = klines["date"].apply(self.__convert_date)

            # 将 volume 转换成 float类型
            klines[["volume"]] = klines[["volume"]].astype(float)
            if frequency in ["2m", "3m"]:
                klines = convert_tdx_futures_kline_frequency(klines, frequency)

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

    @staticmethod
    def fix_yp_date(code: str, dt: str):
        """
        修复夜盘的时间，tdx将夜盘的时间归类到了第二天，修复为前一天
        """
        if len(dt) == 19:
            _format = "%Y-%m-%d %H:%M:%S"
        elif len(dt) == 16:
            _format = "%Y-%m-%d %H:%M"
        else:
            _format = "%Y-%m-%d"
        dt = fun.str_to_datetime(dt, _format)

        if code.startswith("PR"):
            # TODO 香港金融期货时间修复
            # 小时数大于等于 17，则减去 1 天
            if dt.hour >= 17:
                dt = dt - datetime.timedelta(days=1)
            return fun.datetime_to_str(dt)

        if dt.hour >= 21:
            dt = dt - datetime.timedelta(days=1)
        return fun.datetime_to_str(dt)

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

    def all_ticks(self) -> Dict[str, Tick]:
        ticks = {}
        client = TdxExHq_API(raise_exception=True, auto_retry=True)
        with client.connect(self.connect_info["ip"], self.connect_info["port"]):
            for _name, _mc in self.market_maps.items():
                _quotes = []
                _req_start = 0
                while True:
                    _qs = client.get_instrument_quote_list(
                        _mc["market"],
                        _mc["category"],
                        start=_req_start,
                        count=_req_start + 80,
                    )
                    _quotes.extend(_qs)
                    _req_start += 80
                    if len(_qs) < 80:
                        break
                for _quote in _quotes:
                    # OrderedDict([('market', 28), ('code', 'MA2509'),
                    # ('BiShu', 10569), ('ZuoJie', 2262.0), ('JinKai', 2260.0), ('ZuiGao', 2266.0), ('ZuiDi', 2242.0), ('MaiChu', 2258.0), ('KaiCang', 262905),
                    # ('ZongLiang', 254179), ('XianLiang', 2), ('ZongJinE', 5730089472.0), ('NeiPan', 128701), ('WaiPan', 125478),
                    # ('ChiCangLiang', 674677), ('MaiRuJia', 2257.0), ('MaiRuLiang', 72), ('MaiChuJia', 2258.0), ('MaiChuLiang', 25)])

                    if _quote["MaiChu"] == 0.0:
                        continue

                    ticks[f"{_name}.{_quote['code']}"] = Tick(
                        code=f"{_name}.{_quote['code']}",
                        last=_quote["MaiChu"],
                        buy1=_quote["MaiRuJia"],
                        sell1=_quote["MaiChuJia"],
                        low=_quote["ZuiDi"],
                        high=_quote["ZuiGao"],
                        volume=_quote["ZongLiang"],
                        open=_quote["JinKai"],
                        rate=(
                            round(
                                (_quote["MaiChu"] - _quote["ZuoJie"])
                                / _quote["MaiChu"]
                                * 100,
                                2,
                            )
                            if _quote["MaiChu"] > 0
                            else 0
                        ),
                    )
        return ticks

    def now_trading(self):
        """
        返回当前是否是交易时间
        TODO 简单判断 ：9-12 , 13:30-15:00 21:00-02:30
        """
        hour = int(time.strftime("%H"))
        minute = int(time.strftime("%M"))
        if (
            hour in {9, 10, 11, 14, 21, 22, 23, 0, 1}
            or (hour == 13 and minute >= 30)
            or (hour == 2 and minute <= 30)
        ):
            return True
        return False

    @staticmethod
    def __convert_date(dt: datetime.datetime):
        # 通达信行情是后对其的，统一将 日以上级别的行情日期转换成 23点
        return dt.replace(hour=23, minute=0)

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
    ex = ExchangeTDXFutures()
    # print(ex.market_maps)
    # stocks = ex.all_stocks()
    # for s in stocks:
    #     if "HTIL8" in s["code"]:
    #         print(s)
    # print(len(stocks))
    # print(ex.market_maps)
    # for s in stocks:
    #     if '原油' in s["name"]:
    #         print(s)

    # print(ex.to_tdx_code('QS.ZN2306'))

    klines = ex.klines("PR.HHIL8", "5m")
    print(klines)
    #
    # for _f in ex.support_frequencys().keys():
    #     klines = ex.klines("QZ.SR2601", _f)
    #     # klines = ex.klines(ex.default_code(), "60m")
    #     print(_f)
    #     print(len(klines))
    #     print(klines.tail(5))

    # ticks = ex.all_ticks()
    # print(len(ticks))
    # print(len(ticks))
