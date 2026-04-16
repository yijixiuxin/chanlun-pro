import datetime
import traceback
from typing import Dict, List, Union

import akshare as ak
import pandas as pd
import pytz
from pytdx.errors import TdxConnectionError
from pytdx.exhq import TdxExHq_API
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random

from chanlun import fun
from chanlun.base import Market
from chanlun.config import get_data_path
from chanlun.db import db
from chanlun.exchange.exchange import Exchange, Tick, convert_us_tdx_kline_frequency
from chanlun.file_db import FileCacheDB
from chanlun.tools import tdx_best_ip as best_ip


@fun.singleton
class ExchangeTDXUS(Exchange):
    """
    通达信香港行情接口
    """

    g_all_stocks = []

    def __init__(self):
        # super().__init__()

        try:
            # 选择最优的服务器，并保存到 cache 中
            self.connect_info = db.cache_get("tdxex_connect_ip")
            if self.connect_info is None:
                self.connect_info = self.reset_tdx_ip()
                # print(f"最优服务器：{self.connect_info}")
        except Exception:
            print(traceback.format_exc())
            print("通达信 美股行情接口初始化失败，美股行情不可用")

        # 设置时区
        self.tz = pytz.timezone("US/Eastern")

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
        return "AAPL"

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
            "2m": "2m",
            "1m": "1m",
        }

    def all_stocks(self):
        """
        使用 通达信的方式获取所有股票代码
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks
        client = TdxExHq_API(raise_exception=True, auto_retry=True)
        __all_stocks = []
        with client.connect(self.connect_info["ip"], self.connect_info["port"]):
            start_i = 0
            count = 1000
            while True:
                instruments = client.get_instrument_info(start_i, count)
                for _i in instruments:
                    if _i["category"] == 13 and _i["market"] == 74:
                        if "+" in _i["code"] or "=" in _i["code"] or "-" in _i["code"]:
                            continue
                        __all_stocks.append(
                            {
                                "code": _i["code"],
                                "name": _i["name"],
                            }
                        )
                start_i += count
                if len(instruments) < count:
                    break

        self.g_all_stocks = __all_stocks
        # print(f"美股共获取数量：{len(self.g_all_stocks)}")
        return self.g_all_stocks

    def to_tdx_code(self, code):
        """
        转换为 tdx 对应的代码
        """
        return 74, code

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
            args["pages"] = 5
        else:
            args["pages"] = int(args["pages"])

        if "fq_type" not in args.keys():
            args["fq_type"] = "qfq"

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
                klines_df: pd.DataFrame = self.fdb.get_tdx_klines(
                    Market.US.value, code, frequency
                )
                if klines_df is None:
                    # 获取 8*700 = 5600 条数据
                    klines_df = pd.concat(
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
                    klines_df.loc[:, "date"] = pd.to_datetime(klines_df["datetime"])
                    klines_df.sort_values("date", inplace=True)
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
                        old_end_dt = klines_df.iloc[-1]["date"]
                        klines_df = pd.concat([klines_df, _ks], ignore_index=True)
                        # 如果请求的第一个时间大于缓存的最后一个时间，退出
                        if old_end_dt >= new_start_dt:
                            break

            klines_df["date"] = pd.to_datetime(klines_df["datetime"])
            # 删除重复数据
            klines_df = klines_df.drop_duplicates(["date"], keep="last").sort_values(
                "date"
            )
            self.fdb.save_tdx_klines(Market.US.value, code, frequency, klines_df)

            klines_df.loc[:, "date"] = klines_df["date"].apply(self._convert_dt)
            klines_df = klines_df.sort_values("date")
            klines_df.loc[:, "code"] = code
            klines_df.loc[:, "volume"] = klines_df["amount"]

            klines_df = klines_df[
                ["code", "date", "open", "close", "high", "low", "volume"]
            ]

            if frequency in ["10m", "2m"]:
                klines_df = convert_us_tdx_kline_frequency(klines_df, frequency)

            if args["fq_type"] == "qfq":
                return self.klines_qfq(code, klines_df)
            else:
                return klines_df
        except TdxConnectionError:
            self.reset_tdx_ip()
        except Exception as e:
            print(f"获取行情异常 {code} - {frequency} Exception ：{str(e)}")
            traceback.print_exc()

        return None

    def _convert_dt(self, _dt: datetime.datetime):
        """
        将通达信的中国时间，转换成美国东部时间
        """
        if _dt.hour == 15 and _dt.minute == 0:
            # 这个是日线及以上周期的数据
            _dt = _dt.replace(hour=16, minute=0, tzinfo=self.tz)
            return _dt

        _dt = _dt.replace(tzinfo=pytz.timezone("Asia/Shanghai"))

        if _dt.hour in [0, 1, 2, 3, 4, 5]:
            _dt = _dt + datetime.timedelta(days=1)
        return _dt.astimezone(self.tz)

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
                # OrderedDict(
                #     [('market', 1), ('code', '00700'), ('pre_close', 362.8000183105469), ('open', 372.20001220703125),
                #      ('high', 374.8000183105469), ('low', 364.4000244140625), ('price', 367.6000061035156),
                #      ('kaicang', 0), ('zongliang', 17784504), ('xianliang', 1189500), ('neipan', 8892299),
                #      ('waipan', 8892205), ('chicang', 0), ('bid1', 0.0), ('bid2', 0.0), ('bid3', 0.0), ('bid4', 0.0),
                #      ('bid5', 0.0), ('bid_vol1', 0), ('bid_vol2', 0), ('bid_vol3', 0), ('bid_vol4', 0), ('bid_vol5', 0),
                #      ('ask1', 0.0), ('ask2', 0.0), ('ask3', 0.0), ('ask4', 0.0), ('ask5', 0.0), ('ask_vol1', 0),
                #      ('ask_vol2', 0), ('ask_vol3', 0), ('ask_vol4', 0), ('ask_vol5', 0)])
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
                            if _quote["pre_close"] > 0 and _quote["price"] > 0
                            else 0
                        ),
                    )
        return ticks

    def now_trading(self):
        """
        TODO 暂时还没有找到接口，直接硬编码
        周一致周五，美国东部时间，9:30 - 16:00
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

    def klines_qfq(self, code: str, klines: pd.DataFrame):
        try:
            xdxr_path = get_data_path() / "xdxr"
            if xdxr_path.is_dir() is False:
                xdxr_path.mkdir()
            xdxr_file = xdxr_path / f"us_qfq_factor_{code}.csv"
            now_day = fun.datetime_to_str(datetime.datetime.now(), "%Y-%m-%d")
            if (
                xdxr_file.is_file() is False
                or fun.timeint_to_str(int(xdxr_file.stat().st_mtime), "%Y-%m-%d")
                != now_day
            ):
                qfq_factor_df = ak.stock_us_daily(symbol=code, adjust="qfq-factor")
                if qfq_factor_df is not None and len(qfq_factor_df) > 0:
                    qfq_factor_df.to_csv(xdxr_file, index=False)
            else:
                qfq_factor_df = pd.read_csv(xdxr_file)

            if qfq_factor_df is None or len(qfq_factor_df) == 0:
                return klines

            qfq_factor_df["qfq_date"] = pd.to_datetime(
                qfq_factor_df["date"]
            ).dt.tz_localize(self.tz)
            qfq_factor_df["qfq_factor"] = qfq_factor_df["qfq_factor"].astype(float)
            qfq_factor_df = qfq_factor_df.drop(columns=["date", "adjust"])

            # 合并k线与复权因子，进行复权计算
            df = pd.concat([klines, qfq_factor_df], axis=0)
            df["qfq_date"].fillna(df["date"], inplace=True)
            df.sort_values(by="qfq_date", inplace=True)
            df["qfq_factor"].fillna(method="ffill", inplace=True)
            df.dropna(inplace=True)
            df.reset_index(drop=True, inplace=True)

            df["open"] = df["open"] * df["qfq_factor"]
            df["high"] = df["high"] * df["qfq_factor"]
            df["low"] = df["low"] * df["qfq_factor"]
            df["close"] = df["close"] * df["qfq_factor"]
            return df[["code", "date", "open", "high", "low", "close", "volume"]]
        except Exception as e:
            print(f"计算 {code} 复权数据异常：{e}")
            return klines

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
    ex = ExchangeTDXUS()
    # stocks = ex.all_stocks()
    # print(len(stocks))
    # not_stocks = []
    # for s in stocks:
    #     if "做多" in s["name"]:
    #         print(s)
    #         not_stocks.append(s)
    #     if "ETF" in s["name"]:
    #         print(s)
    #         not_stocks.append(s)
    #     if "指数" in s["name"]:
    #         print(s)
    #         not_stocks.append(s)
    #     if "期货" in s["name"]:
    #         print(s)
    #         not_stocks.append(s)
    #     if "基金" in s["name"]:
    #         print(s)
    #         not_stocks.append(s)
    #     if "组合" in s["name"]:
    #         print(s)
    #         not_stocks.append(s)
    # print(not_stocks)
    # print(len(not_stocks))
    # print(stocks)
    #
    #
    # klines = ex.klines(ex.default_code(), "d")
    # print(klines)
    klines = ex.klines("AAPL", "30m")
    print(klines.tail(20))

    # ticks = ex.ticks([ex.default_code()])
    # print(ticks)
