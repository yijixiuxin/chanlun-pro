# 回放行情所需
import datetime
import hashlib
import json
import time
from typing import Dict, List, Union

import pandas as pd
import pytz
from tqdm.auto import tqdm

from chanlun import cl, fun
from chanlun.backtesting.base import MarketDatas
from chanlun.cl_interface import ICL
from chanlun.exchange.exchange_db import ExchangeDB


class BackTestKlines(MarketDatas):
    """
    数据库行情回放
    """

    def __init__(
        self,
        market: str,
        start_date: str,
        end_date: str,
        frequencys: List[str],
        cl_config=None,
    ):
        """
        配置初始化
        :param market: 市场 支持 a hk currency
        :param frequencys:
        :param start_date:
        :param end_date:
        """
        super().__init__(market, frequencys, cl_config)

        self.tz = pytz.timezone("Asia/Shanghai")
        if market == "us":
            self.tz = pytz.timezone("US/Eastern")

        self.market = market
        self.base_code = None
        self.frequencys = frequencys
        self.cl_config = cl_config
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(
                start_date, "%Y-%m-%d %H:%M:%S"
            ).astimezone(self.tz)
        self.start_date = start_date
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(
                end_date, "%Y-%m-%d %H:%M:%S"
            ).astimezone(self.tz)
        self.end_date = end_date

        self.now_date: datetime.datetime = start_date

        # 是否使用 cache 保存所有k线数据，True 会将代码周期时间段内所有数据读取并保存到内存，False 在每次使用的时候从数据库中获取
        # True 在多代码时会占用太多内存，这时可以设置为 False 增加使用数据库按需获取，增加运行时间，减少占用内存空间
        self.load_data_to_cache = True
        self.load_kline_nums = 10000  # 每次重新加载的K线数量
        self.cl_data_kline_max_nums = 50000  # 缠论数据中最大保存的k线数量

        # 是否使用 cache 保存所有k线数据，True 会将代码周期时间段内所有数据读取并保存到内存，False 在每次使用的时候从数据库中获取
        # True 在多代码时会占用太多内存，这时可以设置为 False 增加使用数据库按需获取，增加运行时间，减少占用内存空间
        self.load_data_to_cache = True
        self.load_kline_nums = 10000  # 每次重新加载的K线数量
        self.cl_data_kline_max_nums = 50000  # 缠论数据中最大保存的k线数量
        self.del_volume_zero = False  # 是否删除成交量为 0 的K线数据

        # 保存k线数据
        self.all_klines: Dict[str, pd.DataFrame] = {}

        # 每个周期缓存的k线数据，避免多次请求重复计算
        self.cache_klines: Dict[str, Dict[str, pd.DataFrame]] = {}

        self.ex = ExchangeDB(self.market)

        # 用于循环的日期列表
        self.loop_datetime_list: Dict[str, list] = {}

        # 进度条
        self.bar: Union[tqdm, None] = None

        self.time_fmt = "%Y-%m-%d %H:%M:%S"

        # 统计时间
        self._use_times = {
            "klines": 0,
            "convert_klines": 0,
            "get_cl_data": 0,
            "query_db_klines": 0,
        }

    def init(self, base_code: str, frequency: Union[str, list]):
        # 初始化，获取循环的日期列表
        self.base_code = base_code
        if frequency is None:
            frequency = [self.frequencys[-1]]
        if isinstance(frequency, str):
            frequency = [frequency]
        for _f in frequency:
            klines = self.ex.klines(
                base_code,
                _f,
                start_date=fun.datetime_to_str(self.start_date),
                end_date=fun.datetime_to_str(self.end_date),
                args={"limit": None},
            )
            if klines is None:
                self.loop_datetime_list[_f] = []
                continue
            self.loop_datetime_list[_f] = list(klines["date"].to_list())
            self.loop_datetime_list[_f].sort()

        self.bar = tqdm(
            total=len(list(self.loop_datetime_list.values())[-1]),
            desc=f"Run {base_code}",
        )

    def clear_all_cache(self):
        """
        清除所有可用缓存，释放内存
        """
        self.cache_klines = {}
        self.all_klines = {}
        self.cache_cl_datas = {}
        self.cl_datas = {}
        return True

    def next(self, frequency: str = ""):
        if frequency == "" or frequency is None:
            frequency = self.frequencys[-1]
        if len(self.loop_datetime_list[frequency]) == 0:
            self.clear_all_cache()
            return False
        self.now_date = self.loop_datetime_list[frequency].pop(0)
        # for _f, loop_dt_list in self.loop_datetime_list.items():
        #     self.loop_datetime_list[_f] = [d for d in loop_dt_list if d >= self.now_date]
        # 清除之前的 cl_datas 、klines 缓存，重新计算
        self.cache_cl_datas = {}
        self.cache_klines = {}
        self.bar.update(1)
        return True

    def last_k_info(self, code) -> dict:
        kline = self.klines(code, self.frequencys[-1])
        return {
            "date": kline.iloc[-1]["date"],
            "open": float(kline.iloc[-1]["open"]),
            "close": float(kline.iloc[-1]["close"]),
            "high": float(kline.iloc[-1]["high"]),
            "low": float(kline.iloc[-1]["low"]),
        }

    def get_cl_data(self, code, frequency, cl_config: dict = None) -> ICL:
        _time = time.time()
        try:
            # 根据回测配置，可自定义不同周期所使用的缠论配置项
            if cl_config is None:
                if code in self.cl_config.keys():
                    cl_config = self.cl_config[code]
                elif frequency in self.cl_config.keys():
                    cl_config = self.cl_config[frequency]
                elif "default" in self.cl_config.keys():
                    cl_config = self.cl_config["default"]
                else:
                    cl_config = self.cl_config

            # 将配置项md5哈希，并加入到 key 中，这样可以保存并获取多个缠论配置项的数据
            cl_config_key = json.dumps(cl_config)
            cl_config_key = hashlib.md5(
                cl_config_key.encode(encoding="UTF-8")
            ).hexdigest()

            key = "%s_%s_%s" % (code, frequency, cl_config_key)
            if key in self.cache_cl_datas.keys():
                return self.cache_cl_datas[key]

            if key not in self.cl_datas.keys():
                # 第一次进行计算
                klines = self.klines(code, frequency)
                self.cl_datas[key] = cl.CL(code, frequency, cl_config).process_klines(
                    klines
                )
            else:
                # 更新计算
                cd = self.cl_datas[key]

                # 节省内存，最多存 n k线数据，超过就清空重新计算，必须要大于每次K线获取的数量
                if (
                    self.cl_data_kline_max_nums is not None
                    and len(cd.get_klines()) >= self.cl_data_kline_max_nums
                ):
                    self.cl_datas[key] = cl.CL(code, frequency, cl_config)
                    cd = self.cl_datas[key]

                klines = self.klines(code, frequency)

                if len(klines) > 0:
                    if len(cd.get_klines()) == 0:
                        self.cl_datas[key].process_klines(klines)
                    else:
                        # 判断是追加更新还是从新计算
                        cl_end_time = cd.get_klines()[-1].date
                        kline_start_time = klines.iloc[0]["date"]
                        if cl_end_time > kline_start_time:
                            self.cl_datas[key].process_klines(klines)
                        else:
                            self.cl_datas[key] = cl.CL(
                                code, frequency, cl_config
                            ).process_klines(klines)

            # 回测单次循环周期内，计算过后进行缓存，避免多次计算
            self.cache_cl_datas[key] = self.cl_datas[key]

            if (
                len(klines) > 0
                and self.cl_datas[key].get_src_klines()[-1].date
                != klines.iloc[-1]["date"]
            ):
                raise RuntimeError(
                    f'{code} 计算缠论数据异常，缠论数据最后时间与给定的K线最后时间不一致 【缠论:{self.cl_datas[key].get_src_klines()[-1].date}】 Kline: {klines.iloc[-1]["date"]}'
                )

            return self.cache_cl_datas[key]
        finally:
            self._use_times["get_cl_data"] += time.time() - _time

    def klines(self, code, frequency) -> pd.DataFrame:
        if (
            code in self.cache_klines.keys()
            and len(self.cache_klines[code][frequency]) > 0
        ):
            # 直接从缓存中读取
            return self.cache_klines[code][frequency]

        _time = time.time()
        klines = {}
        if self.load_data_to_cache:
            # 使用缓存
            for _f in self.frequencys:
                key = "%s-%s" % (code, _f)
                if key not in self.all_klines.keys():
                    # 从数据库获取日期区间的所有行情
                    all_klines = self.ex.klines(
                        code,
                        _f,
                        start_date=self._cal_start_date_by_frequency(
                            self.start_date, _f
                        ),
                        end_date=fun.datetime_to_str(self.end_date),
                        args={"limit": None},
                    )
                    self.all_klines[key] = all_klines.sort_values("date").reset_index(
                        drop=True
                    )

            for _f in self.frequencys:
                key = "%s-%s" % (code, _f)
                if self.market in [
                    "currency",
                    "futures",
                    "us",
                ]:  # 后对其的，不能包含当前日期
                    kline = self.all_klines[key][
                        self.all_klines[key]["date"] < self.now_date
                    ][-self.load_kline_nums : :]
                else:
                    kline = self.all_klines[key][
                        self.all_klines[key]["date"] <= self.now_date
                    ][-self.load_kline_nums : :]
                if self.del_volume_zero and len(kline) > 0:
                    kline = kline[kline["volume"] != 0]
                kline = kline.sort_values("date").reset_index(drop=True)
                klines[_f] = kline
        else:
            # 使用数据库按需查询
            for _f in self.frequencys:
                klines[_f] = self.ex.klines(
                    code,
                    _f,
                    end_date=fun.datetime_to_str(self.now_date),
                    args={"limit": 10000},
                )
                if self.del_volume_zero and len(klines[_f]) > 0:
                    klines[_f] = klines[_f][klines[_f]["volume"] != 0]
                klines[_f].sort_values("date", inplace=True)

        self._use_times["klines"] += time.time() - _time

        # 转换周期k线，去除未来数据
        klines = self.convert_klines(code, klines)

        # 判断所有周期的收盘价是否一致，如果不一致说明不同周期的数据不一致
        close_price = None
        for _f, _ks in klines.items():
            if len(_ks) > 0:
                if close_price is None:
                    close_price = _ks.iloc[-1]["close"]
                if close_price != _ks.iloc[-1]["close"]:
                    raise RuntimeWarning(
                        f"{code} 获取K线异常，{_f} 周期的收盘价与其他周期数据不同"
                    )

        # 将结果保存到 缓存中，避免重复读取
        self.cache_klines[code] = klines
        return klines[frequency]

    def convert_klines(self, code: str, klines: Dict[str, pd.DataFrame]):
        """
        转换 kline，去除未来的 kline数据
        :return:
        """
        _time = time.time()
        for i in range(len(self.frequencys), 1, -1):
            min_f = self.frequencys[i - 1]
            max_f = self.frequencys[i - 2]
            new_kline = self.ex.convert_kline_frequency(klines[min_f][-120::], max_f)
            if new_kline is None:
                continue
            new_kline = new_kline.iloc[1::]
            if len(klines[max_f]) > 0 and len(new_kline) > 0:
                # 先删除下大周期的最后一行数据，用合并后的数据代替
                klines[max_f] = klines[max_f].drop(klines[max_f].index[-1])

                klines[max_f] = (
                    pd.concat([klines[max_f], new_kline], ignore_index=True)
                    .drop_duplicates(subset=["date"], keep="last")
                    .sort_values("date")
                    .reset_index(drop=True)
                )

        # 检测在数据列中，是否有大于最后一个时间的行
        for _f, _k_pd in klines.items():
            if len(_k_pd) > 0:
                _last_dt = _k_pd.iloc[-1]["date"]
                if len(_k_pd[_k_pd["date"] > _last_dt]) > 0:
                    raise Exception(
                        f"{code} K线数据异常，有大于最后时间的数据存在 {_last_dt}"
                    )

        self._use_times["convert_klines"] += time.time() - _time
        return klines

    def _cal_start_date_by_frequency(self, start_date: datetime, frequency) -> str:
        """
        按照周期，计算行情获取的开始时间
        :param start_date :
        :param frequency:
        :return:
        """
        market_days_freq_maps = {
            "a": {
                "w": 10000,
                "d": 7000,
                "120m": 500,
                "4h": 500,
                "60m": 500,
                "30m": 700,
                "15m": 350,
                "5m": 600,
                "1m": 100,
            },
            "hk": {
                "d": 5000,
                "120m": 500,
                "4h": 500,
                "60m": 100,
                "30m": 100,
                "15m": 50,
                "5m": 25,
                "1m": 5,
            },
            "us": {
                "w": 365 * 20,
                "d": 365 * 10,
                "120m": 2000,
                "60m": 1000,
                "30m": 500,
                "15m": 250,
                "10m": 200,
                "5m": 80,
                "1m": 15,
            },
            "currency": {
                "w": 2000,
                "d": 2000,
                "6h": 1000,
                "4h": 1000,
                "3h": 1000,
                "120m": 500,
                "60m": 210,
                "30m": 105,
                "15m": 55,
                "10m": 25,
                "8m": 25,
                "5m": 18,
                "1m": 4,
            },
            "futures": {
                "d": 5000,
                "120m": 500,
                "4h": 500,
                "60m": 500,
                "30m": 500,
                "15m": 480,
                "10m": 240,
                "6m": 300,
                "5m": 300,
                "3m": 150,
                "1m": 60,
            },
        }
        for _freq in [
            "w",
            "d",
            "120m",
            "6h",
            "4h",
            "3h",
            "60m",
            "30m",
            "15m",
            "10m",
            "8m",
            "6m",
            "5m",
            "3m",
            "1m",
        ]:
            if _freq == frequency:
                return (
                    start_date
                    - datetime.timedelta(days=market_days_freq_maps[self.market][_freq])
                ).strftime(self.time_fmt)
        raise Exception(f"不支持的周期 {frequency}")


if __name__ == "__main__":
    from chanlun.cl_utils import klines_to_heikin_ashi_klines

    market = "a"
    start = "2015-01-01 00:00:00"
    end = "2024-05-01 00:00:00"
    code = "SH.000001"
    frequencys = ["w", "d", "30m"]
    cl_config = {}
    bkt = BackTestKlines(market, start, end, frequencys, cl_config)
    bkt.init(code, frequencys[-1])

    s_time = time.time()
    while bkt.next():
        k = bkt.klines(code, "d")
        ks = klines_to_heikin_ashi_klines(k.iloc[-1000::])

        # print(
        #     f"{code} - {f} : kline last date : {k.iloc[-1]['date']} close: {k.iloc[-1]['close']}"
        # )
    print(f"总耗时：{time.time() - s_time}")
