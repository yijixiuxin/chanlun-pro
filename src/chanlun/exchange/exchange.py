import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Union

import pandas as pd
import pytz

from chanlun.fun import (
    datetime_to_int,
    datetime_to_str,
    str_to_datetime,
    str_to_timeint,
    timeint_to_datetime,
)

# 统一时区设置
__tz = pytz.timezone("Asia/Shanghai")


@dataclass
class Tick:
    code: str
    last: float
    buy1: float
    sell1: float
    high: float
    low: float
    open: float
    volume: float
    rate: float = 0


class Exchange(ABC):
    """
    交易所类
    """

    @abstractmethod
    def default_code(self) -> str:
        """
        返回WEB默认展示的代码
        """

    @abstractmethod
    def support_frequencys(self) -> dict:
        """
        返回交易所支持的周期对照关系

        内部使用代码 ： WEB端展示名称
        例如 ：{'d': 'Day'}  # 内部使用的周期是 d，在web端展示  Day
        """

    @abstractmethod
    def all_stocks(self):
        """
        获取支持的所有股票列表
        :return:
        """

    @abstractmethod
    def now_trading(self):
        """
        返回当前是否可交易
        :return bool
        """

    @abstractmethod
    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        """
        获取 Kline 线
        :param code:
        :param frequency:
        :param start_date:
        :param end_date:
        :param args:
        :return:
        """

    @abstractmethod
    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取股票列表的 Tick 信息
        :param codes:
        :return:
        """

    @abstractmethod
    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票的基本信息
        :param code:
        :return:
        """

    @abstractmethod
    def stock_owner_plate(self, code: str):
        """
        股票所属板块信息
        :param code:
        :return:
        return {
            'HY': [{'code': '行业代码', 'name': '行业名称'}],
            'GN': [{'code': '概念代码', 'name': '概念名称'}],
        }
        """

    @abstractmethod
    def plate_stocks(self, code: str):
        """
        获取板块股票列表信息
        :param code: 板块代码
        :return:
        return [{'code': 'SH.000001', 'name': '上证指数'}]
        """

    @abstractmethod
    def balance(self):
        """
        账户资产信息
        :return:
        """

    @abstractmethod
    def positions(self, code: str = ""):
        """
        当前账户持仓信息
        :param code:
        :return:
        """

    @abstractmethod
    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单接口
        :param args:
        :param code:
        :param o_type:
        :param amount:
        :return:
        """


def convert_stock_kline_frequency(klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
    """
    转换股票 k 线到指定的周期
    时间是向后对齐的
    :param klines:
    :param to_f:
    :return:
    """

    # 直接使用 pandas 的 resample 方法进行合并周期
    period_maps = {
        "2m": "2min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "d": "D",
        "w": "W",
        "m": "M",
    }
    if to_f in period_maps.keys():
        klines.insert(0, column="date_index", value=klines["date"])
        klines.set_index("date_index", inplace=True)
        period_type = period_maps[to_f]

        # 通达信的时间对其方式，日线及以下是后对其，周与月是前对其（周、月的第一个交易日）
        if to_f in ["w", "m"]:
            period_klines = klines.resample(
                period_type, label="left", closed="right"
            ).first()
        else:
            period_klines = klines.resample(
                period_type, label="left", closed="right"
            ).last()

        period_klines["open"] = (
            klines["open"].resample(period_type, label="left", closed="right").first()
        )
        period_klines["close"] = (
            klines["close"].resample(period_type, label="left", closed="right").last()
        )
        period_klines["high"] = (
            klines["high"].resample(period_type, label="left", closed="right").max()
        )
        period_klines["low"] = (
            klines["low"].resample(period_type, label="left", closed="right").min()
        )
        period_klines["volume"] = (
            klines["volume"].resample(period_type, label="left", closed="right").sum()
        )
        period_klines.dropna(inplace=True)
        period_klines.reset_index(inplace=True)
        period_klines.drop("date_index", axis=1, inplace=True)
        # 后对其的，最后一个k线的时间不是未来的结束时间，需要特殊处理一下
        # 周期是 d、w、m，将时间设置为 15点收盘时间
        if to_f in ["d", "w", "m"]:
            period_klines["date"] = period_klines["date"].map(
                lambda d: d.replace(hour=15, minute=0)
            )

        if to_f in ["5m", "10m", "15m", "30m"]:

            def lts_time(d: datetime.datetime):
                dt_int = datetime_to_int(d)
                seconds = int(to_f.replace("m", "")) * 60
                if dt_int % seconds == 0:
                    return d
                return timeint_to_datetime(dt_int - (dt_int % seconds) + seconds)

            period_klines["date"] = period_klines["date"].map(lts_time)
            period_klines["date"] = pd.to_datetime(period_klines["date"])
        return period_klines[["code", "date", "open", "close", "high", "low", "volume"]]

    # 60m 周期特殊，9:30-10:30/10:30-11:30
    freq_config_maps = {
        "60m": {
            "10:30:00": ["09:00:00", "10:30:00"],
            "11:30:00": ["10:30:01", "11:30:00"],
            "14:00:00": ["13:00:00", "14:00:00"],
            "15:00:00": ["14:00:01", "15:00:00"],
        },
        "120m": {
            "11:30:00": ["09:00:00", "11:30:00"],
            "15:00:00": ["13:00:00", "15:00:00"],
        },
    }
    if to_f not in freq_config_maps.keys():
        raise Exception(f"不支持的转换周期：{to_f}")

    def dt_to_new_dt(config: dict, dt: datetime.datetime):
        """
        将时间转换成合并后的时间值
        """
        date_str = datetime_to_str(dt, "%Y-%m-%d")
        for new_time, range_time in config.items():
            range_start = str_to_timeint(f"{date_str} {range_time[0]}")
            range_end = str_to_timeint(f"{date_str} {range_time[1]}")
            if range_start <= datetime_to_int(dt) <= range_end:
                return str_to_datetime(f"{date_str} {new_time}")
        return None

    # 按照新的日期进行聚合
    klines["new_dt"] = klines["date"].apply(
        lambda _d: dt_to_new_dt(freq_config_maps[to_f], _d)
    )
    klines_groups = klines.groupby(by=["new_dt"]).agg(
        {
            "code": "first",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    klines_groups["date"] = klines_groups.index
    klines_groups.reset_index(drop=True, inplace=True)

    return klines_groups[["code", "date", "open", "close", "high", "low", "volume"]]


def convert_currency_kline_frequency(klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
    """
    数字货币k线转换方法
    """
    period_maps = {
        "2m": "2min",
        "3m": "3min",
        "8m": "8min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "60m": "1H",
        "120m": "2H",
        "3h": "3H",
        "4h": "4H",
        "6h": "6H",
        "d": "D",
        "w": "W-MON",
        "m": "M",
    }
    if len(klines) == 0:
        return klines

    # 将日期转换成 utc 时间
    utc_tz = pytz.timezone("UTC")
    klines["date"] = pd.to_datetime(klines["date"]).dt.tz_convert(utc_tz)

    klines.insert(0, column="date_index", value=klines["date"])
    klines.set_index("date_index", inplace=True)
    period_type = period_maps[to_f]

    period_klines = klines.resample(period_type, label="right", closed="left").first()
    period_klines["open"] = (
        klines["open"].resample(period_type, label="right", closed="left").first()
    )
    period_klines["close"] = (
        klines["close"].resample(period_type, label="right", closed="left").last()
    )
    period_klines["high"] = (
        klines["high"].resample(period_type, label="right", closed="left").max()
    )
    period_klines["low"] = (
        klines["low"].resample(period_type, label="right", closed="left").min()
    )
    period_klines["volume"] = (
        klines["volume"].resample(period_type, label="right", closed="left").sum()
    )
    period_klines.dropna(inplace=True)
    period_klines.reset_index(inplace=True)
    period_klines.drop("date_index", axis=1, inplace=True)

    # 转换完成后，再将日期转换成本地时间
    period_klines["date"] = period_klines["date"].dt.tz_convert(__tz)

    # if to_f in ["d", "w", "m"]:
    #     # 替换日期中的小时数为 8 点
    #     period_klines["date"] = period_klines["date"].map(
    #         lambda d: d.replace(hour=8, minute=0)
    #     )

    return period_klines[["code", "date", "open", "close", "high", "low", "volume"]]


def convert_futures_kline_frequency(
    klines: pd.DataFrame, to_f: str, process_exchange_type="gm"
) -> pd.DataFrame:
    """
    期货数据 转换 k 线到指定的周期
    期货的K线数据日期是向前看其， 10:00 30分钟线表示的是 10:00 - 10:30 分钟数据
    :param klines:
    :param to_f:
    :return:
    """

    # 直接使用 pandas 的 resample 方法进行合并周期
    period_maps = {
        "1m": "1min",
        "2m": "2min",
        "3m": "3min",
        "5m": "5min",
        "6m": "6min",
        "10m": "10min",
        "15m": "15min",
        "d": "D",
        "w": "W",
        "m": "M",
    }
    if to_f in period_maps.keys():
        klines.insert(0, column="date_index", value=klines["date"])
        klines.set_index("date_index", inplace=True)
        period_type = period_maps[to_f]
        # 前对其
        period_klines = klines.resample(
            period_type, label="right", closed="left"
        ).first()
        period_klines["open"] = (
            klines["open"].resample(period_type, label="right", closed="left").first()
        )
        period_klines["close"] = (
            klines["close"].resample(period_type, label="right", closed="left").last()
        )
        period_klines["high"] = (
            klines["high"].resample(period_type, label="right", closed="left").max()
        )
        period_klines["low"] = (
            klines["low"].resample(period_type, label="right", closed="left").min()
        )
        period_klines["volume"] = (
            klines["volume"].resample(period_type, label="right", closed="left").sum()
        )
        if "position" in klines.columns:
            period_klines["position"] = (
                klines["position"]
                .resample(period_type, label="right", closed="left")
                .last()
            )
        period_klines.dropna(inplace=True)
        period_klines.reset_index(inplace=True)
        period_klines.drop("date_index", axis=1, inplace=True)
        if to_f in ["1m", "3m", "5m", "6m", "10m", "15m"]:

            def bts_time(d: datetime.datetime):
                dt_int = datetime_to_int(d)
                seconds = int(to_f.replace("m", "")) * 60
                if dt_int % seconds == 0:
                    return d
                return timeint_to_datetime(dt_int - (dt_int % seconds))

            period_klines["date"] = period_klines["date"].map(bts_time)
        return period_klines

    # 因为 10:15 10:30 休息 15分钟， 这一部分 掘金和天勤上的处理逻辑是不一样的，在合成 30m，60m 数据时时有差异的
    if process_exchange_type == "gm":
        freq_config_maps = {
            # 掘金的处理逻辑，凑够符合分钟数的数据 (有夜盘交易的还是有差异，暂时不考虑)
            "30m": {
                "09:00:00": ["09:00:00", "09:29:59"],
                "09:30:00": ["09:30:00", "09:59:59"],
                "10:00:00": ["10:00:00", "10:44:59"],
                "10:45:00": ["10:45:00", "11:14:59"],
                "11:15:00": ["11:15:00", "13:44:59"],
                "13:45:00": ["13:45:00", "14:14:59"],
                "14:15:00": ["14:15:00", "14:44:59"],
                "14:45:00": ["14:45:00", "14:59:59"],
                "21:00:00": ["21:00:00", "21:29:59"],
                "21:30:00": ["21:30:00", "21:59:59"],
                "22:00:00": ["21:00:00", "22:29:59"],
                "22:30:00": ["21:30:00", "22:59:59"],
                "23:00:00": ["23:00:00", "23:29:59"],
                "23:30:00": ["23:30:00", "23:59:59"],
                "00:00:00": ["00:00:00", "00:29:59"],
                "00:30:00": ["00:30:00", "00:59:59"],
                "01:00:00": ["01:00:00", "01:29:59"],
                "01:30:00": ["01:30:00", "01:59:59"],
                "02:00:00": ["02:00:00", "02:29:59"],
                "02:30:00": ["02:30:00", "02:59:59"],
            },
            "60m": {
                "09:00:00": ["09:00:00", "09:59:59"],
                "10:00:00": ["10:00:00", "11:14:59"],
                "11:15:00": ["11:15:00", "14:14:59"],
                "14:15:00": ["14:15:00", "14:59:59"],
                "21:00:00": ["21:00:00", "21:59:59"],
                "22:00:00": ["21:00:00", "22:59:59"],
                "23:00:00": ["23:00:00", "23:59:59"],
                "00:00:00": ["00:00:00", "00:59:59"],
                "01:00:00": ["01:00:00", "01:59:59"],
                "02:00:00": ["02:00:00", "02:59:59"],
            },
        }
    else:
        freq_config_maps = {
            # 天勤的处理逻辑，直接按照整数处理，前对其
            "30m": {
                "09:00:00": ["09:00:00", "09:29:59"],
                "09:30:00": ["09:30:00", "09:59:59"],
                "10:00:00": ["10:00:00", "10:29:59"],
                "10:30:00": ["10:30:00", "10:59:59"],
                "11:00:00": ["11:00:00", "11:29:59"],
                "11:30:00": ["11:30:00", "11:59:59"],
                "13:00:00": ["13:00:00", "13:29:59"],
                "13:30:00": ["13:30:00", "13:59:59"],
                "14:00:00": ["14:00:00", "14:29:59"],
                "14:30:00": ["14:30:00", "14:59:59"],
                "21:00:00": ["21:00:00", "21:29:59"],
                "21:30:00": ["21:30:00", "21:59:59"],
                "22:00:00": ["21:00:00", "22:29:59"],
                "22:30:00": ["21:30:00", "22:59:59"],
                "23:00:00": ["23:00:00", "23:29:59"],
                "23:30:00": ["23:30:00", "23:59:59"],
                "00:00:00": ["00:00:00", "00:29:59"],
                "00:30:00": ["00:30:00", "00:59:59"],
                "01:00:00": ["01:00:00", "01:29:59"],
                "01:30:00": ["01:30:00", "01:59:59"],
                "02:00:00": ["02:00:00", "02:29:59"],
                "02:30:00": ["02:30:00", "02:59:59"],
            },
            "60m": {
                "09:00:00": ["09:00:00", "09:59:59"],
                "10:00:00": ["10:00:00", "10:59:59"],
                "11:00:00": ["11:00:00", "11:59:59"],
                "13:00:00": ["13:00:00", "13:59:59"],
                "21:00:00": ["21:00:00", "21:59:59"],
                "22:00:00": ["21:00:00", "22:59:59"],
                "23:00:00": ["23:00:00", "23:59:59"],
                "00:00:00": ["00:00:00", "00:59:59"],
                "01:00:00": ["01:00:00", "01:59:59"],
                "02:00:00": ["02:00:00", "02:59:59"],
            },
        }

    if to_f not in freq_config_maps.keys():
        raise Exception(f"不支持的转换周期：{to_f}")

    def dt_to_new_dt(config: dict, dt: datetime.datetime):
        """
        将时间转换成合并后的时间值
        """
        date_str = datetime_to_str(dt, "%Y-%m-%d")
        for new_time, range_time in config.items():
            range_start = str_to_timeint(f"{date_str} {range_time[0]}")
            range_end = str_to_timeint(f"{date_str} {range_time[1]}")
            if range_start <= datetime_to_int(dt) <= range_end:
                return str_to_datetime(f"{date_str} {new_time}")
        return None

    # 按照新的日期进行聚合
    klines["new_dt"] = klines["date"].apply(
        lambda _d: dt_to_new_dt(freq_config_maps[to_f], _d)
    )
    agg_config = {
        "code": "first",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "position" in klines.columns:
        agg_config["position"] = "last"
    klines_groups = klines.groupby(by=["new_dt"]).agg(agg_config)
    klines_groups["date"] = klines_groups.index
    klines_groups.reset_index(drop=True, inplace=True)

    return klines_groups


def convert_tdx_futures_kline_frequency(
    klines: pd.DataFrame, to_f: str
) -> pd.DataFrame:
    """
    期货数据 转换 k 线到指定的周期
    通达信期货数据格式，时间是向后对其，凑够指定分数
    :param klines:
    :param to_f:
    :return:
    """

    # 直接使用 pandas 的 resample 方法进行合并周期
    period_maps = {
        "2m": "2min",
        "3m": "3min",
        "5m": "5min",
        "6m": "6min",
        "10m": "10min",
        "15m": "15min",
        "d": "D",
        "w": "W",
        "m": "M",
    }
    if to_f in period_maps.keys():
        klines.insert(0, column="date_index", value=klines["date"])
        klines.set_index("date_index", inplace=True)
        period_type = period_maps[to_f]
        # 前对其
        period_klines = klines.resample(
            period_type, label="right", closed="left"
        ).first()
        period_klines["open"] = (
            klines["open"].resample(period_type, label="right", closed="left").first()
        )
        period_klines["close"] = (
            klines["close"].resample(period_type, label="right", closed="left").last()
        )
        period_klines["high"] = (
            klines["high"].resample(period_type, label="right", closed="left").max()
        )
        period_klines["low"] = (
            klines["low"].resample(period_type, label="right", closed="left").min()
        )
        period_klines["volume"] = (
            klines["volume"].resample(period_type, label="right", closed="left").sum()
        )
        if "position" in klines.columns:
            period_klines["position"] = (
                klines["position"]
                .resample(period_type, label="right", closed="left")
                .last()
            )
        period_klines.dropna(inplace=True)
        period_klines.reset_index(inplace=True)
        period_klines.drop("date_index", axis=1, inplace=True)
        if to_f in ["1m", "3m", "5m", "6m", "10m", "15m"]:

            def bts_time(d: datetime.datetime):
                dt_int = datetime_to_int(d)
                seconds = int(to_f.replace("m", "")) * 60
                if dt_int % seconds == 0:
                    return d
                return timeint_to_datetime(dt_int - (dt_int % seconds))

            period_klines["date"] = period_klines["date"].map(bts_time)
        return period_klines

    freq_config_maps = {
        # 掘金的处理逻辑，凑够符合分钟数的数据 (有夜盘交易的还是有差异，暂时不考虑)
        "30m": {
            "09:30:00": ["09:00:00", "09:29:59"],
            "10:00:00": ["09:30:00", "09:59:59"],
            "10:45:00": ["10:00:00", "10:44:59"],
            "11:15:00": ["10:45:00", "11:14:59"],
            "13:45:00": ["11:15:00", "13:44:59"],
            "14:15:00": ["13:45:00", "14:14:59"],
            "14:45:00": ["14:15:00", "14:44:59"],
            "15:00:00": ["14:45:00", "15:00:00"],
            "21:30:00": ["21:00:00", "21:29:59"],
            "22:00:00": ["21:30:00", "21:59:59"],
            "22:30:00": ["22:00:00", "22:29:59"],
            "23:00:00": ["22:30:00", "23:00:00"],
            "23:30:00": ["23:00:00", "23:29:59"],
            "00:00:00": ["23:30:00", "23:59:59"],
            "00:30:00": ["00:00:00", "00:29:59"],
            "01:00:00": ["00:30:00", "00:59:59"],
            "01:30:00": ["01:00:00", "01:29:59"],
            "02:00:00": ["01:30:00", "01:59:59"],
            "02:30:00": ["02:00:00", "02:30:00"],
        },
        "60m": {
            "10:00:00": ["09:00:00", "09:59:59"],
            "11:15:00": ["10:00:00", "11:14:59"],
            "14:15:00": ["11:15:00", "14:14:59"],
            "15:00:00": ["14:15:00", "15:00:00"],
            "22:00:00": ["21:00:00", "21:59:59"],
            "23:00:00": ["22:00:00", "23:00:00"],
            "00:00:00": ["23:00:00", "23:59:59"],
            "01:00:00": ["00:00:00", "00:59:59"],
            "02:00:00": ["01:00:00", "02:00:00"],
        },
    }

    if to_f not in freq_config_maps.keys():
        raise Exception(f"不支持的转换周期：{to_f}")

    def dt_to_new_dt(config: dict, dt: datetime.datetime):
        """
        将时间转换成合并后的时间值
        """
        date_str = datetime_to_str(dt, "%Y-%m-%d")
        for new_time, range_time in config.items():
            range_start = str_to_timeint(f"{date_str} {range_time[0]}")
            range_end = str_to_timeint(f"{date_str} {range_time[1]}")
            if range_start <= datetime_to_int(dt) <= range_end:
                return str_to_datetime(f"{date_str} {new_time}")
        return None

    # 按照新的日期进行聚合
    klines["new_dt"] = klines["date"].apply(
        lambda _d: dt_to_new_dt(freq_config_maps[to_f], _d)
    )
    agg_config = {
        "code": "first",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "position" in klines.columns:
        agg_config["position"] = "last"
    klines_groups = klines.groupby(by=["new_dt"]).agg(agg_config)
    klines_groups["date"] = klines_groups.index
    klines_groups.reset_index(drop=True, inplace=True)

    return klines_groups


def convert_us_kline_frequency(klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
    """
    美股k线转换方法
    基于 IB（盈透证券）行情的K线转换，时间是前对其
    """
    period_maps = {
        "2m": "2min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "60m": "1H",
        "120m": "2H",
        "d": "D",
        "w": "W",
        "m": "M",
    }
    if len(klines) == 0:
        return None
    klines.insert(0, column="date_index", value=klines["date"])
    klines.set_index("date_index", inplace=True)
    period_type = period_maps[to_f]

    if to_f in ["w"]:  # 周线是末尾的时间
        period_klines = klines.resample(
            period_type, label="right", closed="left"
        ).last()
    else:
        period_klines = klines.resample(
            period_type, label="right", closed="left"
        ).first()
    period_klines["open"] = (
        klines["open"].resample(period_type, label="right", closed="left").first()
    )
    period_klines["close"] = (
        klines["close"].resample(period_type, label="right", closed="left").last()
    )
    period_klines["high"] = (
        klines["high"].resample(period_type, label="right", closed="left").max()
    )
    period_klines["low"] = (
        klines["low"].resample(period_type, label="right", closed="left").min()
    )
    period_klines["volume"] = (
        klines["volume"].resample(period_type, label="right", closed="left").sum()
    )
    period_klines.dropna(inplace=True)
    period_klines.reset_index(inplace=True)
    period_klines.drop("date_index", axis=1, inplace=True)

    return period_klines[["code", "date", "open", "close", "high", "low", "volume"]]


def convert_us_tdx_kline_frequency(klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
    """
    美股k线转换方法
    基于 通达信行情的K线转换，时间是后对其
    """
    period_maps = {
        "2m": "2min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "60m": "1H",
        "120m": "2H",
        "d": "D",
        "w": "W",
        "m": "M",
    }
    klines.insert(
        0,
        column="date_index",
        value=pd.to_datetime(klines["date"].apply(lambda dt: dt.astimezone(pytz.UTC))),
    )
    klines.set_index("date_index", inplace=True)
    period_type = period_maps[to_f]

    period_klines = klines.resample(period_type, label="left", closed="right").last()
    period_klines["open"] = (
        klines["open"].resample(period_type, label="left", closed="right").first()
    )
    period_klines["high"] = (
        klines["high"].resample(period_type, label="left", closed="right").max()
    )
    period_klines["low"] = (
        klines["low"].resample(period_type, label="left", closed="right").min()
    )
    period_klines["close"] = (
        klines["close"].resample(period_type, label="left", closed="right").last()
    )
    period_klines["volume"] = (
        klines["volume"].resample(period_type, label="left", closed="right").sum()
    )
    period_klines.dropna(inplace=True)
    period_klines.reset_index(inplace=True)
    period_klines.drop("date_index", axis=1, inplace=True)

    period_klines["date"] = period_klines["date"].apply(
        lambda dt: dt.astimezone(pytz.timezone("US/Eastern"))
    )

    return period_klines[["code", "date", "open", "high", "low", "close", "volume"]]


def convert_kline_frequency(
    klines: pd.DataFrame, to_f: str, dt_align_type: str = "eob"
) -> pd.DataFrame:
    """
    通用的k线转换方法
    可通过 dt_align_type 参数，选择对齐方式，默认为 eob，即对齐结束时间，可选值：
    eob: 对齐结束时间，如 9:29:00 对齐到 9:30:00
    bob: 对齐结束时间，如 9:31:00 对齐到 9:30:00

    dt_align_type 对其方式的选择，根据你原始K线的方式来；
    原始K线是 eob，则转换K线也是 eob
    原始K线是 bob，则转换K线也是 bob
    """
    period_maps = {
        "2m": "2min",
        "3m": "3min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "60m": "1H",
        "120m": "2H",
        "d": "D",
        "w": "W",
    }
    if len(klines) == 0:
        return None
    klines.insert(0, column="date_index", value=klines["date"])
    klines.set_index("date_index", inplace=True)
    period_type = period_maps[to_f]
    if dt_align_type == "bob":
        label = "right"
        closed = "left"
        period_klines = klines.resample(period_type, label=label, closed=closed).first()
    else:
        label = "left"
        closed = "right"
        period_klines = klines.resample(period_type, label=label, closed=closed).last()
    period_klines["open"] = (
        klines["open"].resample(period_type, label=label, closed=closed).first()
    )
    period_klines["close"] = (
        klines["close"].resample(period_type, label=label, closed=closed).last()
    )
    period_klines["high"] = (
        klines["high"].resample(period_type, label=label, closed=closed).max()
    )
    period_klines["low"] = (
        klines["low"].resample(period_type, label=label, closed=closed).min()
    )
    period_klines["volume"] = (
        klines["volume"].resample(period_type, label=label, closed=closed).sum()
    )
    if "position" in klines.columns:
        period_klines["position"] = (
            klines["position"].resample(period_type, label=label, closed=closed).last()
        )
    period_klines.dropna(inplace=True)
    period_klines.reset_index(inplace=True)
    period_klines.drop("date_index", axis=1, inplace=True)

    return period_klines


if __name__ == "__main__":
    import pandas as pd

    from chanlun.exchange.exchange_db import ExchangeDB

    code = "SHFE.RB"

    ex = ExchangeDB("futures")
    klines_l = ex.klines(code, "1m")

    print(klines_l.tail(10))

    convert_ch = convert_kline_frequency(klines_l, "3m", dt_align_type="bob")
    print("转换成高周期的最后10根")
    print(convert_ch.tail(10))
    print("Done")

    # convert_klines_d = convert_us_kline_frequency(klines_30m, 'd')
    # print('klines_d')
    # print(klines_d.tail())
    # print('convert_klines_d')
    # print(convert_klines_d.tail())

    # convert_klines_60m = convert_us_kline_frequency(klines_30m, '60m')
    # print('klines_60m')
    # print(klines_60m.tail())
    # print('convert_klines_60m')
    # print(convert_klines_60m.tail())

    # print('klines_30m')
    # print(klines_30m.tail(30))
    # print(klines_30m.tail(30))
    # print(klines_30m.tail(30))
    # print(klines_30m.tail(30))
