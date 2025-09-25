import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Union

import pandas as pd
import pytz
from chanlun.base import Market

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
        "1m": "1min",
        "2m": "2min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "d": "D",
        "w": "W",
        "m": "M",
    }
    code = klines["code"].iloc[0]

    if to_f in period_maps.keys():
        klines.insert(0, column="date_index", value=klines["date"])
        klines.set_index("date_index", inplace=True)
        period_type = period_maps[to_f]

        agg_dict = {
            "open": "first",
            "close": "last",
            "high": "max",
            "low": "min",
            "volume": "sum",
        }

        # 通达信的时间对其方式，日线及以下是后对其，周与月是前对其（周、月的第一个交易日）
        if to_f in ["w", "m"]:
            agg_dict["date"] = "first"
        else:
            agg_dict["date"] = "last"

        period_klines = klines.resample(period_type, label="left", closed="right").agg(
            agg_dict
        )
        period_klines["code"] = code
        period_klines["frequency"] = to_f

        # 后对其的，最后一个k线的时间不是未来的结束时间，需要特殊处理一下
        # 周期是 d、w、m，将时间设置为 15点收盘时间
        if to_f in ["d", "w", "m"]:
            period_klines["date"] = pd.to_datetime(
                {
                    "year": period_klines["date"].dt.year,
                    "month": period_klines["date"].dt.month,
                    "day": period_klines["date"].dt.day,
                }
            ) + pd.Timedelta(hours=15)
            period_klines["date"] = pd.to_datetime(
                period_klines["date"]
            ).dt.tz_localize(__tz)

        if to_f in ["2m", "5m", "10m", "15m", "30m"]:
            period_klines.loc[:, "date"] = period_klines.index
            period_klines["date"] = period_klines["date"] + pd.to_timedelta(
                period_maps[to_f]
            )

        period_klines.dropna(inplace=True)
        period_klines.reset_index(inplace=True)

        return period_klines[
            ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
        ]

    # 60m 周期特殊，9:30-10:30/10:30-11:30
    freq_config_maps = {
        "60m": {
            "10:30:00": ["09:00:00+08:00", "10:30:00+08:00"],
            "11:30:00": ["10:31:00+08:00", "11:30:00+08:00"],
            "14:00:00": ["13:00:00+08:00", "14:00:00+08:00"],
            "15:00:00": ["14:01:00+08:00", "15:00:00+08:00"],
        },
        "120m": {
            "11:30:00": ["09:00:00+08:00", "11:30:00+08:00"],
            "15:00:00": ["13:00:00+08:00", "15:00:00+08:00"],
        },
    }
    if to_f not in freq_config_maps.keys():
        raise Exception(f"不支持的转换周期：{to_f}")

    klines["new_dt"] = pd.NaT
    date_only = klines["date"].dt.date.astype(str)
    for new_time_str, range_time_str in freq_config_maps[to_f].items():
        start_time_str, end_time_str = range_time_str
        range_start_dt = pd.to_datetime(date_only + " " + start_time_str)
        range_end_dt = pd.to_datetime(date_only + " " + end_time_str)
        target_new_dt = pd.to_datetime(date_only + " " + new_time_str)

        mask = (klines["date"] >= range_start_dt) & (klines["date"] <= range_end_dt)

        klines.loc[mask, "new_dt"] = target_new_dt

    if klines["new_dt"].isnull().any():
        failed_dates = klines.loc[klines["new_dt"].isnull(), "date"]
        raise Exception(
            f"{code} {to_f} 周期转换时间范围错误，以下时间未能匹配配置： {failed_dates.tolist()}"
        )

    klines_groups = klines.groupby(by=["new_dt"]).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    klines_groups["code"] = code
    klines_groups["frequency"] = to_f
    klines_groups["date"] = klines_groups.index
    # 转换完成后，再将日期转换成本地时间
    klines_groups["date"] = pd.to_datetime(klines_groups["date"]).dt.tz_localize(__tz)

    klines_groups.reset_index(drop=True, inplace=True)

    return klines_groups[
        ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
    ]


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
    }
    if len(klines) == 0:
        return klines

    code = klines.iloc[0]["code"]

    if to_f == "d":
        # 日期的特殊处理
        mask = (klines["date"].dt.time >= pd.to_datetime("08:00:00").time()) | (
            klines["date"].dt.time < pd.to_datetime("08:00:00").time()
        )
        klines = klines.assign(
            trade_day=lambda x: pd.to_datetime(x["date"].dt.date)
            - pd.to_timedelta((x["date"].dt.hour < 8).astype(int), unit="D")
        )
        grouped = klines[mask].groupby("trade_day")
        period_klines = pd.DataFrame(
            {
                "date": grouped["trade_day"]
                .first()
                .apply(lambda x: x.replace(hour=8, minute=0, second=0, tzinfo=__tz)),
                "frequency": to_f,
                "code": grouped["code"].first(),
                "open": grouped["open"].first(),
                "close": grouped["close"].last(),
                "high": grouped["high"].max(),
                "low": grouped["low"].min(),
                "volume": grouped["volume"].sum(),
            }
        )
        period_klines = period_klines.reset_index(drop=True)
        # period_klines["date"] = period_klines["date"].dt.tz_convert(__tz)
        return period_klines[
            ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
        ]

    # 删除 volume 列为 0 的行
    klines = klines[klines["volume"] != 0]

    klines.insert(0, column="date_index", value=klines["date"])
    klines.set_index("date_index", inplace=True)
    period_type = period_maps[to_f]

    agg_dict = {
        "date": "first",
        "open": "first",
        "close": "last",
        "high": "max",
        "low": "min",
        "volume": "sum",
    }

    period_klines = klines.resample(period_type, label="right", closed="left").agg(
        agg_dict
    )

    period_klines.loc[:, "date"] = period_klines.index
    period_klines["date"] = period_klines["date"] - pd.to_timedelta(period_maps[to_f])
    period_klines.loc[:, "code"] = code
    period_klines.loc[:, "frequency"] = to_f

    period_klines.dropna(inplace=True)
    period_klines.reset_index(inplace=True)
    period_klines.drop("date_index", axis=1, inplace=True)

    return period_klines[
        ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
    ]


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
    code = klines.iloc[0]["code"]

    if to_f in period_maps.keys():
        klines.insert(0, column="date_index", value=klines["date"])
        klines.set_index("date_index", inplace=True)
        period_type = period_maps[to_f]
        agg_dict = {
            "code": "first",
            "date": "first",
            "open": "first",
            "close": "last",
            "high": "max",
            "low": "min",
            "volume": "sum",
        }
        if "position" in klines.columns:
            agg_dict["position"] = "last"
        period_klines = klines.resample(period_type, label="right", closed="left").agg(
            agg_dict
        )

        if to_f in ["1m", "3m", "5m", "6m", "10m", "15m"]:
            period_klines["date"] = period_klines.index
            period_klines["date"] = period_klines["date"] - pd.to_timedelta(
                period_maps[to_f]
            )

        period_klines.dropna(inplace=True)
        period_klines.reset_index(inplace=True)
        period_klines.drop("date_index", axis=1, inplace=True)
        return period_klines[["code", "date", "open", "close", "high", "low", "volume"]]

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
                "22:00:00": ["22:00:00", "22:29:59"],
                "22:30:00": ["22:30:00", "22:59:59"],
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
                "14:00:00": ["14:00:00", "15:00:00"],
                "21:00:00": ["21:00:00", "21:59:59"],
                "22:00:00": ["22:00:00", "22:59:59"],
                "23:00:00": ["23:00:00", "23:59:59"],
                "00:00:00": ["00:00:00", "00:59:59"],
                "01:00:00": ["01:00:00", "01:59:59"],
                "02:00:00": ["02:00:00", "02:59:59"],
            },
        }

    if to_f not in freq_config_maps.keys():
        raise Exception(f"不支持的转换周期：{to_f}")

    klines["new_dt"] = pd.Series(dtype="datetime64[ns, Asia/Shanghai]")
    # 如果是 hour 0 minutes 0 的日期，则减一分钟
    mask = (klines["date"].dt.hour == 0) & (klines["date"].dt.minute == 0)
    klines.loc[mask, "date"] = klines.loc[mask, "date"] - pd.Timedelta(minutes=1)

    date_only = klines["date"].dt.normalize()
    for new_time_str, range_time_str in freq_config_maps[to_f].items():
        start_time_str, end_time_str = range_time_str
        start_time = pd.to_timedelta(start_time_str)
        end_time = pd.to_timedelta(end_time_str)

        range_start_dt = date_only + start_time

        if end_time_str == "00:00:00":
            range_end_dt = date_only + pd.Timedelta(days=1)
        else:
            range_end_dt = date_only + end_time
        if end_time_str == "00:00:00":
            target_new_dt = date_only + pd.Timedelta(days=1)
        else:
            target_new_dt = date_only + pd.to_timedelta(new_time_str)

        mask = (klines["date"] >= range_start_dt) & (klines["date"] <= range_end_dt)
        klines.loc[mask, "new_dt"] = target_new_dt

    if klines["new_dt"].isnull().any():
        failed_dates = klines.loc[klines["new_dt"].isnull(), "date"]
        raise Exception(
            f"期货周期转换时间范围错误，{code} - {to_f} 以下时间未能匹配配置： {failed_dates.tolist()}"
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

    return klines_groups[["code", "date", "open", "close", "high", "low", "volume"]]


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
    code = klines.iloc[0]["code"]

    if to_f in period_maps.keys():
        if to_f in ["d", "w"]:
            # 如果是日线，在 21 点之后的，算下一天的
            klines["date"] = klines["date"].apply(
                lambda x: (
                    x + datetime.timedelta(hours=3)
                    if x.hour in [21, 22, 23, 0, 1, 2]
                    else x
                )
            )

        klines.insert(0, column="date_index", value=klines["date"])
        klines.set_index("date_index", inplace=True)
        period_type = period_maps[to_f]
        # 前对其
        agg_dict = {
            "date": "last",
            "open": "first",
            "close": "last",
            "high": "max",
            "low": "min",
            "volume": "sum",
        }
        period_klines = klines.resample(period_type, label="left", closed="right").agg(
            agg_dict
        )
        # 如果是日线的，统一将时间修改为 15:00
        if to_f in ["d", "w"]:
            period_klines["date"] = pd.to_datetime(
                {
                    "year": period_klines["date"].dt.year,
                    "month": period_klines["date"].dt.month,
                    "day": period_klines["date"].dt.day,
                }
            ) + pd.Timedelta(hours=15)
            period_klines["date"] = pd.to_datetime(
                period_klines["date"]
            ).dt.tz_localize(__tz)

        if to_f in ["2m", "5m", "6m", "10m", "15m"]:
            period_klines.loc[:, "date"] = period_klines.index
            period_klines["date"] = period_klines["date"] + pd.to_timedelta(
                period_maps[to_f]
            )
        period_klines = period_klines.dropna().reset_index()

        period_klines["code"] = code
        period_klines["frequency"] = to_f

        return period_klines[
            ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
        ]

    freq_config_maps = {
        "30m": {
            "09:30:00": ["09:00:00", "09:30:00"],
            "10:00:00": ["09:31:00", "10:00:00"],
            "10:45:00": ["10:01:00", "10:45:00"],
            "11:15:00": ["10:46:00", "11:15:00"],
            "13:45:00": ["11:16:00", "13:45:00"],
            "14:15:00": ["13:46:00", "14:15:00"],
            "14:45:00": ["14:16:00", "14:45:00"],
            "15:00:00": ["14:46:00", "15:00:00"],
            "21:30:00": ["21:00:00", "21:30:00"],
            "22:00:00": ["21:31:00", "22:00:00"],
            "22:30:00": ["22:01:00", "22:30:00"],
            "23:00:00": ["22:31:00", "23:00:00"],
            "23:30:00": ["23:01:00", "23:30:00"],
            "00:00:00": ["23:31:00", "00:00:00"],
            "00:30:00": ["00:01:00", "00:30:00"],
            "01:00:00": ["00:31:00", "01:00:00"],
            "01:30:00": ["01:01:00", "01:30:00"],
            "02:00:00": ["01:31:00", "02:00:00"],
            "02:30:00": ["02:01:00", "02:30:00"],
        },
        "60m": {
            "10:00:00": ["09:00:00", "10:00:00"],
            "11:15:00": ["10:01:00", "11:15:00"],
            "14:15:00": ["11:16:00", "14:15:00"],
            "15:00:00": ["14:16:00", "15:00:00"],
            "22:00:00": ["21:00:00", "22:00:00"],
            "23:00:00": ["22:01:00", "23:00:00"],
            "00:00:00": ["23:01:00", "00:00:00"],
            "01:00:00": ["00:01:00", "01:00:00"],
            "02:00:00": ["01:01:00", "02:00:00"],
        },
    }
    # 有夜盘的到 02:30:00 的，60m的处理比较特殊
    if (
        code.startswith("QS.AU")
        or code.startswith("QS.AG")
        or code.startswith("QS.SC")
        or code.startswith("TI.T")
    ):
        freq_config_maps["60m"] = {
            "09:30:00": ["02:01:00", "09:30:00"],
            "10:45:00": ["09:31:00", "10:45:00"],
            "13:45:00": ["10:46:00", "13:45:00"],
            "14:45:00": ["13:46:00", "14:45:00"],
            "15:00:00": ["14:46:00", "15:00:00"],
            "22:00:00": ["21:00:00", "22:00:00"],
            "23:00:00": ["22:01:00", "23:00:00"],
            "00:00:00": ["23:01:00", "00:00:00"],
            "01:00:00": ["00:01:00", "01:00:00"],
            "02:00:00": ["01:01:00", "02:00:00"],
        }

    if (
        code.startswith("CZ.TL")
        or code.startswith("CZ.T")
        or code.startswith("CZ.TF")
        or code.startswith("CZ.TS")
        or code.startswith("CZ.IC")
        or code.startswith("CZ.IH")
        or code.startswith("CZ.IM")
        or code.startswith("CZ.IF")
    ):
        freq_config_maps["30m"] = {
            "10:00:00": ["09:30:00", "10:00:00"],
            "10:30:00": ["10:01:00", "10:30:00"],
            "11:00:00": ["10:31:00", "11:00:00"],
            "11:30:00": ["11:01:00", "11:30:00"],
            "13:30:00": ["13:01:00", "13:30:00"],
            "14:00:00": ["13:31:00", "14:00:00"],
            "14:30:00": ["14:01:00", "14:30:00"],
            "15:00:00": ["14:31:00", "15:00:00"],
            "15:15:00": ["15:01:00", "15:15:00"],
        }
        freq_config_maps["60m"] = {
            "10:30:00": ["09:30:00", "10:30:00"],
            "11:30:00": ["10:31:00", "11:30:00"],
            "14:00:00": ["13:00:00", "14:00:00"],
            "15:00:00": ["14:01:00", "15:00:00"],
            "15:15:00": ["15:01:00", "15:15:00"],
        }

    if to_f not in freq_config_maps.keys():
        raise Exception(f"不支持的转换周期：{to_f}")

    klines["new_dt"] = pd.Series(dtype="datetime64[ns, Asia/Shanghai]")
    # 如果是 hour 0 minutes 0 的日期，则减一分钟
    mask = (klines["date"].dt.hour == 0) & (klines["date"].dt.minute == 0)
    klines.loc[mask, "date"] = klines.loc[mask, "date"] - pd.Timedelta(minutes=1)

    date_only = klines["date"].dt.normalize()
    for new_time_str, range_time_str in freq_config_maps[to_f].items():
        start_time_str, end_time_str = range_time_str
        start_time = pd.to_timedelta(start_time_str)
        end_time = pd.to_timedelta(end_time_str)

        range_start_dt = date_only + start_time

        if end_time_str == "00:00:00":
            range_end_dt = date_only + pd.Timedelta(days=1)
        else:
            range_end_dt = date_only + end_time
        if end_time_str == "00:00:00":
            target_new_dt = date_only + pd.Timedelta(days=1)
        else:
            target_new_dt = date_only + pd.to_timedelta(new_time_str)

        mask = (klines["date"] >= range_start_dt) & (klines["date"] <= range_end_dt)
        klines.loc[mask, "new_dt"] = target_new_dt

    if klines["new_dt"].isnull().any():
        failed_dates = klines.loc[klines["new_dt"].isnull(), "date"]
        raise Exception(
            f"期货周期转换时间范围错误，{code} - {to_f} 以下时间未能匹配配置： {failed_dates.tolist()}"
        )

    agg_config = {
        "code": "first",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    klines_groups = klines.groupby(by=["new_dt"]).agg(agg_config)
    klines_groups["date"] = klines_groups.index
    klines_groups["frequency"] = to_f
    klines_groups.reset_index(drop=True, inplace=True)

    return klines_groups[
        ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
    ]


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
    agg_dict = {
        "code": "first",
        "date": "first",
        "open": "first",
        "close": "last",
        "high": "max",
        "low": "min",
        "volume": "sum",
    }

    if to_f in ["w"]:  # 周线是末尾的时间
        agg_dict["date"] = "last"

    period_klines = klines.resample(period_type, label="right", closed="left").agg(
        agg_dict
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
    agg_dict = {
        "code": "first",
        "date": "last",
        "open": "first",
        "close": "last",
        "high": "max",
        "low": "min",
        "volume": "sum",
    }
    period_klines = klines.resample(period_type, label="left", closed="right").agg(
        agg_dict
    )

    period_klines.dropna(inplace=True)
    period_klines.reset_index(inplace=True)
    period_klines.drop("date_index", axis=1, inplace=True)

    # period_klines["date"] = period_klines["date"].apply(
    #     lambda dt: dt.astimezone(pytz.timezone("US/Eastern"))
    # )

    return period_klines[["code", "date", "open", "high", "low", "close", "volume"]]


def get_ny_future_trade_day(dt: pd.Timestamp) -> pd.Timestamp:
    if dt.hour < 6:
        trade_day = (dt - pd.Timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:
        trade_day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return trade_day


def get_ny_future_trade_week(dt: pd.Timestamp) -> pd.Timestamp:
    trade_day = get_ny_future_trade_day(dt)
    week_start = trade_day - pd.Timedelta(days=trade_day.weekday())
    return week_start


def convert_tdx_ny_f_kline_frequency(klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
    """
    纽约期货的k线转换
    交易时间：周一到周五 早上6点，到第二天早上5点
    :param klines:
    :param to_f:
    :return:
    """

    period_maps = {
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "60m": "60min",
        "d": "D",
        "w": "W",
    }

    code = klines["code"].iloc[0]

    # 删除 volume == 0 的列
    klines = klines[klines["volume"] != 0]

    if to_f == "d":
        klines = klines.copy()
        klines["trade_day"] = klines["date"].apply(get_ny_future_trade_day)
        grouped = klines.groupby("trade_day")
        period_klines = pd.DataFrame(
            {
                "date": grouped["trade_day"]
                .first()
                .apply(lambda x: x.replace(hour=15, minute=0, second=0)),
                "frequency": to_f,
                "code": grouped["code"].first(),
                "open": grouped["open"].first(),
                "close": grouped["close"].last(),
                "high": grouped["high"].max(),
                "low": grouped["low"].min(),
                "volume": grouped["volume"].sum(),
            }
        )
        period_klines = period_klines.reset_index(drop=True)
        return period_klines[
            ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
        ]
    elif to_f == "w":
        klines = klines.copy()
        klines["trade_week"] = klines["date"].apply(get_ny_future_trade_week)
        grouped = klines.groupby("trade_week")
        period_klines = pd.DataFrame(
            {
                "date": grouped["date"]
                .max()
                .apply(lambda x: x.replace(hour=15, minute=0, second=0)),
                "frequency": to_f,
                "code": grouped["code"].first(),
                "open": grouped["open"].first(),
                "close": grouped["close"].last(),
                "high": grouped["high"].max(),
                "low": grouped["low"].min(),
                "volume": grouped["volume"].sum(),
            }
        )
        period_klines = period_klines.reset_index(drop=True)
        return period_klines[
            ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
        ]
    else:
        klines = klines.copy()
        klines.insert(0, column="date_index", value=klines["date"])
        klines.set_index("date_index", inplace=True)
        period_type = period_maps[to_f]

        agg_dict = {
            "date": "last",
            "open": "first",
            "close": "last",
            "high": "max",
            "low": "min",
            "volume": "sum",
        }
        period_klines = klines.resample(period_type, label="left", closed="right").agg(
            agg_dict
        )
        period_klines["code"] = code
        period_klines["frequency"] = to_f

        period_klines.dropna(inplace=True)
        if to_f in ["5m", "15m", "30m", "60m"]:
            period_klines.loc[:, "date"] = period_klines.index
            period_klines["date"] = period_klines["date"] + pd.to_timedelta(
                period_maps[to_f]
            )

        period_klines.reset_index(inplace=True)
        return period_klines[
            ["date", "frequency", "code", "high", "low", "open", "close", "volume"]
        ]


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

    agg_dict = {
        "code": "first",
        "open": "first",
        "close": "last",
        "high": "max",
        "low": "min",
        "volume": "sum",
    }
    if "position" in klines.columns:
        agg_dict["position"] = "last"

    if dt_align_type == "bob":
        agg_dict["date"] = "first"
        period_klines = klines.resample(period_type, label="right", closed="left").agg(
            agg_dict
        )
    else:
        agg_dict["date"] = "last"
        period_klines = klines.resample(period_type, label="left", closed="right").agg(
            agg_dict
        )

    period_klines.dropna(inplace=True)
    period_klines.reset_index(inplace=True)
    period_klines.drop("date_index", axis=1, inplace=True)

    return period_klines


if __name__ == "__main__":
    import pandas as pd

    from chanlun.exchange.exchange_db import ExchangeDB
    from chanlun.exchange.exchange_tq import ExchangeTq

    ex = ExchangeTq()
    code = ex.default_code()
    to_f = "15m"

    klines_1m = ex.klines(code, "1m")

    print(f"1分钟k线数据：{len(klines_1m)}")
    print(klines_1m.tail(10))

    src_klines_to_f = ex.klines(code, to_f)
    print(f"原始 {to_f} 数据")
    print(src_klines_to_f.tail(10))

    convert_ch = convert_futures_kline_frequency(
        klines_1m, to_f, process_exchange_type="tq"
    )
    print(f"转换后的 {to_f} 数据")
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
