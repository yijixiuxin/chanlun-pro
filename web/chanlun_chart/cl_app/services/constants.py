"""
Shared market and chart constants for cl_app.

This module centralizes static mappings and derived values previously
defined inline in the Flask app. Importing from here reduces duplication
and makes it easier to test and evolve supported markets and resolutions.
"""

from tzlocal import get_localzone

from chanlun.base import Market
from chanlun.exchange import get_exchange


# 项目中的周期与 tv 的周期对应表
frequency_maps = {
    "10s": "10S",
    "30s": "30S",
    "1m": "1",
    "2m": "2",
    "3m": "3",
    "5m": "5",
    "10m": "10",
    "15m": "15",
    "30m": "30",
    "60m": "60",
    "120m": "120",
    "3h": "180",
    "4h": "240",
    "d": "1D",
    "2d": "2D",
    "w": "1W",
    "m": "1M",
    "y": "12M",
}

# tv 的周期与项目中的周期对应表
resolution_maps = dict(zip(frequency_maps.values(), frequency_maps.keys()))


# 各个市场支持的时间周期
market_frequencys = {
    "a": list(get_exchange(Market.A).support_frequencys().keys()),
    "hk": list(get_exchange(Market.HK).support_frequencys().keys()),
    "fx": list(get_exchange(Market.FX).support_frequencys().keys()),
    "us": list(get_exchange(Market.US).support_frequencys().keys()),
    "futures": list(get_exchange(Market.FUTURES).support_frequencys().keys()),
    "ny_futures": list(get_exchange(Market.NY_FUTURES).support_frequencys().keys()),
    "currency": list(get_exchange(Market.CURRENCY).support_frequencys().keys()),
    "currency_spot": list(
        get_exchange(Market.CURRENCY_SPOT).support_frequencys().keys()
    ),
}


# 各个交易所默认的标的
market_default_codes = {
    "a": get_exchange(Market.A).default_code(),
    "hk": get_exchange(Market.HK).default_code(),
    "fx": get_exchange(Market.FX).default_code(),
    "us": get_exchange(Market.US).default_code(),
    "futures": get_exchange(Market.FUTURES).default_code(),
    "ny_futures": get_exchange(Market.NY_FUTURES).default_code(),
    "currency": get_exchange(Market.CURRENCY).default_code(),
    "currency_spot": get_exchange(Market.CURRENCY_SPOT).default_code(),
}


# 各个市场的交易时间
market_session = {
    "a": "24x7",
    "hk": "24x7",
    "fx": "24x7",
    "us": "24x7",
    "futures": "24x7",
    "ny_futures": "24x7",
    "currency": "24x7",
    "currency_spot": "24x7",
}


# 各个交易所的时区 统一时区
market_timezone = {
    "a": "Asia/Shanghai",
    "hk": "Asia/Shanghai",
    "fx": "Asia/Shanghai",
    "us": "America/New_York",
    "futures": "Asia/Shanghai",
    "ny_futures": "Asia/Shanghai",
    "currency": str(get_localzone()),
    "currency_spot": str(get_localzone()),
}


# 市场类型
market_types = {
    "a": "stock",
    "hk": "stock",
    "fx": "stock",
    "us": "stock",
    "futures": "futures",
    "ny_futures": "futures",
    "currency": "crypto",
    "currency_spot": "crypto",
}


__all__ = [
    "frequency_maps",
    "resolution_maps",
    "market_frequencys",
    "market_default_codes",
    "market_session",
    "market_timezone",
    "market_types",
]