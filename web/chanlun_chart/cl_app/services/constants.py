"""
Shared market and chart constants for cl_app.

This module centralizes static mappings and derived values previously
defined inline in the Flask app. Importing from here reduces duplication
and makes it easier to test and evolve supported markets and resolutions.
"""

import threading

from tzlocal import get_localzone

from chanlun.base import Market
from chanlun.exchange import get_exchange
from chanlun.tools.log_util import LogUtil


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


_ALL_MARKETS = [
    ("a", Market.A),
    ("hk", Market.HK),
    ("fx", Market.FX),
    ("us", Market.US),
    ("futures", Market.FUTURES),
    ("ny_futures", Market.NY_FUTURES),
    ("currency", Market.CURRENCY),
    ("currency_spot", Market.CURRENCY_SPOT),
]


class _LazyMarketDict(dict):
    """在首次访问时才初始化交易所对象，避免阻塞启动。"""

    def __init__(self, builder):
        super().__init__()
        self._builder = builder
        self._loaded = False
        self._lock = threading.Lock()

    def _ensure_loaded(self):
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return
            self.update(self._builder())
            self._loaded = True

    def __getitem__(self, key):
        self._ensure_loaded()
        return super().__getitem__(key)

    def __contains__(self, key):
        self._ensure_loaded()
        return super().__contains__(key)

    def get(self, key, default=None):
        self._ensure_loaded()
        return super().get(key, default)

    def keys(self):
        self._ensure_loaded()
        return super().keys()

    def values(self):
        self._ensure_loaded()
        return super().values()

    def items(self):
        self._ensure_loaded()
        return super().items()

    def __iter__(self):
        self._ensure_loaded()
        return super().__iter__()

    def __len__(self):
        self._ensure_loaded()
        return super().__len__()


def _build_market_frequencys():
    result = {}
    for key, market in _ALL_MARKETS:
        try:
            result[key] = list(get_exchange(market).support_frequencys().keys())
        except Exception as e:
            LogUtil.warning(f"获取 {key} 支持周期失败: {e}")
            result[key] = []
    return result


def _build_market_default_codes():
    result = {}
    for key, market in _ALL_MARKETS:
        try:
            result[key] = get_exchange(market).default_code()
        except Exception as e:
            LogUtil.warning(f"获取 {key} 默认代码失败: {e}")
            result[key] = ""
    return result


# 懒加载：首次访问时才创建交易所对象，不阻塞启动
market_frequencys = _LazyMarketDict(_build_market_frequencys)
market_default_codes = _LazyMarketDict(_build_market_default_codes)


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