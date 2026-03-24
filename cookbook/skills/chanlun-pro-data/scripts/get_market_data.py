"""行情数据获取模块

提供统一接口获取多市场、多周期的行情数据
"""

from typing import Dict, List

import pandas as pd

from chanlun.base import Market
from chanlun.exchange import get_exchange

# 市场名称映射
MARKET_NAMES = {
    "a": "沪深A股",
    "hk": "港股",
    "futures": "国内期货",
    "ny_futures": "美股期货",
    "currency": "数字货币合约",
    "currency_spot": "数字货币现货",
    "us": "美股",
    "fx": "外汇",
}

# 周期名称映射
FREQUENCY_NAMES = {
    "1m": "1分钟",
    "5m": "5分钟",
    "15m": "15分钟",
    "30m": "30分钟",
    "60m": "60分钟",
    "d": "日线",
    "w": "周线",
    "m": "月线",
}


def list_supported_markets() -> Dict[str, str]:
    """返回支持的市场列表"""
    return MARKET_NAMES.copy()


def list_supported_frequencies(market: str = None) -> Dict[str, str]:
    """
    返回支持的周期列表

    Args:
        market: 市场标识，如不指定则返回所有通用周期名称映射；
                指定时返回该市场实际支持的周期（通过 Exchange.support_frequencys 获取）

    Returns:
        Dict[str, str]，key 为周期代码，value 为周期名称

    Example:
        >>> list_supported_frequencies()
        {'1m': '1分钟', '5m': '5分钟', '15m': '15分钟', '30m': '30分钟', '60m': '60分钟', 'd': '日线', 'w': '周线', 'm': '月线'}
        >>> list_supported_frequencies('a')
        {'d': 'Day', 'w': 'Week', 'm': 'Month'}
    """
    if market is None:
        return FREQUENCY_NAMES.copy()

    market_enum = _get_market_enum(market)
    ex = get_exchange(market_enum)
    return ex.support_frequencys()


def list_all_market_frequencies() -> Dict[str, Dict[str, str]]:
    """
    返回所有市场各自支持的周期列表

    Returns:
        Dict[market, Dict[str, str]]，key 为市场标识，value 为该市场的周期映射

    Example:
        >>> all_freqs = list_all_market_frequencies()
        >>> print(all_freqs['a'])   # A股支持的周期
        >>> print(all_freqs['hk'])  # 港股支持的周期
    """
    result = {}
    for market_id in MARKET_NAMES.keys():
        try:
            market_enum = _get_market_enum(market_id)
            ex = get_exchange(market_enum)
            result[market_id] = ex.support_frequencys()
        except Exception as e:
            print(f"获取 {market_id} 周期信息失败: {e}")
            result[market_id] = {}
    return result


def _get_market_enum(market: str) -> Market:
    """将市场字符串转换为Market枚举"""
    market_map = {
        "a": Market.A,
        "hk": Market.HK,
        "futures": Market.FUTURES,
        "ny_futures": Market.NY_FUTURES,
        "currency": Market.CURRENCY,
        "currency_spot": Market.CURRENCY_SPOT,
        "us": Market.US,
        "fx": Market.FX,
    }
    if market not in market_map:
        raise ValueError(
            f"不支持的市场: {market}，支持的市场: {list(market_map.keys())}"
        )
    return market_map[market]


def get_market_data(market: str, code: str, frequency: str) -> pd.DataFrame:
    """
    获取单个标的的行情数据

    Args:
        market: 市场标识，如 'a', 'hk', 'futures' 等
        code: 标的代码，如 'SH.600519', 'BTC.USDT' 等
        frequency: 周期，如 '1m', '5m', '15m', '30m', '60m', 'd', 'w', 'm'

    Returns:
        DataFrame，包含 date, open, high, low, close, volume 列

    Raises:
        ValueError: 不支持的市场或周期

    Example:
        >>> df = get_market_data('a', 'SH.600519', 'd', '2024-01-01', '2024-12-31')
        >>> print(df.head())
    """
    market_enum = _get_market_enum(market)
    ex = get_exchange(market_enum)

    klines = ex.klines(code=code, frequency=frequency)

    if klines is None or len(klines) == 0:
        return pd.DataFrame()

    # 确保列名标准化
    if "date" not in klines.columns:
        if "datetime" in klines.columns:
            klines = klines.rename(columns={"datetime": "date"})
        elif "time" in klines.columns:
            klines = klines.rename(columns={"time": "date"})

    return klines


def get_multiple_market_data(
    market: str, codes: List[str], frequency: str
) -> Dict[str, pd.DataFrame]:
    """
    获取多个标的的行情数据

    Args:
        market: 市场标识
        codes: 标的代码列表
        frequency: 周期

    Returns:
        Dict[code, DataFrame]，key为标的代码，value为K线数据

    Example:
        >>> data = get_multiple_market_data('a', ['SH.600519', 'SH.601398'], 'd')
        >>> for code, df in data.items():
        ...     print(f"{code}: {len(df)} bars")
    """
    result = {}
    for code in codes:
        try:
            df = get_market_data(market, code, frequency)
            result[code] = df
        except Exception as e:
            print(f"获取 {code} 数据失败: {e}")
            result[code] = pd.DataFrame()
    return result
