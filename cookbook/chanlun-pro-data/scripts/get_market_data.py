"""行情数据获取模块

提供统一接口获取多市场、多周期的行情数据
"""

from typing import Dict, List, Optional, Union
import pandas as pd

from chanlun.base import Market
from chanlun.exchange import get_exchange


# 市场名称映射
MARKET_NAMES = {
    'a': '沪深A股',
    'hk': '港股',
    'futures': '国内期货',
    'ny_futures': '美股期货',
    'currency': '数字货币合约',
    'currency_spot': '数字货币现货',
    'us': '美股',
    'fx': '外汇',
}

# 周期名称映射
FREQUENCY_NAMES = {
    '1m': '1分钟',
    '5m': '5分钟',
    '15m': '15分钟',
    '30m': '30分钟',
    '60m': '60分钟',
    'd': '日线',
    'w': '周线',
    'm': '月线',
}


def list_supported_markets() -> Dict[str, str]:
    """返回支持的市场列表"""
    return MARKET_NAMES.copy()


def list_supported_frequencies() -> Dict[str, str]:
    """返回支持的周期列表"""
    return FREQUENCY_NAMES.copy()


def _get_market_enum(market: str) -> Market:
    """将市场字符串转换为Market枚举"""
    market_map = {
        'a': Market.A,
        'hk': Market.HK,
        'futures': Market.FUTURES,
        'ny_futures': Market.NY_FUTURES,
        'currency': Market.CURRENCY,
        'currency_spot': Market.CURRENCY_SPOT,
        'us': Market.US,
        'fx': Market.FX,
    }
    if market not in market_map:
        raise ValueError(f"不支持的市场: {market}，支持的市场: {list(market_map.keys())}")
    return market_map[market]


def get_market_data(
    market: str,
    code: str,
    frequency: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    获取单个标的的行情数据

    Args:
        market: 市场标识，如 'a', 'hk', 'futures' 等
        code: 标的代码，如 'SH.600519', 'BTC.USDT' 等
        frequency: 周期，如 '1m', '5m', '15m', '30m', '60m', 'd', 'w', 'm'
        start_date: 开始日期，格式 'YYYY-MM-DD'
        end_date: 结束日期，格式 'YYYY-MM-DD'

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

    klines = ex.klines(
        code=code,
        frequency=frequency,
        start_date=start_date,
        end_date=end_date,
    )

    if klines is None or len(klines) == 0:
        return pd.DataFrame()

    # 确保列名标准化
    if 'date' not in klines.columns:
        if 'datetime' in klines.columns:
            klines = klines.rename(columns={'datetime': 'date'})
        elif 'time' in klines.columns:
            klines = klines.rename(columns={'time': 'date'})

    return klines


def get_multiple_market_data(
    market: str,
    codes: List[str],
    frequency: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    获取多个标的的行情数据

    Args:
        market: 市场标识
        codes: 标的代码列表
        frequency: 周期
        start_date: 开始日期
        end_date: 结束日期

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
            df = get_market_data(market, code, frequency, start_date, end_date)
            result[code] = df
        except Exception as e:
            print(f"获取 {code} 数据失败: {e}")
            result[code] = pd.DataFrame()
    return result
