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


def _json_serial(obj):
    """JSON序列化辅助函数，处理Timestamp等类型"""
    from pandas import Timestamp

    if isinstance(obj, Timestamp):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="行情数据获取工具")
    parser.add_argument("--fun", type=str, required=True, help="要调用的方法名")
    parser.add_argument("--market", type=str, help="市场标识")
    parser.add_argument("--code", type=str, help="标的代码")
    parser.add_argument("--codes", type=str, help="标的代码列表，逗号分隔")
    parser.add_argument("--frequency", type=str, help="周期")

    args = parser.parse_args()

    # 构建参数字典
    kwargs = {}
    if args.market:
        kwargs["market"] = args.market
    if args.code:
        kwargs["code"] = args.code
    if args.codes:
        kwargs["codes"] = args.codes.split(",")
    if args.frequency:
        kwargs["frequency"] = args.frequency

    # 调用指定方法
    fun_map = {
        "list_supported_markets": list_supported_markets,
        "list_supported_frequencies": list_supported_frequencies,
        "list_all_market_frequencies": list_all_market_frequencies,
        "get_market_data": get_market_data,
        "get_multiple_market_data": get_multiple_market_data,
    }

    if args.fun not in fun_map:
        print(f"不支持的方法: {args.fun}")
        exit(1)

    def _process_result(obj):
        """处理结果中的DataFrame对象"""
        if hasattr(obj, "to_dict"):
            # 只返回最后50行数据
            obj = obj.tail(50)
            return obj.to_dict(orient="records")
        elif isinstance(obj, dict):
            return {k: _process_result(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_process_result(item) for item in obj]
        else:
            return obj

    try:
        result = fun_map[args.fun](**kwargs)
        # 处理结果中的DataFrame对象
        processed_result = _process_result(result)
        # 输出处理后的结果
        print(
            json.dumps(
                processed_result, ensure_ascii=False, indent=2, default=_json_serial
            )
        )
    except Exception as e:
        print(f"执行出错: {e}")
        import traceback

        traceback.print_exc()
