from enum import Enum


class Market(Enum):
    """
    交易市场
    """

    A = "a"  # A股
    HK = "hk"  # 港股
    FUTURES = "futures"  # 期货
    NY_FUTURES = "ny_futures"  # 纽约期货
    CURRENCY = "currency"  # 数字货币合约
    CURRENCY_SPOT = "currency_spot"  # 数字货币现货
    US = "us"  # 美股
    FX = "fx"  # 外汇


if __name__ == "__main__":
    for market in Market:
        print(market.name, market.value)
