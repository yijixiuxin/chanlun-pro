from enum import Enum


class Market(Enum):
    """
    交易市场
    """

    A = "a"
    HK = "hk"
    FUTURES = "futures"
    CURRENCY = "currency"
    CURRENCY_SPOT = "currency_spot"
    US = "us"
