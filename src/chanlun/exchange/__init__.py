from enum import Enum
from chanlun import config

from chanlun.exchange.exchange import Exchange

# 全局保存交易所对象，避免创建多个交易所对象
g_exchange_obj = {}


class Market(Enum):
    """
    交易市场
    """

    A = "a"
    HK = "hk"
    FUTURES = "futures"
    CURRENCY = "currency"
    US = "us"


def get_exchange(market: Market) -> Exchange:
    """
    获取市场的交易所对象，根据config配置中设置的进行获取
    """
    global g_exchange_obj
    if market.value in g_exchange_obj.keys():
        return g_exchange_obj[market.value]

    if market == Market.A:
        # 沪深 A股 交易所
        if config.EXCHANGE_A == "tdx":
            from chanlun.exchange.exchange_tdx import ExchangeTDX

            g_exchange_obj[market.value] = ExchangeTDX()
        elif config.EXCHANGE_A == "futu":
            from chanlun.exchange.exchange_futu import ExchangeFutu

            g_exchange_obj[market.value] = ExchangeFutu()
        elif config.EXCHANGE_A == "baostock":
            from chanlun.exchange.exchange_baostock import ExchangeBaostock

            g_exchange_obj[market.value] = ExchangeBaostock()
        elif config.EXCHANGE_A == "db":
            from chanlun.exchange.exchange_db import ExchangeDB

            g_exchange_obj[market.value] = ExchangeDB("a")
        else:
            raise Exception(f"不支持的沪深交易所 {config.EXCHANGE_A}")

    elif market == Market.HK:
        # 港股 交易所
        if config.EXCHANGE_HK == "tdx_hk":
            from chanlun.exchange.exchange_tdx_hk import ExchangeTDXHK

            g_exchange_obj[market.value] = ExchangeTDXHK()
        elif config.EXCHANGE_HK == "futu":
            from chanlun.exchange.exchange_futu import ExchangeFutu

            g_exchange_obj[market.value] = ExchangeFutu()
        elif config.EXCHANGE_HK == "db":
            from chanlun.exchange.exchange_db import ExchangeDB

            g_exchange_obj[market.value] = ExchangeDB("hk")
        else:
            raise Exception(f"不支持的香港交易所 {config.EXCHANGE_HK}")

    elif market == Market.FUTURES:
        # 期货 交易所
        if config.EXCHANGE_FUTURES == "tq":
            from chanlun.exchange.exchange_tq import ExchangeTq

            g_exchange_obj[market.value] = ExchangeTq()
        elif config.EXCHANGE_FUTURES == "tdx_futures":
            from chanlun.exchange.exchange_tdx_futures import ExchangeTDXFutures

            g_exchange_obj[market.value] = ExchangeTDXFutures()
        elif config.EXCHANGE_FUTURES == "db":
            from chanlun.exchange.exchange_db import ExchangeDB

            g_exchange_obj[market.value] = ExchangeDB("futures")
        else:
            raise Exception(f"不支持的期货交易所 {config.EXCHANGE_FUTURES}")

    elif market == Market.CURRENCY:
        # 数字货币 交易所
        if config.EXCHANGE_CURRENCY == "binance":
            from chanlun.exchange.exchange_binance import ExchangeBinance

            g_exchange_obj[market.value] = ExchangeBinance()
        elif config.EXCHANGE_CURRENCY == "zb":
            from chanlun.exchange.exchange_zb import ExchangeZB

            g_exchange_obj[market.value] = ExchangeZB()
        elif config.EXCHANGE_CURRENCY == "db":
            from chanlun.exchange.exchange_db import ExchangeDB

            g_exchange_obj[market.value] = ExchangeDB("currency")
        else:
            raise Exception(f"不支持的数字货币交易所 {config.EXCHANGE_CURRENCY}")

    elif market == Market.US:
        # 美股 交易所
        if config.EXCHANGE_US == "alpaca":
            from chanlun.exchange.exchange_alpaca import ExchangeAlpaca

            g_exchange_obj[market.value] = ExchangeAlpaca()
        elif config.EXCHANGE_US == "polygon":
            from chanlun.exchange.exchange_polygon import ExchangePolygon

            g_exchange_obj[market.value] = ExchangePolygon()
        elif config.EXCHANGE_US == "ib":
            from chanlun.exchange.exchange_ib import ExchangeIB

            g_exchange_obj[market.value] = ExchangeIB()
        elif config.EXCHANGE_US == "tdx_us":
            from chanlun.exchange.exchange_tdx_us import ExchangeTDXUS

            g_exchange_obj[market.value] = ExchangeTDXUS()
        elif config.EXCHANGE_US == "db":
            from chanlun.exchange.exchange_db import ExchangeDB

            g_exchange_obj[market.value] = ExchangeDB("us")
        else:
            raise Exception(f"不支持的美股交易所 {config.EXCHANGE_US}")

    return g_exchange_obj[market.value]
