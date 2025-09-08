from __future__ import absolute_import, print_function

import datetime
import time

from gm.api import (
    ADJUST_PREV,
    SEC_TYPE_STOCK,
    Context,
    get_instruments,
    history_n,
    schedule,
    stk_get_daily_mktvalue_pt,
    subscribe,
)

from chanlun import fun
from chanlun.backtesting.base import Strategy
from chanlun.cl_utils import query_cl_chart_config
from chanlun.strategy import strategy_demo
from cl_myquant.base import MyQuantData, MyQuantTrader

# 定义全局变量
market_data: MyQuantData  # 数据对象
trader: MyQuantTrader  # 交易对象
strategy: Strategy  # 策略对象
# 使用的缠论配置
cl_config = query_cl_chart_config("a", "SH.000001")


def init(context):
    global market_data, trader, strategy
    # 初始化数据对象、策略、交易对象
    market_data = MyQuantData(context, ["1d"], cl_config)
    strategy = strategy_demo.StrategyDemo()
    trader = MyQuantTrader("a", context)
    trader.set_data(market_data)
    trader.set_strategy(strategy)

    # 每月执行一遍选股
    schedule(schedule_func=xuangu_sz, date_rule="1m", time_rule="9:20:00")


def xuangu_sz(context: Context):
    """
    根据基本面选股，选择出来的进行订阅，并执行交易策略
    """
    global market_data

    # 查询所有股票标的
    instruments = get_instruments(
        symbols=None,
        exchanges=["SHSE", "SZSE"],
        sec_types=SEC_TYPE_STOCK,
        names=None,
        skip_suspended=True,
        skip_st=True,
        fields=None,
        df=False,
    )
    # 上市日期要大于 1 年
    symbols = [
        _i["symbol"]
        for _i in instruments
        if _i["listed_date"] < context.now - datetime.timedelta(days=364)
    ]
    print(f"获取所有上市大于1年的股票标的数量{len(symbols)}")

    s = time.time()
    # 查询基本面数据，并筛选（这里简单用市值作为筛选条件）
    fundamentals = stk_get_daily_mktvalue_pt(
        symbols=symbols,
        fields="tot_mv",
        trade_date=fun.datetime_to_str(context.now, "%Y-%m-%d"),
        df=False,
    )
    print("基本面数据查询用时：", time.time() - s)

    fundamentals = sorted(fundamentals, key=lambda f: f["tot_mv"], reverse=True)
    symbols = [_f["symbol"] for _f in fundamentals[0:100]]
    print(
        f"获取 {fun.datetime_to_str(context.now, '%Y-%m-%d')} 市值前100的股票列表：{symbols}"
    )

    # symbols = ['SHSE.601857', 'SHSE.601398', 'SHSE.601288', 'SHSE.601988', 'SHSE.600028']

    # 查询当前持仓
    positions = context.account().positions()
    pos_symbols = [_p["symbol"] for _p in positions if _p["amount"] > 0]
    print(f"当前持仓股票列表：{pos_symbols}")

    symbols = symbols + pos_symbols
    symbols = list(set(symbols))

    # 进行重新订阅（取消之前的订阅）
    subscribe(symbols=symbols, frequency="1d", count=2000, unsubscribe_previous=True)

    # 初始化缠论数据
    market_data.init_cl_datas(symbols, ["1d"])

    return symbols


def xuangu_zf(context: Context):
    """
    TODO 涨跌幅排行的效果不好（不管是涨幅高的还是涨幅低的）
    根据进二十日涨幅前100选股
    """
    global market_data

    # 查询所有股票标的
    instruments = get_instruments(
        symbols=None,
        exchanges=["SHSE", "SZSE"],
        sec_types=SEC_TYPE_STOCK,
        names=None,
        skip_suspended=True,
        skip_st=True,
        fields=None,
        df=False,
    )
    # 上市日期要大于 1 年
    symbols = [
        _i["symbol"]
        for _i in instruments
        if _i["listed_date"] < context.now - datetime.timedelta(days=364)
    ]
    print(f"获取所有上市大于1年的股票标的数量{len(symbols)}")

    # 查获取市值大于100亿的股票列表
    fundamentals = stk_get_daily_mktvalue_pt(
        symbols=symbols,
        trade_date=fun.datetime_to_str(context.now, "%Y-%m-%d"),
        fields="tot_mv",
        df=False,
    )

    fundamentals = sorted(fundamentals, key=lambda f: f["tot_mv"], reverse=True)
    symbols = [_f["symbol"] for _f in fundamentals if _f["tot_mv"] > 10000000000]
    print(
        f"获取 {fun.datetime_to_str(context.now, '%Y-%m-%d')} 市值大于100亿的股票列表：{len(symbols)}"
    )

    # 计算股票 20 日涨跌幅
    symbol_20day_rank = []
    for symbol in symbols:
        try:
            bar = history_n(
                symbol=symbol,
                frequency="1d",
                count=20,
                end_time=context.now,
                fields="symbol,eob,open,high,low,close",
                adjust=ADJUST_PREV,
                df=True,
            )
            if len(bar) == 20:
                change = (bar["close"].iloc[-1] - bar["close"].iloc[0]) / bar[
                    "close"
                ].iloc[0]
                symbol_20day_rank.append({"symbol": symbol, "change": change})
        except KeyError:
            pass
    # 涨跌幅排序
    symbol_20day_rank.sort(key=lambda r: r["change"], reverse=False)
    # 选取排名前 50 选手
    symbols = [s["symbol"] for s in symbol_20day_rank[0:50]]
    print("排行前50股票代码：", symbols)

    # 查询当前持仓
    positions = context.account().positions()
    pos_symbols = [_p["symbol"] for _p in positions if _p["amount"] > 0]
    print(f"当前持仓股票列表：{pos_symbols}")

    symbols = symbols + pos_symbols
    symbols = list(set(symbols))

    # 进行重新订阅（取消之前的订阅）
    subscribe(symbols=symbols, frequency="1d", count=2000, unsubscribe_previous=True)

    # 初始化缠论数据
    market_data.init_cl_datas(symbols, ["1d"])

    return symbols


def on_bar(context, bars):
    global market_data, trader
    # print(f"on bar : {bars[0]['symbol']} {bars[0]['eob']}")
    # 更新缠论数据
    market_data.update_bars(bars)

    # 执行策略
    trader.run(bars[0]["symbol"])
