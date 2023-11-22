# coding=utf-8
from __future__ import print_function, absolute_import
from gm.api import *

'''
        strategy_id策略ID, 由系统生成
        filename文件名, 请与本文件名保持一致
        mode运行模式, 实时模式:MODE_LIVE回测模式:MODE_BACKTEST
        token绑定计算机的ID, 可在系统设置-密钥管理中生成
        backtest_start_time回测开始时间
        backtest_end_time回测结束时间
        backtest_adjust股票复权方式, 不复权:ADJUST_NONE前复权:ADJUST_PREV后复权:ADJUST_POST
        backtest_initial_cash回测初始资金
        backtest_commission_ratio回测佣金比例
        backtest_slippage_ratio回测滑点比例
'''
# TODO 这里的 strategy_id 替换为自己的
run(strategy_id='17ed07ef-dfff-11ec-8613-7a4f4371764a',
    filename='strategy/strategy_son_level_12mmd.py',
    mode=MODE_BACKTEST,
    # TODO 这里的 token 替换为自己的
    token='7eb4ebc68c8aaa69261b9e9d01541a6067cc7453',
    backtest_start_time='2022-01-01 08:00:00',
    backtest_end_time='2022-05-01 16:00:00',
    backtest_adjust=ADJUST_POST,
    backtest_initial_cash=10000000,
    backtest_commission_ratio=0.0001,
    backtest_slippage_ratio=0.0001)
