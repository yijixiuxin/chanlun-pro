# coding=utf-8
from __future__ import absolute_import, print_function

from gm.api import ADJUST_PREV, MODE_BACKTEST, run, set_serv_addr, set_token

from chanlun import config

# 如在远程执行，需要制定掘金终端地址  https://www.myquant.cn/docs/gm3_faq/154#b244aeed0032526e
set_serv_addr(config.GM_SERVER_ADDR)
# 设置token， 查看已有token ID,在用户-秘钥管理里获取
set_token(config.GM_TOKEN)


"""
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
"""

strategy_filename = "strategy/strategy_test.py"

# TODO 这里的 strategy_id 替换为自己的
run(
    strategy_id="2cd63317-8c4a-11f0-987e-5847ca718ef4",
    filename=strategy_filename,
    mode=MODE_BACKTEST,
    # TODO 这里的 token 替换为自己的
    token=config.GM_TOKEN,
    backtest_start_time="2025-06-01 09:30:00",
    backtest_end_time="2025-09-01 15:00:00",
    backtest_adjust=ADJUST_PREV,
    backtest_initial_cash=10000000,
    backtest_commission_ratio=0.0001,
    backtest_slippage_ratio=0.0001,
    backtest_match_mode=1,
)
