import traceback

from gm.api import *
from chanlun import config
from chanlun import rd
import pandas as pd


def init(context):
    redis_obj = rd.Robj()

    frequency_map = {
        # 股票支持的时间周期
        '1m': '60s', '5m': '300s', '15m': '900s', '30m': '1800s', '60m': '3600s', 'd': '1d',
        # 期货支持的时间周期 +
        '10s': '10s', '30s': '30s'
    }

    while True:
        code_info: str = ''
        try:
            code_info: tuple = redis_obj.blpop('gm_sync', 0)
            code_info: str = code_info[1]
            code_info: list = code_info.split(':')
            code = code_info[0]
            frequency = frequency_map[code_info[1]]
            # 订阅
            print(f'掘金：开始订阅 {code} - {frequency} 行情')
            subscribe(code, frequency=frequency, count=5000, unsubscribe_previous=True)
            # 获取并保存到 redis 中
            klines: pd.DataFrame = context.data(symbol=code, frequency=frequency, count=5000)
            klines.loc[:, 'code'] = klines['symbol']
            klines.loc[:, 'date'] = pd.to_datetime(klines['eob'])
            klines = klines[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
            print(f'掘金：获取到 {code} - {frequency} 行情数据 {len(klines)} 条，开始保存到 Redis 中')
            klines_json = klines.to_json(date_format='epoch', orient='split')

            redis_obj.setex(f'gm_{code_info[0]}:{code_info[1]}', 60, klines_json)

        except Exception as e:
            print(f'掘金任务获取 {code_info} 行情数据异常', traceback.format_exc())


def on_bar(context, bars):
    pass


def task_gm_sync():
    """
    掘金行情
    """
    if config.GM_SERVER_ADDR == '' or config.GM_TOKEN == '':
        return False

    set_serv_addr(config.GM_SERVER_ADDR)
    run(strategy_id='07c08563-a4a8-11ea-a682-7085c223669d',
        filename='task_gm.py',
        mode=MODE_LIVE,
        token=config.GM_TOKEN,
        backtest_start_time='2020-09-01 08:00:00',
        backtest_end_time='2020-10-01 16:00:00',
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=500000,
        backtest_commission_ratio=0.0001,
        backtest_slippage_ratio=0.0001)
    return True


if __name__ == '__main__':
    task_gm_sync()
