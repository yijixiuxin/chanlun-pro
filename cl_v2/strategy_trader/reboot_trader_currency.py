import logging
import sys
import time
import traceback
import pickle
import threading

cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")

from cl_v2 import exchange_binance
from cl_v2 import rd
from cl_v2 import cl
from cl_v2.my import strategy_demo
from cl_v2.my import trader_currency

logging.basicConfig(filename=sys.path[0] + '/logs/trader_currency.log', level='INFO',
                    format='%(asctime)s - %(levelname)s : %(message)s')

logging.info('数字货币自动化交易程序')


def position_symbols(td: trader_currency.CurrencyTrader):
    """
    获取当前持仓中的交易对儿
    :param td:
    :return:
    """
    codes = []
    for code in td.positions:
        for mmd in td.positions[code]:
            pos = td.positions[code][mmd]
            if pos.balance > 0:
                codes.append(code)
    codes = list(set(codes))
    return codes


try:
    exchange = exchange_binance.ExchangeBinance()
    run_num = 40
    run_codes = exchange.ticker24HrRank(run_num)

    p_redis_key = 'trader_currency'

    # 从 Redis 中恢复交易对象
    p_bytes = rd.get_byte(p_redis_key)
    if p_bytes is not None:
        TR = pickle.loads(p_bytes)
    else:
        STR = strategy_demo.Strategy_Demo()
        TR = trader_currency.CurrencyTrader('Currency', is_stock=False, is_futures=True, log=logging.info, mmds=None)
        TR.set_strategy(STR)

    # 单独设置一些参数，更新之前缓存的参数
    TR.allow_mmds = None

    while True:
        try:
            seconds = int(time.time())

            if seconds % (60 * 60) == 0:
                # 每一个小时，更新 24 小时交易量排行代码
                run_codes = exchange.ticker24HrRank(run_num)
                logging.info('Run symbols: %s' % run_codes)

            if seconds % (5 * 60) != 0:
                time.sleep(1)
                continue

            # 增加当前持仓中的交易对儿
            run_codes = position_symbols(TR) + run_codes
            run_codes = list(set(run_codes))

            for code in run_codes:
                try:
                    # logging.info('Run Symbol: ' + symbol)
                    cl_datas = {}
                    for f in ['30m', '5m']:
                        klines = exchange.klines(code, f)
                        cl_datas[f] = cl.CL(code, klines, f)

                    TR.run(code, cl_datas)
                except Exception as e:
                    logging.error(traceback.format_exc())

            # 保存对象到 Redis 中
            p_obj = pickle.dumps(TR)
            rd.save_byte(p_redis_key, p_obj)

        except Exception as e:
            logging.error(str(e))

except Exception as e:
    logging.error(str(e))
finally:
    logging.info('Done')

exit()
