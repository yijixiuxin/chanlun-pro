import logging
import sys
import time
import traceback
import pickle
import threading

cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")

from cl_v2 import exchange_futu
from cl_v2 import rd
from cl_v2 import cl
from cl_v2.my import strategy_demo
from cl_v2.my import trader_hk_stock

logging.basicConfig(filename=sys.path[0] + '/logs/trader_hk_stock.log', level='INFO',
                    format='%(asctime)s - %(levelname)s : %(message)s')

logging.info('港股自动化交易程序')

# 保存交易日期列表
G_Trade_days = None

exchange = exchange_futu.ExchangeFutu()


def now_is_trade():
    """
    当前是否交易时间
    :return:
    """
    global G_Trade_days
    if G_Trade_days is None:
        G_Trade_days = exchange.market_trade_days('hk')

    now_date = time.strftime('%Y-%m-%d')
    if G_Trade_days[-1]['time'] < now_date:
        G_Trade_days = exchange.market_trade_days('hk')

    for _t in G_Trade_days:
        if _t['time'] == now_date:
            hour = int(time.strftime('%H'))
            minute = int(time.strftime('%M'))
            if _t['trade_date_type'] == 'WHOLE' or _t['trade_date_type'] == 'MORNING':
                # 上午的时间检查
                if (hour == 9 and minute >= 30) or hour in [10, 11]:
                    return True
            if _t['trade_date_type'] == 'WHOLE' or _t['trade_date_type'] == 'AFTERNOON':
                # 下午的时间检查
                if hour in [13, 14, 15]:
                    return True

    return False


def position_codes(td: trader_hk_stock.HKStockTrader):
    """
    获取当前持仓中的股票代码
    :param td:
    :return:
    """
    codes = []
    for _c in td.positions:
        for mmd in td.positions[_c]:
            pos = td.positions[_c][mmd]
            if pos.balance > 0:
                codes.append(_c)
    codes = list(set(codes))
    return codes


try:
    run_codes = ['HK.00189', 'HK.01072']

    p_redis_key = 'trader_hk_stock'

    # 从 Redis 中恢复交易对象
    p_bytes = rd.get_byte(p_redis_key)
    if p_bytes is not None:
        TR = pickle.loads(p_bytes)
    else:
        STR = strategy_demo.Strategy_Demo()
        TR = trader_hk_stock.HKStockTrader('HKStock', is_stock=False, is_futures=False, log=logging.info,
                                           is_save_kline=False, mmds=['1buy', '2buy', '3buy'])
        TR.set_eye(STR)

    # 单独设置一些参数，更新之前缓存的参数
    TR.is_stock = False
    TR.allow_mmds = ['1buy', '2buy', '3buy']

    while True:
        try:
            seconds = int(time.time())

            if seconds % (5 * 60) != 0:
                time.sleep(1)
                continue

            # 判断是否是交易时间
            if now_is_trade() is False:
                continue

            # 增加当前持仓中的交易对儿
            run_codes = position_codes(TR) + run_codes
            run_codes = list(set(run_codes))

            for code in run_codes:
                try:
                    # logging.info('Run code : ' + code)
                    klines = {}
                    for f in ['30m', '5m']:
                        klines[f] = exchange.klines(code, f)
                    cl_datas = cl.batch_cls(code, klines)

                    TR.run(code, cl_datas)
                except Exception as e:
                    logging.error(traceback.format_exc())

            # 保存对象到 Redis 中
            p_obj = pickle.dumps(TR)
            rd.save_byte(p_redis_key, p_obj)

        except Exception as e:
            logging.error(traceback.format_exc())

except Exception as e:
    logging.error(traceback.format_exc())
finally:
    logging.info('Done')

exit()
