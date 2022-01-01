import sys

cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")

import time
import logging
import traceback

from cl_v2 import exchange_futu
from cl_v2 import fun

logging.basicConfig(filename=sys.path[0] + '/logs/reboot_gc_kanpan.log', level='INFO',
                    format='%(asctime)s - %(levelname)s : %(message)s')

logging.info('观察仓自动看盘程序执行')

# 股票检查
try:
    exchange = exchange_futu.ExchangeFutu()
    if exchange.is_trade_day():

        # 运行股票配置
        run_config = {
            # '美锦能源': dict(run=['5m'], ding=[], di=['5m'], beichi=True, buy=True, sell=False),
        }

        user_stocks = exchange.zixuan_stocks('今日关注')
        for stock in user_stocks:
            time.sleep(3)
            code = stock['code']
            name = stock['name']
            logging.info('Run : ' + code + ' - ' + name)

            # 顶底停顿配置
            ding_td = []
            di_td = ['30m']
            check_beichi = True
            check_buy = True
            check_sell = False
            if name in run_config:
                ding_td = run_config[name]['ding']
                di_td = run_config[name]['di']
                check_beichi = run_config[name]['beichi']
                check_buy = run_config[name]['buy']
                check_sell = run_config[name]['sell']

            try:
                run_frequencys = ['d', '30m']
                if name in run_config:
                    run_frequencys = run_config[name]['run']

                jhs = fun.monitoring_code(
                    'hk' if 'HK.' in code else 'a',
                    code, name, run_frequencys,
                    {'beichi': check_beichi, 'buy': check_buy, 'sell': check_sell, 'ding': ding_td, 'di': di_td},
                    is_send_msg=True)

            except Exception as e:
                logging.error('Exception : ' + code + name)
                logging.error(traceback.format_exc())
                continue
except:
    pass

logging.info('Done')
exit()
