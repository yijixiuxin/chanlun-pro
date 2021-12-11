import sys
cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")

import time
import logging
import traceback

from cl_v2 import exchange_binance
from cl_v2 import fun

logging.basicConfig(filename=sys.path[0] + '/logs/reboot_currency_kanpan.log', level='INFO',
                    format='%(asctime)s - %(levelname)s : %(message)s')

logging.info('数字货币自动看盘程序执行')

try:
    exchange = exchange_binance.ExchangeBinance()
    stocks = exchange.all_stocks()
    for stock in stocks:
        code = stock['code']
        time.sleep(1)
        logging.info('Run : ' + code)
        try:
            frequencys = ['d', '4h', '60m']
            jhs = fun.monitoring_code('currency', code, code, frequencys,
                                  {'beichi': True, 'buy': True, 'sell': True, 'ding': ['d', '4h'], 'di': ['d', '4h']},
                                  is_send_msg=False)
        except Exception as e:
            logging.error('Exception : ' + code)
            logging.error(traceback.format_exc())

except:
    logging.error(traceback.format_exc())

exit()
