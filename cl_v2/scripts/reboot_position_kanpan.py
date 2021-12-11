import sys

cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")


import time
import logging
import traceback

from cl_v2 import exchange_futu
from cl_v2 import fun

logging.basicConfig(filename=sys.path[0] + '/logs/reboot_position_kanpan.log', level='INFO',
                    format='%(asctime)s - %(levelname)s : %(message)s')

logging.info('股票自动看盘程序执行')

# 运行股票配置
run_config = {
    '上证指数': dict(run=['60m', '30m', '15m'], ding=['60m', '30m', '15m'], di=['60m', '30m', '15m'], beichi=True, buy=True, sell=True),
}
# 股票检查
try:
    exchange = exchange_futu.ExchangeFutu()
    if exchange.is_trade_day():
        # 获取我当前的持仓
        user_stocks = exchange.zixuan_stocks('我的持仓')
        for stock in user_stocks:
            time.sleep(2)
            code = stock['code']
            name = stock['name']
            logging.info('Run : ' + code + ' - ' + name)

            # 顶底停顿配置
            ding_td = ['30m', '5m']
            di_td = ['30m', '5m']
            check_beichi = True
            check_buy = False
            check_sell = True
            if name in run_config:
                ding_td = run_config[name]['ding']
                di_td = run_config[name]['di']
                check_beichi = run_config[name]['beichi']
                check_buy = run_config[name]['buy']
                check_sell = run_config[name]['sell']

            try:
                run_frequencys = ['60m', '30m', '5m']
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
    logging.error(traceback.format_exc())

logging.info('Done')
exit()
