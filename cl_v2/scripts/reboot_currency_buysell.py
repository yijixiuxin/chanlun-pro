import sys

cur_path = sys.path[0]
sys.path.append(sys.path[0] + "/../..")

import logging
import traceback

from cl_v2 import exchange_binance
from cl_v2 import rd
from cl_v2 import cl
from cl_v2 import fun

"""
实现的根据简单设置，进行数字货币交易脚本
"""

logging.basicConfig(filename=sys.path[0] + '/logs/reboot_currency_buysell.log', level='INFO',
                    format='%(asctime)s - %(levelname)s : %(message)s')

logging.info('数字货币买卖程序执行')


def judge_bi(bi: cl.BI, fangxiang: str, beichi: str, td: str, bi_done: str, mmds: list):
    """
    判断笔是否满足条件
    :param bi:
    :param fangxiang:
    :param beichi:
    :param td:
    :param bi_done:
    :param mmds:
    :return:
    """
    is_trade = True
    if is_trade and bi.type != fangxiang:
        is_trade = False
    if is_trade and beichi == 'true' and bi.qs_beichi is False and bi.pz_beichi is False:
        is_trade = False
    if is_trade and td == 'true' and bi.td is False:
        is_trade = False
    if is_trade and bi_done == 'true' and bi.done is False:
        is_trade = False
    if is_trade and len(mmds) > 0 and len(set_setting(bi.mmds) & set_setting(mmds)) == 0:
        is_trade = False
    return is_trade


try:
    exchange = exchange_binance.ExchangeBinance()
    # 查询持仓，按照持仓的配置，盯盘
    positions = exchange.positions()
    for pos in positions:
        symbol = pos['symbol']

        # 查询止盈比例
        profit_rate = rd.currency_pos_profit_rate_query(symbol)
        if profit_rate > 0 and float(pos['percentage']) >= float(profit_rate):
            if pos['side'] == 'long':
                # 多仓触发止盈
                logging.info('%s 多仓触发止盈比例 %s < %s，执行清仓' % (symbol, profit_rate, pos['percentage']))
                res = exchange.order(symbol, 'close_long', pos['contracts'])
                if res is not False:
                    rd.currency_pos_loss_price_save(symbol, 0)
                    rd.currency_pos_profit_rate_save(symbol, 0)
                    rd.currency_position_check_setting_clear(symbol)

                    msg = '交易类型 close_long 平仓价格 %s 数量 %s 盈亏 %s (%.2f%%) 止盈比例 %s' % \
                          (res['price'], res['amount'], pos['unrealizedPnl'], pos['percentage'], profit_rate)
                    fun.send_dd_msg('currency', '%s 触发止盈，清仓；信息：%s' % (symbol, msg))
                    # 记录操作记录
                    rd.currency_opt_record_save(symbol, '止盈交易：%s' % msg)
                else:
                    fun.send_dd_msg('currency', '%s 多仓触发止盈，下单失败' % symbol)
            if pos['side'] == 'short':
                # 空仓触发止损
                logging.info('%s 空仓仓触发止盈比例 %s < %s，执行清仓' % (symbol, profit_rate, pos['percentage']))
                res = exchange.order(symbol, 'close_short', pos['contracts'])
                if res is not False:
                    rd.currency_pos_loss_price_save(symbol, 0)
                    rd.currency_pos_profit_rate_save(symbol, 0)
                    rd.currency_position_check_setting_clear(symbol)
                    msg = '交易类型 close_short 平仓价格 %s 数量 %s 盈亏 %s (%.2f%%) 止盈比例 %s' % \
                          (res['price'], res['amount'], pos['unrealizedPnl'], pos['percentage'], profit_rate)
                    fun.send_dd_msg('currency', '%s 触发止盈，清仓；信息：%s' % (symbol, msg))
                    # 记录操作记录
                    rd.currency_opt_record_save(symbol, '止盈交易：%s' % msg)
                else:
                    fun.send_dd_msg('currency', '%s 空仓触发止盈，下单失败' % symbol)

        # 查询止损价格
        loss_price = rd.currency_pos_loss_price_query(symbol)
        if loss_price > 0:
            ticks = exchange.ticks([symbol])
            if pos['side'] == 'long' and ticks[symbol].last < loss_price:
                # 多仓触发止损
                logging.info('%s 多仓触发止损价格 %s < %s，执行清仓' % (symbol, ticks[symbol].last, loss_price))
                res = exchange.order(symbol, 'close_long', pos['contracts'])
                if res is not False:
                    rd.currency_pos_loss_price_save(symbol, 0)
                    rd.currency_position_check_setting_clear(symbol)

                    msg = '交易类型 close_long 平仓价格 %s 数量 %s 盈亏 %s (%.2f%%) 当前价格 %s 止损价格 %s' % \
                          (res['price'], res['amount'], pos['unrealizedPnl'], pos['percentage'], ticks[symbol].last,
                           loss_price)
                    fun.send_dd_msg('currency', '%s 触发止损，清仓；信息：%s' % (symbol, msg))
                    # 记录操作记录
                    rd.currency_opt_record_save(symbol, '止损交易：%s' % msg)
                else:
                    fun.send_dd_msg('currency', '%s 多仓触发止损，下单失败' % symbol)
            if pos['side'] == 'short' and ticks[symbol].last > loss_price:
                # 空仓触发止损
                logging.info('%s 空仓触发止损价格 %s > %s，执行清仓' % (symbol, ticks[symbol].last, loss_price))
                res = exchange.order(symbol, 'close_short', pos['contracts'])
                if res is not False:
                    rd.currency_pos_loss_price_save(symbol, 0)
                    rd.currency_position_check_setting_clear(symbol)
                    msg = '交易类型 close_short 平仓价格 %s 数量 %s 盈亏 %s (%.2f%%) 当前价格 %s 止损价格 %s' % \
                          (res['price'], res['amount'], pos['unrealizedPnl'], pos['percentage'], ticks[symbol].last,
                           loss_price)
                    fun.send_dd_msg('currency', '%s 触发止损，清仓；信息：%s' % (symbol, msg))
                    # 记录操作记录
                    rd.currency_opt_record_save(symbol, '止损交易：%s' % msg)
                else:
                    fun.send_dd_msg('currency', '%s 空仓触发止损，下单失败' % symbol)

        check_setting = rd.currency_position_check_setting_query(symbol)

        for set_setting in check_setting:
            try:
                frequency = set_setting['frequency']
                fangxiang = set_setting['fangxiang']
                beichi = set_setting['beichi']
                td = set_setting['td']
                bi_done = set_setting['bi_done']
                mmds = set_setting['mmds']
                if len(mmds) > 0:
                    mmds = mmds.split('/')

                # 获取数据
                klines = exchange.klines(symbol, frequency)
                cd = cl.CL(symbol, klines, frequency)
                if len(cd.bis) <= 0:
                    continue
                bi = cd.bis[-1]

                # 判断条件
                trade = judge_bi(bi, fangxiang, beichi, td, bi_done, mmds)
                if trade:
                    # 条件成立，执行清仓操作
                    close_res = False
                    if pos['side'] == 'long':
                        close_res = exchange.order(symbol, 'close_long', pos['contracts'])
                    elif pos['side'] == 'short':
                        close_res = exchange.order(symbol, 'close_short', pos['contracts'])
                    # 清除持仓配置
                    if close_res is not False:
                        rd.currency_position_check_setting_clear(symbol)

                        msg = '交易类型 close_%s 平仓价格 %s 数量 %s 盈亏 %s (%.2f%%) 设置条件 %s-%s（ BC %s TD %s Done %s MMD %s） 当前笔 (BC %s TD %s Done %s MMD %s)' % \
                              (pos['side'], close_res['price'], close_res['amount'], pos['unrealizedPnl'],
                               pos['percentage'], frequency, fangxiang, beichi, td, bi_done, mmds,
                               (bi.qs_beichi or bi.pz_beichi), bi.td, bi.done, bi.mmds)
                        fun.send_dd_msg('currency', '%s 触发条件，平仓；信息：%s' % (symbol, msg))
                        # 记录操作记录
                        rd.currency_opt_record_save(symbol, '条件平仓交易：%s' % msg)
                    else:
                        fun.send_dd_msg('currency', '%s 触发平仓，平仓下单失败' % symbol)
            except Exception as e:
                logging.error('Exception : ' + symbol)
                logging.error(traceback.format_exc())

    # 查询开仓的配置，按照开仓的配置，盯盘
    open_settings = rd.currency_open_setting_query()
    for set_setting in open_settings:
        try:
            symbol = set_setting['symbol']
            open_usdt = float(set_setting['open_usdt'])
            trade_type = set_setting['trade_type']
            leverage = int(set_setting['leverage'])

            frequency = set_setting['frequency']
            fangxiang = set_setting['fangxiang']
            beichi = set_setting['beichi']
            td = set_setting['td']
            bi_done = set_setting['bi_done']
            mmds = set_setting['mmds']
            if len(mmds) > 0:
                mmds = mmds.split('/')

            # 获取数据
            klines = exchange.klines(symbol, frequency)
            cd = cl.CL(symbol, klines, frequency)
            if len(cd.bis) <= 0:
                continue
            bi = cd.bis[-1]
            trade = judge_bi(bi, fangxiang, beichi, td, bi_done, mmds)
            if trade:
                ticks = exchange.ticks([symbol])
                amount = (open_usdt / ticks[symbol].last) * leverage
                open_res = False
                if trade_type == 'open_long':
                    open_res = exchange.order(symbol, 'open_long', amount, {'leverage': leverage})
                elif trade_type == 'open_short':
                    open_res = exchange.order(symbol, 'open_short', amount, {'leverage': leverage})
                if open_res is not False:
                    # 设置初始的止损价格
                    if trade_type == 'open_long':
                        rd.currency_pos_loss_price_save(symbol, bi.low)
                    else:
                        rd.currency_pos_loss_price_save(symbol, bi.high)

                    rd.currency_open_setting_clear(symbol)
                    msg = '交易类型 %s 开仓价格 %s 数量 %s 占用保证金 %s 设置条件 %s-%s（ BC %s TD %s Done %s MMD %s） 当前笔 (BC %s TD %s Done %s MMD %s)' % \
                          (trade_type, open_res['price'], open_res['amount'], open_usdt, frequency, fangxiang, beichi,
                           td, bi_done, mmds,
                           (bi.qs_beichi or bi.pz_beichi), bi.td, bi.done, bi.mmds)
                    fun.send_dd_msg('currency', '%s 触发条件，开仓；信息：%s' % (symbol, msg))
                    # 记录操作记录
                    rd.currency_opt_record_save(symbol, '条件开仓交易：%s' % msg)

                else:
                    fun.send_dd_msg('currency', '%s 触发开仓，开仓下单失败' % symbol)
        except Exception as e:
            logging.error('Exception : ' + symbol)
            logging.error(traceback.format_exc())

except:
    logging.error(traceback.format_exc())
finally:
    logging.info('Done')

exit()
