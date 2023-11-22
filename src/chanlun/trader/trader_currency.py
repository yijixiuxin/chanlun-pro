import datetime

from chanlun.exchange.exchange_binance import ExchangeBinance
from chanlun import rd, fun
from chanlun import zixuan
from chanlun.backtesting.base import Operation, POSITION
from chanlun.backtesting.backtest_trader import BackTestTrader

"""
指定使用 binance 接口
"""


class TraderCurrency(BackTestTrader):
    """
    数字货币交易者
    """

    def __init__(self, name, log=None):
        super().__init__(name=name, mode='real', is_stock=False, is_futures=True, log=log)

        self.ex = ExchangeBinance()

        # 分仓数
        self.poss_max = 8
        # 使用的杠杆倍数
        self.leverage = 2

        self.zx = zixuan.ZiXuan('currency')

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float = None):
        try:
            positions = self.ex.positions()
            if len(positions) >= self.poss_max:
                fun.send_dd_msg('currency', f'{code} open buy 下单失败，达到最大开仓数量')
                return False
            balance = self.ex.balance()
            open_usdt = (balance['free'] / (self.poss_max - len(positions)) * 0.98)
            ticks = self.ex.ticks([code])
            amount = (open_usdt / ticks[code].last) * self.leverage
            res = self.ex.order(code, 'open_long', amount, {'leverage': self.leverage})
            if res is False:
                fun.send_dd_msg('currency', f'{code} open buy 下单失败')
                return False
            msg = f"开多仓 {code} 价格 {res['price']} 数量 {open_usdt} 原因 {opt.msg}"
            fun.send_dd_msg('currency', msg)
            rd.currency_opt_record_save(code, f'策略交易：{msg}')

            self.zx.add_stock('我的持仓', code, code)

            # 保存订单记录到 Redis 中
            save_order = {
                'code': code,
                'name': code,
                'datetime': fun.datetime_to_str(datetime.datetime.now()),
                'type': 'open_long',
                'price': res['price'],
                'amount': res['amount'],
                'info': opt.msg
            }
            rd.order_save('currency', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('currency', f'{code} open buy 异常: {str(e)}')
            return False

    # 做空卖出
    def open_sell(self, code, opt: Operation, amount: float = None):
        try:
            positions = self.ex.positions()
            if len(positions) >= self.poss_max:
                fun.send_dd_msg('currency', f'{code} open sell 下单失败，达到最大开仓数量')
                return False
            balance = self.ex.balance()
            open_usdt = (balance['free'] / (self.poss_max - len(positions)) * 0.98)

            ticks = self.ex.ticks([code])
            amount = (open_usdt / ticks[code].last) * self.leverage
            res = self.ex.order(code, 'open_short', amount, {'leverage': self.leverage})
            if res is False:
                fun.send_dd_msg('currency', f'{code} open sell 下单失败')
                return False
            msg = f"开空仓 {code} 价格 {res['price']} 数量 {open_usdt} 原因 {opt.msg}"
            fun.send_dd_msg('currency', msg)
            rd.currency_opt_record_save(code, f'策略交易：{msg}')

            self.zx.add_stock('我的持仓', code, code)

            # 保存订单记录到 Redis 中
            save_order = {
                'code': code,
                'name': code,
                'datetime': fun.datetime_to_str(datetime.datetime.now()),
                'type': 'open_short',
                'price': res['price'],
                'amount': res['amount'],
                'info': opt.msg
            }
            rd.order_save('currency', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('currency', f'{code} open sell 异常: {str(e)}')
            return False

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt: Operation):
        try:
            hold_position = self.ex.positions(code)
            if len(hold_position) == 0:
                return {'price': pos.price, 'amount': pos.amount}
            hold_position = hold_position[0]

            res = self.ex.order(code, 'close_long', pos.amount)
            if res is False:
                fun.send_dd_msg('currency', f'{code} 下单失败')
                return False
            msg = '平多仓 %s 价格 %s 数量 %s 盈亏 %s (%.2f%%) 原因 %s' % (
                code, res['price'], res['amount'], hold_position['unrealizedPnl'], hold_position['percentage'],
                opt.msg)
            fun.send_dd_msg('currency', msg)
            rd.currency_opt_record_save(code, f'策略交易：{msg}')

            self.zx.del_stock('我的持仓', code)

            # 保存订单记录到 Redis 中
            save_order = {
                'code': code,
                'name': code,
                'datetime': fun.datetime_to_str(datetime.datetime.now()),
                'type': 'close_long',
                'price': res['price'],
                'amount': res['amount'],
                'info': opt.msg
            }
            rd.order_save('currency', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('currency', f'{code} close buy 异常: {str(e)}')
            return False

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt: Operation):
        try:
            hold_position = self.ex.positions(code)
            if len(hold_position) == 0:
                return {'price': pos.price, 'amount': pos.amount}
            hold_position = hold_position[0]

            res = self.ex.order(code, 'close_short', pos.amount)
            if res is False:
                fun.send_dd_msg('currency', f'{code} 下单失败')
                return False
            msg = '平空仓 %s 价格 %s 数量 %s 盈亏 %s (%.2f%%) 原因 %s' % (
                code, res['price'], res['amount'], hold_position['unrealizedPnl'], hold_position['percentage'],
                opt.msg)
            fun.send_dd_msg('currency', msg)
            rd.currency_opt_record_save(code, f'策略交易：{msg}')

            self.zx.del_stock('我的持仓', code)

            # 保存订单记录到 Redis 中
            save_order = {
                'code': code,
                'name': code,
                'datetime': fun.datetime_to_str(datetime.datetime.now()),
                'type': 'close_short',
                'price': res['price'],
                'amount': res['amount'],
                'info': opt.msg
            }
            rd.order_save('currency', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('currency', f'{code} close sell 异常: {str(e)}')
            return False
