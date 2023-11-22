import datetime

from chanlun.exchange.exchange_tq import ExchangeTq
from chanlun import fun
from chanlun import rd, zixuan
from chanlun.backtesting.base import Operation, POSITION
from chanlun.backtesting.backtest_trader import BackTestTrader

"""
期货使用天勤自带的模拟账号测试
"""


class TraderFutures(BackTestTrader):
    """
    期货交易 Demo
    """

    def __init__(self, name, log=None):
        super().__init__(name=name, mode='real', is_stock=False, is_futures=True, log=log)
        self.ex = ExchangeTq(use_account=True)

        self.zx = zixuan.ZiXuan('futures')

        # 每单交易手数
        self.unit_volume = 2

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float = None):
        try:
            positions = self.ex.positions(code)
            if len(positions) > 0 and positions[code].pos_long > 0:
                return False

            res = self.ex.order(code, 'open_long', self.unit_volume)
            if res is False or res['price'] is None:
                fun.send_dd_msg('futures', f'{code} open long 下单失败')
                return False

            stock_info = self.ex.stock_info(code)

            msg = f"开多仓 {code} 价格 {res['price']} 数量 {self.unit_volume} 原因 {opt.msg}"
            fun.send_dd_msg('futures', msg)

            self.zx.add_stock('我的持仓', stock_info['code'], stock_info['name'])

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
            rd.order_save('futures', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('futures', f'{code} open long 异常: {str(e)}')
            return False

    # 做空卖出
    def open_sell(self, code, opt: Operation, amount: float = None):
        try:
            positions = self.ex.positions(code)
            if len(positions) > 0 and positions[code].pos_short > 0:
                return False

            res = self.ex.order(code, 'open_short', self.unit_volume)
            if res is False or res['price'] is None:
                fun.send_dd_msg('futures', f'{code} open short 下单失败')
                return False

            stock_info = self.ex.stock_info(code)

            msg = f"开空仓 {code} 价格 {res['price']} 数量 {self.unit_volume} 原因 {opt.msg}"
            fun.send_dd_msg('futures', msg)

            self.zx.add_stock('我的持仓', stock_info['code'], stock_info['name'])

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
            rd.order_save('futures', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('futures', f'{code} open short 异常: {str(e)}')
            return False

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt: Operation):
        try:
            hold_position = self.ex.positions(code)
            if len(hold_position) == 0 or hold_position[code].pos_long == 0:
                # 当前无持仓，不进行操作
                return {'price': pos.price, 'amount': pos.amount}
            hold_position = hold_position[code]

            res = self.ex.order(code, 'close_long', pos.amount)
            if res is False or res['price'] is None:
                fun.send_dd_msg('futures', f'{code} 下单失败')
                return False
            msg = f"平多仓 {code} 价格 {res['price']} 数量 {res['amount']} 盈亏 {hold_position.float_profit}  原因 {opt.msg}"

            fun.send_dd_msg('futures', msg)

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
            rd.order_save('futures', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('futures', f'{code} close buy 异常: {str(e)}')
            return False

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt: Operation):
        try:
            hold_position = self.ex.positions(code)
            if len(hold_position) == 0 or hold_position[code].pos_short == 0:
                # 当前无持仓，不进行操作
                return {'price': pos.price, 'amount': pos.amount}
            hold_position = hold_position[code]

            res = self.ex.order(code, 'close_short', pos.amount)
            if res is False or res['price'] is None:
                fun.send_dd_msg('futures', f'{code} 下单失败')
                return False
            msg = f"平空仓 {code} 价格 {res['price']} 数量 {res['amount']} 盈亏 {hold_position.float_profit}  原因 {opt.msg}"

            fun.send_dd_msg('futures', msg)

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
            rd.order_save('futures', code, save_order)

            return {'price': res['price'], 'amount': res['amount']}
        except Exception as e:
            fun.send_dd_msg('futures', f'{code} close sell 异常: {str(e)}')
            return False
