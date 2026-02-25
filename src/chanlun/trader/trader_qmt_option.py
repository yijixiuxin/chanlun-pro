import datetime
import time
from typing import List
from chanlun import utils, zixuan
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import POSITION, Operation
from chanlun.exchange.exchange_qmt import ExchangeQMTOption
from chanlun.db import db
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount, XtAsset, XtOrder, XtPosition


class MyXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        """
        连接断开
        :return:
        """
        print("connection lost")

    def on_stock_order(self, order):
        """
        委托回报推送
        :param order: XtOrder对象
        :return:
        """
        print(f"on order callback: {order.stock_code} {order.order_status} {order.order_sysid}")

    def on_stock_trade(self, trade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        print(f"on trade callback: {trade.account_id} {trade.stock_code} {trade.order_id}")

    def on_order_error(self, order_error):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        print(f"on order_error callback: {order_error.order_id} {order_error.error_id} {order_error.error_msg}")

    def on_cancel_error(self, cancel_error):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        print(f"on cancel_error callback: {cancel_error.order_id} {cancel_error.error_id} {cancel_error.error_msg}")

    def on_order_stock_async_response(self, response):
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        print(f"on_order_stock_async_response: {response.account_id} {response.order_id} {response.seq}")

    def on_account_status(self, status):
        """
        :param response: XtAccountStatus 对象
        :return:
        """
        print(f"on_account_status: {status.account_id} {status.account_type} {status.status}")


class QMTTraderOption(BackTestTrader):
    """
    QMT 期权交易对象
    """

    def __init__(self, name, log=None):
        # 期权通常归类为 futures 市场类型 或者 独立的 options，chanlun 框架中暂时用 futures 代替衍生品或者需要新增 market
        # 这里为了兼容 BackTestTrader，暂用 futures，或者用户如果定义了 options 也可以。
        # 假设 market="futures" 逻辑通用于衍生品（支持做空）
        super().__init__(name=name, mode="online", market="option", log=log)
        self.ex = ExchangeQMTOption()

        self.zx = zixuan.ZiXuan("option") 
        self.zx_group = "QMT期权"

        # 最大持仓数量
        self.max_pos = 5

        # path为mini qmt客户端安装目录下userdata_mini路径
        self.qmt_path = r"D:\国金证券QMT交易端\userdata_mini"
        # session_id为会话编号
        self.session_id = int(time.time())
        self.xt_trader = XtQuantTrader(self.qmt_path, self.session_id)
        # 创建资金账号，账号类型为 FUTURE
        self.acc = StockAccount("809222890", xtconstant.ACCOUNT_TYPE_DICT.OPTION.FUTURE_OPTION_ACCOUNT)  # TODO 替换自己的期权资金账号
        
        # 创建交易回调类对象
        self.trader_callback = MyXtQuantTraderCallback()
        self.xt_trader.register_callback(self.trader_callback)
        # 启动交易线程
        self.xt_trader.start()
        # 建立交易连接
        connect_result = self.xt_trader.connect()
        print("建立交易连接 (0表示成功)：", connect_result)
        # 订阅回调
        subscribe_result = self.xt_trader.subscribe(self.acc)
        print("交易回调进行订阅 (0表示成功):", subscribe_result)

    def close(self):
        self.xt_trader.unsubscribe(self.acc)
        self.xt_trader.stop()

    def _wait_order_deal(self, order_id):
        """等待订单成交"""
        for _ in range(10):
            time.sleep(1)
            order = self.xt_trader.query_stock_order(self.acc, order_id)
            if order and order.order_status in [xtconstant.ORDER_SUCCEEDED, xtconstant.ORDER_PART_SUCCEEDED]:
                return order
            if order and order.order_status in [xtconstant.ORDER_FAILED, xtconstant.ORDER_CANCELED]:
                print(f"订单失败或取消 {order.order_status}")
                return None
        print("订单超时未完全成交")
        return None

    # 做多买入 (买入开仓)
    def open_buy(self, code, opt: Operation, amount: float = None):
        tick = self.ex.ticks([code])
        if code not in tick.keys():
            return False
        price = tick[code].last

        is_real_trade = True
        
        hold_positions = self.xt_trader.query_stock_positions(self.acc)
        hold_pos_num = len([_p for _p in hold_positions if _p.volume > 0]) if hold_positions else 0
        if hold_pos_num >= self.max_pos:
            is_real_trade = False

        stock = self.ex.stock_info(code)
        if stock is None:
            return False

        if is_real_trade:
            account = self.xt_trader.query_stock_asset(self.acc)
            if account is None:
                is_real_trade = False
            else:
                if amount is None:
                    amount = 1 
                
                # 期权买入开仓 -> STOCK_BUY
                order_id = self.xt_trader.order_stock(
                    account=self.acc,
                    stock_code=self.ex.code_to_qmt(code),
                    order_type=xtconstant.STOCK_BUY, 
                    order_volume=int(amount),
                    price_type=xtconstant.MARKET_PEGS, 
                    price=price, 
                    strategy_name="cl",
                    order_remark=opt.msg
                )
                print(f"期权买入开仓 {code} 数量 {amount} 订单ID {order_id}")
                
                order = self._wait_order_deal(order_id)
                if order:
                    price = order.traded_price
                    amount = order.traded_volume
                else:
                    self.xt_trader.cancel_order_stock(self.acc, order_id)
                    is_real_trade = False

        if not is_real_trade:
             price = tick[code].last
             if amount is None: amount = 1

        msg = f"[{'实盘' if is_real_trade else '模拟'}] 期权买入开仓 {code}-{stock['name']} 价格 {price} 数量 {amount} 原因 {opt.msg}"
        utils.send_fs_msg("option_trader", "期权交易提醒", [msg])
        self.zx.add_stock("我的持仓", stock["code"], stock["name"])
        
        db.order_save("futures", code, stock["name"], "open_buy", price, amount, opt.msg, datetime.datetime.now())
        return {"price": price, "amount": amount}

    # 做空卖出 (卖出开仓 - 义务方)
    def open_sell(self, code, opt: Operation, amount: float = None):
        tick = self.ex.ticks([code])
        if code not in tick.keys():
            return False
        price = tick[code].last
        
        is_real_trade = True
        hold_positions = self.xt_trader.query_stock_positions(self.acc)
        hold_pos_num = len([_p for _p in hold_positions if _p.volume > 0]) if hold_positions else 0
        if hold_pos_num >= self.max_pos:
            is_real_trade = False

        stock = self.ex.stock_info(code)
        if stock is None: return False

        if is_real_trade:
            account = self.xt_trader.query_stock_asset(self.acc)
            if account is None: is_real_trade = False
            else:
                if amount is None: amount = 1
                
                # 期权卖出开仓 -> STOCK_SELL
                order_id = self.xt_trader.order_stock(
                    account=self.acc,
                    stock_code=self.ex.code_to_qmt(code),
                    order_type=xtconstant.STOCK_SELL, 
                    order_volume=int(amount),
                    price_type=xtconstant.MARKET_PEGS,
                    price=price,
                    strategy_name="cl",
                    order_remark=opt.msg
                )
                print(f"期权卖出开仓 {code} 数量 {amount} 订单ID {order_id}")
                
                order = self._wait_order_deal(order_id)
                if order:
                    price = order.traded_price
                    amount = order.traded_volume
                else:
                    self.xt_trader.cancel_order_stock(self.acc, order_id)
                    is_real_trade = False

        if not is_real_trade:
             price = tick[code].last
             if amount is None: amount = 1

        msg = f"[{'实盘' if is_real_trade else '模拟'}] 期权卖出开仓 {code}-{stock['name']} 价格 {price} 数量 {amount} 原因 {opt.msg}"
        utils.send_fs_msg("option_trader", "期权交易提醒", [msg])
        self.zx.add_stock("我的持仓", stock["code"], stock["name"])
        
        db.order_save("futures", code, stock["name"], "open_sell", price, amount, opt.msg, datetime.datetime.now())
        return {"price": price, "amount": amount}

    # 做多平仓 (卖出平仓 - 权利方平仓)
    def close_buy(self, code, pos: POSITION, opt):
        tick = self.ex.ticks([code])
        if code not in tick.keys(): return False
        stock = self.ex.stock_info(code)
        
        price = tick[code].last
        amount = pos.amount
        
        is_real_trade = False
        hold_positions = self.xt_trader.query_stock_positions(self.acc)
        for _p in hold_positions:
            if _p.stock_code == self.ex.code_to_qmt(code) and _p.can_use_volume > 0:
                is_real_trade = True
                amount = min(amount, _p.can_use_volume)
                break
        
        if is_real_trade:
            # 卖出平仓 -> STOCK_SELL
            order_id = self.xt_trader.order_stock(
                account=self.acc,
                stock_code=self.ex.code_to_qmt(code),
                order_type=xtconstant.STOCK_SELL, 
                order_volume=int(amount),
                price_type=xtconstant.MARKET_PEGS,
                price=price,
                strategy_name="cl",
                order_remark=opt.msg
            )
            print(f"期权卖出平仓 {code} 数量 {amount} 订单ID {order_id}")
            order = self._wait_order_deal(order_id)
            if order:
                price = order.traded_price
                amount = order.traded_volume
            else:
                self.xt_trader.cancel_order_stock(self.acc, order_id)
        
        if not is_real_trade:
            price = tick[code].last
        
        msg = f"[{'实盘' if is_real_trade else '模拟'}] 期权卖出平仓 {code}-{stock['name']} 价格 {price} 数量 {amount} 原因 {opt.msg}"
        utils.send_fs_msg("option_trader", "期权交易提醒", [msg])
        self.zx.del_stock("我的持仓", stock["code"])
        
        db.order_save("futures", code, stock["name"], "close_buy", price, amount, opt.msg, datetime.datetime.now())
        return {"price": price, "amount": amount}

    # 做空平仓 (买入平仓 - 义务方平仓)
    def close_sell(self, code, pos: POSITION, opt):
        tick = self.ex.ticks([code])
        if code not in tick.keys(): return False
        stock = self.ex.stock_info(code)
        
        price = tick[code].last
        amount = pos.amount
        
        is_real_trade = False
        hold_positions = self.xt_trader.query_stock_positions(self.acc)
        for _p in hold_positions:
            if _p.stock_code == self.ex.code_to_qmt(code) and _p.can_use_volume > 0:
                is_real_trade = True
                amount = min(amount, _p.can_use_volume)
                break
        
        if is_real_trade:
            # 买入平仓 -> STOCK_BUY
            order_id = self.xt_trader.order_stock(
                account=self.acc,
                stock_code=self.ex.code_to_qmt(code),
                order_type=xtconstant.STOCK_BUY, 
                order_volume=int(amount),
                price_type=xtconstant.MARKET_PEGS,
                price=price,
                strategy_name="cl",
                order_remark=opt.msg
            )
            print(f"期权买入平仓 {code} 数量 {amount} 订单ID {order_id}")
            order = self._wait_order_deal(order_id)
            if order:
                price = order.traded_price
                amount = order.traded_volume
            else:
                self.xt_trader.cancel_order_stock(self.acc, order_id)

        if not is_real_trade:
            price = tick[code].last
        
        msg = f"[{'实盘' if is_real_trade else '模拟'}] 期权买入平仓 {code}-{stock['name']} 价格 {price} 数量 {amount} 原因 {opt.msg}"
        utils.send_fs_msg("option_trader", "期权交易提醒", [msg])
        self.zx.del_stock("我的持仓", stock["code"])
        
        db.order_save("futures", code, stock["name"], "close_sell", price, amount, opt.msg, datetime.datetime.now())
        return {"price": price, "amount": amount}

if __name__ == "__main__":
    qmt_trader = QMTTraderOption("qmt_option_trader")
