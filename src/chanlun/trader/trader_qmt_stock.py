import datetime
import time
from typing import List
from chanlun import utils, zixuan
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import POSITION, Operation
from chanlun.exchange.exchange_qmt import ExchangeQMTStock
from chanlun.db import db

from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount, XtAsset, XtOrder, XtPosition
from xtquant import xtconstant


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


class QMTTraderStock(BackTestTrader):
    """
    QMT A股票交易对象
    """

    def __init__(self, name, log=None):
        super().__init__(name=name, mode="online", market="a", log=log)
        self.ex = ExchangeQMTStock()

        self.zx = zixuan.ZiXuan("a")
        self.zx_group = "QMT交易"

        # 最大持仓数量，当前持仓大于这个数量，则不进行实际交易
        self.max_pos = 5

        # path为mini qmt客户端安装目录下userdata_mini路径
        self.qmt_path = r"D:\国金证券QMT交易端\userdata_mini"
        # session_id为会话编号，策略使用方对于不同的Python策略需要使用不同的会话编号
        self.session_id = int(time.time())
        self.xt_trader = XtQuantTrader(self.qmt_path, self.session_id)
        # 创建资金账号为******的证券账号对象
        self.acc = StockAccount("8886136661")  # TODO 替换自己的资金账号
        # 创建交易回调类对象，并声明接收回调
        self.trader_callback = MyXtQuantTraderCallback()
        self.xt_trader.register_callback(self.trader_callback)
        # 启动交易线程
        self.xt_trader.start()
        # 建立交易连接，返回0表示连接成功
        connect_result = self.xt_trader.connect()
        print("建立交易连接 (0表示成功)：", connect_result)
        # 对交易回调进行订阅，订阅后可以收到交易主推，返回0表示订阅成功
        subscribe_result = self.xt_trader.subscribe(self.acc)
        print("交易回调进行订阅 (0表示成功):", subscribe_result)

    def close(self):
        self.xt_trader.unsubscribe(self.acc)
        self.xt_trader.stop()

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float = None):
        tick = self.ex.ticks([code])
        if code not in tick.keys():
            return False
        
        price = tick[code].last

        is_real_trade = True
        # 检查持仓数量
        hold_positions: List[XtPosition] = self.xt_trader.query_stock_positions(
            self.acc
        )
        hold_pos_num = len([_p for _p in hold_positions if _p.volume > 0])
        if hold_positions is not None and hold_pos_num >= self.max_pos:
            is_real_trade = False

        stock = self.ex.stock_info(code)
        if stock is None:
            return False

        if is_real_trade:
            # 计算开仓金额
            account: XtAsset = self.xt_trader.query_stock_asset(self.acc)
            # 可用资金
            if account is None:
                print("获取资金账户失败")
                return False
                
            balance = round((account.cash * 0.98) / (self.max_pos - hold_pos_num), 0)
            amount = int(balance / price / 100) * 100
            if amount < 100:
                is_real_trade = False
            else:
                order_id = self.xt_trader.order_stock(
                    account=self.acc,
                    stock_code=self.ex.code_to_qmt(code),
                    order_type=xtconstant.STOCK_BUY,
                    order_volume=amount,
                    price_type=(
                        xtconstant.MARKET_SH_CONVERT_5_LIMIT
                        if "SH" in code
                        else xtconstant.MARKET_SZ_CONVERT_5_CANCEL
                    ),
                    price=0, # 市价单价格传0
                    strategy_name="cl",
                    order_remark=opt.msg,
                )
                print(f"发起买入 {code} 数量 {amount} 订单ID {order_id}")
                
                # 循环检查订单成交情况，最多等待 10秒
                for _ in range(10):
                    time.sleep(1)
                    order = self.xt_trader.query_stock_order(self.acc, order_id)
                    if order and order.order_status in [xtconstant.ORDER_SUCCEEDED, xtconstant.ORDER_PART_SUCCEEDED]:
                        price = order.traded_price
                        amount = order.traded_volume
                        break
                    if order and order.order_status in [xtconstant.ORDER_FAILED, xtconstant.ORDER_CANCELED]:
                        print(f"订单失败或取消 {order.order_status}")
                        is_real_trade = False
                        break
                else:
                    # 超时未成交，撤单
                    print("买入超时，尝试撤单")
                    self.xt_trader.cancel_order_stock(self.acc, order_id)
                    is_real_trade = False

        if is_real_trade is False:
            # 模拟交易或者实盘失败转模拟
            balance = 50000
            price = tick[code].last
            amount = int(balance / price / 100) * 100

        msg = f"[{'实盘' if is_real_trade else '模拟'}] 股票买入 {code}-{stock['name']} 价格 {price} 数量 {amount} 原因 {opt.msg}"
        utils.send_fs_msg("a_trader", "沪深交易提醒", [msg])

        self.zx.add_stock("我的持仓", stock["code"], stock["name"])

        # 保存订单记录到 数据库 中，这样可以在图表中标识出买卖卖出的位置
        db.order_save(
            "a",
            code,
            stock["name"],
            "buy",
            price,
            amount,
            opt.msg,
            datetime.datetime.now(),
        )

        return {"price": price, "amount": amount}

    # 做空卖出
    def open_sell(self, code, opt: Operation, amount: float = None):
        # A股普通账户不支持做空
        return False

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt):
        tick = self.ex.ticks([code])
        if code not in tick.keys():
            return False
        stock = self.ex.stock_info(code)
        if stock is None:
            return False

        price = tick[code].last
        amount = pos.amount

        is_real_trade = False
        hold_positions: List[XtPosition] = self.xt_trader.query_stock_positions(
            self.acc
        )
        hold_pos: XtPosition = None
        for _p in hold_positions:
            if _p.can_use_volume > 0 and _p.stock_code == self.ex.code_to_qmt(code):
                hold_pos = _p
                is_real_trade = True
                break

        if is_real_trade:
            amount = min(pos.amount, hold_pos.can_use_volume)
            if amount <= 0:
                print(f"可用持仓不足 {code}")
                return False

            order_id = self.xt_trader.order_stock(
                account=self.acc,
                stock_code=self.ex.code_to_qmt(code),
                order_type=xtconstant.STOCK_SELL,
                order_volume=amount,
                price_type=(
                    xtconstant.MARKET_SH_CONVERT_5_LIMIT
                    if "SH" in code
                    else xtconstant.MARKET_SZ_CONVERT_5_CANCEL
                ),
                price=0,
                strategy_name="cl",
                order_remark=opt.msg,
            )
            print(f"发起卖出 {code} 数量 {amount} 订单ID {order_id}")
            
            # 循环检查订单成交情况
            for _ in range(10):
                time.sleep(1)
                order = self.xt_trader.query_stock_order(self.acc, order_id)
                if order and order.order_status in [xtconstant.ORDER_SUCCEEDED, xtconstant.ORDER_PART_SUCCEEDED]:
                    price = order.traded_price
                    amount = order.traded_volume
                    break
                if order and order.order_status in [xtconstant.ORDER_FAILED, xtconstant.ORDER_CANCELED]:
                     print(f"订单失败或取消 {order.order_status}")
                     is_real_trade = False # 标记为失败，转模拟记录
                     break
            else:
                 print("卖出超时，尝试撤单")
                 self.xt_trader.cancel_order_stock(self.acc, order_id)
                 # 卖出超时，可能部分成交，这里简化处理，假设未成交部分转模拟或下次再卖
                 # 为保持逻辑简单，若超时未成交，暂不视为成功

        if is_real_trade is False:
            price = tick[code].last
            amount = pos.amount

        msg = (
            f"[{'实盘' if is_real_trade else '模拟'}] 股票卖出 {code}-{stock['name']} 价格 {price} 数量 {amount} 原因 {opt.msg}"
        )
        utils.send_fs_msg("a_trader", "沪深交易提醒", [msg])

        self.zx.del_stock("我的持仓", stock["code"])

        # 保存订单记录到 数据库 中
        db.order_save(
            "a",
            code,
            stock["name"],
            "sell",
            price,
            amount,
            opt.msg,
            datetime.datetime.now(),
        )

        return {"price": price, "amount": amount}

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt):
        # A股普通账户不支持
        return False


if __name__ == "__main__":
    qmt_trader = QMTTraderStock("qmt_trader")

    # 简单的测试逻辑
    account = qmt_trader.xt_trader.query_stock_asset(qmt_trader.acc)
    print(account)

    code = "SZ.002230"
    opt = Operation(code, "buy", "3buy", 0, {}, "测试买入")
    trade_res = qmt_trader.open_buy(code, opt)
    print(trade_res)

    qmt_trader.close()
