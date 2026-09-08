import datetime

from chanlun import utils, zixuan
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import POSITION, Operation
from chanlun.db import db
from chanlun.exchange.exchange_qmt import ExchangeQMT


class QMTTraderStock(BackTestTrader):
    """
    QMT A股票交易对象（通过大QMT桥接服务完成交易）

    交易接口（balance/positions/order）均由 ExchangeQMT 实现，
    内部先查询持仓，再按目标数量下单，未成交/部分成交会撤单重下，直到达到目标。
    """

    def __init__(self, name, account_id=None, log=None):
        super().__init__(name=name, mode="online", market="a", log=log)
        self.ex = ExchangeQMT(account_id=account_id)

        self.zx = zixuan.ZiXuan("a")
        self.zx_group = "QMT交易"

        # 最大持仓数量，当前持仓大于这个数量，则不进行实际交易
        self.max_pos = 3

    def close(self):
        pass

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float | None = None):
        tick = self.ex.ticks([code])
        if code not in tick:
            return False
        stock = self.ex.stock_info(code)
        if stock is None:
            return False

        positions = self.ex.positions()
        hold_pos = next((_p for _p in positions if _p["code"] == code), None)
        current = hold_pos["amount"] if hold_pos else 0

        # 持仓数量达到上限，不再开新仓
        if len(positions) >= self.max_pos:
            return False

        # 计算可买数量：按可用资金分配
        account = self.ex.balance()
        balance = round((account["cash"] * 0.98) / (self.max_pos - len(positions)), 0)
        price = tick[code].last
        buy_amount = int(balance / price / 100) * 100 if price > 0 else 0
        if buy_amount < 100:
            return False

        target = current + buy_amount
        res = self.ex.order(code, "buy", target)
        if res is False:
            return False

        price = res["price"] or price
        buy_amount = res["amount"] or 0
        if buy_amount <= 0:
            return False

        msg = f"股票买入 {code}-{stock['name']} 价格 {price} 数量 {buy_amount} 原因 {opt.msg}"
        utils.send_fs_msg("a_trader", "沪深交易提醒", [msg])

        self.zx.add_stock("我的持仓", stock["code"], stock["name"])

        # 保存订单记录到 数据库 中，这样可以在图表中标识出买卖卖出的位置
        db.order_save(
            "a",
            code,
            stock["name"],
            "buy",
            price,
            buy_amount,
            opt.msg,
            datetime.datetime.now(),  # noqa: DTZ005
        )

        return {"price": price, "amount": buy_amount}

    # 做空卖出（A股不支持做空）
    def open_sell(self, code, opt: Operation, amount: float | None = None):
        return False

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt):
        tick = self.ex.ticks([code])
        if code not in tick:
            return False
        stock = self.ex.stock_info(code)
        if stock is None:
            return False

        positions = self.ex.positions(code)
        hold_pos = positions[0] if positions else None
        if hold_pos is None or hold_pos["can_sell_amount"] <= 0:
            return False

        current = hold_pos["amount"]
        sell_amount = min(pos.amount, hold_pos["can_sell_amount"])
        if sell_amount <= 0:
            return False

        res = self.ex.order(code, "sell", max(0, current - sell_amount))
        if res is False:
            return False

        price = res["price"] or tick[code].last
        sell_amount = res["amount"] or 0
        if sell_amount <= 0:
            return False

        msg = f"股票卖出 {code}-{stock['name']} 价格 {price} 数量 {sell_amount} 原因 {opt.msg}"
        utils.send_fs_msg("a_trader", "沪深交易提醒", [msg])

        self.zx.del_stock("我的持仓", stock["code"])

        # 保存订单记录到 数据库 中
        db.order_save(
            "a",
            code,
            stock["name"],
            "sell",
            price,
            sell_amount,
            opt.msg,
            datetime.datetime.now(),  # noqa: DTZ005
        )

        return {"price": price, "amount": sell_amount}

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt):
        return False


if __name__ == "__main__":
    qmt_trader = QMTTraderStock("qmt_trader")

    account = qmt_trader.ex.balance()
    print(account)

    code = "SH.603755"
    opt = Operation(code, "buy", "3buy", 0, {}, "测试买入")
    trade_res = qmt_trader.open_buy(code, opt)
    print(trade_res)

    qmt_trader.close()
