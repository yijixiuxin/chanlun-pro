import datetime

from chanlun import utils, zixuan
from chanlun.backtesting.backtest_trader import BackTestTrader
from chanlun.backtesting.base import POSITION, Operation
from chanlun.db import db
from chanlun.exchange.exchange_qmt import ExchangeQMT
from chanlun.trader.qmt_gui import QMTGuiTrader


class QMTTraderStock(BackTestTrader):
    """
    QMT A股票交易对象（pyautogui + 剪贴板/OCR 操作 QMT 交易端）

    大QMT桥接（Redis/外部文件/网络）被禁用后，交易不再走桥接服务，改由
    QMTGuiTrader 操作交易端界面完成：
        买卖 -> trade_target（先查持仓，再按目标数量下单，未成交撤单重下）
        持仓 / 委托 / 资金 -> 列表右键「复制数据」后读剪贴板

    客户端路径、资金账号、交易密码由调用方传入，不在代码中写死。
    self.ex（ExchangeQMT）保留给仍可用的行情接口，交易不再经过它。
    """

    def __init__(self, name, account_id=None, log=None, exe_path=None, password=None):
        super().__init__(name=name, mode="online", market="a", log=log)

        if not account_id or not exe_path or not password:
            raise ValueError(
                "必须提供 QMT 客户端路径、资金账号、交易密码："
                "QMTTraderStock(name, account_id=..., exe_path=..., password=...)"
            )

        self.ex = ExchangeQMT(account_id=account_id)
        self.gui = QMTGuiTrader(
            exe_path=exe_path, account=account_id, password=password
        )

        self.zx = zixuan.ZiXuan("a")
        self.zx_group = "QMT交易"

        # 最大持仓数量，当前持仓大于这个数量，则不进行实际交易
        self.max_pos = 3

    def close(self):
        self.gui.close()

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float | None = None):
        # 下单面板带出的名称/最新价就是交易端实时行情，不依赖桥接
        info = self.gui.panel_info(code, want_buy=True)
        if not info.code or info.last <= 0:
            return False

        positions = self.gui.positions()
        # 持仓数量达到上限，不再开新仓
        if len(positions) >= self.max_pos:
            return False

        # 计算可买数量：按可用资金平均分配给剩余仓位
        account = self.gui.balance()
        slots = self.max_pos - len(positions)
        balance = round(account["cash"] * 0.98 / slots, 0)
        buy_amount = int(balance / info.last / 100) * 100
        if buy_amount < 100:
            return False

        # 按目标持仓数量下单（当前持仓 + 本次买入数量）
        current = next((p["amount"] for p in positions if p["code"] == info.code), 0)
        res = self.gui.trade_target(
            info.code, "buy", current + buy_amount, price=info.last
        )
        if not res["done"] or res["amount"] <= 0:
            return False

        price, buy_amount = res["price"], res["amount"]
        msg = (
            f"股票买入 {code}-{info.name} 价格 {price} 数量 {buy_amount} 原因 {opt.msg}"
        )
        utils.send_fs_msg("a_trader", "沪深交易提醒", [msg])

        self.zx.add_stock("我的持仓", info.code, info.name)

        # 保存订单记录到 数据库 中，这样可以在图表中标识出买卖卖出的位置
        db.order_save(
            "a",
            code,
            info.name,
            "buy",
            price,
            buy_amount,
            opt.msg,
            datetime.datetime.now(),
        )

        return {"price": price, "amount": buy_amount}

    # 做空卖出（A股不支持做空）
    def open_sell(self, code, opt: Operation, amount: float | None = None):
        return False

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt):
        positions = self.gui.positions(code)
        hold_pos = positions[0] if positions else None
        if hold_pos is None or hold_pos["can_sell_amount"] <= 0:
            return False

        sell_amount = int(min(pos.amount, hold_pos["can_sell_amount"]))
        if sell_amount <= 0:
            return False

        # 按目标持仓数量下单（当前持仓 - 本次卖出数量）
        res = self.gui.trade_target(code, "sell", hold_pos["amount"] - sell_amount)
        if not res["done"] or res["amount"] <= 0:
            return False

        price, sell_amount = res["price"], res["amount"]
        msg = f"股票卖出 {code}-{hold_pos['name']} 价格 {price} 数量 {sell_amount} 原因 {opt.msg}"
        utils.send_fs_msg("a_trader", "沪深交易提醒", [msg])

        self.zx.del_stock("我的持仓", hold_pos["code"])

        # 保存订单记录到 数据库 中
        db.order_save(
            "a",
            code,
            hold_pos["name"],
            "sell",
            price,
            sell_amount,
            opt.msg,
            datetime.datetime.now(),
        )

        return {"price": price, "amount": sell_amount}

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt):
        return False
