import pickle
import time

from chanlun.backtesting.base import Strategy, Operation, POSITION, MarketDatas
from chanlun.cl_interface import *
from chanlun.file_db import fdb


class BackTestTrader(object):
    """
    回测交易（可继承支持实盘）
    """

    def __init__(
        self,
        name,
        mode="signal",
        is_stock=True,
        is_futures=False,
        init_balance=100000,
        fee_rate=0.0005,
        max_pos=10,
        log=None,
    ):
        """
        交易者初始化
        :param name: 交易者名称
        :param mode: 执行模式 signal 测试信号模式，固定金额开仓；trade 实际买卖模式；real 线上实盘交易
        :param is_stock: 是否是股票交易（决定当日是否可以卖出）
        :param is_futures: 是否是期货交易（决定是否可以做空）
        :param init_balance: 初始资金
        :param fee_rate: 手续费比例
        """

        # 策略基本信息
        self.name = name
        self.mode = mode
        self.is_stock = is_stock
        self.is_futures = is_futures
        self.allow_mmds = None

        # 资金情况
        self.balance = init_balance if mode == "trade" else 0
        self.fee_rate = fee_rate
        self.fee_total = 0
        self.max_pos = max_pos

        # 是否打印日志
        self.log = log
        self.log_history = []

        # 时间统计
        self._use_times = {
            "strategy_close": 0,
            "strategy_open": 0,
            "execute": 0,
            "position_record": 0,
        }

        # 策略对象
        self.strategy: Strategy = None

        # 回测数据对象
        self.datas: MarketDatas = None

        # 当前持仓信息
        self.positions: Dict[str, Dict[str, POSITION]] = {}
        self.positions_history: Dict[str, List[POSITION]] = {}
        # 持仓盈亏记录
        self.hold_profit_history = {}
        # 资产历史
        self.balance_history: Dict[str, float] = {}

        # 代码订单信息
        self.orders = {}

        # 统计结果数据
        self.results = {
            "1buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "2buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l2buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "3buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l3buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "down_bi_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "down_xd_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "down_pz_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "down_qs_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "1sell": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "2sell": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l2sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "3sell": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l3sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_bi_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_xd_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_pz_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_qs_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
        }

        # 记录持仓盈亏、资金历史的格式化日期形式
        self.record_dt_format = "%Y-%m-%d %H:%M:%S"

        # 在执行策略前，手动指定执行的开始时间
        self.begin_run_dt: datetime.datetime = None

        # 缓冲区的执行操作，用于在特定时间点批量进行开盘检测后，对要执行的开盘信号再次进行过滤筛选
        self.buffer_opts: List[Operation] = []

    def set_strategy(self, _strategy: Strategy):
        """
        设置策略对象
        :param _strategy:
        :return:
        """
        self.strategy = _strategy

    def set_data(self, _data: MarketDatas):
        """
        设置数据对象
        """
        self.datas = _data

    def save_to_pkl(self, key: str):
        """
        将对象数据保存到 pkl 文件中
        """
        save_infos = {
            "name": self.name,
            "mode": self.mode,
            "is_stock": self.is_stock,
            "is_futures": self.is_futures,
            "allow_mmds": self.allow_mmds,
            "balance": self.balance,
            "fee_rate": self.fee_rate,
            "fee_total": self.fee_total,
            "max_pos": self.max_pos,
            "positions": self.positions,
            "positions_history": self.positions_history,
            "hold_profit_history": self.hold_profit_history,
            "balance_history": self.balance_history,
            "orders": self.orders,
            "results": self.results,
        }
        if key is not None:
            fdb.cache_pkl_to_file(key, save_infos)
        return save_infos

    def load_from_pkl(self, key: str, save_infos: dict = None):
        """
        从 pkl 文件 中恢复之前的数据
        """
        if save_infos is None:
            save_infos = fdb.cache_pkl_from_file(key)
            if save_infos is None:
                return False
        if "name" in save_infos.keys():
            self.name = save_infos["name"]
            self.mode = save_infos["mode"]
            self.is_stock = save_infos["is_stock"]
            self.is_futures = save_infos["is_stock"]
            self.allow_mmds = save_infos["allow_mmds"]
            self.balance = save_infos["balance"]
            self.fee_rate = save_infos["fee_rate"]
            self.fee_total = save_infos["fee_total"]
            self.max_pos = save_infos["max_pos"]
            self.results = save_infos["results"]

        self.positions = save_infos["positions"]
        self.positions_history = save_infos["positions_history"]
        self.hold_profit_history = save_infos["hold_profit_history"]
        self.balance_history = save_infos["balance_history"]
        self.orders = save_infos["orders"]

        return True

    def get_price(self, code):
        """
        回测中方法，获取股票代码当前的价格，根据最小周期 k 线收盘价
        """
        price_info = self.datas.last_k_info(code)
        return price_info

    def get_now_datetime(self):
        """
        获取当前时间
        """
        if self.mode == "real":
            return datetime.datetime.now()
        # 回测时用回测的当前时间
        return self.datas.now_date

    # 运行的唯一入口
    def run(self, code, is_filter=False):
        # 如果设置开始执行时间，并且当前时间小于等于设置的时间，则不执行策略
        if (
            self.begin_run_dt is not None
            and self.begin_run_dt >= self.get_now_datetime()
        ):
            return True

        # 优先检查持仓情况
        if code in self.positions:
            for mmd in self.positions[code]:
                pos = self.positions[code][mmd]
                if pos.balance == 0:
                    continue

                _time = time.time()
                opt = self.strategy.close(
                    code=code, mmd=mmd, pos=pos, market_data=self.datas
                )
                self._use_times["strategy_close"] += time.time() - _time

                if opt is False or opt is None:
                    continue
                if isinstance(opt, Operation):
                    opt.code = code
                    opt = [opt]
                for o in opt:
                    o.code = code
                    self.execute(code, o)

        # 再执行检查机会方法
        poss: Dict[str, POSITION] = (
            self.positions[code] if code in self.positions.keys() else {}
        )
        poss = {k: v for k, v in poss.items() if v.balance > 0}  # 只获取有持仓的记录

        _time = time.time()
        opts = self.strategy.open(code=code, market_data=self.datas, poss=poss)
        self._use_times["strategy_open"] += time.time() - _time

        for opt in opts:
            opt.code = code
            if is_filter:
                # 如果是过滤模式，将操作记录到缓冲区，等待批量执行
                self.buffer_opts.append(opt)
            else:
                self.execute(code, opt)

        return True

    def run_buffer_opts(self):
        """
        执行缓冲区的操作
        """
        for opt in self.buffer_opts:
            self.execute(opt.code, opt)
        self.buffer_opts = []

    # 运行结束，统一清仓
    def end(self):
        for code in self.positions:
            for mmd in self.positions[code]:
                pos = self.positions[code][mmd]
                if pos.balance > 0:
                    self.execute(code, Operation(opt="sell", mmd=mmd, msg="退出"))
        return True

    def update_position_record(self):
        """
        更新所有持仓的盈亏情况
        """
        total_hold_profit = 0
        for code in self.positions.keys():
            now_profit, hold_balance = self.position_record(code)
            if self.mode == "trade":
                total_hold_profit += now_profit + hold_balance
            else:
                total_hold_profit += now_profit
        now_datetime = self.get_now_datetime().strftime(self.record_dt_format)
        self.balance_history[now_datetime] = total_hold_profit + self.balance

    def position_record(self, code: str) -> Tuple[float, float]:
        """
        持仓记录更新
        :param code:
        :return: 返回持仓的总金额（包括持仓盈亏）
        """
        s_time = time.time()

        hold_balance = 0
        now_profit = 0
        if code not in self.hold_profit_history.keys():
            self.hold_profit_history[code] = {}

        if code in self.positions.keys():
            for pos in self.positions[code].values():
                if pos.balance == 0:
                    continue
                price_info = self.get_price(code)
                if pos.type == "做多":
                    high_profit_rate = round(
                        (
                            (price_info["high"] - pos.price)
                            / pos.price
                            * (pos.balance * pos.now_pos_rate)
                            + pos.profit
                        )
                        / pos.balance
                        * 100,
                        4,
                    )
                    low_profit_rate = round(
                        (
                            (price_info["low"] - pos.price)
                            / pos.price
                            * (pos.balance * pos.now_pos_rate)
                            + pos.profit
                        )
                        / pos.balance
                        * 100,
                        4,
                    )
                    pos.max_profit_rate = max(pos.max_profit_rate, high_profit_rate)
                    pos.max_loss_rate = min(pos.max_loss_rate, low_profit_rate)

                    pos.profit_rate = round(
                        (
                            (price_info["close"] - pos.price)
                            / pos.price
                            * (pos.balance * pos.now_pos_rate)
                            + pos.profit
                        )
                        / pos.balance
                        * 100,
                        4,
                    )
                    now_profit += pos.profit_rate / 100 * pos.balance
                if pos.type == "做空":
                    high_profit_rate = round(
                        (
                            (pos.price - price_info["low"])
                            / pos.price
                            * (pos.balance * pos.now_pos_rate)
                            + pos.profit
                        )
                        / pos.balance
                        * 100,
                        4,
                    )
                    low_profit_rate = round(
                        (
                            (pos.price - price_info["high"])
                            / pos.price
                            * (pos.balance * pos.now_pos_rate)
                            + pos.profit
                        )
                        / pos.balance
                        * 100,
                        4,
                    )
                    pos.max_profit_rate = max(pos.max_profit_rate, high_profit_rate)
                    pos.max_loss_rate = min(pos.max_loss_rate, low_profit_rate)

                    pos.profit_rate = round(
                        (
                            (pos.price - price_info["close"])
                            / pos.price
                            * (pos.balance * pos.now_pos_rate)
                            + pos.profit
                        )
                        / pos.balance
                        * 100,
                        4,
                    )
                    now_profit += pos.profit_rate / 100 * pos.balance

                # 当前盈亏，需要加上锁仓的盈亏金额
                for lock_pos in pos.lock_positions.values():
                    if lock_pos.balance == 0:
                        now_profit += (lock_pos.amount * lock_pos.price) * (
                            lock_pos.profit_rate / 100
                        )
                    else:
                        if lock_pos.balance != 0 and lock_pos.type == "做多":
                            now_profit += (
                                (price_info["close"] - lock_pos.price) / pos.price
                            ) * (lock_pos.amount * lock_pos.price)
                        elif lock_pos.balance != 0 and lock_pos.type == "做空":
                            now_profit += (
                                (lock_pos.price - price_info["close"]) / pos.price
                            ) * (lock_pos.amount * lock_pos.price)
                        hold_balance += lock_pos.balance

                hold_balance += pos.balance * pos.now_pos_rate
        self.hold_profit_history[code][
            self.get_now_datetime().strftime(self.record_dt_format)
        ] = now_profit

        self._use_times["position_record"] += time.time() - s_time
        return now_profit, hold_balance

    def position_codes(self):
        """
        获取当前持仓中的股票代码，根据开仓时间排序，最新开仓的在前面
        """
        poss = []
        for _c in self.positions.keys():
            for mmd in self.positions[_c]:
                _pos = self.positions[_c][mmd]
                if _pos.balance > 0:
                    poss.append(
                        {
                            "code": _pos.code,
                            "mmd": _pos.mmd,
                            "open_datetime": _pos.open_datetime,
                            "close_datetime": _pos.close_datetime,
                            "type": _pos.type,
                            "price": _pos.price,
                            "amount": _pos.amount,
                            "loss_price": _pos.loss_price,
                            "profit_rate": _pos.profit_rate,
                            "max_profit_rate": _pos.max_profit_rate,
                            "max_loss_rate": _pos.max_loss_rate,
                            "open_msg": _pos.open_msg,
                            "close_msg": _pos.close_msg,
                        }
                    )

        if not poss:
            return []

        poss = pd.DataFrame(poss)
        poss = poss.sort_values("open_datetime", ascending=False)
        codes = list(poss["code"].to_numpy())
        return codes

    def hold_positions(self):
        """
        返回所有持仓记录
        """
        poss: List[POSITION] = []
        for code in self.positions.keys():
            for mmd, pos in self.positions[code].items():
                if pos.balance != 0:
                    poss.append(pos)
        return poss

    # 查询代码买卖点的持仓信息
    def query_code_mmd_pos(self, code: str, mmd: str) -> POSITION:
        if code in self.positions:
            if mmd in self.positions[code]:
                return self.positions[code][mmd]
            else:
                self.positions[code][mmd] = POSITION(code=code, mmd=mmd)
        else:
            self.positions[code] = {mmd: POSITION(code=code, mmd=mmd)}
        return self.positions[code][mmd]

    def reset_pos(self, code: str, mmd: str):
        # 增加进入历史
        self.positions[code][mmd].close_datetime = self.get_now_datetime().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if code not in self.positions_history:
            self.positions_history[code] = []
        self.positions_history[code].append(self.positions[code][mmd])

        self.positions[code][mmd] = POSITION(code=code, mmd=mmd)
        return

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float = None):
        if self.mode == "signal":
            use_balance = 100000 * min(1.0, opt.pos_rate)
            price = self.get_price(code)["close"]
            amount = round((use_balance / price) * 0.99, 4)
            return {"price": price, "amount": amount}
        else:
            if len(self.hold_positions()) >= self.max_pos:
                return False
            price = self.get_price(code)["close"]

            if amount is None:
                use_balance = (
                    self.balance / (self.max_pos - len(self.hold_positions()))
                ) * 0.99
                use_balance *= min(1.0, opt.pos_rate)
                amount = use_balance / price
            else:
                use_balance = price * amount

            if amount < 0:
                return False
            if use_balance > self.balance:
                self._print_log("%s - %s 做多开仓 资金余额不足" % (code, opt.mmd))
                return False

            fee = use_balance * self.fee_rate
            self.balance -= use_balance + fee
            self.fee_total += fee

            return {"price": price, "amount": amount}

    # 做空卖出
    def open_sell(self, code, opt: Operation, amount: float = None):
        if self.mode == "signal":
            use_balance = 100000 * min(1.0, opt.pos_rate)
            price = self.get_price(code)["close"]
            amount = round((use_balance / price) * 0.99, 4)
            return {"price": price, "amount": amount}
        else:
            if len(self.hold_positions()) >= self.max_pos:
                return False
            price = self.get_price(code)["close"]

            if amount is None:
                use_balance = (
                    self.balance / (self.max_pos - len(self.hold_positions()))
                ) * 0.99
                use_balance *= min(1.0, opt.pos_rate)
                amount = use_balance / price
            else:
                use_balance = price * amount

            if amount < 0:
                return False

            if use_balance > self.balance:
                self._print_log("%s - %s 做空开仓 资金余额不足" % (code, opt.mmd))
                return False

            fee = use_balance * self.fee_rate
            self.balance -= use_balance + fee
            self.fee_total += fee

            return {"price": price, "amount": amount}

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt: Operation):
        # 如果操作中设置了止损价格，则按照止损价格执行，否则按照最新价格执行
        if opt.loss_price != 0:
            price = opt.loss_price
        else:
            price = self.get_price(code)["close"]

        amount = pos.amount * opt.pos_rate

        if self.mode == "signal":
            net_profit = (price * amount) - (pos.price * amount)
            self.balance += net_profit
            return {"price": price, "amount": amount}
        else:
            hold_balance = price * amount
            fee = hold_balance * self.fee_rate
            self.balance += hold_balance - fee
            self.fee_total += fee
            return {"price": price, "amount": amount}

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt: Operation):
        # 如果操作中设置了止损价格，则按照止损价格执行，否则按照最新价格执行
        if opt.loss_price != 0:
            price = opt.loss_price
        else:
            price = self.get_price(code)["close"]

        amount = pos.amount * opt.pos_rate

        if self.mode == "signal":
            net_profit = (pos.price * amount) - (price * amount)
            self.balance += net_profit
            return {"price": price, "amount": amount}
        else:
            hold_balance = price * amount
            pos_balance = pos.price * amount
            profit = pos_balance - hold_balance
            fee = hold_balance * self.fee_rate
            self.balance += pos_balance + profit - fee
            self.fee_total += fee

            return {"price": price, "amount": amount}

    # 打印日志信息
    def _print_log(self, msg):
        self.log_history.append(msg)
        if self.log:
            self.log(msg)
        return

    # 执行操作
    def execute(self, code, opt: Operation):
        _time = time.time()
        try:
            opt_mmd = opt.mmd
            # 检查是否在允许做的买卖点上
            if self.allow_mmds is not None and opt_mmd not in self.allow_mmds:
                return True

            pos = self.query_code_mmd_pos(code, opt_mmd)
            res = None
            order_type = None

            # 期货，进行锁仓操作
            if self.is_futures and opt.opt == "lock":
                return self.lock_position(code, pos, opt)
            # 期货，进行平仓锁仓操作
            if self.is_futures and opt.opt == "unlock":
                return self.unlock_position(code, pos, opt)

            # 买点，买入，开仓做多
            if "buy" in opt_mmd and opt.opt == "buy":
                # 判断当前是否满仓
                if pos.now_pos_rate >= 1:
                    return False
                # 唯一key判断
                if opt.key in pos.open_keys.keys():
                    return False
                # 修正错误的开仓比例
                opt.pos_rate = min(1.0 - pos.now_pos_rate, opt.pos_rate)

                res = self.open_buy(code, opt)
                if res is False:
                    return False

                pos.type = "做多"
                pos.price = res["price"]
                pos.amount = res["amount"]
                pos.balance = res["price"] * res["amount"]
                pos.loss_price = opt.loss_price
                pos.open_date = self.get_now_datetime().strftime("%Y-%m-%d")
                pos.open_datetime = self.get_now_datetime().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                pos.open_msg = opt.msg
                pos.info = opt.info
                pos.now_pos_rate += min(1.0, opt.pos_rate)
                pos.open_keys[opt.key] = opt.pos_rate

                order_type = "open_long"

                self._print_log(
                    f"[{code} - {self.get_now_datetime()}] // {opt_mmd} 做多买入（{res['price']} - {res['amount']}），原因： {opt.msg}"
                )

            # 卖点，买入，开仓做空（期货）
            if self.is_futures and "sell" in opt_mmd and opt.opt == "buy":
                # 判断当前是否满仓
                if pos.now_pos_rate >= 1:
                    return False
                # 唯一key判断
                if opt.key in pos.open_keys.keys():
                    return False
                # 修正错误的开仓比例
                opt.pos_rate = min(1.0 - pos.now_pos_rate, opt.pos_rate)

                res = self.open_sell(code, opt)
                if res is False:
                    return False
                pos.type = "做空"
                pos.price = res["price"]
                pos.amount = res["amount"]
                pos.balance = res["price"] * res["amount"]
                pos.loss_price = opt.loss_price
                pos.open_date = self.get_now_datetime().strftime("%Y-%m-%d")
                pos.open_datetime = self.get_now_datetime().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                pos.open_msg = opt.msg
                pos.info = opt.info
                pos.now_pos_rate += min(1.0, opt.pos_rate)
                pos.open_keys[opt.key] = opt.pos_rate

                order_type = "open_short"

                self._print_log(
                    f"[{code} - {self.get_now_datetime()}] // {opt_mmd} 做空卖出（{res['price']} - {res['amount']}），原因： {opt.msg}"
                )

            # 卖点，卖出，平仓做空（期货）
            if self.is_futures and "sell" in opt_mmd and opt.opt == "sell":
                # 判断当前是否有仓位
                if pos.now_pos_rate <= 0:
                    return False
                # 唯一key判断
                if opt.key in pos.close_keys.keys():
                    return False
                # 修正错误的平仓比例
                opt.pos_rate = (
                    pos.now_pos_rate
                    if pos.now_pos_rate < opt.pos_rate
                    else opt.pos_rate
                )

                if self.is_stock and pos.open_date == self.get_now_datetime().strftime(
                    "%Y-%m-%d"
                ):
                    # 股票交易，当日不能卖出
                    return False
                if len(pos.lock_positions) > 0:
                    # 有锁仓记录，先平仓锁仓记录
                    self.unlock_position(code, pos, opt)

                res = self.close_sell(code, pos, opt)
                if res is False:
                    return False

                sell_balance = res["price"] * res["amount"]
                hold_balance = pos.balance * opt.pos_rate

                # 做空收益：持仓金额 减去 卖出金额的 差价，加上锁仓的收益 - 手续费（双边手续费）
                fee_use = sell_balance * self.fee_rate * 2
                profit = (
                    hold_balance
                    - sell_balance
                    + sum(
                        [
                            (_p.amount * _p.price) * (_p.profit_rate / 100)
                            for _p in pos.lock_positions.values()
                        ]
                    )
                    - fee_use
                )
                profit_rate = round((profit / hold_balance) * 100, 2)

                self._print_log(
                    "[%s - %s] // %s 平仓做空（%s - %s） 盈亏：%s (%.2f%%)，原因： %s"
                    % (
                        code,
                        self.get_now_datetime(),
                        opt_mmd,
                        res["price"],
                        res["amount"],
                        profit,
                        profit_rate,
                        opt.msg,
                    )
                )

                if self.mode == "signal":
                    self.fee_total += fee_use

                pos.profit += profit
                pos.now_pos_rate -= opt.pos_rate
                pos.close_keys[opt.key] = opt.pos_rate

                if pos.now_pos_rate <= 0:
                    if pos.profit > 0:
                        # 盈利
                        self.results[opt_mmd]["win_num"] += 1
                        self.results[opt_mmd]["win_balance"] += pos.profit
                    else:
                        # 亏损
                        self.results[opt_mmd]["loss_num"] += 1
                        self.results[opt_mmd]["loss_balance"] += abs(pos.profit)

                    profit_rate = round((pos.profit / pos.balance) * 100, 2)
                    pos.profit_rate = profit_rate
                    pos.close_msg = opt.msg
                    # 清空持仓
                    self.reset_pos(code, opt_mmd)

                order_type = "close_short"

            # 买点，卖出，平仓做多
            if "buy" in opt_mmd and opt.opt == "sell":
                # 判断当前是否有仓位
                if pos.now_pos_rate <= 0:
                    return False
                # 唯一key判断
                if opt.key in pos.close_keys.keys():
                    return False
                # 修正错误的平仓比例
                opt.pos_rate = (
                    pos.now_pos_rate
                    if pos.now_pos_rate < opt.pos_rate
                    else opt.pos_rate
                )

                if self.is_stock and pos.open_date == self.get_now_datetime().strftime(
                    "%Y-%m-%d"
                ):
                    # 股票交易，当日不能卖出
                    return False
                if len(pos.lock_positions) > 0:
                    # 有锁仓记录，先平仓锁仓记录
                    self.unlock_position(code, pos, opt)

                res = self.close_buy(code, pos, opt)
                if res is False:
                    return False

                sell_balance = res["price"] * res["amount"]
                hold_balance = pos.balance * opt.pos_rate
                # 做出收益：卖出金额 减去 持有金额的 差价，加上锁仓的收益 - 手续费（双边手续费）
                fee_use = sell_balance * self.fee_rate * 2
                profit = (
                    sell_balance
                    - hold_balance
                    + sum(
                        [
                            (_p.amount * _p.price) * (_p.profit_rate / 100)
                            for _p in pos.lock_positions.values()
                        ]
                    )
                    - fee_use
                )
                profit_rate = round((profit / hold_balance) * 100, 2)

                self._print_log(
                    "[%s - %s] // %s 平仓做多（%s - %s） 盈亏：%s  (%.2f%%)，原因： %s"
                    % (
                        code,
                        self.get_now_datetime(),
                        opt_mmd,
                        res["price"],
                        res["amount"],
                        profit,
                        profit_rate,
                        opt.msg,
                    )
                )

                pos.profit += profit
                pos.now_pos_rate -= opt.pos_rate
                pos.close_keys[opt.key] = opt.pos_rate

                if self.mode == "signal":
                    self.fee_total += fee_use

                if pos.now_pos_rate <= 0:
                    if pos.profit > 0:
                        # 盈利
                        self.results[opt_mmd]["win_num"] += 1
                        self.results[opt_mmd]["win_balance"] += pos.profit
                    else:
                        # 亏损
                        self.results[opt_mmd]["loss_num"] += 1
                        self.results[opt_mmd]["loss_balance"] += abs(pos.profit)

                    profit_rate = round((pos.profit / pos.balance) * 100, 2)
                    pos.profit_rate = profit_rate
                    pos.close_msg = opt.msg
                    # 清空持仓
                    self.reset_pos(code, opt_mmd)

                order_type = "close_long"

            if res:
                # 记录订单信息
                if code not in self.orders:
                    self.orders[code] = []
                self.orders[code].append(
                    {
                        "datetime": self.get_now_datetime(),
                        "type": order_type,
                        "price": res["price"],
                        "amount": res["amount"],
                        "info": opt.msg,
                    }
                )
                return True

            return False
        finally:
            self._use_times["execute"] += time.time() - _time

    def lock_position(self, code, pos: POSITION, opt: Operation):
        """
        进行锁仓操作
        """
        res = None
        order_type = None
        # 检查是否已经进行锁仓
        lock_balance = (
            0
            if len(pos.lock_positions) == 0
            else max([_p.balance for _p in pos.lock_positions.values()])
        )
        if lock_balance != 0:
            return False
        if "buy" in pos.mmd:
            # 之前仓位买入做多，锁仓进行相反的操作
            res = self.open_sell(
                code,
                Operation(opt="buy", mmd="1sell", loss_price=0, info={}, msg="锁仓"),
                pos.amount,
            )
            order_type = "open_short"
            if res is False:
                self._print_log(f"{code} 进行锁仓失败")
                return None
            lock_position = POSITION(
                code,
                mmd="1sell",
                type="做空",
                balance=res["price"] * res["amount"],
                price=res["price"],
                amount=res["amount"],
                loss_price=0,
                open_date=self.get_now_datetime().strftime("%Y-%m-%d"),
                open_datetime=self.get_now_datetime().strftime("%Y-%m-%d %H:%M:%S"),
                open_msg="锁仓",
            )
            pos.lock_positions[lock_position.open_datetime] = lock_position
            self._print_log(
                f"[{code} - {self.get_now_datetime()}] // 锁仓 做空卖出（{res['price']} - {res['amount']}）"
            )
        elif "sell" in pos.mmd:
            # 之前仓位卖出做空，锁仓进行相反的操作
            res = self.open_buy(
                code,
                Operation(opt="buy", mmd="1buy", loss_price=0, info={}, msg="锁仓"),
                pos.amount,
            )
            order_type = "open_long"
            if res is False:
                self._print_log(f"{code} 进行锁仓失败")
                return None
            lock_position = POSITION(
                code,
                mmd="1buy",
                type="做多",
                balance=res["price"] * res["amount"],
                price=res["price"],
                amount=res["amount"],
                loss_price=0,
                open_date=self.get_now_datetime().strftime("%Y-%m-%d"),
                open_datetime=self.get_now_datetime().strftime("%Y-%m-%d %H:%M:%S"),
                open_msg="锁仓",
            )
            pos.lock_positions[lock_position.open_datetime] = lock_position
            self._print_log(
                f"[{code} - {self.get_now_datetime()}] // 锁仓 做多买入（{res['price']} - {res['amount']}）"
            )

        # 记录订单信息
        if code not in self.orders:
            self.orders[code] = []
        self.orders[code].append(
            {
                "datetime": self.get_now_datetime(),
                "type": order_type,
                "price": res["price"],
                "amount": res["amount"],
                "info": "锁仓",
            }
        )
        return True

    def unlock_position(self, code, pos: POSITION, opt: Operation):
        """
        平仓锁仓操作
        """
        # 检查是否已经进行锁仓
        res = None
        order_type = None
        lock_balance = (
            0
            if len(pos.lock_positions) == 0
            else max([_p.balance for _p in pos.lock_positions.values()])
        )
        if lock_balance == 0:
            return False
        lock_position = [_p for _p in pos.lock_positions.values() if _p.balance > 0]
        lock_position = lock_position[0]
        if "buy" in lock_position.mmd:
            # 锁仓的持仓是做多买入，进行平仓
            res = self.close_buy(
                code,
                lock_position,
                Operation(
                    opt="sell",
                    mmd=lock_position.mmd,
                    loss_price=0,
                    info={},
                    msg="平仓锁仓",
                ),
            )
            order_type = "close_long"
            if res is False:
                self._print_log(f"{code} 进行平仓锁仓失败")
                return False
            lock_position.profit_rate = (
                (res["price"] - lock_position.price) / lock_position.price * 100
            )
            lock_position.balance = 0
            lock_position.close_datetime = self.get_now_datetime().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            self._print_log(
                f"[{code} - {self.get_now_datetime()}] // 平仓锁仓 平仓卖出（{res['price']} - {res['amount']}）"
            )
        elif "sell" in lock_position.mmd:
            # 锁仓的持仓是做空卖出，进行平仓
            res = self.close_sell(
                code,
                lock_position,
                Operation(
                    opt="sell",
                    mmd=lock_position.mmd,
                    loss_price=0,
                    info={},
                    msg="平仓锁仓",
                ),
            )
            order_type = "close_short"
            if res is False:
                self._print_log(f"{code} 进行平仓锁仓失败")
                return False
            lock_position.profit_rate = (
                (lock_position.price - res["price"]) / lock_position.price * 100
            )
            lock_position.balance = 0
            lock_position.close_datetime = self.get_now_datetime().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            self._print_log(
                f"[{code} - {self.get_now_datetime()}] // 平仓锁仓 平仓买入（{res['price']} - {res['amount']}）"
            )
        # 记录订单信息
        if code not in self.orders:
            self.orders[code] = []
        self.orders[code].append(
            {
                "datetime": self.get_now_datetime(),
                "type": order_type,
                "price": res["price"],
                "amount": res["amount"],
                "info": "平仓锁仓",
            }
        )
        return True
