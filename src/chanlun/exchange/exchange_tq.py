import datetime
import math
import threading
import time
from typing import Dict, List, Union

import pandas as pd
import pytz
import tqsdk
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random
from tqsdk.objs import Account, Position, Quote

from chanlun import config, fun
from chanlun.exchange.exchange import Exchange, Tick


@fun.singleton
class ExchangeTq(Exchange):
    """
    天勤期货行情
    """

    g_all_stocks = []
    g_api: tqsdk.TqApi = None
    g_account: tqsdk.TqAccount = None
    g_account_enable: bool = False

    def __init__(self, use_simulate_account=True):
        # 是否使用模拟账号，进行交易测试（这种模式无需设置实盘账号）
        self.use_simulate_account = use_simulate_account

        # 命令任务队列
        self.command_tasks: List[str] = []
        # 记录已经收到并执行的命令
        self.past_commands = []
        # K线返回对象
        self.res_klines: Dict[str, pd.DataFrame] = {}
        # Tick 返回对象
        self.res_ticks: Dict[str, Quote] = {}

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        # 运行的子进程
        self.stop_thread = False
        self.t = threading.Thread(target=self.thread_run_tasks)
        self.t.start()

    def default_code(self):
        return "KQ.m@SHFE.rb"

    def support_frequencys(self):
        return {
            "w": "W",
            "d": "D",
            "120m": "2H",
            "60m": "1H",
            "30m": "30m",
            "15m": "15m",
            "10m": "10m",
            "6m": "6m",
            "5m": "5m",
            "3m": "3m",
            "2m": "2m",
            "1m": "1m",
            "30s": "30s",
            "10s": "10s",
        }

    def close_task_thread(self):
        self.stop_thread = True
        time.sleep(1)
        return True

    def restart_task_thread(self):
        self.close_task_thread()
        time.sleep(2)
        self.stop_thread = False
        self.t = threading.Thread(target=self.thread_run_tasks)
        self.t.start()
        return True

    def thread_run_tasks(self):
        """
        子进程发送并更新行情请求
        """
        print("启动天勤子进程任务-更新K线与tick数据")

        async def get_tick(code):
            quote = await self.get_api().get_quote(code)
            self.res_ticks[code] = quote
            async with self.get_api().register_update_notify() as update_chan:
                async for _ in update_chan:
                    if self.get_api().is_changing(quote):
                        # print(f'Tick {code} 更新信息：', quote)
                        self.res_ticks[code] = quote

        async def get_kline(code, frequency):
            kline = await self.get_api().get_kline_serial(
                code, duration_seconds=frequency, data_length=8000
            )
            self.res_klines[f"{code}_{frequency}"] = kline
            async with self.get_api().register_update_notify() as update_chan:
                async for _ in update_chan:
                    if self.get_api().is_changing(kline):
                        # print(f'Kline {code} {frequency} 更新信息：', len(kline))
                        self.res_klines[f"{code}_{frequency}"] = kline

        def reset_api(force: bool = False):
            print("天勤 : 重启服务")
            try:
                self.close_api()
            except Exception:
                pass
            self.res_klines = {}
            self.res_ticks = {}
            self.past_commands = []

        while True:
            try:
                if self.stop_thread:
                    print("退出天勤任务子线程")
                    break
                while len(self.command_tasks) > 0:
                    commands = self.command_tasks.pop()
                    if commands in self.past_commands:
                        continue
                    self.past_commands.append(commands)
                    commands = commands.split(":")
                    if commands[0] == "kline":
                        print("执行 Kline 命令：", ":".join(commands))
                        self.get_api().create_task(
                            get_kline(commands[1], int(commands[2]))
                        )
                    elif commands[0] == "tick":
                        print("执行 Tick 命令：", ":".join(commands))
                        self.get_api().create_task(get_tick(commands[1]))
                self.get_api().wait_update(time.time() + 1)
            except Exception as e:
                print(f"天勤 循环等待更新行情数据异常 {e}，重启")
                reset_api(force=True)
                time.sleep(5)

    def get_api(self, use_account=False):
        """
        获取 天勤API 对象
        use_account : 标记是否使用账户对象，在特殊时间，账户是无法登录的，这时候只能使用行情服务，使用账户则会报错
        """
        # 这时候使用账户模式，但是账户并不可用，尝试关闭 API，并重新创建 账户 API 连接
        if (
            use_account is True
            and self.g_account_enable is False
            and self.g_api is not None
        ):
            self.g_api.close()
            self.g_api = None

        if self.g_api is None:
            account = self.get_account()
            if use_account and account is None:
                raise Exception(
                    "使用实盘账户操作，但是并没有配置实盘账户，请检查实盘配置"
                )
            try:
                self.g_api = tqsdk.TqApi(
                    account=account, auth=tqsdk.TqAuth(config.TQ_USER, config.TQ_PWD)
                )
                self.g_account_enable = True
            except Exception as e:
                print(
                    "初始化默认的天勤 API 报错，重新尝试初始化无账户的 API：", {str(e)}
                )
                self.g_api = tqsdk.TqApi(
                    auth=tqsdk.TqAuth(config.TQ_USER, config.TQ_PWD)
                )
                self.g_account_enable = False

        return self.g_api

    def close_api(self):
        if self.g_api is not None:
            self.g_api.close()
            self.g_api = None
        return True

    def get_account(self):
        # 使用快期的模拟账号
        if self.use_simulate_account:
            if self.g_account is None:
                self.g_account = tqsdk.TqKq()
            return self.g_account

        # 天勤的实盘账号，如果有设置则使用
        if config.TQ_SP_ACCOUNT == "":
            return None
        if self.g_account is None:
            self.g_account = tqsdk.TqAccount(
                config.TQ_SP_NAME, config.TQ_SP_ACCOUNT, config.TQ_SP_PWD
            )
        return self.g_account

    def all_stocks(self):
        """
        获取支持的所有股票列表
        :return:
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        codes = []
        for c in ["FUTURE", "CONT"]:
            codes += self.get_api().query_quotes(ins_class=c, expired=False)
            # print(f'tq type {c} codes : {len(codes)}')
        infos = self.get_api().query_symbol_info(codes)

        __all_stocks = []
        for code in codes:
            code_df = infos[infos["instrument_id"] == code].iloc[0]
            if code_df["expired"]:
                continue
            __all_stocks.append(
                {
                    "code": code,
                    "name": code_df["instrument_name"],
                }
            )
        self.g_all_stocks = __all_stocks
        return self.g_all_stocks

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=5),
        retry=retry_if_result(lambda _r: _r is None),
    )
    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        """
        获取 Kline 线
        :param code:
        :param frequency:
        :param start_date:
        :param end_date:
        :param args:
        :return:
        """
        if args is None:
            args = {}
        if "limit" not in args.keys():
            args["limit"] = 2000
        frequency_maps = {
            "w": 7 * 24 * 60 * 60,
            "d": 24 * 60 * 60,
            "60m": 60 * 60,
            "30m": 30 * 60,
            "15m": 15 * 60,
            "10m": 10 * 60,
            "6m": 6 * 60,
            "5m": 5 * 60,
            "3m": 3 * 60,
            "2m": 2 * 60,
            "1m": 1 * 60,
            "30s": 30,
            "10s": 10,
        }
        if start_date is not None and end_date is not None:
            raise Exception("期货行情不支持历史数据查询，因为账号不是专业版，没权限")

        # 添加命令
        kline_key = f"{code}_{frequency_maps[frequency]}"
        self.command_tasks.append(f"kline:{code}:{frequency_maps[frequency]}")
        # 获取返回的K线
        klines = None
        try_nums = 0
        while True:
            if kline_key not in self.res_klines.keys():
                time.sleep(1)
                try_nums += 1
                if try_nums > 5:  # 5秒后没有结果直接返回空
                    return None
                continue
            klines = self.res_klines[kline_key]
            break
        if klines is None:
            return None
        klines.loc[:, "date"] = klines["datetime"].apply(
            lambda x: datetime.datetime.fromtimestamp(x / 1e9)
        )
        # 转换时区
        klines["date"] = klines["date"].dt.tz_localize(self.tz)
        klines.loc[:, "code"] = code

        return klines[["code", "date", "open", "close", "high", "low", "volume"]]

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取代码列表的 Tick 信息
        :param codes:
        :return:
        """
        # 循环增加命令
        for code in codes:
            self.command_tasks.append(f"tick:{code}")
        time.sleep(1)
        # 循环获取更新后的 tick
        res_ticks = {}
        for code in codes:
            try_nums = 0
            while True:
                if code not in self.res_ticks.keys():
                    time.sleep(1)
                    try_nums += 1
                    if try_nums > 3:
                        break
                    continue
                tick = self.res_ticks[code]
                res_ticks[code] = Tick(
                    code=code,
                    last=0 if math.isnan(tick["last_price"]) else tick["last_price"],
                    buy1=0 if math.isnan(tick["bid_price1"]) else tick["bid_price1"],
                    sell1=0 if math.isnan(tick["ask_price1"]) else tick["ask_price1"],
                    high=0 if math.isnan(tick["highest"]) else tick["highest"],
                    low=0 if math.isnan(tick["lowest"]) else tick["lowest"],
                    open=0 if math.isnan(tick["open"]) else tick["open"],
                    volume=0 if math.isnan(tick["volume"]) else tick["volume"],
                    rate=(
                        0
                        if math.isnan(tick["pre_settlement"])
                        else round(
                            (tick["last_price"] - tick["pre_settlement"])
                            / tick["pre_settlement"]
                            * 100,
                            2,
                        )
                    ),
                )
                break
        return res_ticks

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票的基本信息
        :param code:
        :return:
        """
        all_stocks = self.all_stocks()
        return next(
            (stock for stock in all_stocks if stock["code"] == code),
            {"code": code, "name": code},
        )

    def now_trading(self):
        """
        返回当前是否是交易时间
        TODO 简单判断 ：9-12 , 13:30-15:00 21:00-02:30
        """
        hour = int(time.strftime("%H"))
        minute = int(time.strftime("%M"))
        if (
            hour in {9, 10, 11, 14, 21, 22, 23, 0, 1}
            or (hour == 13 and minute >= 30)
            or (hour == 2 and minute <= 30)
        ):
            return True
        return False

    def balance(self) -> Account:
        """
        获取账户资产
        """
        api = self.get_api(use_account=True)
        if self.g_account_enable is False:
            raise Exception("账户链接失败，暂时不可用，请稍后尝试")

        account = api.get_account()
        api.wait_update(time.time() + 2)
        return account

    def positions(self, code: str = None) -> Dict[str, Position]:
        """
        获取持仓
        """
        api = self.get_api(use_account=True)
        if self.g_account_enable is False:
            raise Exception("账户链接失败，暂时不可用，请稍后尝试")

        positions = api.get_position(symbol=code)
        api.wait_update(time.time() + 2)
        if isinstance(positions, Position):
            if positions["pos_long"] != 0 or positions["pos_short"] != 0:
                return {code: positions}
            else:
                return {}
        else:
            return {
                _code: positions[_code]
                for _code in positions.keys()
                if positions[_code]["pos_long"] != 0
                or positions[_code]["pos_short"] != 0
            }

    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单接口，默认使用盘口的买一卖一价格成交，知道所有手数成交后返回
        """
        if args is None:
            args = {}

        if o_type == "open_long":
            direction = "BUY"
            offset = "OPEN"
        elif o_type == "open_short":
            direction = "SELL"
            offset = "OPEN"
        elif o_type == "close_long":
            direction = "SELL"
            offset = "CLOSE"
        elif o_type == "close_short":
            direction = "BUY"
            offset = "CLOSE"
        else:
            raise Exception("期货下单类型错误")

        api = self.get_api(use_account=True)
        if self.g_account_enable is False:
            raise Exception("账户链接失败，暂时不可用，请稍后尝试")

        # 查询持仓
        if offset == "CLOSE":
            pos = self.positions(code)[code]
            if direction == "BUY":  # 平空，检查空仓
                if pos.pos_short < amount:
                    # 持仓手数少于要平仓的，修正为持仓数量
                    amount = pos.pos_short

                if "SHFE" in code or "INE.sc" in code:
                    if pos.pos_short_his >= amount:
                        offset = "CLOSE"
                    elif pos.pos_short_today >= amount:
                        offset = "CLOSETODAY"
                    else:
                        # 持仓不够，返回错误
                        return False
            else:
                if pos.pos_long < amount:
                    # 持仓手数少于要平仓的，修正为持仓数量
                    amount = pos.pos_long

                if "SHFE" in code or "INE.sc" in code:
                    if pos.pos_long_his >= amount:
                        offset = "CLOSE"
                    elif pos.pos_long_today >= amount:
                        offset = "CLOSETODAY"
                    else:
                        # 持仓不够，返回错误
                        return False

        order = None

        amount_left = amount
        while amount_left > 0:
            quote = api.get_quote(code)
            api.wait_update(time.time() + 2)
            price = quote.ask_price1 if direction == "BUY" else quote.bid_price1
            if price is None:
                continue
            order = api.insert_order(
                code,
                direction=direction,
                offset=offset,
                volume=int(amount_left),
                limit_price=price,
            )
            api.wait_update(time.time() + 5)

            if order.status == "FINISHED":
                if order.is_error:
                    print(f"下单失败，原因：{order.last_msg}")
                    return False
                break
            else:
                # 取消订单，未成交的部分继续挂单
                self.cancel_order(order)
                if order.is_error:
                    print(f"下单失败，原因：{order.last_msg}")
                    return False
                amount_left = order.volume_left

        if order is None:
            return False

        return {"id": order.order_id, "price": order.trade_price, "amount": amount}

    def all_orders(self):
        """
        获取所有订单 (有效订单)
        """
        api = self.get_api(use_account=True)
        if self.g_account_enable is False:
            raise Exception("账户链接失败，暂时不可用，请稍后尝试")

        orders = api.get_order()
        api.wait_update(time.time() + 5)

        res_orders = []
        for _id in orders:
            _o = orders[_id]
            if _o.status == "ALIVE":
                res_orders.append(_o)

        return res_orders

    def cancel_all_orders(self):
        """
        撤销所有订单
        """
        api = self.get_api(use_account=True)
        if self.g_account_enable is False:
            raise Exception("账户链接失败，暂时不可用，请稍后尝试")

        orders = api.get_order()
        api.wait_update(time.time() + 2)
        for _id in orders:
            _o = orders[_id]
            if _o.status == "ALIVE":
                # 有效的订单，进行撤单处理
                self.cancel_order(_o)

        return True

    def cancel_order(self, order):
        """
        取消订单，直到订单取消成功
        """
        api = self.get_api(use_account=True)
        if self.g_account_enable is False:
            raise Exception("账户链接失败，暂时不可用，请稍后尝试")

        while True:
            api.cancel_order(order)
            api.wait_update(time.time() + 2)
            if order.status == "FINISHED":
                break

        return None

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")


if __name__ == "__main__":
    ex = ExchangeTq(use_simulate_account=False)

    # print("all_stocks", len(ex.all_stocks()))
    # for c in ['FUTURE', 'CONT']:
    #     res = ex.get_api().query_quotes(ins_class=c)
    #     print(c, len(res))

    # main_codes = ex.get_api().query_cont_quotes()
    # print(main_codes)

    # klines = ex.klines("KQ.m@SHFE.ss", "10m")
    # print(klines.tail())

    # klines = klines[klines['date'] <= '2023-10-16 15:00:00']

    # print(len(klines), klines.tail(20))

    # tick = ex.ticks(['DCE.l2401'])
    # print(tick)

    balance = ex.balance()
    print(balance)

    # ex.close_task_thread()
    # ex.restart_task_thread()
    # ex.close_task_thread()
    # ex.close_api()
    print("Done")

    # ex.close_api()
