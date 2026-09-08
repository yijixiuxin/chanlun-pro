import datetime
import time

import pandas as pd
import pytz
from tenacity import retry, retry_if_result, stop_after_attempt, wait_random

from chanlun import fun
from chanlun.exchange.exchange import Exchange, Tick, convert_stock_kline_frequency
from chanlun.tools.qmt_bridge_client import get_client

"""
QMT 沪深行情（大QMT桥接模式）

通过 bigqmt_server 目录下的桥接服务（运行在大QMT内置 Python 中）获取行情，
替代原先 miniQMT 的 xtdata 服务。大QMT侧使用原生代码格式（600519.SH），
本项目统一转换为通达信格式（SH.600519）。

交易：balance/positions/order/order_detail/order_cancel 均通过桥接服务完成，
所有交易接口需指定 account_id（由用户在初始化 ExchangeQMT 时传入）；
账号类型固定为股票（stock）。
"""


class ExchangeQMT(Exchange):
    g_all_stocks = []  # noqa: RUF012

    def __init__(self, account_id=None):
        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

        self.use_times = {}

        # 大QMT桥接客户端（内部对多个服务端口动态路由）
        self.bridge = get_client()

        # 交易账号（所有交易接口均需指定，类型固定为股票 stock）
        self.account_id = account_id or ""
        self.account_type = "stock"

    def add_use_time(self, name: str, time: int):
        if name not in self.use_times:
            self.use_times[name] = 0
        self.use_times[name] += time

    def code_to_tdx(self, code: str):
        """
        兼容之前的通达信格式,和 qmt 代码格式进行转换
        """
        _c = code.split(".")
        if len(_c[0]) == 6:
            return _c[1] + "." + _c[0]
        else:
            return _c[0] + "." + _c[1]

    def code_to_qmt(self, code: str):
        """
        兼容之前的通达信格式,和 qmt 代码格式进行转换
        """
        _c = code.split(".")
        if len(_c[0]) == 6:
            return _c[0] + "." + _c[1]
        else:
            return _c[1] + "." + _c[0]

    def default_code(self):
        return "SH.000001"

    def support_frequencys(self):
        return {
            "m": "m",
            "w": "w",
            "d": "1d",
            "60m": "1h",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }

    def all_stocks(self):
        """
        获取所有股票代码（股票/ETF/指数，过滤黑名单）
        """
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        s_time = time.time()
        # 通过大QMT桥接获取全部证券列表（大QMT侧已按股票/ETF/指数过滤）
        stock_list = self.bridge.all_stocks()

        all_stocks = []
        for _s in stock_list:
            _tdx_code = self.code_to_tdx(_s.get("code", ""))
            _price_tick = _s.get("price_tick") or 0.01
            all_stocks.append(
                {
                    "code": _tdx_code,
                    "name": _s.get("name") or _tdx_code,
                    "precision": fun.reverse_decimal_to_power_of_ten(_price_tick),
                }
            )

        self.g_all_stocks = all_stocks
        self.add_use_time("all_stocks", time.time() - s_time)

        # print(f"股票共获取数量：{len(self.g_all_stocks)}")
        # print(f"耗时：{time.time() - s_time}")
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
        start_date: str | None = None,
        end_date: str | None = None,
        args=None,
    ) -> pd.DataFrame | None:
        """获取 K 线（只支持日线及以下：1m/5m/15m/30m/60m/d）。

        每次请求都会在 QMT 侧先 download_history_data 下载最新数据（否则只能取到本地缓存）；
        只取非复权数据（dividend_type='none'），便于持续增量更新。

        start_date：统一起始时间。
            - None：按周期条数推算一个粗略起始时间（首次/全量）
            - 字符串（YYYYMMDD 或 YYYYMMDDHHMMSS，含 '-' 亦可）：只取该时间之后的数据（增量）
        args.req_counts / args.limit：请求 K 线条数（默认按周期）。
        """
        frequency_map = {
            "m": "1d",
            "w": "1d",
            "d": "1d",
            "60m": "5m",
            "30m": "5m",
            "15m": "5m",
            "5m": "5m",
            "1m": "1m",
        }
        frequency_count = {
            "m": 18000,
            "w": 8000,
            "d": 8000,
            "60m": 8000 * 12,
            "30m": 8000 * 6,
            "15m": 8000 * 3,
            "5m": 8000,
            "1m": 8000,
        }
        if frequency not in frequency_map:
            raise ValueError(
                f"不支持的频率 {frequency}: 仅支持日线及以下（1m/5m/15m/30m/60m/d/w/m）"
            )

        # 指定请求数据条数
        req_counts = frequency_count[frequency]
        if args is not None:
            req_counts = args.get("req_counts", args.get("limit", req_counts))

        # 统一 start_date：未传则按周期条数推算粗略起始时间（避免从最早历史下载）；
        # 传入则只取该时间之后的数据（条数由时间窗口决定，置 -1 不限）
        if not start_date:
            period = frequency_map[frequency]
            bars_per_day = {"1m": 240, "5m": 48, "1d": 1}.get(period, 240)
            days_back = req_counts // bars_per_day + 30  # +30 天缓冲（周末/节假日）
            start_date = (
                datetime.datetime.now() - datetime.timedelta(days=days_back)  # noqa: DTZ005
            ).strftime("%Y%m%d")
        else:
            req_counts = -1

        qmt_code = self.code_to_qmt(code)
        start_time = start_date.replace("-", "") if start_date else ""

        # 通过大QMT桥接获取历史/实时行情（服务端每次都会先 download_history_data）
        s_time = time.time()
        payload = self.bridge.history_data(
            stock_code=qmt_code,
            period=frequency_map[frequency],
            start_time=start_time,
            end_time="",
            count=req_counts,
            dividend_type="front_ratio",
            fields="time,open,high,low,close,volume",
        )
        self.add_use_time("get_market_data", time.time() - s_time)

        rows = payload.get("data") or []
        if len(rows) == 0:
            return None

        s_time = time.time()
        klines_df = pd.DataFrame(rows)
        klines_df["code"] = code

        klines_df["date"] = pd.to_datetime(klines_df["time"]).dt.tz_localize(self.tz)
        klines_df = klines_df[
            ["code", "date", "open", "high", "low", "close", "volume"]
        ]
        # 将 open high low close volume 转换为 float
        klines_df[["open", "high", "low", "close", "volume"]] = klines_df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)
        klines_df = klines_df.sort_values("date")

        # 如果日线，小时设置为15点
        if frequency == "d":
            klines_df["date"] = klines_df["date"].apply(lambda x: x.replace(hour=15))

        if frequency not in ["d", "5m", "1m"]:
            klines_df = convert_stock_kline_frequency(klines_df, frequency)

        if req_counts > 0:
            klines_df = klines_df.iloc[-req_counts:]

        self.add_use_time("klines", time.time() - s_time)

        # 如果是 1 分钟级别，将 09:30 的成交量合并到 09:31
        time_0930 = pd.to_datetime("09:30").time()
        time_0931 = pd.to_datetime("09:31").time()
        if frequency == "1m":
            # 按交易日处理 09:30 与 09:31 的成交量
            for _, day_df in klines_df.groupby(klines_df["date"].dt.date):
                idx_0930 = day_df.index[day_df["date"].dt.time == time_0930]
                idx_0931 = day_df.index[day_df["date"].dt.time == time_0931]
                if len(idx_0930) == 1 and len(idx_0931) == 1:
                    klines_df.loc[idx_0931, "volume"] += klines_df.loc[
                        idx_0930, "volume"
                    ].values

        # 删除时间是 09:30 的数据
        time_mask = klines_df["date"].dt.time == time_0930
        klines_df = klines_df[~time_mask]

        # 删除时间大于今天日期的数据
        today = pd.Timestamp.now(tz=self.tz).normalize()
        klines_df = klines_df[klines_df["date"].dt.date <= today.date()]

        return klines_df

    def stock_info(self, code: str) -> dict | None:
        """
        获取股票名称
        """
        qmt_code = self.code_to_qmt(code)
        detail = self.bridge.instrument_detail(qmt_code)
        return {
            "code": code,
            "name": detail.get("stock_name") or "",
            "precision": fun.reverse_decimal_to_power_of_ten(
                detail.get("price_tick") or 0.01
            ),
        }

    def ticks(self, codes: list[str]) -> dict[str, Tick]:
        """
        获取 tick 信息
        """
        ticks = {}
        if len(codes) == 0:
            return ticks
        qmt_codes = [self.code_to_qmt(_c) for _c in codes]
        qmt_ticks = self.bridge.ticks(qmt_codes)
        for _c, _t in qmt_ticks.items():
            _tdx_code = self.code_to_tdx(_c)
            _last_close = _t.get("lastClose") or 0
            ticks[_tdx_code] = Tick(
                code=_tdx_code,
                last=_t.get("lastPrice") or 0,
                buy1=(_t.get("bidPrice") or [0])[0],
                sell1=(_t.get("askPrice") or [0])[0],
                high=_t.get("high") or 0,
                low=_t.get("low") or 0,
                open=_t.get("open") or 0,
                volume=_t.get("volume") or 0,
                rate=(
                    ((_t.get("lastPrice") or 0) - _last_close) / _last_close * 100
                    if _last_close != 0
                    else 0
                ),
            )

        return ticks

    def all_ticks(self) -> dict[str, Tick]:
        ticks = {}
        # 通过桥接获取全市场 tick（大QMT侧已按股票/ETF/指数过滤），黑名单在项目侧完成
        qmt_ticks = self.bridge.all_ticks()
        for _c, _t in qmt_ticks.items():
            _tdx_code = self.code_to_tdx(_c)
            _last_close = _t.get("lastClose") or 0
            ticks[_tdx_code] = Tick(
                code=_tdx_code,
                last=_t.get("lastPrice") or 0,
                buy1=(_t.get("bidPrice") or [0])[0],
                sell1=(_t.get("askPrice") or [0])[0],
                high=_t.get("high") or 0,
                low=_t.get("low") or 0,
                open=_t.get("open") or 0,
                volume=_t.get("volume") or 0,
                rate=(
                    ((_t.get("lastPrice") or 0) - _last_close) / _last_close * 100
                    if _last_close != 0
                    else 0
                ),
            )

        return ticks

    def get_divid_factors(self, stock_code: str) -> pd.DataFrame:
        """
        获取股票除权除息信息
        """
        rows = self.bridge.divid_factors(self.code_to_qmt(stock_code))
        if len(rows) == 0:
            return None
        df = pd.DataFrame(rows)
        if "time" in df.columns and len(df) > 0:
            # time 为毫秒时间戳，转成可读日期
            if df["time"].dtype.kind in "iuf" and df["time"].iloc[0] is not None:
                df["divid_date"] = pd.to_datetime(df["time"] / 1000, unit="s")
            else:
                df["divid_date"] = pd.to_datetime(df["time"])
        df.loc[:, "stock_code"] = stock_code
        return df

    def now_trading(self):
        """
        返回当前是否是交易时间
        周一至周五，09:30-11:30 13:00-15:00
        """
        now_dt = datetime.datetime.now()  # noqa: DTZ005
        if now_dt.weekday() in [5, 6]:  # 周六日不交易
            return False
        hour = now_dt.hour
        minute = now_dt.minute
        if hour == 9 and minute >= 30:
            return True
        if hour in [10, 13, 14]:
            return True
        return bool(hour == 11 and minute < 30)

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")  # noqa: TRY002

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")  # noqa: TRY002

    def balance(self):
        """
        查询账户资金。

        Returns:
            dict: 账户资金字段（get_trade_detail_data(..., 资金/account)）
                total_money      总资产/账户市值（total_balance）
                cash             可用资金（available_cash）
                market_value     证券市值
                total_assets     总资产
                position_profit  持仓盈亏
                total_debit      总负债（融资融券用）
                assure_asset     担保/信用资产（融资融券用）
        """
        payload = self.bridge.balance(self.account_id)
        return {
            "total_money": payload.get("total_balance") or 0,  # 总资产/账户市值
            "cash": payload.get("available_cash") or 0,  # 可用资金
            "market_value": payload.get("market_value") or 0,  # 证券市值
            "total_assets": payload.get("total_assets") or 0,  # 总资产
            "position_profit": payload.get("position_profit") or 0,  # 持仓盈亏
            "total_debit": payload.get("total_debit") or 0,  # 总负债（融资融券用）
            "assure_asset": payload.get("assure_asset")
            or 0,  # 担保/信用资产（融资融券用）
        }

    def positions(self, code: str = ""):
        """
        查询账户持仓。

        Args:
            code: 可选，TDX 格式代码，按代码过滤

        Returns:
            list[dict]: 持仓列表（get_trade_detail_data(..., 持仓/position)）
                基础：code(TDX)/name/direction/stock_holder/market
                数量：amount(持仓量)/can_sell_amount(可用可卖)/frozen_amount(冻结)/
                      on_road_amount(在途)/yesterday_amount(昨日)
                价格盈亏：price(开仓价)/avg_open_price(开仓均价)/last_price(最新价)/
                        settlement_price(结算价)/market_value(市值)/profit(盈亏比例)/
                        profit_val(浮动盈亏)/position_profit(持仓盈亏)/position_cost(持仓成本)
        """
        qmt_code = self.code_to_qmt(code) if code else ""
        data = self.bridge.positions(self.account_id, code=qmt_code)
        result = []
        for p in data:
            p_code = p.get("code") or ""
            result.append(
                {
                    "code": self.code_to_tdx(
                        p_code
                    ),  # 证券代码（通达信格式，如 600519.SH）
                    "name": p.get("name") or "",  # 证券名称
                    "direction": p.get("direction")
                    or 0,  # 买卖方向（48=买/多，49=卖/空）
                    "market": p.get("market") or "",  # 证券市场/交易所代码
                    "stock_holder": p.get("stock_holder") or "",  # 股东账号
                    "amount": p.get("volume") or 0,  # 持仓量/当前股数
                    "can_sell_amount": p.get("can_use_volume")
                    or 0,  # 可用数量（可卖，A股T+1）
                    "frozen_amount": p.get("frozen_volume") or 0,  # 冻结数量
                    "on_road_amount": p.get("on_road_volume") or 0,  # 在途数量
                    "yesterday_amount": p.get("yesterday_volume") or 0,  # 昨日股份余额
                    "price": p.get("open_price") or 0,  # 开仓价/成本价
                    "avg_open_price": p.get("avg_open_price") or 0,  # 开仓均价
                    "last_price": p.get("last_price") or 0,  # 最新价/当前价
                    "settlement_price": p.get("settlement_price") or 0,  # 结算价
                    "market_value": p.get("market_value") or 0,  # 当前市值
                    "instrument_value": p.get("instrument_value") or 0,  # 合约价值
                    "profit": p.get("profit_rate") or 0,  # 持仓盈亏比例
                    "profit_val": p.get("float_profit") or 0,  # 浮动盈亏
                    "position_profit": p.get("position_profit") or 0,  # 持仓盈亏
                    "position_cost": p.get("position_cost") or 0,  # 持仓成本
                    "open_cost": p.get("open_cost") or 0,  # 开仓成本
                }
            )
        return result

    def _position_amount(self, account_id, code, field="volume"):
        """查询指定 QMT 代码的当前持仓数量。

        桥接返回的持仓 dict 中持仓数量字段为 volume（可用/可卖为 can_use_volume）。
        """
        data = self.bridge.positions(account_id, code=code)
        for p in data:
            if p.get("code") == code:
                return p.get(field) or 0
        return 0

    def order(self, code: str, o_type: str, amount: float, args=None):
        """
        下单接口：按照给定的目标数量完成交易。

        amount 表示目标持仓数量（股）：先查询当前持仓，再根据与目标的差值下单；
        若未成交或部分成交，取消该委托并重新下单剩余数量，直到达到目标数量。
        买入数量向下取整为 100 的整数倍；卖出数量受可用持仓（T+1）限制。

        Returns:
            dict: {"price", "amount", "done", "attempt", "position"}
                  amount 为本次实际成交数量；position 为最终持仓数量
        """
        args = args or {}
        account_id = args.get("account_id") or self.account_id
        max_attempt = int(args.get("max_attempt", 5))
        wait_fill = float(args.get("wait_fill", 2.0))
        price = float(args.get("price", 0))
        pr_type = int(args.get("pr_type", 11))
        reason = str(args.get("reason", "") or "")
        qmt_code = self.code_to_qmt(code)

        target = int(amount)
        is_buy = o_type == "buy"
        if is_buy:
            target = int(target / 100) * 100  # 买入需为 100 整数倍
        current = int(self._position_amount(account_id, qmt_code))

        # 已达到目标，无需操作
        if (is_buy and current >= target) or (not is_buy and current <= target):
            return {
                "price": 0,
                "amount": 0,
                "done": True,
                "attempt": 0,
                "position": current,
            }

        need = target - current if is_buy else current - target
        if not is_buy:
            # 卖出受可用持仓限制（A股 T+1）
            available = int(
                self._position_amount(account_id, qmt_code, "can_use_volume")
            )
            need = min(need, available)
        if need <= 0:
            return {
                "price": 0,
                "amount": 0,
                "done": False,
                "attempt": 0,
                "position": current,
            }

        side = "buy" if is_buy else "sell"
        total_filled = 0
        traded_price = 0
        attempt = 0
        order_id = ""

        while attempt < max_attempt and need > 0:
            attempt += 1
            order_volume = int(need)
            if is_buy:
                order_volume = int(order_volume / 100) * 100
            if order_volume <= 0:
                break
            try:
                res = self.bridge.order(
                    account_id,
                    qmt_code,
                    side,
                    order_volume,
                    price=price,
                    pr_type=pr_type,
                    reason=reason,
                )
            except Exception:  # noqa: BLE001
                time.sleep(wait_fill)
                continue
            order_id = res.get("order_id", "") or ""
            if not order_id or order_id == "unknown":
                time.sleep(wait_fill)
                continue
            filled = self._wait_fill(account_id, order_id, wait_fill)
            total_filled += filled
            if filled >= need:
                break
            # 未成交或部分成交，撤单后重新下单剩余数量
            if order_id and order_id != "unknown":
                try:
                    self.bridge.order_cancel(account_id, order_id)
                except Exception:  # noqa: BLE001, S110
                    pass
            need = need - filled
            time.sleep(wait_fill)

        # 成交均价取最后一次下单成交价（也可用最新价兜底）
        if total_filled > 0 and order_id:
            try:
                detail = self.bridge.order_detail(account_id, order_id)
                traded_price = (detail.get("order") or {}).get("traded_price") or 0
            except Exception:  # noqa: BLE001
                traded_price = 0
        if traded_price == 0:
            ticks = self.ticks([code])
            traded_price = ticks[code].last if code in ticks else 0

        final_amount = int(self._position_amount(account_id, qmt_code))
        # 实际成交数量优先取持仓变化量（兜底为各次委托累计成交量）
        traded = (
            max(0, final_amount - current) if is_buy else max(0, current - final_amount)
        )
        return {
            "price": traded_price,
            "amount": traded or total_filled,
            "done": (is_buy and final_amount >= target)
            or (not is_buy and final_amount <= target),
            "attempt": attempt,
            "position": final_amount,
        }

    def _wait_fill(self, account_id, order_id, wait_fill):
        """轮询订单成交情况，返回已成交量。"""
        deadline = time.time() + wait_fill
        while time.time() < deadline:
            try:
                payload = self.bridge.order_detail(account_id, order_id)
            except Exception:  # noqa: BLE001
                return 0
            order = payload.get("order") or {}
            filled = int(order.get("traded_volume") or 0)
            if filled > 0:
                return filled
            time.sleep(0.5)
        # 超时后最后确认一次
        try:
            payload = self.bridge.order_detail(account_id, order_id)
            return int((payload.get("order") or {}).get("traded_volume") or 0)
        except Exception:  # noqa: BLE001
            return 0

    def order_detail(self, order_id: str = "", args=None):
        """
        查询订单详情。

        Returns:
            dict: {"order": {...}}，未找到时 order 为 None。
                order 委托字段（get_trade_detail_data(..., "order")）：
                    order_id      委托号（优先 m_strOrderSysID）
                    code/name     证券代码/名称
                    market        证券市场（交易所代码）
                    order_type    委托类别/业务类型
                    direction     买卖方向（48=买/多，49=卖/空）
                    offset_flag   开平标志
                    price_type    下单选价类型（5=最新价，11=指定价/模型价，14=对手价）
                    order_volume  最初委托量（股）
                    price         委托价/限价
                    traded_price  成交均价
                    traded_volume 已成交量
                    cancel_volume 已撤数量
                    trade_amount  成交额
                    status        委托状态（50=已报，52=已成，53=已撤，54=废单）
                    submit_status 提交状态
                    status_msg    状态/错误/废单信息
                    order_date    委托日期
                    order_time    委托时间
                    strategy_name 策略名
                    order_remark  委托备注/用户自设委托ID（userOrderId）
        """
        args = args or {}
        account_id = args.get("account_id") or self.account_id
        return self.bridge.order_detail(account_id, order_id)

    def order_cancel(self, order_id: str = "", args=None):
        """
        取消订单。
        Returns:
            dict: {"canceled": bool, "order_id": ...}
        """
        args = args or {}
        account_id = args.get("account_id") or self.account_id
        return self.bridge.order_cancel(account_id, order_id)


if __name__ == "__main__":
    import os

    account_id = os.getenv("BIG_QMT_ACCOUNT_ID", "")
    print("账户ID : ", account_id)
    ex = ExchangeQMT(account_id=account_id)

    # stocks = ex.all_stocks()
    # stock_maps = {}
    # for _s in stocks:
    #     stock_maps[_s["code"][0:5]] = _s
    # for _t, _s in stock_maps.items():
    #     print(_t, _s)
    # print(len(stocks))

    # klines = ex.klines(
    #     "SH.600519",
    #     "30m",
    # )
    # print(klines)

    # 2026-02-04 日期的 volume 累加
    # klines["ddd"] = klines["date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    # volume = klines[klines["ddd"] == "2026-02-04"]["volume"].sum()

    # print(klines[klines["ddd"] == "2026-02-04"])
    # print(volume)

    # stock = ex.stock_info("SH.000001")
    # print(stock)

    # df = ex.get_divid_factors("SH.600519")
    # print(df)

    # error_codes = []
    # ticks = ex.all_ticks()
    # print(len(ticks))
    # print(ticks["SH.519909"])
    # for _c, _t in ticks.items():
    #     if _t.buy1 == 0.0 and _t.sell1 == 0.0 and _t.volume == 0:
    #         error_codes.append(_c)
    # print(error_codes)

    # ticks = ex.ticks(["SH.519909"])
    # print(ticks)

    # 测试获取持仓
    # positions = ex.positions()
    # print(len(positions))
    # for _p in positions:
    #     print(_p)

    # 测试获取资金
    # balance = ex.balance()
    # print(balance)

    # 下单测试
    # order = ex.order("SH.515880", "buy", 300)
    # print(order)

    # 查询订单详情
    # res = ex.order_detail("635052213")
    # print(res)

    # 撤销订单
    # res = ex.order_cancel("2164")
    # print(res)
