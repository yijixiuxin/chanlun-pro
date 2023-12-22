import random
from tenacity import retry, stop_after_attempt, wait_random, retry_if_result
from chanlun import config, fun
from chanlun.exchange.exchange import *

from futu import *

g_ctx = None
g_ttx = None


def CTX():
    """
    返回 富途行情 对象
    """
    global g_ctx
    if config.FUTU_HOST == "":
        return None
    if g_ctx is None:
        g_ctx = OpenQuoteContext(
            host=config.FUTU_HOST, port=config.FUTU_PORT, is_encrypt=False
        )
    if random.randint(0, 100) > 90:
        # 随机执行，订阅数量大于  90 ，则关闭所有订阅
        ret, sub_data = g_ctx.query_subscription()
        if ret == RET_OK and sub_data["own_used"] >= 90:
            # 取消订阅
            g_ctx.unsubscribe_all()
    return g_ctx


def TTX() -> [OpenSecTradeContext, None]:
    """
    返回富途交易对象
    """
    global g_ttx
    if config.FUTU_HOST == "":
        return None
    if g_ttx is None:
        g_ttx = OpenSecTradeContext(
            filter_trdmarket=TrdMarket.HK,
            host=config.FUTU_HOST,
            port=config.FUTU_PORT,
            security_firm=SecurityFirm.FUTUSECURITIES,
        )
    return g_ttx


@fun.singleton
class ExchangeFutu(Exchange):
    """
    富途交易所接口
    """

    g_all_stocks = []
    g_trade_days = None

    def __init__(self):
        SysConfig.set_all_thread_daemon(True)

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

    def default_code(self):
        return "HK.00700"

    def support_frequencys(self):
        return {
            "y": "Year",
            "m": "Month",
            "w": "Week",
            "d": "Day",
            "120m": "2H",
            "60m": "1H",
            "30m": "30m",
            "15m": "15m",
            "10m": "10m",
            "5m": "5m",
            "1m": "1m",
        }

    def all_stocks(self):
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks
        __all_stocks = []
        ret, data = CTX().get_plate_stock("HK.BK1910")
        if ret == RET_OK:
            for s in data.iterrows():
                __all_stocks.append({"code": s[1]["code"], "name": s[1]["stock_name"]})
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
    ) -> [pd.DataFrame, None]:
        if args is None:
            args = {}
        frequency_map = {
            "1m": {"ktype": KLType.K_1M, "subtype": SubType.K_1M},
            "5m": {"ktype": KLType.K_5M, "subtype": SubType.K_5M},
            "10m": {"ktype": KLType.K_5M, "subtype": SubType.K_5M},
            "15m": {"ktype": KLType.K_15M, "subtype": SubType.K_15M},
            "30m": {"ktype": KLType.K_30M, "subtype": SubType.K_30M},
            "60m": {"ktype": KLType.K_60M, "subtype": SubType.K_60M},
            "120m": {"ktype": KLType.K_60M, "subtype": SubType.K_60M},
            "d": {"ktype": KLType.K_DAY, "subtype": SubType.K_DAY},
            "w": {"ktype": KLType.K_WEEK, "subtype": SubType.K_WEEK},
            "m": {"ktype": KLType.K_MON, "subtype": SubType.K_MON},
            "y": {"ktype": KLType.K_YEAR, "subtype": SubType.K_YEAR},
        }

        if "is_history" not in args.keys():
            args["is_history"] = False

        if "fq" not in args.keys():
            args["fq"] = AuType.QFQ

        try:
            if start_date is None and end_date is None and args["is_history"] is False:
                # 获取实时 K 线数据
                # 订阅
                CTX().subscribe(
                    [code],
                    [frequency_map[frequency]["subtype"]],
                    is_first_push=False,
                    subscribe_push=False,
                )
                # 获取 K 线
                ret, kline = CTX().get_cur_kline(
                    code, 1000, frequency_map[frequency]["subtype"], args["fq"]
                )
                if ret != RET_OK:
                    print(kline)
                    return None
            else:
                if start_date is None and end_date is not None:
                    time_format = "%Y-%m-%d %H:%M:%S"
                    if len(end_date) == 10:
                        time_format = "%Y-%m-%d"
                    end_datetime = dt.datetime(
                        *time.strptime(end_date, time_format)[:6]
                    )
                    if frequency == "1m":
                        start_date = (end_datetime - dt.timedelta(days=5)).strftime(
                            time_format
                        )
                    elif frequency == "5m":
                        start_date = (end_datetime - dt.timedelta(days=25)).strftime(
                            time_format
                        )
                    elif frequency == "30m":
                        start_date = (end_datetime - dt.timedelta(days=150)).strftime(
                            time_format
                        )
                    elif frequency == "d":
                        start_date = (end_datetime - dt.timedelta(days=1500)).strftime(
                            time_format
                        )
                    elif frequency == "w":
                        start_date = (end_datetime - dt.timedelta(days=2500)).strftime(
                            time_format
                        )
                ret, kline, pk = CTX().request_history_kline(
                    code=code,
                    start=start_date,
                    end=end_date,
                    max_count=None,
                    ktype=frequency_map[frequency]["ktype"],
                    autype=args["fq"],
                )
            kline["date"] = pd.to_datetime(kline["time_key"]).dt.tz_localize(self.tz)
            kline["date"] = kline["date"].apply(self.__convert_date)
            kline = kline[["code", "date", "open", "close", "high", "low", "volume"]]
            if frequency == "120m" and len(kline) > 0:
                kline = convert_stock_kline_frequency(kline, "120m")
            if frequency == "10m" and len(kline) > 0:
                kline = convert_stock_kline_frequency(kline, "10m")
            return kline
        except Exception as e:
            print(f"Futu 请求 {code} - {frequency} 行情异常：{e}")

        return None

    @staticmethod
    def __convert_date(dt):
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            return dt.replace(hour=16, minute=0)
        return dt

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        # CTX().subscribe(codes, [SubType.QUOTE], subscribe_push=False)
        ret, data = CTX().get_market_snapshot(codes)
        if ret == RET_OK:
            return {
                _d[1]["code"]: Tick(
                    code=_d[1]["code"],
                    last=_d[1]["last_price"],
                    high=_d[1]["high_price"],
                    low=_d[1]["low_price"],
                    open=_d[1]["open_price"],
                    volume=_d[1]["volume"],
                    buy1=_d[1]["bid_price"],
                    sell1=_d[1]["ask_price"],
                    rate=round(
                        (_d[1]["last_price"] - _d[1]["prev_close_price"])
                        / _d[1]["prev_close_price"]
                        * 100,
                        2,
                    ),
                )
                for _d in data.iterrows()
            }

        print("Ticks Error : ", data)
        return {}

    def stock_info(self, code: str) -> [Dict, None]:
        ret, data = CTX().get_stock_basicinfo(None, SecurityType.STOCK, [code])
        if ret == RET_OK:
            return {
                "code": data.iloc[0]["code"],
                "name": data.iloc[0]["name"],
                "lot_size": data.iloc[0]["lot_size"],
                "stock_type": data.iloc[0]["stock_type"],
            }
        return None

    @staticmethod
    def market_trade_days(market):
        """
        指定市场的交易时间
        :return:
        """
        market_map = {"hk": TradeDateMarket.HK, "cn": TradeDateMarket.CN}
        ret, data = CTX().request_trading_days(
            market=market_map[market], start=time.strftime("%Y-%m-%d")
        )
        return data if ret == RET_OK else None

    def now_trading(self):
        """
        返回当前是否是交易时间
        :return:
        """
        if self.g_trade_days is None:
            self.g_trade_days = self.market_trade_days("hk")

        now_date = time.strftime("%Y-%m-%d")
        if self.g_trade_days[-1]["time"] < now_date:
            self.g_trade_days = self.market_trade_days("hk")

        for _t in self.g_trade_days:
            if _t["time"] == now_date:
                hour = int(time.strftime("%H"))
                minute = int(time.strftime("%M"))
                # 上午的时间检查
                if _t["trade_date_type"] in ["WHOLE", "MORNING"] and (
                    (hour == 9 and minute >= 30) or hour in {10, 11}
                ):
                    return True
                # 下午的时间检查
                if _t["trade_date_type"] in ["WHOLE", "AFTERNOON"] and hour in {
                    13,
                    14,
                    15,
                }:
                    return True
        return False

    @staticmethod
    def query_kline_edu():
        ret, data = CTX().get_history_kl_quota(get_detail=False)
        if ret == RET_OK:
            print(data)
        else:
            print("error:", data)

    def stock_owner_plate(self, code: str):
        plate_infos = {"HY": [], "GN": []}
        ret, data = CTX().get_owner_plate([code])
        if ret == RET_OK:
            for p in data.iterrows():
                if p[1]["plate_type"] == "INDUSTRY":
                    plate_infos["HY"].append(
                        {"code": p[1]["plate_code"], "name": p[1]["plate_name"]}
                    )
                if p[1]["plate_type"] == "CONCEPT":
                    plate_infos["GN"].append(
                        {"code": p[1]["plate_code"], "name": p[1]["plate_name"]}
                    )
        return plate_infos

    def plate_stocks(self, code: str):
        stocks = []
        ret, data = CTX().get_plate_stock(
            code, sort_field=SortField.CHANGE_RATE, ascend=False
        )
        if ret == RET_OK:
            stocks.extend(
                {"code": s[1]["code"], "name": s[1]["stock_name"]}
                for s in data.iterrows()
            )

        return stocks

    def balance(self):
        ret, account = TTX().accinfo_query()
        if ret == RET_OK:
            return {
                "power": account.iloc[0]["power"],
                "max_power_short": account.iloc[0]["max_power_short"],
                "net_cash_power": account.iloc[0]["net_cash_power"],
                "total_assets": account.iloc[0]["total_assets"],
                "cash": account.iloc[0]["cash"],
                "market_val": account.iloc[0]["market_val"],
                "long_mv": account.iloc[0]["long_mv"],
                "short_mv": account.iloc[0]["short_mv"],
            }
        return None

    def positions(self, code=""):
        ret, poss = TTX().position_list_query(code=code)
        if ret == RET_OK:
            return [
                {
                    "code": _p[1]["code"],
                    "name": _p[1]["stock_name"],
                    "type": _p[1]["position_side"],
                    "amount": _p[1]["qty"],
                    "can_sell_amount": _p[1]["can_sell_qty"],
                    "price": _p[1]["cost_price"],
                    "profit": _p[1]["pl_ratio"],
                    "profit_val": _p[1]["pl_val"],
                }
                for _p in poss.iterrows()
                if _p[1]["qty"] != 0.0
            ]

        else:
            print("Position Error : ", poss)
        return []

    @staticmethod
    def can_trade_val(code):
        """
        查询股票可以交易的数量
        :param code:
        :return:
        """
        ret, data = TTX().acctradinginfo_query(
            order_type=OrderType.MARKET, code=code, price=0
        )
        if ret == RET_OK:
            return {
                "max_cash_buy": data.iloc[0]["max_cash_buy"],
                "max_margin_buy": data.iloc[0]["max_cash_and_margin_buy"],
                "max_position_sell": data.iloc[0]["max_position_sell"],
                "max_margin_short": data.iloc[0]["max_sell_short"],
                "max_buy_back": data.iloc[0]["max_buy_back"],
            }
        print("Can Trade Val Error : ", data)
        return None

    def order(self, code, o_type, amount, args=None):
        order_type_map = {"buy": TrdSide.BUY, "sell": TrdSide.SELL}
        TTX().unlock_trade(config.FUTU_UNLOCK_PWD)  # 先解锁交易
        ret, data = TTX().place_order(
            price=0,
            qty=amount,
            code=code,
            order_type=OrderType.MARKET,
            trd_side=order_type_map[o_type],
        )
        if ret == RET_OK:
            time.sleep(5)
            ret, o = TTX().order_list_query(order_id=data.iloc[0]["order_id"])
            if ret == RET_OK:
                return {
                    "id": o.iloc[0]["order_id"],
                    "code": o.iloc[0]["code"],
                    "name": o.iloc[0]["stock_name"],
                    "type": o.iloc[0]["trd_side"],
                    "order_type": o.iloc[0]["order_type"],
                    "order_status": o.iloc[0]["order_status"],
                    "price": o.iloc[0]["price"],
                    "amount": o.iloc[0]["qty"],
                    "dealt_amount": o.iloc[0]["dealt_qty"],
                    "dealt_avg_price": o.iloc[0]["dealt_avg_price"],
                }
            print("Order Get Order Error : ", o)
        else:
            print("Order Error : ", data)

        return False


if __name__ == "__main__":
    ex = ExchangeFutu()
    klines = ex.klines("HK.00700", "d")
    print(klines.tail())
