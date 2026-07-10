"""
通达信量化版本行情接口（基于 tqcenter）
支持 A股、港股、美股、期货市场
"""

import datetime
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd
import pytz

from chanlun.base import Market
from chanlun.config import TDX_PATH
from chanlun.exchange.exchange import Exchange, Tick

# 将 tqcenter 路径加入 sys.path
_tq_user_path = str(Path(TDX_PATH) / "PYPlugins" / "user")
if _tq_user_path not in sys.path:
    sys.path.insert(0, _tq_user_path)

from tqcenter import tq  # noqa: E402

# 初始化 tq 连接
tq.initialize(__file__)


# ========================
# ExchangeTDXQuant
# ========================


class ExchangeTDXQuant(Exchange):
    """通达信量化行情接口（基于 tqcenter）"""

    # 市场 → tq 参数
    _CONFIG = {
        Market.A: {"tq_market": "5", "default_code": "SH.000001"},
        Market.HK: {"tq_market": "102", "default_code": "HK.00700"},
        Market.US: {"tq_market": "103", "default_code": "AAPL"},
        Market.FUTURES: {"tq_market": "101", "default_code": "SHF.AG00"},
    }

    # 周期映射：项目周期 → tq 周期
    _FREQ_TO_TQ = {
        "y": "1y",
        "m": "1mon",
        "w": "1w",
        "d": "1d",
        "60m": "1h",
        "30m": "30m",
        "15m": "15m",
        "5m": "5m",
        "1m": "1m",
    }

    def __init__(self, market: Market = Market.A):
        cfg = self._CONFIG.get(market)
        if cfg is None:
            raise ValueError(f"不支持的市场类型: {market}")

        self.market = market
        self._cfg = cfg
        self.tz = pytz.timezone("Asia/Shanghai")
        self._all_stocks: List[dict] = []

    # ========================
    # 代码格式转换
    # ========================

    def _to_tq(self, code: str) -> str:
        """项目代码 → tq 代码"""
        if self.market == Market.US:
            return f"{code}.US"
        if "." not in code:
            return code
        a, b = code.split(".", 1)
        return f"{b}.{a}"

    def _from_tq(self, tq_code: str) -> str:
        """tq 代码 → 项目代码"""
        if self.market == Market.US:
            return tq_code.rsplit(".", 1)[0]
        if "." not in tq_code:
            return tq_code
        parts = tq_code.rsplit(".", 1)
        return f"{parts[1]}.{parts[0]}"

    # ========================
    # Exchange 接口实现
    # ========================

    def default_code(self) -> str:
        return self._cfg["default_code"]

    def support_frequencys(self) -> dict:
        return {
            "y": "Y",
            "m": "M",
            "w": "W",
            "d": "D",
            "60m": "60m",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
        }

    def now_trading(self) -> bool:
        """判断当前是否交易时间，根据市场类型返回不同规则"""
        if self.market == Market.A:
            # A股：周一至周五 09:30-11:30, 13:00-15:00
            now = datetime.datetime.now()
            if now.weekday() in [5, 6]:
                return False
            h, m = now.hour, now.minute
            return (h == 9 and m >= 30) or h in [10, 13, 14] or (h == 11 and m < 30)

        elif self.market == Market.HK:
            # 港股：9:00-16:00（简单判断）
            hour = int(time.strftime("%H"))
            return hour in {9, 10, 11, 12, 13, 14, 15}

        elif self.market == Market.FUTURES:
            # 期货：9:00-12:00, 13:30-15:00, 21:00-02:30
            hour = int(time.strftime("%H"))
            minute = int(time.strftime("%M"))
            return (
                hour in {9, 10, 11, 14, 21, 22, 23, 0, 1}
                or (hour == 13 and minute >= 30)
                or (hour == 2 and minute <= 30)
            )

        elif self.market == Market.US:
            # 美股：美国东部时间 周一至周五 9:30-16:00
            tz = pytz.timezone("US/Eastern")
            now = datetime.datetime.now(tz)
            if now.weekday() in [5, 6]:
                return False
            h, m = now.hour, now.minute
            return (h == 9 and m >= 30) or (10 <= h < 16)

        return False

    def all_stocks(self):
        """获取股票列表"""
        if self._all_stocks:
            return self._all_stocks

        items = tq.get_stock_list(self._cfg["tq_market"], list_type=1)
        stocks = []

        # 美股过滤掉含特殊字符的代码（如 AAC=.US, ACHR+.US, BRK.A.US 等）
        _us_valid = (
            re.compile(r"^[A-Za-z0-9]+\.US$").match
            if self.market == Market.US
            else None
        )

        for item in items:
            tq_code = item["Code"] if isinstance(item, dict) else item

            # 美股：跳过含特殊字符的无效代码
            if _us_valid and not _us_valid(tq_code):
                continue

            # 港股，跳过后缀不是 .HK 的代码
            if self.market == Market.HK and not tq_code.endswith(".HK"):
                continue

            name = item.get("Name", tq_code) if isinstance(item, dict) else tq_code
            code = self._from_tq(tq_code)

            # 通过 get_stock_info 获取 type 和 precision
            stock_type, precision = self._get_stock_type_precision(tq_code)

            stocks.append(
                {
                    "code": code,
                    "name": name,
                    "type": stock_type,
                    "precision": precision,
                }
            )
        self._all_stocks = stocks
        return stocks

    @staticmethod
    def _get_stock_type_precision(tq_code: str):
        """通过 get_stock_info 获取股票类型和精度，失败时使用默认值"""
        try:
            info = tq.get_stock_info(tq_code, field_list=["XsFlag", "HSStockKind"])
        except Exception:
            return "stock_cn", 100

        if not info:
            return "stock_cn", 100

        # precision: 10^XsFlag（价格小数位数）
        xs_flag = str(info.get("XsFlag", "2"))
        try:
            precision = 10 ** int(xs_flag)
        except (ValueError, TypeError):
            precision = 100

        # type: HSStockKind 0=指数 1=主板 2=北证 3=创业板 4=科创板 5=B股 6=债券 7=基金 8=权证 9=其它
        kind = str(info.get("HSStockKind", "1"))
        if kind == "0":
            stock_type = "index_cn"
        elif kind == "6":
            stock_type = "bond_cn"
        elif kind == "7":
            stock_type = "etf_cn"
        else:
            stock_type = "stock_cn"

        return stock_type, precision

    def stock_info(self, code: str) -> Union[Dict, None]:
        """获取股票基本信息"""
        for s in self.all_stocks():
            if s["code"] == code:
                return s
        return None

    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        """获取 K 线数据"""
        tq_period = self._FREQ_TO_TQ.get(frequency)
        if tq_period is None:
            return None

        tq_code = self._to_tq(code)
        if args is None:
            args = {}

        # 复权参数
        fq = args.get("fq", "qfq")
        fq_map = {"qfq": "front", "hfq": "back", "none": "none"}
        dividend_type = fq_map.get(fq, "none")

        # count 数量
        count = args.get("count", 5000)

        # 日期格式转换：YYYY-MM-DD → YYYYMMDD
        def _fmt(s: str) -> str:
            if s is None:
                return datetime.datetime.now().strftime("%Y%m%d")
            return s.replace("-", "")

        try:
            result = tq.get_market_data(
                stock_list=[tq_code],
                period=tq_period,
                start_time="",
                end_time=_fmt(end_date),
                count=count,
                dividend_type=dividend_type,
            )
        except Exception:
            return None

        if not result:
            return pd.DataFrame()

        # get_market_data 返回 {'Open': DataFrame, 'Close': DataFrame, ...}
        # 每个 DataFrame 的 index 是时间，columns 是股票代码
        col_map = {
            "open": "Open",
            "close": "Close",
            "high": "High",
            "low": "Low",
            "volume": "Volume",
        }
        data = {}
        for proj_name, tq_name in col_map.items():
            if tq_name in result and tq_code in result[tq_name].columns:
                data[proj_name] = result[tq_name][tq_code]

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df.index.name = "date"
        df = df.reset_index()

        # 时区处理
        if df["date"].dt.tz is None:
            df["date"] = df["date"].dt.tz_localize(self.tz)

        df["code"] = code
        df["frequency"] = frequency

        # 日线及以上周期，时间统一为 15:00
        if frequency in ("d", "w", "m", "y"):
            df["date"] = df["date"].apply(lambda d: d.replace(hour=15, minute=0))

        return df[["date", "code", "open", "close", "high", "low", "volume"]]

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """获取 Tick 数据"""
        ticks = {}
        for code in codes:
            tq_code = self._to_tq(code)
            snap = tq.get_market_snapshot(tq_code)
            if not snap:
                continue
            try:
                price = float(snap.get("Now", 0) or 0)
                last_close = float(snap.get("LastClose", 0) or 0)
                buy_p = snap.get("Buyp", [])
                sell_p = snap.get("Sellp", [])
                ticks[code] = Tick(
                    code=code,
                    last=price,
                    buy1=float(buy_p[0]) if buy_p else 0,
                    sell1=float(sell_p[0]) if sell_p else 0,
                    high=float(snap.get("Max", 0) or 0),
                    low=float(snap.get("Min", 0) or 0),
                    open=float(snap.get("Open", 0) or 0),
                    volume=float(snap.get("Volume", 0) or 0),
                    rate=(
                        round((price - last_close) / last_close * 100, 2)
                        if last_close != 0
                        else 0
                    ),
                )
            except (ValueError, TypeError):
                continue
        return ticks

    def stock_owner_plate(self, code: str):
        """股票所属板块信息（仅返回行业和概念板块）"""
        tq_code = self._to_tq(code)
        try:
            blocks = tq.get_relation(tq_code)
        except Exception:
            return {"HY": [], "GN": []}

        if not blocks:
            return {"HY": [], "GN": []}

        hys, gns = [], []
        for item in blocks:
            block_type = item.get("BlockType", "")
            if block_type not in ("行业", "概念"):
                continue

            block_code = item.get("BlockCode", "0")
            block_name = item.get("BlockName", "")
            if not block_name:
                continue

            # BlockCode 为 "0" 时没有板块代码，用名称代替
            code = (
                self._from_tq(block_code)
                if block_code and block_code != "0"
                else block_name
            )
            entry = {"code": code, "name": block_name}

            if block_type == "行业":
                hys.append(entry)
            else:
                gns.append(entry)

        return {"HY": hys, "GN": gns}

    def plate_stocks(self, code: str):
        """板块成分股"""
        tq_block_code = self._to_tq(code)
        try:
            tq_codes = tq.get_stock_list_in_sector(tq_block_code)
        except Exception:
            return []

        stocks = []
        for tc in tq_codes:
            c = self._from_tq(tc)
            info = self.stock_info(c) or {
                "code": c,
                "name": c,
                "type": "",
                "precision": 100,
            }
            stocks.append(info)
        return stocks

    def balance(self):
        raise Exception("交易所不支持")

    def positions(self, code: str = ""):
        raise Exception("交易所不支持")

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise Exception("交易所不支持")


if __name__ == "__main__":

    ex = ExchangeTDXQuant(Market.A)
    # stocks = ex.all_stocks()
    # print(stocks)
    # print(len(stocks))

    # info = tq.get_stock_info("ZROZ.US", field_list=["XsFlag", "HSStockKind"])
    # print(info)

    klines = ex.klines("SZ.000001", "d")
    print(klines)

    # 回调函数 功能为收到更新后请求最新的report数据

    # def my_callback_func(data_str):
    #     print("Callback received data:", data_str)
    #     code_json = json.loads(data_str)
    #     print(f"codes = {code_json.get('Code')}")
    #     print(code_json)
    #     # report_ptr = tq.get_report_data(code_json.get("Code"))
    #     # print(report_ptr)
    #     return None

    # sub_hq = tq.subscribe_hq(stock_list=["000001.SZ"], callback=my_callback_func)
    # print(sub_hq)

    refresh_kline = tq.refresh_kline(stock_list=["000001.SZ"], period="1m")
    print("1m klines: ", refresh_kline)
    refresh_kline = tq.refresh_kline(stock_list=["000001.SZ"], period="5m")
    print("5m klines: ", refresh_kline)
    refresh_kline = tq.refresh_kline(stock_list=["000001.SZ"], period="1d")
    print("1d klines: ", refresh_kline)

    refresh_cache = tq.refresh_cache()
    print(refresh_cache)

    # time.sleep(20)
    # print("Done")

    ticks = ex.ticks(["SZ.000001"])
    print(ticks)

    # bk = ex.stock_owner_plate("SZ.000001")
    # print(bk)

    # bk_codes = ex.plate_stocks("SH.881386")
    # print(bk_codes)
