import datetime
import time
from typing import Dict, List, Union

import pandas as pd
import pytz
try:
    from tenacity import retry, retry_if_result, stop_after_attempt, wait_random
except Exception:
    def retry(*args, **kwargs):
        def _decorator(func):
            return func

        return _decorator

    def retry_if_result(*args, **kwargs):
        return None

    def stop_after_attempt(*args, **kwargs):
        return None

    def wait_random(*args, **kwargs):
        return None

from chanlun import fun
from chanlun.exchange.exchange import Exchange, Tick, convert_stock_kline_frequency
try:
    from xtquant import xtdata
except Exception:
    xtdata = None

import re
"""
QMT 沪深行情
"""


class ExchangeQMT(Exchange):
    """
    QMT 行情基类
    """
    g_all_stocks = []

    def __init__(self):
        if xtdata is None:
            raise Exception("xtquant 未安装或不可用，无法使用 QMT 行情。请确认 xtquant 已放入 src 目录并可正常 import。")

        xtdata.enable_hello = False

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")

    def default_code(self):
        return "SH.000001"

    def support_frequencys(self):
        return {
            "y": "1y",
            "m": "1mon",
            "w": "1w",
            "d": "1d",
            "60m": "1h",
            "30m": "30m",
            "15m": "15m",
            "5m": "5m",
            "1m": "1m",
            "3m": "3m",
            "10m": "10m",
            "2h": "2h",
            "4h": "4h",
            "6h": "6h",
        }

    def code_to_tdx(self, code: str):
        """
        QMT 代码转 TDX 代码
        需要在子类实现
        """
        return code

    def code_to_qmt(self, code: str):
        """
        TDX 代码转 QMT 代码
        需要在子类实现
        """
        return code

    def all_stocks(self):
        """
        获取所有代码
        需要在子类实现
        """
        return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=5),
        retry=retry_if_result(lambda _r: _r is None),
        retry_error_callback=lambda _retry_state: None,
    )
    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> Union[pd.DataFrame, None]:
        frequency_map = {
            "y": "1d",
            "m": "1d",
            "w": "1d",
            "d": "1d",
            "1d": "1d",
            "60m": "5m",
            "30m": "5m",
            "15m": "5m",
            "5m": "5m",
            "3m": "1m",
            "1m": "1m",
            "10m": "5m",
            "2h": "5m",
            "4h": "5m",
            "6h": "5m",
        }
        # TODO 多数据，自定义周去转换消耗时间比较长，可修改为增量替换
        frequency_count = {
            "y": -1,
            "m": 8000 * 20,
            "w": 8000 * 5,
            "d": 8000,
            "1d": 8000,
            "60m": 8000 * 12,
            "30m": 8000 * 6,
            "15m": 8000 * 3,
            "5m": 8000,
            "10m": 8000 * 2,
            "3m": 8000 * 3,
            "1m": 8000,
            "2h": 8000 * 24,
            "4h": 8000 * 48,
            "6h": 8000 * 72,
        }
        # 复权方式
        dividend_type = "front"
        if args is not None and "dividend_type" in args:
            dividend_type = args["dividend_type"]

        # 指定请求数据条数
        req_counts = frequency_count[frequency]
        if args is not None and "req_counts" in args:
            req_counts = args["req_counts"]
        if start_date:
            req_counts = -1

        qmt_code = self.code_to_qmt(code)

        # 首先检查当前是否有数据
        kline_exists = xtdata.get_market_data(
            field_list=[],
            stock_list=[qmt_code],
            period=frequency_map[frequency],
            start_time="",
            end_time="",
            count=1,
            dividend_type=dividend_type,
            fill_data=False,
        )
        if kline_exists is None or kline_exists["time"].empty:
            # 如果没有数据，则全量下载
            s_time = time.time()
            # 根据周期，决定下载的时间起始日期
            if frequency in ["1m", "3m"]:
                download_start_date = fun.datetime_to_str(
                    datetime.datetime.now() - datetime.timedelta(days=180), "%Y%m%d"
                )
            elif frequency in ["5m", "10m", "15m", "30m", "60m", "2h", "4h", "6h"]:
                download_start_date = fun.datetime_to_str(
                    datetime.datetime.now() - datetime.timedelta(days=2880), "%Y%m%d"
                )
            else:
                download_start_date = ""
            if args is not None and "download_start_date" in args:
                download_start_date = args["download_start_date"]

            xtdata.download_history_data(
                qmt_code,
                frequency_map[frequency],
                start_time=download_start_date,
                end_time="",
                incrementally=True,
            )
            # print(f"{code}-{frequency} 全量下载历史数据耗时：{time.time() - s_time}")
        else:
            # 增量下载
            # s_time = time.time()
            xtdata.download_history_data(
                qmt_code,
                frequency_map[frequency],
                start_time="",
                end_time="",
                incrementally=True,
            )
            # print(f"{code}-{frequency} 增量下载历史数据耗时：{time.time() - s_time}")

        # s_time = time.time()
        qmt_klines = xtdata.get_market_data(
            field_list=[],
            stock_list=[qmt_code],
            period=frequency_map[frequency],
            start_time=start_date.replace("-", "").replace(":", "").replace(" ", "") if start_date else "",
            end_time="",
            count=req_counts,
            dividend_type=dividend_type,
            fill_data=False,
        )
        # print(f"{code}-{frequency} 获取历史数据耗时：{time.time() - s_time}")

        if qmt_klines is None:
            return None
        
        # 检查是否有数据
        for _k, _v in qmt_klines.items():
            if _v.empty:
                return None

        s_time = time.time()
        try:
            klines_df = pd.DataFrame(
                {key: value.values[0] for key, value in qmt_klines.items()}
            )
        except Exception as e:
            print(f"QMT klines error: {e}")
            return None
        klines_df["code"] = code

        klines_df["date"] = pd.to_datetime(
            klines_df["time"], unit="ms", utc=True
        ).dt.tz_convert(self.tz)
        klines_df = klines_df[
            ["code", "date", "open", "high", "low", "close", "volume"]
        ]
        # 将 open high low close volume 转换为 float
        klines_df[["open", "high", "low", "close", "volume"]] = klines_df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)
        klines_df = klines_df.sort_values("date")

        # 如果日线，小时设置为15点
        if frequency in ["d", "w", "m", "y"]:
            klines_df["date"] = klines_df["date"].apply(lambda x: x.replace(hour=15))

        # print(f"{code}-{frequency} 获取历史数据转换耗时：{time.time() - s_time}")

        if frequency not in ["d","5m", "1m"]:
            # s_time = time.time()
            klines_df = convert_stock_kline_frequency(klines_df, frequency)
            # print(f"{code}-{frequency} 转换历史周期数据耗时：{time.time() - s_time}")

        # print(klines_df.tail(2))
        if req_counts > 0:
            klines_df = klines_df.iloc[-req_counts:]

        return klines_df

    def stock_info(self, code: str) -> Union[Dict, None]:
        """
        获取股票名称
        """
        qmt_code = self.code_to_qmt(code)
        stock_detail = xtdata.get_instrument_detail(qmt_code, False)
        if stock_detail is None or "InstrumentName" not in stock_detail:
            return None
        return {
            "code": code,
            "name": stock_detail["InstrumentName"],
            "precision": fun.reverse_decimal_to_power_of_ten(stock_detail["PriceTick"]),
        }

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        """
        获取 tick 信息
        """
        ticks = {}
        if len(codes) == 0:
            return ticks
        qmt_ticks = xtdata.get_full_tick([self.code_to_qmt(_c) for _c in codes])
        for _c, _t in qmt_ticks.items():
            ticks[self.code_to_tdx(_c)] = Tick(
                code=self.code_to_tdx(_c),
                last=_t["lastPrice"],
                buy1=_t["bidPrice"][0],
                sell1=_t["askPrice"][0],
                high=_t["high"],
                low=_t["low"],
                open=_t["open"],
                volume=_t["volume"],
                rate=(_t["lastPrice"] - _t["lastClose"]) / _t["lastClose"] * 100,
            )

        return ticks

    def all_ticks(self) -> Dict[str, Tick]:
        """
        获取所有 Tick 数据
        """
        ticks = {}
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]
        
        # 根据子类逻辑获取所有 tick
        # 这里需要一个获取所有 tick 的方法，或者让子类重写 all_ticks
        # 为了复用，可以在子类中定义 market_list 供 get_full_tick 使用
        return {} # Should be overridden or implemented via common logic

    def get_divid_factors(self, stock_code: str) -> pd.DataFrame:
        """
        获取股票除权除息信息
        """
        df = xtdata.get_divid_factors(self.code_to_qmt(stock_code))
        if df is None or df.empty:
            return None
        df.loc[:, "stock_code"] = stock_code
        df["divid_date"] = pd.to_datetime(df["time"] / 1000, unit="s")
        return df

    def subscribe_all_ticks(self, callback):
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in all_codes:
                    continue
                callback(_tdx_code, _tick)

        # 订阅逻辑需要子类提供 market list
        pass

    def subscribe_stocks_quotes(self, codes: List[str], callback):
        """
        订阅股票行情
        """
        qmt_codes = [self.code_to_qmt(_c) for _c in codes]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in codes:
                    continue
                callback(_tdx_code, _tick)

        xtdata.subscribe_whole_quote(qmt_codes, on_tick)
        xtdata.run()

    def now_trading(self):
        """
        返回当前是否是交易时间
        """
        now = datetime.datetime.now()
        weekday = now.weekday() # 0-6 (Mon-Sun)
        hour = now.hour
        minute = now.minute
        time_float = hour + minute / 60.0

        if weekday == 6:
            return False
        
        if weekday == 5:
            if time_float >= 2.5:
                return False
        
        if 9.0 <= time_float <= 11.5:
            return True
            
        if 13.0 <= time_float <= 15.25:
            return True
            
        if time_float >= 21.0:
            return True
            
        if weekday == 0 and time_float < 9.0:
            return False
            
        if 0.0 <= time_float <= 2.5:
            return True
            
        return False

    def stock_owner_plate(self, code: str):
        raise Exception("交易所不支持")

    def plate_stocks(self, code: str):
        raise Exception("交易所不支持")

    def balance(self):
        raise Exception("QMT 交易功能在 trader 目录实现")

    def positions(self, code: str = ""):
        raise Exception("QMT 交易功能在 trader 目录实现")

    def order(self, code: str, o_type: str, amount: float, args=None):
        raise Exception("QMT 交易功能在 trader 目录实现")


class ExchangeQMTStock(ExchangeQMT):
    """
    QMT A股行情
    """
    g_all_stocks = []
    def code_to_tdx(self, code: str):
        """
        QMT 格式：600519.SH
        TDX 格式：SH.600519
        """
        if "." not in code:
            return code
        _c = code.split(".")
        if _c[1] in ["SH", "SZ", "BJ"]:
            return _c[1] + "." + _c[0]
        return code

    def code_to_qmt(self, code: str):
        """
        TDX 格式：SH.600519
        QMT 格式：600519.SH
        """
        if "." not in code:
            return code
        _c = code.split(".")
        if _c[0] in ["SH", "SZ", "BJ"]:
            return _c[1] + "." + _c[0]
        return code

    def all_stocks(self):
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        # 黑名单 code
        black_codes = [
            "SZ.399290", "SZ.399289", "SZ.399302", "SZ.399298", "SZ.399481",
            "SZ.399299", "SZ.399301", "SH.000013", "SH.000022", "SH.000116",
            "SH.000061", "SH.000101", "SH.000012", "SZ.988201", "SZ.980068",
            "SZ.980001", "SZ.980023",
        ]

        ticks = xtdata.get_full_tick(["SH", "SZ", "BJ"])
        tick_codes = list(ticks.keys())

        all_stocks = []
        for _c in tick_codes:
            _stock_type: dict = xtdata.get_instrument_type(_c)
            if _stock_type.get("stock") or _stock_type.get("etf") or _stock_type.get("index"):
                pass
            else:
                continue
            _stock = self.stock_info(self.code_to_tdx(_c))
            if _stock:
                all_stocks.append(_stock)

        all_stocks = [_s for _s in all_stocks if _s["code"] not in black_codes]
        self.g_all_stocks = all_stocks
        return self.g_all_stocks

    def all_ticks(self) -> Dict[str, Tick]:
        ticks = {}
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]
        qmt_ticks = xtdata.get_full_tick(["SH", "SZ", "BJ"])
        for _c, _t in qmt_ticks.items():
            _tdx_code = self.code_to_tdx(_c)
            if _tdx_code not in all_codes:
                continue
            ticks[_tdx_code] = Tick(
                code=_tdx_code,
                last=_t["lastPrice"],
                buy1=_t["bidPrice"][0],
                sell1=_t["askPrice"][0],
                high=_t["high"],
                low=_t["low"],
                open=_t["open"],
                volume=_t["volume"],
                rate=(_t["lastPrice"] - _t["lastClose"]) / _t["lastClose"] * 100 if _t["lastClose"] != 0 else 0,
            )
        return ticks

    def subscribe_all_ticks(self, callback):
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in all_codes:
                    continue
                callback(_tdx_code, _tick)

        xtdata.subscribe_whole_quote(["SH", "SZ", "BJ"], on_tick)
        xtdata.run()


class ExchangeQMTFutures(ExchangeQMT):
    """
    QMT 期货行情
    """
    g_all_stocks = []
    def default_code(self):
        return "SHFE.rb2310"

    def code_to_tdx(self, code: str):
        """
        QMT 格式：rb2306.SF
        TDX 格式：SHFE.rb2306
        """
        if "." not in code:
            return code
        _c = code.split(".")
        qmt_markets = ["SF", "IF", "DF", "ZF", "INE", "GF"]
        qmt_to_tdx_map = {
            "SF": "SHFE", "DF": "DCE", "ZF": "CZCE",
            "IF": "CFFEX", "INE": "INE", "GF": "GFEX"
        }
        if _c[1] in qmt_markets:
            market = _c[1]
            if market in qmt_to_tdx_map:
                market = qmt_to_tdx_map[market]
            return market + "." + _c[0]
        return code

    def code_to_qmt(self, code: str):
        """
        TDX 格式：SHFE.rb2306
        QMT 格式：rb2306.SF
        """
        if "." not in code:
            return code
        _c = code.split(".")
        tdx_to_qmt_map = {
            "SHFE": "SF", "DCE": "DF", "CZCE": "ZF",
            "CFFEX": "IF", "INE": "INE", "GFEX": "GF"
        }
        if _c[0] in tdx_to_qmt_map:
            market = tdx_to_qmt_map[_c[0]]
            return _c[1] + "." + market
        return code

    def all_stocks(self):
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        # 获取所有期货 tick
        ticks = xtdata.get_full_tick(["SF", "IF", "DF", "ZF", "INE", "GF"])
        tick_codes = list(ticks.keys())

        all_stocks = []
        for _c in tick_codes:
            # 过滤掉组合合约
            if "&" in _c or " " in _c:
                continue
            # 过滤过期期权
            if "-C-" in _c or "-P-" in _c :
                # # 标的编码和-C或-P之间是4位数字前2位不是当前年份26，剔除
                # if not re.match(r"^[0-9]{2}" + str(datetime.datetime.now().year)[2:] + r"[0-9]{2}$", _c.split("-")[0]):
                #     continue
                continue

            # 使用正则过滤纯期货合约（剔除期权）
            # QMT 期货代码格式：Code.Market (rb2310.SF)
            # 正则匹配 Code 部分：^[a-zA-Z]+[0-9]+$ (字母+数字)
            # 剔除 rb2310P3000 (字母+数字+字母+数字)
            try:
                code_body = _c.split(".")[0]
                if not re.match(r"^[a-zA-Z]+[0-9]+$", code_body):
                    continue
            except Exception:
                continue

            # 尝试获取类型判断，如果获取不到，默认保留（如果是纯期货格式）
            _stock_type: dict = xtdata.get_instrument_type(_c)
            if _stock_type and not _stock_type.get("future"):
                continue
            
            # 过滤掉非主力合约或过期合约? 
            # 暂时不过滤，获取所有
            _stock = self.stock_info(self.code_to_tdx(_c))
            if _stock:
                all_stocks.append(_stock)

        self.g_all_stocks = all_stocks
        return self.g_all_stocks

    def all_ticks(self) -> Dict[str, Tick]:
        ticks = {}
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]
        qmt_ticks = xtdata.get_full_tick(["SF", "IF", "DF", "ZF", "INE", "GF"])
        for _c, _t in qmt_ticks.items():
            _tdx_code = self.code_to_tdx(_c)
            if _tdx_code not in all_codes:
                continue
            ticks[_tdx_code] = Tick(
                code=_tdx_code,
                last=_t["lastPrice"],
                buy1=_t["bidPrice"][0],
                sell1=_t["askPrice"][0],
                high=_t["high"],
                low=_t["low"],
                open=_t["open"],
                volume=_t["volume"],
                rate=(_t["lastPrice"] - _t["lastClose"]) / _t["lastClose"] * 100 if _t["lastClose"] != 0 else 0,
            )
        return ticks

    def subscribe_all_ticks(self, callback):
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in all_codes:
                    continue
                callback(_tdx_code, _tick)

        xtdata.subscribe_whole_quote(["SF", "IF", "DF", "ZF", "INE", "GF"], on_tick)
        xtdata.run()


class ExchangeQMTOption(ExchangeQMT):
    """
    QMT 期权行情
    """
    g_all_stocks = []
    def default_code(self):
        return "10005329.SHO" # 示例期权代码

    def code_to_tdx(self, code: str):
        """
        QMT 格式：ag2602-C-28000.SF
        TDX 格式：SHFE.ag2602-C-28000
        """
        if "." not in code:
            return code
        _c = code.split(".")
        if _c[1] in ["SH", "SZ", "BJ"]:
            return _c[1] + "." + _c[0]
        qmt_markets = ["SF", "IF", "DF", "ZF", "INE", "GF"]
        qmt_to_tdx_map = {
            "SF": "SHFE", "DF": "DCE", "ZF": "CZCE",
            "IF": "CFFEX", "INE": "INE", "GF": "GFEX"
        }
        if _c[1] in qmt_markets:
            market = _c[1]
            if market in qmt_to_tdx_map:
                market = qmt_to_tdx_map[market]
            return market + "." + _c[0]
        return code

    def code_to_qmt(self, code: str):
        """
        QMT 格式：ag2602-C-28000.SF
        TDX 格式：SHFE.ag2602-C-28000
        """
        if "." not in code:
            return code
        _c = code.split(".")
        if _c[1] in ["SH", "SZ", "BJ"]:
            return _c[1] + "." + _c[0]
        tdx_to_qmt_map = {
            "SHFE": "SF", "DCE": "DF", "CZCE": "ZF",
            "CFFEX": "IF", "INE": "INE", "GFEX": "GF"
        }
        if _c[0] in tdx_to_qmt_map:
            market = tdx_to_qmt_map[_c[0]]
            return _c[1] + "." + market
        return code

    def all_stocks(self):
        if len(self.g_all_stocks) > 0:
            return self.g_all_stocks

        # 获取所有期权 tick
        # 期权市场通常包括 沪深ETF期权(SHO, SZO) 和 股指期权/商品期权(中金所IF, 郑商所ZF, 大商所DF, 上期所SF)
        markets = ["SHO", "SZO", "SF", "IF", "DF", "ZF", "INE", "GF"]
        ticks = xtdata.get_full_tick(markets)
        tick_codes = list(ticks.keys())

        all_stocks = []
        for _c in tick_codes:
                       # 过滤掉组合合约
            if "&" in _c or " " in _c:
                continue
            # 由于 get_instrument_type 对期权返回空字典，我们改用合约代码特征过滤
            base_code = _c.split(".")[0]
            digits_groups = re.findall(r"\d+", base_code)
            has_cp_flag = "-C-" in _c or "-P-" in _c or re.search(r"[CP]", base_code, re.IGNORECASE)
            is_etf_option = ".SHO" in _c or ".SZO" in _c
            is_option = is_etf_option or (has_cp_flag and len(digits_groups) >= 2)
            if not is_option:
                continue

            if not is_etf_option and has_cp_flag and digits_groups:
                exp_num = digits_groups[0]
                if len(exp_num) >= 4:
                    year = int(exp_num[:2])
                    now_year = int(datetime.datetime.now().strftime("%y"))
                    next_year = (now_year + 1) % 100
                    if year not in [now_year, next_year]:
                        continue
            
            # 过滤掉过期合约? (可选)
            # 暂时保留所有
            
            _stock = self.stock_info(self.code_to_tdx(_c))
            if _stock:
                all_stocks.append(_stock)

        self.g_all_stocks = all_stocks
        return self.g_all_stocks

    def all_ticks(self) -> Dict[str, Tick]:
        ticks = {}
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]
        markets = ["SHO", "SZO", "SF", "IF", "DF", "ZF", "INE", "GF"]
        qmt_ticks = xtdata.get_full_tick(markets)
        for _c, _t in qmt_ticks.items():
            _tdx_code = self.code_to_tdx(_c)
            if _tdx_code not in all_codes:
                continue
            ticks[_tdx_code] = Tick(
                code=_tdx_code,
                last=_t["lastPrice"],
                buy1=_t["bidPrice"][0],
                sell1=_t["askPrice"][0],
                high=_t["high"],
                low=_t["low"],
                open=_t["open"],
                volume=_t["volume"],
                rate=(_t["lastPrice"] - _t["lastClose"]) / _t["lastClose"] * 100 if _t["lastClose"] != 0 else 0,
            )
        return ticks

    def subscribe_all_ticks(self, callback):
        all_stocks = self.all_stocks()
        all_codes = [_s["code"] for _s in all_stocks]
        markets = ["SHO", "SZO", "SF", "IF", "DF", "ZF", "INE", "GF"]

        def on_tick(_ticks):
            for _code, _tick in _ticks.items():
                _tdx_code = self.code_to_tdx(_code)
                if _tdx_code not in all_codes:
                    continue
                callback(_tdx_code, _tick)

        xtdata.subscribe_whole_quote(markets, on_tick)
        xtdata.run()


if __name__ == "__main__":
    ex = ExchangeQMTStock()
    # ex = ExchangeQMTFutures()

    # stocks = ex.all_stocks()
    # stock_maps = {}
    # for _s in stocks:
    #     stock_maps[_s["code"][0:5]] = _s
    # for _t, _s in stock_maps.items():
    #     print(_t, _s)
    # print(len(stocks))

    # klines = ex.klines(
    #     "SH.000001",
    #     "d",
    # )
    # print(klines)

    # stock = ex.stock_info("SH.000001")
    # print(stock)

    # df = ex.get_divid_factors("SH.600519")

    # print(df)

    def on_klines(_qmt_code, tick):
        if _qmt_code != "600519.SH":
            return
        print(
            _qmt_code,
            "最新价格",
            tick["lastPrice"],
            " 时间：",
            fun.timeint_to_datetime(int(tick["time"] / 1000)),
        )
        _tdx_code = ex.code_to_tdx(_qmt_code)
        print(tick)
        # for _f in ["1m", "5m", "d"]:
        #     print(f"周期：{_f}")
        #     klines_df = ex.klines(_tdx_code, _f, args={"req_counts": 2})

        #     print(klines_df)
        print("-" * 20)

    ex.subscribe_all_ticks(on_klines)

    # ticks = ex.all_ticks()
    # print(len(ticks))
