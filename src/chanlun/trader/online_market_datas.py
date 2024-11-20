"""
线上行情数据获取对象，用于实盘交易执行
"""

from typing import List, Dict

import pandas as pd

from chanlun.backtesting.base import MarketDatas
from chanlun.cl_interface import ICL
from chanlun.exchange.exchange import Exchange
from chanlun.file_db import FileCacheDB


class OnlineMarketDatas(MarketDatas):
    """
    线上实盘交易数据获取类
    """

    def __init__(
        self,
        market: str,
        frequencys: List[str],
        ex: Exchange,
        cl_config: dict,
        use_cache=True,
    ):
        """
        初始化
        use_cache 是否使用缓存，如果设置为 True，在每次循环都需要显式调用 clear_cache 清除缓存，避免后续无法获取最新行情数据
        """
        super().__init__(market, frequencys, cl_config)
        self.ex = ex
        self.fdb = FileCacheDB()

        self.use_cache = use_cache

        # 缓存请求的 k 线数据，需要显示清除，key 为 code_frequency 格式
        self.cache_klines: Dict[str, pd.DataFrame] = {}

    def clear_cache(self):
        """
        需要在实盘每次循环完后清空缓存
        """
        # 缓存请求的 k 线数据，需要显示清除，key 为 code_frequency 格式
        self.cache_klines = {}
        return True

    def klines(self, code, frequency) -> pd.DataFrame:
        key = f"{code}_{frequency}"
        if self.use_cache and key in self.cache_klines.keys():
            return self.cache_klines[key]
        klines = self.ex.klines(code, frequency)  # TDX 接口尽量返回数据多一些
        if self.use_cache:
            self.cache_klines[key] = klines
        return klines

    def last_k_info(self, code) -> dict:
        klines = self.klines(code, self.frequencys[-1])
        return {
            "date": klines.iloc[-1]["date"],
            "open": float(klines.iloc[-1]["open"]),
            "close": float(klines.iloc[-1]["close"]),
            "high": float(klines.iloc[-1]["high"]),
            "low": float(klines.iloc[-1]["low"]),
        }

    def get_cl_data(self, code, frequency, cl_config: dict = None) -> ICL:
        # 根据回测配置，可自定义不同周期所使用的缠论配置项
        if code in self.cl_config.keys():
            cl_config = self.cl_config[code]
        elif frequency in self.cl_config.keys():
            cl_config = self.cl_config[frequency]
        elif "default" in self.cl_config.keys():
            cl_config = self.cl_config["default"]
        else:
            cl_config = self.cl_config

        klines = self.klines(code, frequency)

        cd = self.fdb.get_web_cl_data(self.market, code, frequency, cl_config, klines)
        return cd
