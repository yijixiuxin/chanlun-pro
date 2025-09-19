# -*- coding: utf-8 -*-
"""
技术指标计算模块
负责根据输入的K线数据计算如MACD等技术指标。
"""
from typing import List, Dict
import numpy as np
from talib import abstract

from chanlun.core.cl_interface import Kline  # 修改：导入 Kline 而不是 CLKline


class MACD:
    """
    MACD指标计算类
    - 每次都基于全量K线数据进行计算
    - 统一管理MACD相关数据 (DIF, DEA, HIST)
    """

    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        """
        初始化MACD计算器
        Args:
            fast_period (int): DIF的快速EMA周期
            slow_period (int): DIF的慢速EMA周期
            signal_period (int): DEA的EMA周期
        """
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

        # 存储计算结果
        self.dif: List[float] = []
        self.dea: List[float] = []
        self.hist: List[float] = []

    def process_macd(self, klines: List[Kline]):
        """
        使用全量的K线数据更新MACD计算。

        Args:
            klines (List[Kline]): 全量的K线数据列表
        """
        # --- 执行计算 ---
        # 确保有足够的K线来计算MACD
        if len(klines) < self.slow_period:
            # 数据不足，清空结果
            self.dif, self.dea, self.hist = [], [], []
            return

        # 提取收盘价
        close_prices = np.array([k.c for k in klines])

        # 使用talib计算MACD
        # abstract.MACD 返回 (macd, macdsignal, macdhist)
        macd_result = abstract.MACD(
            close_prices,
            fastperiod=self.fast_period,
            slowperiod=self.slow_period,
            signalperiod=self.signal_period
        )

        # 更新结果，talib返回的是numpy数组，转换为列表
        self.dif = macd_result[0].tolist()
        self.dea = macd_result[1].tolist()
        self.hist = macd_result[2].tolist()

    def get_results(self) -> Dict:
        """
        获取格式化的MACD指标数据

        Returns:
            Dict: 包含DIF, DEA, HIST列表的字典
        """
        return {
            'macd': {
                'dif': self.dif,
                'dea': self.dea,
                'hist': self.hist
            }
        }
