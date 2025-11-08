# -*- coding: utf-8 -*-
"""
技术指标计算模块
负责根据输入的K线数据计算如MACD等技术指标。
"""
from typing import List, Dict
import numpy as np
from talib import abstract

from chanlun.core.cl_interface import Kline


class MACD:
    """
    MACD指标计算类
    - 每次都基于全量K线数据进行计算
    - 统一管理MACD相关数据 (DIF, DEA, HIST)
    - 优化了Numpy数组的创建
    - 增加了对空数据和计算异常的处理
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

        # 存储计算结果，初始化为空列表
        # 类型提示：列表中可能包含 float 或 nan (nan 也是 float 类型)
        self.dif: List[float] = []
        self.dea: List[float] = []
        self.hist: List[float] = []

    def process_macd(self, klines: List[Kline]):
        """
        使用全量的K线数据更新MACD计算。
        (此方法会覆盖上一次的计算结果)

        Args:
            klines (List[Kline]): 全量的K线数据列表
        """
        # --- 1. 数据有效性检查 ---
        if not klines:
            # 如果没有K线数据，清空结果并返回
            self.dif, self.dea, self.hist = [], [], []
            return

        # --- 2. 提取收盘价 ---
        # 使用 np.fromiter 替代 [k.c for k in klines] + np.array()
        close_prices = np.fromiter((k.c for k in klines), dtype=np.float64, count=len(klines))

        # --- 3. 执行计算 ---
        # abstract.MACD 返回 (macd, macdsignal, macdhist)
        macd_result = abstract.MACD(
            close_prices,
            fastperiod=self.fast_period,
            slowperiod=self.slow_period,
            signalperiod=self.signal_period
        )

        # --- 4. 更新实例属性 ---
        # talib返回的是numpy数组，转换为列表
        # .tolist() 会将 np.nan 转换为 Python 的 float 'nan'
        self.dif = macd_result[0].tolist()
        self.dea = macd_result[1].tolist()
        self.hist = macd_result[2].tolist()

    def get_results(self) -> Dict[str, Dict[str, List[float]]]:
        """
        获取格式化的MACD指标数据

        Returns:
            Dict[str, Dict[str, List[float]]]:
                包含DIF, DEA, HIST列表的字典，格式为:
                {'macd': {'dif': [...], 'dea': [...], 'hist': [...]}}
        """
        return {
            'macd': {
                'dif': self.dif,
                'dea': self.dea,
                'hist': self.hist
            }
        }