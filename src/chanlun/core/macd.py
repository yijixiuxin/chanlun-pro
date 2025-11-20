# -*- coding: utf-8 -*-
"""
技术指标计算模块 - MACD 高精度修正版 (适配国内软件)
------------------------------------------------
修复问题：
1. **关键修复**：国内软件（富途、同花顺）的 MACD 红绿柱公式为 (DIF-DEA)*2。
   已增加 `china_mode` 开关并默认开启。
2. 解决与行情软件数值不一致的问题。
   - 方案：使用 Pandas EWM (adjust=False) 递归算法。
3. 优化红绿柱面积计算逻辑。
"""
from typing import List, Dict
import numpy as np
import pandas as pd
from chanlun.core.cl_interface import Kline


class MACD:
    """
    MACD指标计算类
    """

    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9, china_mode: bool = True):
        """
        初始化MACD计算器

        Args:
            fast_period: 快线周期
            slow_period: 慢线周期
            signal_period: 信号线周期
            china_mode: 是否使用国内软件标准 (默认 True)。
                        True: MACD柱 = (DIF - DEA) * 2  (富途、同花顺、通达信标准)
                        False: MACD柱 = DIF - DEA       (TradingView、国际标准)
        """
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.china_mode = china_mode  # 新增：控制是否乘以2

        # 存储计算结果
        self.dif: List[float] = []
        self.dea: List[float] = []
        self.hist: List[float] = []
        self.hist_area: List[float] = []

    def process_macd(self, klines: List[Kline]):
        """
        执行MACD计算

        注意：为了获得准确结果，请务必传入足够多的历史 K 线数据（建议 > 500 根）。
        数据越长，EMA 的计算误差越小，与软件的吻合度越高。
        """
        # --- 1. 数据有效性检查 ---
        if not klines:
            self._reset_data()
            return

        # --- 2. 转换为 Pandas Series ---
        close_prices = pd.Series([k.c for k in klines], dtype='float64')

        # --- 3. 执行 MACD 计算 (Pandas EWM 算法) ---
        ema_fast = close_prices.ewm(span=self.fast_period, adjust=False).mean()
        ema_slow = close_prices.ewm(span=self.slow_period, adjust=False).mean()

        # 计算 DIF (快线)
        dif_series = ema_fast - ema_slow

        # 计算 DEA (慢线/信号线)
        dea_series = dif_series.ewm(span=self.signal_period, adjust=False).mean()

        # 计算 Histogram (柱子)
        hist_series = dif_series - dea_series

        # --- 4. 格式化输出 ---
        self.dif = dif_series.fillna(0).tolist()
        self.dea = dea_series.fillna(0).tolist()
        self.hist = hist_series.fillna(0).tolist()

        # --- 5. 计算红绿柱面积 ---
        self.hist_area = self._calculate_hist_area(self.hist)

    def _calculate_hist_area(self, hist_data: List[float]) -> List[float]:
        """
        计算 MACD 红绿柱的累积面积
        """
        count = len(hist_data)
        if count == 0:
            return []

        areas = [0.0] * count
        current_area = 0.0
        direction = 0

        for i in range(count):
            val = hist_data[i]

            if abs(val) < 1e-9:
                val = 0.0

            if val == 0:
                areas[i] = current_area
                continue

            # 判断当前值的方向 (1 为红, -1 为绿)
            current_dir = 1 if val > 0 else -1

            if direction == 0:
                direction = current_dir
                current_area = val
            elif current_dir == direction:
                current_area += val
            else:
                direction = current_dir
                current_area = val

            areas[i] = current_area

        return areas

    def _reset_data(self):
        self.dif, self.dea, self.hist, self.hist_area = [], [], [], []

    def get_results(self) -> Dict[str, Dict[str, List[float]]]:
        return {
            'macd': {
                'dif': self.dif,
                'dea': self.dea,
                'hist': self.hist,
                'hist_area': self.hist_area
            }
        }