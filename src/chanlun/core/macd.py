# -*- coding: utf-8 -*-
"""
技术指标计算模块 - MACD 高精度修正版 (支持增量更新)
------------------------------------------------
修改内容：
1. 引入增量计算逻辑 (Incremental Logic)，避免每次全量重算。
2. 支持 'Tick级别更新' (更新最后一根) 和 'Bar级别更新' (新增K线)。
3. 优化 hist_area 算法，准确处理红绿柱切换时的面积归档。
"""
from typing import List, Dict, Tuple
import pandas as pd
from chanlun.core.cl_interface import Kline


class MACD:
    """
    MACD指标计算类 (增量优化版)
    """

    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9, china_mode: bool = True):
        """
        Args:
            fast_period: 快线周期
            slow_period: 慢线周期
            signal_period: 信号线周期
            china_mode: 是否使用国内软件标准 (默认 True, (DIF-DEA)*2)
        """
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.china_mode = china_mode

        # 缓存计算参数 (用于 EMA 增量计算)
        self.fast_alpha = 2 / (self.fast_period + 1)
        self.slow_alpha = 2 / (self.slow_period + 1)
        self.signal_alpha = 2 / (self.signal_period + 1)

        # 存储计算结果
        self.dif: List[float] = []
        self.dea: List[float] = []
        self.hist: List[float] = []
        self.hist_area: List[float] = []

        # 内部状态缓存 (用于增量计算)
        # 存储的是"上一根已完成K线"的EMA值
        self._ema_fast_val: float = 0.0
        self._ema_slow_val: float = 0.0
        self._dea_val: float = 0.0

        # 记录上次处理的数据长度，用于判断是"新增"还是"更新"
        self._last_kline_count = 0

    def process_macd(self, klines: List[Kline]):
        """
        执行MACD计算 (支持增量)
        """
        current_count = len(klines)

        # 1. 数据重置或异常情况
        if current_count == 0:
            self._reset_data()
            return

        # 2. 判断更新模式
        if self._last_kline_count == 0 or current_count < self._last_kline_count:
            # [模式A] 全量计算 (初始化或历史数据变动)
            self._full_calculation(klines)

        elif current_count == self._last_kline_count:
            # [模式B] Tick 更新 (只更新最后一根)
            # 回滚状态到倒数第二根 (把上次临时算的最后一根删掉)
            self._rollback_state()
            # 重新计算最后一根
            self._incremental_calculation(klines[-1], is_update_last=True)

        elif current_count > self._last_kline_count:
            # [模式C] 新增 Bar (追加计算)
            # 计算新增的部分 (通常是1根)
            new_klines = klines[self._last_kline_count:]
            for k in new_klines:
                self._incremental_calculation(k, is_update_last=False)

        # 更新长度记录
        self._last_kline_count = current_count

    def _full_calculation(self, klines: List[Kline]):
        """全量 Pandas 计算 (初始化用)"""
        close_prices = pd.Series([k.c for k in klines], dtype='float64')

        # Pandas EWM 计算
        ema_fast = close_prices.ewm(span=self.fast_period, adjust=False).mean()
        ema_slow = close_prices.ewm(span=self.slow_period, adjust=False).mean()

        dif_series = ema_fast - ema_slow
        dea_series = dif_series.ewm(span=self.signal_period, adjust=False).mean()

        # 保存结果列表
        self.dif = dif_series.fillna(0).tolist()
        self.dea = dea_series.fillna(0).tolist()

        # 计算 Hist
        hist_series = self.dif - self.dea if not isinstance(self.dif, list) else \
            pd.Series(self.dif) - pd.Series(self.dea)

        if self.china_mode:
            hist_series *= 2

        self.hist = hist_series.fillna(0).tolist()

        # 缓存 EMA 状态 (取最后一个值，作为下一根K线计算的基准)
        if len(klines) > 0:
            self._ema_fast_val = ema_fast.iloc[-1]
            self._ema_slow_val = ema_slow.iloc[-1]
            self._dea_val = dea_series.iloc[-1]

        # 全量计算面积
        self.hist_area = []  # 清空
        self._calculate_hist_area_incremental(self.hist, start_index=0)

    def _incremental_calculation(self, kline: Kline, is_update_last: bool = False):
        """增量单根计算 (手动实现 EMA 公式)"""
        close = kline.c

        # 1. 计算 Fast EMA
        # 公式: EMA_today = alpha * Price + (1 - alpha) * EMA_prev
        new_ema_fast = (close * self.fast_alpha) + (self._ema_fast_val * (1 - self.fast_alpha))

        # 2. 计算 Slow EMA
        new_ema_slow = (close * self.slow_alpha) + (self._ema_slow_val * (1 - self.slow_alpha))

        # 3. 计算 DIF
        new_dif = new_ema_fast - new_ema_slow

        # 4. 计算 DEA (是对 DIF 的 EMA)
        # 注意：这里的 _dea_val 是上一根 K 线的 DEA
        new_dea = (new_dif * self.signal_alpha) + (self._dea_val * (1 - self.signal_alpha))

        # 5. 计算 Hist
        new_hist = (new_dif - new_dea)
        if self.china_mode:
            new_hist *= 2

        # 6. 更新列表
        if is_update_last:
            # Tick 更新模式：直接替换列表最后一个值
            # 列表在 _rollback_state 时已经 pop 过了，所以这里是 append
            # 但为了逻辑清晰，如果 rollback 了，就等同于 append
            # 这里为了保险，检查列表长度。如果 rollback 成功，这里应该是 append
            self.dif.append(new_dif)
            self.dea.append(new_dea)
            self.hist.append(new_hist)
            # 关键：Tick 更新模式下，**不要**更新 self._ema_xx_val
            # 因为 _ema_xx_val 必须保持为"上一根已完成K线"的值，供下一次 tick 计算使用
        else:
            # New Bar 模式
            self.dif.append(new_dif)
            self.dea.append(new_dea)
            self.hist.append(new_hist)
            # 关键：Bar 完成，更新 EMA 状态基准
            self._ema_fast_val = new_ema_fast
            self._ema_slow_val = new_ema_slow
            self._dea_val = new_dea

        # 7. 计算面积 (仅计算最后一个)
        # start_index 是当前加入元素的索引
        self._calculate_hist_area_incremental(self.hist, start_index=len(self.hist) - 1)

    def _rollback_state(self):
        """当检测到是更新当前 Bar 时，移除列表最后一位，准备重新计算"""
        if self.dif:
            self.dif.pop()
            self.dea.pop()
            self.hist.pop()
            self.hist_area.pop()

    def _calculate_hist_area_incremental(self, hist_data: List[float], start_index: int):
        """
        增量计算面积
        :param start_index: 开始计算的索引。
        """
        # 1. 初始化状态
        if start_index == 0:
            current_area = 0.0
            direction = 0
            self.hist_area = [0.0] * len(hist_data)
        else:
            # 继承前一个状态
            prev_idx = start_index - 1
            if prev_idx >= 0:
                current_area = self.hist_area[prev_idx]
                val_prev = hist_data[prev_idx]
                if val_prev > 0:
                    direction = 1
                elif val_prev < 0:
                    direction = -1
                else:
                    direction = 0
            else:
                current_area = 0.0
                direction = 0

            # 补齐数组长度
            if len(self.hist_area) < len(hist_data):
                self.hist_area.extend([0.0] * (len(hist_data) - len(self.hist_area)))

        # 2. 循环计算 (增量模式下循环次数通常为 1)
        for i in range(start_index, len(hist_data)):
            val = hist_data[i]

            if abs(val) < 1e-9:
                val = 0.0

            if val == 0:
                self.hist_area[i] = current_area
                continue

            current_dir = 1 if val > 0 else -1

            if direction == 0:
                direction = current_dir
                current_area = val
            elif current_dir == direction:
                # 同向累加
                current_area += val
            else:
                # 反向重置：新周期的开始，面积等于当前值
                direction = current_dir
                current_area = val

            self.hist_area[i] = current_area

    def _reset_data(self):
        self.dif, self.dea, self.hist, self.hist_area = [], [], [], []
        self._last_kline_count = 0
        self._ema_fast_val = 0.0
        self._ema_slow_val = 0.0
        self._dea_val = 0.0

    def get_results(self) -> Dict[str, Dict[str, List[float]]]:
        return {
            'macd': {
                'dif': self.dif,
                'dea': self.dea,
                'hist': self.hist,
                'hist_area': self.hist_area
            }
        }