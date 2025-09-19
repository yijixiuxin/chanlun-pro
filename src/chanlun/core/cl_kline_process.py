# -*- coding: utf-8 -*-
"""
缠论K线包含关系处理模块 (重构版)
支持增量更新，并仅返回新增或变更的K线
"""
from typing import List
from chanlun.core.cl_interface import Kline, CLKline
# 导入日志工具
from chanlun.tools.log_util import LogUtil


class CL_Kline_Process:
    """
    该类负责处理K线的包含关系，并支持增量更新。
    它接收原始K线列表，并生成经过包含关系处理后的缠论K线列表。
    `process_cl_klines` 方法经过优化，仅返回新增或因合并而更新的 CLKline。
    """

    def __init__(self):
        self.cl_klines: List[CLKline] = []
        # 跟踪最后一个已处理的原始K线索引，用于判断是否需要更新
        self._last_src_kline_index = -1

    def _need_merge(self, k1: CLKline, k2: CLKline) -> bool:
        """
        判断两根缠论K线是否存在包含关系
        """
        k1_contains_k2 = k1.h >= k2.h and k1.l <= k2.l
        k2_contains_k1 = k2.h >= k1.h and k2.l <= k1.l
        return k1_contains_k2 or k2_contains_k1

    def _merge_klines(self, k1: CLKline, k2: CLKline, direction: str) -> CLKline:
        """
        根据指定方向合并两根K线
        """
        if direction == 'up':
            h, l = max(k1.h, k2.h), max(k1.l, k2.l)
            date, k_index = (k1.date, k1.k_index) if k1.h > k2.h else (k2.date, k2.k_index)
        else:  # 'down'
            h, l = min(k1.h, k2.h), min(k1.l, k2.l)
            date, k_index = (k1.date, k1.k_index) if k1.l < k2.l else (k2.date, k2.k_index)

        merged = CLKline(
            k_index=k_index, date=date, h=h, l=l, o=h, c=l, a=k1.a + k2.a,
            klines=k1.klines + k2.klines, index=k1.index, _n=k1.n + k2.n, _q=k1.q
        )
        merged.up_qs = direction
        LogUtil.info(f"Merging klines. Direction: {direction}. Result h:{merged.h:.2f}, l:{merged.l:.2f}")
        return merged

    def process_cl_klines(self, src_klines: List[Kline]) -> List[CLKline]:
        """
        遍历原始K线，进行包含关系处理，生成缠论K线。
        支持增量更新，只处理和返回新增或更新的缠论K线。
        """
        if not src_klines:
            return []

        # 如果没有新的K线数据，直接返回空列表
        if len(src_klines) - 1 <= self._last_src_kline_index:
            LogUtil.info("No new klines to process.")
            return []

        # --- 确定处理的起始位置 ---
        start_src_index = 0
        # 记录返回的 cl_klines 在 self.cl_klines 中的起始索引
        return_cl_klines_start_index = 0

        if self.cl_klines and self._last_src_kline_index >= 0:
            # 增量更新：需要回溯一根K线，因为最后一根cl_kline可能需要和新的kline合并
            # 找到最后一根 cl_kline 对应的第一根 src_kline 的索引
            last_cl_k = self.cl_klines.pop()
            if last_cl_k.klines:
                start_src_index = last_cl_k.klines[0].index
            else:  # 理论上不应该发生，作为保护
                start_src_index = self._last_src_kline_index

            return_cl_klines_start_index = len(self.cl_klines)
            LogUtil.info(f"Incremental update. Rewinding to src_kline index {start_src_index} for re-processing.")
        else:
            # 首次全量处理
            LogUtil.info("First run, processing all klines.")
            self.cl_klines.clear()
            self._last_src_kline_index = -1

        # --- 主循环，处理K线 ---
        for i in range(start_src_index, len(src_klines)):
            current_k = src_klines[i]

            cl_k = CLKline(
                k_index=current_k.index, date=current_k.date, h=current_k.h, l=current_k.l,
                o=current_k.o, c=current_k.c, a=current_k.a, klines=[current_k],
                index=len(self.cl_klines), _n=1
            )


            if not self.cl_klines:
                self.cl_klines.append(cl_k)
                LogUtil.info(f"Added first CL Kline (index {cl_k.index}) from src_kline {current_k.index}")
                continue

            last_cl_k = self.cl_klines[-1]

            # 检查是否有缺口
            has_gap = cl_k.l > last_cl_k.h or cl_k.h < last_cl_k.l
            if has_gap:
                cl_k.q = True
                cl_k.index = len(self.cl_klines)
                self.cl_klines.append(cl_k)
                LogUtil.info(f"Gap detected. Added new CL Kline (index {cl_k.index}) from src_kline {current_k.index}")
                continue

            # 判断是否需要合并
            if self._need_merge(last_cl_k, cl_k):
                # 确定合并方向
                direction = 'up'  # 默认向上
                if last_cl_k.up_qs is not None:
                    # 如果上一根合并K线已经有方向，则继承该方向
                    direction = last_cl_k.up_qs
                elif len(self.cl_klines) >= 2:
                    # 标准情况：比较最后两根已处理K线的高点来定方向
                    if self.cl_klines[-1].h < self.cl_klines[-2].h:
                        direction = 'down'
                else:
                    # 边缘情况：第一次发生包含，根据当前K线与上一根K线关系定方向
                    if cl_k.h > last_cl_k.h:
                        direction = 'up'
                    elif cl_k.h < last_cl_k.h:
                        direction = 'down'
                    else:  # 高点相同
                        direction = 'up' if cl_k.l > last_cl_k.l else 'down'

                merged_k = self._merge_klines(last_cl_k, cl_k, direction)
                self.cl_klines[-1] = merged_k
            else:
                cl_k.index = len(self.cl_klines)
                self.cl_klines.append(cl_k)
                LogUtil.info(f"No merge. Added new CL Kline (index {cl_k.index}) from src_kline {current_k.index}")

        # --- 更新状态并返回结果 ---
        self._last_src_kline_index = len(src_klines) - 1

        new_and_updated_klines = self.cl_klines[return_cl_klines_start_index:]

        LogUtil.info(f"Processing finished. Total CL klines: {len(self.cl_klines)}. "
                     f"Last processed src kline index: {self._last_src_kline_index}. "
                     f"Returning {len(new_and_updated_klines)} klines.")

        return new_and_updated_klines
