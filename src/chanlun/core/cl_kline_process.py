# -*- coding: utf-8 -*-
"""
缠论K线包含关系处理模块
支持增量更新，并仅返回新增或变更的K线
"""
from typing import List
from chanlun.core.cl_interface import Kline, CLKline
# 导入日志工具
from chanlun.tools.log_util import LogUtil


class CL_Kline_Process:
    """
    该类负责处理K线的包含关系，并支持增量更新。

    它接收原始K线列表(src_klines)，并生成经过包含关系处理后的缠论K线列表(cl_klines)。
    `process_cl_klines` 方法经过优化，仅返回新增或因合并而更新的 CLKline。
    """

    def __init__(self):
        """
        初始化处理器
        """
        self.cl_klines: List[CLKline] = []
        # 跟踪最后一个已处理的 *原始* K线索引，用于判断是否需要增量更新
        self._last_src_kline_index: int = -1

    def _need_merge(self, k1: CLKline, k2: CLKline) -> bool:
        """
        判断两根缠论K线是否存在包含关系
        """
        # k1 包含 k2
        k1_contains_k2 = k1.h >= k2.h and k1.l <= k2.l
        # k2 包含 k1
        k2_contains_k1 = k2.h >= k1.h and k2.l <= k1.l
        return k1_contains_k2 or k2_contains_k1

    def _merge_klines(self, k1: CLKline, k2: CLKline, direction: str) -> CLKline:
        """
        根据指定方向合并两根K线

        Args:
            k1 (CLKline): 较早的K线 (即 self.cl_klines[-1])
            k2 (CLKline): 较新的K线 (即从当前 src_kline 生成的)
            direction (str): 合并方向, 'up' 或 'down'

        Returns:
            CLKline: 合并后的新K线
        """
        if direction == 'up':
            # 向上合并：取 高-高, 高-低
            h, l = max(k1.h, k2.h), max(k1.l, k2.l)
            # 合并后的K线应为阳线 (o=l, c=h)
            o, c = l, h
            # 日期和k_index 取高点所在K线的
            date, k_index = (k1.date, k1.k_index) if k1.h > k2.h else (k2.date, k2.k_index)
        else:  # 'down'
            # 向下合并：取 低-高, 低-低
            h, l = min(k1.h, k2.h), min(k1.l, k2.l)
            # 合并后的K线应为阴线 (o=h, c=l)
            o, c = h, l
            # 日期和k_index 取低点所在K线的
            date, k_index = (k1.date, k1.k_index) if k1.l < k2.l else (k2.date, k2.k_index)

        merged = CLKline(
            k_index=k_index, date=date, h=h, l=l, o=o, c=c, a=k1.a + k2.a,
            klines=k1.klines + k2.klines,  # 累积所有原始K线
            index=k1.index,  # 保持第一根K线的索引
            _n=k1.n + k2.n,  # 累积合并的K线数量
            _q=k1.q  # 缺口属性继承自第一根K线
        )
        merged.up_qs = direction  # 记录合并方向
        return merged

    def process_cl_klines(self, src_klines: List[Kline]) -> List[CLKline]:
        """
        遍历原始K线，进行包含关系处理，生成缠论K线。
        支持增量更新，只处理和返回新增或更新的缠论K线。

        Args:
            src_klines (List[Kline]): 完整的原始K线数据列表

        Returns:
            List[CLKline]: 本次调用新增或更新的缠论K线列表
        """
        if not src_klines:
            return []

        # 如果没有新的K线数据 (比较最后一个原始K线的索引)
        if len(src_klines) - 1 <= self._last_src_kline_index:
            LogUtil.info("没有新的K线需要处理。")
            return []

        # --- 1. 确定处理的起始位置 ---
        start_src_index: int = 0
        # 记录返回的 cl_klines 在 self.cl_klines 中的起始索引
        return_cl_klines_start_index: int = 0

        if self.cl_klines and self._last_src_kline_index >= 0:
            # --- 增量更新 ---
            # 需要回溯一根K线，因为最后一根 cl_kline 可能需要和新的 src_kline 合并
            LogUtil.info(f"增量更新开始。回溯最后一根 CL Kline (index {self.cl_klines[-1].index})")
            last_cl_k = self.cl_klines.pop()

            # 找到这根被弹出的 cl_kline 是由哪根 src_kline *开始* 构成的
            if last_cl_k.klines:
                start_src_index = last_cl_k.klines[0].index
            else:
                # 保护性分支，理论上 klines 不应为空
                # 回到上一次处理的最后一根K线的位置
                start_src_index = self._last_src_kline_index

            # 记录返回列表的起始位置 (即当前 cl_klines 的末尾)
            return_cl_klines_start_index = len(self.cl_klines)
            LogUtil.info(f"将从 src_kline 索引 {start_src_index} 开始重新处理。")

        else:
            # --- 首次全量处理 ---
            LogUtil.info("首次运行，处理所有K线。")
            self.cl_klines.clear()
            self._last_src_kline_index = -1

        # --- 2. 主循环，处理K线 ---
        for i in range(start_src_index, len(src_klines)):
            current_k = src_klines[i]

            # 将原始K线 包装成 缠论K线
            cl_k = CLKline(
                k_index=current_k.index, date=current_k.date, h=current_k.h, l=current_k.l,
                o=current_k.o, c=current_k.c, a=current_k.a, klines=[current_k],
                index=len(self.cl_klines),  # 临时索引，后续可能被修改
                _n=1
            )

            if not self.cl_klines:
                # 添加第一根K线
                self.cl_klines.append(cl_k)
                LogUtil.info(f"添加第一根 CL Kline (index {cl_k.index}) from src_kline {current_k.index}")
                continue

            # 获取处理后的最后一根K线，用于比较
            last_cl_k = self.cl_klines[-1]

            # 检查是否有缺口 (新K线与最后一根K线完全不重合)
            has_gap = cl_k.l > last_cl_k.h or cl_k.h < last_cl_k.l
            if has_gap:
                cl_k.q = True  # 标记为有缺口
                cl_k.index = len(self.cl_klines)  # 确定其最终索引
                self.cl_klines.append(cl_k)
                LogUtil.info(f"检测到缺口。添加新 CL Kline (index {cl_k.index}) from src_kline {current_k.index}")
                continue

            # 检查是否需要合并 (有重叠区域)
            if self._need_merge(last_cl_k, cl_k):
                # --- A. 需要合并 ---

                # 确定合并方向
                direction = 'up'  # 默认向上
                if last_cl_k.up_qs is not None:
                    # 1. 如果上一根K线已是合并K线，继承其方向
                    direction = last_cl_k.up_qs
                elif len(self.cl_klines) >= 2:
                    # 2. 标准情况：比较最后两根已处理K线的高点来定方向
                    if self.cl_klines[-1].h < self.cl_klines[-2].h:
                        direction = 'down'
                    # (如果 >=，则保持默认 'up')
                else:
                    # 3. 边缘情况：第一次发生包含 (只有一根K线)
                    #    比较当前K线和上一根K线
                    if cl_k.h > last_cl_k.h:
                        direction = 'up'
                    elif cl_k.h < last_cl_k.h:
                        direction = 'down'
                    else:  # 高点相同，比较低点
                        direction = 'up' if cl_k.l > last_cl_k.l else 'down'

                # 执行合并
                merged_k = self._merge_klines(last_cl_k, cl_k, direction)

                # 用合并后的K线 *替换* 列表中的最后一根
                self.cl_klines[-1] = merged_k
                LogUtil.info(
                    f"合并 CL Kline (index {merged_k.index}) with src_kline {current_k.index}. Direction: {direction}")
            else:
                # --- B. 无缺口，无合并 ---
                # (即K线有重叠，但不构成包含关系，是独立K线)
                cl_k.index = len(self.cl_klines)  # 确定其最终索引
                self.cl_klines.append(cl_k)

        # --- 3. 更新状态并返回结果 ---
        self._last_src_kline_index = len(src_klines) - 1

        # 仅返回本次调用新增或更新的K线
        new_and_updated_klines = self.cl_klines[return_cl_klines_start_index:]

        LogUtil.info(f"处理完成。总 CL klines: {len(self.cl_klines)}. "
                     f"最后处理的 src kline 索引: {self._last_src_kline_index}. "
                     f"返回 {len(new_and_updated_klines)} 根 K线。")

        return new_and_updated_klines