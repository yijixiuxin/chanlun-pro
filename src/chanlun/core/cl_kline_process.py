# -*- coding: utf-8 -*-
"""
缠论K线包含关系处理模块 - 修复增量更新问题
"""
from typing import List
from chanlun.core.cl_interface import Kline, CLKline
# 导入日志工具
from chanlun.tools.log_util import LogUtil


class CL_Kline_Process:
    """
    该类负责处理K线的包含关系，并支持增量更新。
    已修复：支持对同一 index 的 K 线进行数值更新（Re-paint）。
    """

    def __init__(self):
        self.cl_klines: List[CLKline] = []
        # 跟踪最后一个已处理的 *原始* K线索引
        self._last_src_kline_index: int = -1

    def _need_merge(self, k1: CLKline, k2: CLKline) -> bool:
        """判断两根缠论K线是否存在包含关系"""
        # k1 包含 k2
        k1_contains_k2 = k1.h >= k2.h and k1.l <= k2.l
        # k2 包含 k1
        k2_contains_k1 = k2.h >= k1.h and k2.l <= k1.l
        return k1_contains_k2 or k2_contains_k1

    def _merge_klines(self, k1: CLKline, k2: CLKline, direction: str) -> CLKline:
        """根据指定方向合并两根K线"""
        if direction == 'up':
            # 向上合并：取 高-高, 高-低
            h, l = max(k1.h, k2.h), max(k1.l, k2.l)
            o, c = l, h  # 示意性赋值，实际上缠论合并K线不强调OC，但保持逻辑一致
            date, k_index = (k1.date, k1.k_index) if k1.h > k2.h else (k2.date, k2.k_index)
        else:  # 'down'
            # 向下合并：取 低-高, 低-低
            h, l = min(k1.h, k2.h), min(k1.l, k2.l)
            o, c = h, l
            date, k_index = (k1.date, k1.k_index) if k1.l < k2.l else (k2.date, k2.k_index)

        merged = CLKline(
            k_index=k_index, date=date, h=h, l=l, o=o, c=c, a=k1.a + k2.a,
            klines=k1.klines + k2.klines,  # 累积所有原始K线
            index=k1.index,  # 保持第一根K线的索引
            _n=k1.n + k2.n,  # 累积合并的K线数量
            _q=k1.q  # 缺口属性继承
        )
        merged.up_qs = direction
        return merged

    def _process_one_kline(self, current_k: Kline):
        """
        核心原子操作：将一根原始K线加入到 cl_klines 序列中。
        """
        # --- A. 初始化第一根缠论 K 线 ---
        if not self.cl_klines:
            new_cl_k = CLKline(
                k_index=current_k.index, date=current_k.date,
                h=current_k.h, l=current_k.l, o=current_k.o, c=current_k.c, a=current_k.a,
                klines=[current_k], index=0, _n=1
            )
            self.cl_klines.append(new_cl_k)
            self._last_src_kline_index = current_k.index
            return

        # --- B. 获取当前最新的缠论 K 线 ---
        last_cl_k = self.cl_klines[-1]

        # 创建新对象的包装
        new_cl_k = CLKline(
            k_index=current_k.index, date=current_k.date,
            h=current_k.h, l=current_k.l, o=current_k.o, c=current_k.c, a=current_k.a,
            klines=[current_k], index=len(self.cl_klines), _n=1
        )

        # --- C. 判断包含关系 ---
        # 1. 检查是否有缺口 (即无重叠)
        has_gap = new_cl_k.l > last_cl_k.h or new_cl_k.h < last_cl_k.l

        if has_gap:
            new_cl_k.q = True
            self.cl_klines.append(new_cl_k)

        elif self._need_merge(last_cl_k, new_cl_k):
            # 确定合并方向
            direction = 'up'  # 默认
            if last_cl_k.up_qs is not None:
                direction = last_cl_k.up_qs
            elif len(self.cl_klines) >= 2:
                prev_cl_k = self.cl_klines[-2]
                if last_cl_k.h < prev_cl_k.h:
                    direction = 'down'
            else:
                # 第一根和第二根的处理
                if new_cl_k.h > last_cl_k.h:
                    direction = 'up'
                elif new_cl_k.h < last_cl_k.h:
                    direction = 'down'
                else:
                    direction = 'up' if new_cl_k.l > last_cl_k.l else 'down'

            merged_k = self._merge_klines(last_cl_k, new_cl_k, direction)
            # 原地替换
            merged_k.index = last_cl_k.index
            self.cl_klines[-1] = merged_k

        else:
            # 有重叠但无包含，追加
            self.cl_klines.append(new_cl_k)

        # --- D. 更新状态 ---
        self._last_src_kline_index = current_k.index

    def process_cl_klines(self, src_klines: List[Kline]):
        """
        处理原始K线列表。
        修复逻辑：如果遇到 index 等于 _last_src_kline_index，视为更新操作，执行回滚并重新计算。
        """
        if not src_klines:
            return []

        # 记录起始返回点（注意：如果发生回滚，列表长度可能会暂时变短，
        # 但我们总是希望返回“受影响”的部分，所以取 max(0, len-1) 是安全的起点）
        return_start_idx = max(0, len(self.cl_klines) - 1)
        has_processed = False

        for current_k in src_klines:

            # --- 情况1: 这是一个旧数据 (完全忽略) ---
            if current_k.index < self._last_src_kline_index:
                continue

            has_processed = True

            # --- 情况2: 这是一个更新数据 (Index 相同，数据变动) ---
            if current_k.index == self._last_src_kline_index:
                # LogUtil.debug(f"检测到K线更新: index={current_k.index}, 执行回滚重算...")

                # 1. 弹出最后一根受影响的 CLKline
                if self.cl_klines:
                    dirty_cl_k = self.cl_klines.pop()

                    # 2. 从这根脏 K 线中，分离出 *之前已经确认的* 原始 K 线
                    #    逻辑：dirty_cl_k 可能由 [8017, 8018] 合并而成。
                    #    现在 8018 更新了，我们要保留 8017，扔掉旧的 8018，然后放入新的 8018。
                    valid_prev_klines = [k for k in dirty_cl_k.klines if k.index < current_k.index]

                    # 3. 修正 _last_src_kline_index
                    #    我们需要将其回退到 valid_prev_klines 的最后一个 index
                    #    如果 valid_prev_klines 为空，说明之前那个 cl_k 就是由单根 8018 构成的，
                    #    那么回退到上一个 cl_kline 的结束点 (或 -1)
                    if valid_prev_klines:
                        self._last_src_kline_index = valid_prev_klines[-1].index
                    else:
                        # 尝试找更前面的
                        if self.cl_klines:
                            # 取当前队尾包含的最后一个原始K线index
                            self._last_src_kline_index = self.cl_klines[-1].klines[-1].index
                        else:
                            self._last_src_kline_index = -1

                    # 4. 【关键】先重放之前的有效 K 线，恢复现场
                    for prev_k in valid_prev_klines:
                        self._process_one_kline(prev_k)

                # 5. 最后处理当前这根更新后的 K 线
                self._process_one_kline(current_k)

            # --- 情况3: 这是一个新数据 (Index 更大) ---
            else:
                self._process_one_kline(current_k)
