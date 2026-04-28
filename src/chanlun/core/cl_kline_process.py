# -*- coding: utf-8 -*-
"""
缠论K线包含关系处理模块 - 修复增量更新问题
"""
from typing import List
from chanlun.core.cl_interface import Kline, CLKline


class CL_Kline_Process:
    """
    处理原始K线的包含关系，并将结果维护在内部的 ``self.cl_klines`` 中。

    该类不是“传入列表 -> 返回结果列表”的纯函数，而是一个有状态的增量更新器：
    - 输入 ``src_klines`` 用于驱动内部 ``cl_klines`` 状态更新
    - 结果由调用方通过 ``self.cl_klines`` 读取
    - 支持对最后一根原始K线进行同 index 的数值更新（re-paint）
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

    @staticmethod
    def _resolve_direction(last_cl_k: CLKline, new_cl_k: CLKline) -> str:
        """无上下文时按 new vs last 的高低关系决定合并方向。

        缠论合并方向的本质是"沿趋势走"：
          - new 比 last 高（h>且 l>）→ 上升趋势 → 'up'（合并取较高的高/低）
          - new 比 last 低 → 下降趋势 → 'down'
          - 其他情况按 high 优先比较，high 相等时用 low 兜底，
            两者都相等时默认 'up'（极少出现，且对后续分型不影响）。
        """
        if new_cl_k.h > last_cl_k.h:
            return 'up'
        if new_cl_k.h < last_cl_k.h:
            return 'down'
        # high 相等时，用 low 决定
        if new_cl_k.l > last_cl_k.l:
            return 'up'
        if new_cl_k.l < last_cl_k.l:
            return 'down'
        # 完全相同：方向不影响合并结果（h/l 都一样），随便给一个
        return 'up'

    def _merge_klines(self, k1: CLKline, k2: CLKline, direction: str) -> CLKline:
        """根据指定方向合并两根K线。"""
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
            # k_index 继续落在原始K线坐标系中，供 MACD 切片、角度计算等逻辑使用。
            k_index=k_index, date=date, h=h, l=l, o=o, c=c, a=k1.a + k2.a,
            klines=k1.klines + k2.klines,  # 累积所有原始K线
            # index 保持缠论K线序号稳定，供 BiCalculator 增量回退与成笔距离判断使用。
            index=k1.index,
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

        # 创建新对象的包装：
        # - k_index 使用原始K线坐标
        # - index 使用缠论K线序号
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
            # 优先级：
            #   1. 上一根缠论 K 已经标记过方向（last_cl_k.up_qs）→ 沿用
            #   2. 上一根缠论 K 与更前一根有明确高低关系 → 用上文趋势判断
            #   3. 都不可用 → 用 new_cl_k 与 last_cl_k 的高低做兜底
            #
            # ★ A3 修复：原代码在 elif 链里 default 'up'，当 len(cl_klines)==1 或
            # last_cl_k.h == prev_cl_k.h 时会强制按 'up' 合并，可能把"高高低低"
            # 错合成"高高高高"，永久污染整段缠论 K 线序列。
            # 现在所有未确定的分支都收敛到 _resolve_direction 兜底，避免方向错判。
            direction: str
            if last_cl_k.up_qs is not None:
                direction = last_cl_k.up_qs
            elif len(self.cl_klines) >= 2:
                prev_cl_k = self.cl_klines[-2]
                if last_cl_k.h > prev_cl_k.h and last_cl_k.l > prev_cl_k.l:
                    direction = 'up'
                elif last_cl_k.h < prev_cl_k.h and last_cl_k.l < prev_cl_k.l:
                    direction = 'down'
                else:
                    direction = self._resolve_direction(last_cl_k, new_cl_k)
            else:
                direction = self._resolve_direction(last_cl_k, new_cl_k)

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
        基于原始K线序列增量更新内部 ``self.cl_klines`` 状态。

        注意：
        - 该方法的结果保存在 ``self.cl_klines`` 中
        - 调用方不应依赖其返回值
        - 传入的 ``src_klines`` 用于驱动内部状态更新，而不是作为纯函数输入返回新列表
        """
        if not src_klines:
            return

        for current_k in src_klines:

            # --- 情况1: 这是一个旧数据 ---
            if current_k.index < self._last_src_kline_index:
                continue

            # --- 情况2: 这是一个更新数据 ---
            if current_k.index == self._last_src_kline_index:
                # ★ D2 优化：原实现无条件 pop dirty_cl_k → 重放 valid_prev_klines → 处理 current_k。
                # 这在「dirty_cl_k 只含 current_k 一根」的最常见场景里会做很多无意义工作：
                #   - dirty_cl_k 只包含 current_k 时，valid_prev_klines 是空的，
                #     重放循环本身就跳过；但 pop 之后 _process_one_kline(current_k)
                #     还要重新跑一次 _need_merge / 合并方向判断，事实上等价于
                #     「在原位修改 cl_klines[-1] 的 h/l/c/o/a」。
                # 所以拆成两个分支：
                #   - fast path: dirty_cl_k 只含 current_k → 原地重做最后一个 cl_k 的字段
                #   - slow path: dirty_cl_k 由多根原始 K 合并而来 → 走原来的 pop+重放逻辑
                if not self.cl_klines:
                    self._process_one_kline(current_k)
                    continue

                dirty_cl_k = self.cl_klines[-1]
                # fast path：脏 cl_k 只由当前这根原始 K 构成
                if (
                    len(dirty_cl_k.klines) == 1
                    and dirty_cl_k.klines[0].index == current_k.index
                ):
                    # 直接 pop 然后单根 _process_one_kline 等价于在原位重算，
                    # 但避免了「重放 valid_prev_klines」的循环开销
                    # （这里就算是空循环也省下了一次列表推导）
                    self.cl_klines.pop()
                    # 把 _last_src_kline_index 回退到上一根的边界，
                    # _process_one_kline 内部会按当前 cl_klines 状态正确续上
                    if self.cl_klines:
                        self._last_src_kline_index = self.cl_klines[-1].klines[-1].index
                    else:
                        self._last_src_kline_index = -1
                    self._process_one_kline(current_k)
                    continue

                # slow path：原通用逻辑，处理「dirty_cl_k 由多根原始 K 合并而来」
                self.cl_klines.pop()
                valid_prev_klines = [k for k in dirty_cl_k.klines if k.index < current_k.index]

                if valid_prev_klines:
                    self._last_src_kline_index = valid_prev_klines[-1].index
                else:
                    if self.cl_klines:
                        self._last_src_kline_index = self.cl_klines[-1].klines[-1].index
                    else:
                        self._last_src_kline_index = -1

                # 重放之前的有效 K 线，恢复现场
                for prev_k in valid_prev_klines:
                    self._process_one_kline(prev_k)

                # 处理当前这根更新后的 K 线
                self._process_one_kline(current_k)

            # --- 情况3: 这是一个新数据 (Index 更大) ---
            else:
                self._process_one_kline(current_k)