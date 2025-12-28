# -*- coding: utf-8 -*-
from typing import List, Optional
from chanlun.core.cl_interface import FX, BI, CLKline


class BiCalculator:
    """
    笔计算器
    修复了增量计算时可能丢失待定笔（pending_bi）的问题。
    """

    def __init__(self):
        self.bis: List[BI] = []
        self.fxs: List[FX] = []
        self.pending_bi: Optional[BI] = None
        self.bi_index: int = 0
        self.cl_klines: List[CLKline] = []

    def _check_stroke_validity(self, fx1: FX, fx2: FX) -> bool:
        """检查两个分型是否能构成有效的一笔。"""
        # 1. 顶底分型必须不同
        if fx1.type == fx2.type:
            return False

        # 2. 顶分型与底分型之间至少隔一根K线 (索引差 >= 4)
        if abs(fx2.k.index - fx1.k.index) < 4:
            return False

        # 3. 顶底分型的高低点验证
        if fx1.type == 'ding':
            if fx2.val >= fx1.val:  # 底分型底不能高于顶分型底 (严格讲是底分型底 < 顶分型底)
                # 注意：这里用 val (极值) 比较更通用
                return False
        else:  # fx1.type == 'di'
            if fx2.val <= fx1.val:  # 顶分型顶不能低于底分型顶
                return False

        return True

    def calculate(self, cl_klines: List[CLKline]):
        """
        支持增量计算的笔识别逻辑。
        """
        # --- 1. 数据校验与增量判断 ---
        if not cl_klines:
            return

        is_incremental = bool(self.cl_klines)
        if is_incremental and cl_klines[-1].index <= self.cl_klines[-1].index:
            return

        self.cl_klines = cl_klines

        # --- 2. 确定计算起始点 (回退逻辑) ---
        start_index = 1

        if is_incremental:
            # 策略：只要是增量，就假设最后一笔（无论是完成还是未完成）的状态是不稳定的
            # 尤其是最后一笔未完成时，必须重算。
            # 如果最后一笔已完成，为了应对包含关系变动导致分型重构的情况，建议也回退。

            if self.bis:
                last_bi = self.bis[-1]

                # 如果最后一笔未完成，肯定要弹出重算
                if not last_bi.end.done:
                    self.bis.pop()
                    # 弹出后，当前的 bi_index 需要回退
                    self.bi_index = last_bi.index
                    # 起点回退到这笔的开始位置，重新扫描，看能否形成新的形态
                    start_index = last_bi.start.k.index
                else:
                    # 如果最后一笔已完成，从它结束的位置开始往后找
                    # 注意：已完成的笔，其 end 分型是确定的，我们从 end 分型所在 K 线开始扫描
                    start_index = last_bi.end.k.index

            elif self.fxs:
                start_index = self.fxs[-1].k.index

            # 清理 pending_bi，因为我们要重新从 start_index 处构建它
            self.pending_bi = None

            # 清理过期的分型缓存 (保留 start_index 之前的，因为它们可能作为起点)
            # 注意：这里保留 < start_index 的分型。
            # 当循环从 start_index 开始时，会重新生成该位置及之后的分型。
            self.fxs = [fx for fx in self.fxs if fx.k.index < start_index]

            start_index = max(1, start_index)

        else:
            # 全量计算
            self.bis, self.fxs, self.pending_bi, self.bi_index = [], [], None, 0

        # --- 3. 核心计算循环 ---
        i = start_index
        total_len = len(self.cl_klines)

        while i < total_len - 1:
            k_left = self.cl_klines[i - 1]
            k_curr = self.cl_klines[i]
            k_right = self.cl_klines[i + 1]

            current_fx = self._find_fractal(k_left, k_curr, k_right)

            if not current_fx:
                i += 1
                continue

            self.fxs.append(current_fx)

            # === 分支 A: 当前没有正在构建的笔 ===
            if not self.pending_bi:
                # 寻找起点的策略：
                start_fx = None

                # 情况1: 如果已经有历史笔，起点必须是上一笔的终点
                if self.bis:
                    start_fx = self.bis[-1].end
                # 情况2: 如果是第一笔，从分型列表中找倒数第二个
                elif len(self.fxs) >= 2:
                    start_fx = self.fxs[-2]

                if start_fx:
                    # 尝试构建新笔
                    if self._check_stroke_validity(start_fx, current_fx):
                        bi_type = 'up' if start_fx.type == 'di' else 'down'
                        self.pending_bi = BI(
                            start=start_fx,
                            end=current_fx,
                            _type=bi_type,
                            index=self.bi_index
                        )
                        self.pending_bi.end.done = False  # 标记为未完成
                        self.bi_index += 1
                        i += 1  # 即使成笔，也不要跳跃太多，以免漏掉紧随其后的分型变化
                    else:
                        i += 1
                else:
                    i += 1

            # === 分支 B: 已有正在构建的笔 (判断延伸或结束) ===
            else:
                end_fx_of_pending = self.pending_bi.end

                # 1. 同向分型：尝试延伸
                if current_fx.type == end_fx_of_pending.type:
                    # 顶分型更高，或底分型更低
                    if (end_fx_of_pending.type == 'ding' and current_fx.val > end_fx_of_pending.val) or \
                            (end_fx_of_pending.type == 'di' and current_fx.val < end_fx_of_pending.val):
                        # 更新当前待定笔的终点
                        self.pending_bi.end = current_fx
                        self.pending_bi.end.done = False
                        # 延伸后，可以适当跳过一根，但保守起见 i+=1
                        i += 1
                    else:
                        i += 1

                # 2. 反向分型：尝试结束当前笔，并开启新笔
                else:
                    if self._check_stroke_validity(end_fx_of_pending, current_fx):
                        # ---> 确认旧笔结束 <---
                        self.pending_bi.end.done = True
                        self.bis.append(self.pending_bi)

                        # ---> 生成下一笔的雏形 <---
                        # 新笔起点 = 旧笔终点
                        start_fx_new = self.pending_bi.end
                        bi_type_new = 'up' if start_fx_new.type == 'di' else 'down'

                        self.pending_bi = BI(
                            start=start_fx_new,
                            end=current_fx,
                            _type=bi_type_new,
                            index=self.bi_index
                        )
                        self.pending_bi.end.done = False
                        self.bi_index += 1
                        i += 1  # 找到新分型后，继续往后看
                    else:
                        # 虽是反向分型，但不满足成笔条件（如力度不够），忽略
                        i += 1

        # --- 4. 收尾：将最后的 pending_bi 放入列表供展示 ---
        # 注意：pending_bi 此时应该是一个 done=False 的笔，代表当前正在运行的那一笔
        if self.pending_bi:
            # 如果 bis 为空，直接加
            if not self.bis:
                self.bis.append(self.pending_bi)
            # 如果 bis 不为空，且 pending_bi 不是 bis 里的最后一个对象，则添加
            elif self.bis[-1] != self.pending_bi:
                self.bis.append(self.pending_bi)

            # 确保状态正确
            self.bis[-1].end.done = False

    def _find_fractal(self, k1: CLKline, k2: CLKline, k3: CLKline) -> Optional[FX]:
        """
        简化版分型识别
        """
        if k2.h > k1.h and k2.h > k3.h and k2.l > k1.l and k2.l > k3.l:
            return FX(_type='ding', k=k2, klines=[k1, k2, k3], val=k2.h)
        if k2.l < k1.l and k2.l < k3.l and k2.h < k1.h and k2.h < k3.h:
            return FX(_type='di', k=k2, klines=[k1, k2, k3], val=k2.l)
        return None