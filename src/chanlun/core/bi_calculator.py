# -*- coding: utf-8 -*-
from typing import List, Optional

from chanlun.core.cl_interface import FX, BI, CLKline


class BiCalculator:
    """
    笔计算器。

    对外仍保持：
    - self.fxs 为识别出的分型列表
    - self.bis 为用于展示/下游消费的笔列表，最后一笔可能未完成

    对内采用“已确认笔 + 当前待定笔”的状态机，每次在最新缠论 K 线上重放，
    优先保证结果正确与全量/增量一致性。
    """

    def __init__(self, bi_mode: str = 'strict'):
        self.bis: List[BI] = []
        self.fxs: List[FX] = []
        self.confirmed_bis: List[BI] = []
        self.pending_bi: Optional[BI] = None
        self.bi_index: int = 0
        self.cl_klines: List[CLKline] = []
        self.bi_mode = bi_mode  # 'strict' (严格笔) 或 'new' (新笔)
        self._last_kline_snapshot: Optional[tuple] = None

    def _check_stroke_validity(self, fx1: FX, fx2: FX) -> bool:
        """检查两个分型是否能构成有效的一笔。"""
        if fx1.type == fx2.type:
            return False

        min_distance = 4 if self.bi_mode == 'strict' else 3
        if fx2.k.index <= fx1.k.index:
            return False
        if (fx2.k.index - fx1.k.index) < min_distance:
            return False

        if fx1.type == 'ding':
            if fx2.val >= fx1.val:
                return False
        else:
            if fx2.val <= fx1.val:
                return False

        return True

    @staticmethod
    def _is_more_extreme(new_fx: FX, old_fx: FX) -> bool:
        if new_fx.type != old_fx.type:
            return False
        if new_fx.type == 'ding':
            return new_fx.val > old_fx.val
        return new_fx.val < old_fx.val

    def _find_fractal(self, k1: CLKline, k2: CLKline, k3: CLKline) -> Optional[FX]:
        """简化版分型识别。"""
        if k2.h > k1.h and k2.h > k3.h and k2.l > k1.l and k2.l > k3.l:
            return FX(_type='ding', k=k2, klines=[k1, k2, k3], val=k2.h)
        if k2.l < k1.l and k2.l < k3.l and k2.h < k1.h and k2.h < k3.h:
            return FX(_type='di', k=k2, klines=[k1, k2, k3], val=k2.l)
        return None

    def _collect_fxs(self, cl_klines: List[CLKline]) -> List[FX]:
        fxs: List[FX] = []
        for i in range(1, len(cl_klines) - 1):
            current_fx = self._find_fractal(cl_klines[i - 1], cl_klines[i], cl_klines[i + 1])
            if current_fx is None:
                continue
            current_fx.index = len(fxs)
            fxs.append(current_fx)
        return fxs

    def _compress_fxs(self, fxs: List[FX]) -> List[FX]:
        """
        压缩连续同类分型，只保留更极端的那个。

        这样可以避免首笔构造阶段被连续同类分型干扰，但不会跨越中间的反向分型。
        """
        effective_fxs: List[FX] = []
        for fx in fxs:
            if not effective_fxs:
                effective_fxs.append(fx)
                continue

            last_fx = effective_fxs[-1]
            if last_fx.type == fx.type:
                if self._is_more_extreme(fx, last_fx):
                    effective_fxs[-1] = fx
            else:
                effective_fxs.append(fx)
        return effective_fxs

    def _create_bi(self, start_fx: FX, end_fx: FX, index: int, done: bool) -> BI:
        bi_type = 'up' if start_fx.type == 'di' else 'down'
        bi = BI(start=start_fx, end=end_fx, _type=bi_type, index=index)
        bi.end.done = done
        return bi

    def _reindex_bis(self):
        for i, bi in enumerate(self.confirmed_bis):
            bi.index = i
            bi.end.done = True

        if self.pending_bi is not None:
            self.pending_bi.index = len(self.confirmed_bis)
            self.pending_bi.end.done = False

        self.bi_index = len(self.confirmed_bis) + (1 if self.pending_bi is not None else 0)
        self.bis = list(self.confirmed_bis)
        if self.pending_bi is not None:
            self.bis.append(self.pending_bi)

    def _rebuild_from_fxs(self, fxs: List[FX]):
        self.confirmed_bis = []
        self.pending_bi = None

        effective_fxs = self._compress_fxs(fxs)
        reopenable_bi: Optional[BI] = None
        next_bi_index = 0

        for fx_pos, current_fx in enumerate(effective_fxs):
            if self.pending_bi is None:
                start_fx = None
                if self.confirmed_bis:
                    start_fx = self.confirmed_bis[-1].end
                elif fx_pos > 0:
                    start_fx = effective_fxs[fx_pos - 1]

                if start_fx and self._check_stroke_validity(start_fx, current_fx):
                    self.pending_bi = self._create_bi(start_fx, current_fx, next_bi_index, False)
                    reopenable_bi = self.confirmed_bis[-1] if self.confirmed_bis else None
                    next_bi_index += 1
                continue

            end_fx_of_pending = self.pending_bi.end

            if current_fx.type == end_fx_of_pending.type:
                if self._is_more_extreme(current_fx, end_fx_of_pending):
                    self.pending_bi.end = current_fx
                    self.pending_bi.end.done = False
                continue

            if self._check_stroke_validity(end_fx_of_pending, current_fx):
                self.pending_bi.end.done = True
                self.confirmed_bis.append(self.pending_bi)

                start_fx_new = self.pending_bi.end
                self.pending_bi = self._create_bi(start_fx_new, current_fx, next_bi_index, False)
                reopenable_bi = self.confirmed_bis[-1]
                next_bi_index += 1
                continue

            should_reopen_prev_bi = (
                reopenable_bi is not None
                and self.confirmed_bis
                and reopenable_bi is self.confirmed_bis[-1]
                and reopenable_bi.end.type == current_fx.type
                and self._is_more_extreme(current_fx, reopenable_bi.end)
            )

            if should_reopen_prev_bi:
                reopened_bi = self.confirmed_bis.pop()
                reopened_bi.end = current_fx
                reopened_bi.end.done = False
                self.pending_bi = reopened_bi
                reopenable_bi = None

        self._reindex_bis()

    def _snapshot_matches(self, cl_klines: List[CLKline]) -> bool:
        if not self._last_kline_snapshot or not cl_klines:
            return False
        current_last = cl_klines[-1]
        last_idx, last_h, last_l = self._last_kline_snapshot
        return (
            current_last.index == last_idx
            and current_last.h == last_h
            and current_last.l == last_l
        )

    def _update_snapshot(self):
        if not self.cl_klines:
            self._last_kline_snapshot = None
            return
        last_k = self.cl_klines[-1]
        self._last_kline_snapshot = (last_k.index, last_k.h, last_k.l)

    def calculate(self, cl_klines: List[CLKline]):
        """
        计算笔列表。

        当前实现采用尾部全重放策略：当缠论 K 线发生变化时，重新根据完整分型序列构建笔，
        以保证复杂边界 case 下的正确性与增量/全量一致性。
        """
        if not cl_klines:
            self.cl_klines = []
            self.fxs = []
            self.confirmed_bis = []
            self.pending_bi = None
            self.bis = []
            self.bi_index = 0
            self._last_kline_snapshot = None
            return

        if self._snapshot_matches(cl_klines):
            return

        self.cl_klines = cl_klines
        self.fxs = self._collect_fxs(cl_klines)
        self._rebuild_from_fxs(self.fxs)
        self._update_snapshot()
