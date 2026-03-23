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
        """
        统一重排输出笔的索引与 done 状态。

        内部计算时，confirmed_bis / pending_bi 会在不同分支里被重建或替换，
        因此在最终输出前统一做一次标准化：
        - confirmed_bis 全部标记为 done=True
        - pending_bi 若存在，永远放在最后，且 done=False
        - self.bis 作为对外展示视图，由两者重新拼装
        """
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

    def _observation_passed(self, candidate_fx: FX, current_k_index: int) -> bool:
        """
        判断候选反向分型的最小观察窗口是否已经走完。

        这里实现的是你描述的“确认时机”规则：
        - 反向分型刚出现时，只能让当前笔进入“待确认”状态
        - 必须继续观察一个最小成笔距离窗口
        - 如果窗口内没有出现把当前笔终点打掉的更极端同向分型，
          当前笔才可以被正式确认
        """
        min_distance = 4 if self.bi_mode == 'strict' else 3
        return (current_k_index - candidate_fx.k.index) >= min_distance

    def _seal_active_bi(self, active_bi: BI, next_bi: BI):
        """
        将当前活动笔正式确认，并把预览中的下一笔切换为新的活动笔。

        active_bi: 已经走完观察窗口、可以锁定的前一笔
        next_bi:   由反向分型触发、转正后的当前进行中笔
        """
        active_bi.end.done = True
        self.confirmed_bis.append(active_bi)
        next_bi.end.done = False
        return next_bi

    def _rebuild_from_fxs(self, fxs: List[FX]):
        """
        从压缩后的分型序列中，按“活动笔 + 预览下一笔”的状态机重建笔。

        状态含义：
        - active_bi：当前正在运行、但未必已经最终锁定的笔
        - preview_pending_bi：由一个反向分型触发出来的“预览下一笔”

        关键规则：
        1. 反向分型第一次满足成笔条件时，不立即确认 active_bi，
           只是生成 preview_pending_bi
        2. 只有当 preview_pending_bi 的起点分型走完最小观察窗口后，
           active_bi 才真正锁定
        3. 如果在观察窗口内出现了更极端的 active_bi 同向分型，
           则 active_bi 延伸，preview_pending_bi 作废
        4. 如果在观察窗口内出现了 preview_pending_bi 同向的更极端分型，
           则只更新 preview_pending_bi 的终点
        """
        self.confirmed_bis = []
        self.pending_bi = None

        effective_fxs = self._compress_fxs(fxs)
        active_bi: Optional[BI] = None
        preview_pending_bi: Optional[BI] = None

        fx_pos = 0
        while fx_pos < len(effective_fxs):
            current_fx = effective_fxs[fx_pos]

            # 如果预览下一笔已经存在，并且它的起点分型观察窗口走完，
            # 则当前活动笔可以正式确认，预览笔转正为新的活动笔。
            if (
                active_bi is not None
                and preview_pending_bi is not None
                and self._observation_passed(preview_pending_bi.end, current_fx.k.index)
            ):
                active_bi = self._seal_active_bi(active_bi, preview_pending_bi)
                preview_pending_bi = None
                continue

            # 尚未形成任何活动笔时，只尝试从相邻有效分型中起第一笔。
            if active_bi is None:
                if fx_pos > 0 and self._check_stroke_validity(effective_fxs[fx_pos - 1], current_fx):
                    active_bi = self._create_bi(effective_fxs[fx_pos - 1], current_fx, 0, False)
                fx_pos += 1
                continue

            # 当前只有活动笔，还没有进入“预览下一笔”阶段。
            if preview_pending_bi is None:
                if current_fx.type == active_bi.end.type:
                    # 同向更极端分型，直接延伸活动笔终点。
                    if self._is_more_extreme(current_fx, active_bi.end):
                        active_bi.end = current_fx
                        active_bi.end.done = False
                elif self._check_stroke_validity(active_bi.end, current_fx):
                    # 第一个满足条件的反向分型出现，先生成预览下一笔，
                    # 但此时 active_bi 还不能立即确认。
                    preview_pending_bi = self._create_bi(active_bi.end, current_fx, 0, False)
                fx_pos += 1
                continue

            # 已进入观察窗口：
            # - 若出现更极端的 active_bi 同向分型，说明前一笔还不稳，延伸它并废弃预览笔
            # - 若出现预览笔同向的更极端分型，只更新预览笔终点
            if current_fx.type == active_bi.end.type:
                if self._is_more_extreme(current_fx, active_bi.end):
                    active_bi.end = current_fx
                    active_bi.end.done = False
                    preview_pending_bi = None
            elif self._is_more_extreme(current_fx, preview_pending_bi.end):
                preview_pending_bi.end = current_fx
                preview_pending_bi.end.done = False

            fx_pos += 1

        # 扫描结束后，如果预览笔的观察窗口已经被最后一根缠论 K 线走完，
        # 则把当前活动笔锁定，并让预览笔转正为新的 pending_bi。
        if (
            active_bi is not None
            and preview_pending_bi is not None
            and self.cl_klines
            and self._observation_passed(preview_pending_bi.end, self.cl_klines[-1].index)
        ):
            active_bi = self._seal_active_bi(active_bi, preview_pending_bi)
            preview_pending_bi = None

        # 对外暴露的 pending_bi 永远是“当前正在进行中”的最后一笔。
        self.pending_bi = active_bi
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
