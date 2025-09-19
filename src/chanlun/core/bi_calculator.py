# -*- coding: utf-8 -*-
"""
笔计算模块 (重构为类)
负责根据缠论K线，识别分型并连接成笔。
"""
from typing import List, Tuple, Optional

from chanlun.core.cl_interface import FX, BI, CLKline
from chanlun.tools.log_util import LogUtil


class BiCalculator:
    """
    笔计算器
    封装了分型识别和笔的计算逻辑。
    支持全量和增量计算。当增量数据传入时，它会从上次的状态继续，并仅返回新形成的结果。
    """

    def __init__(self):
        self.bis: List[BI] = []
        self.fxs: List[FX] = []
        self.pending_bi: Optional[BI] = None
        self.bi_index: int = 0
        self.cl_klines: List[CLKline] = []

    def _check_stroke_validity(self, fx1: FX, fx2: FX) -> bool:
        """
        检查两个分型是否能构成有效的一笔。
        """
        # 规则1: 必须是顶底分型相连
        if fx1.type == fx2.type:
            LogUtil.info(f"无效笔尝试 (同类分型): K线索引 {fx1.k.index} ({fx1.type}) to {fx2.k.index} ({fx2.type})")
            return False

        # 规则2: 分型之间必须至少有1根不属于分型的K线
        if abs(fx2.k.index - fx1.k.index) < 4:
            LogUtil.info(f"无效笔尝试 (K线太近): K线索引 {fx1.k.index} to {fx2.k.index}")
            return False

        h1 = max(k.h for k in fx1.klines)
        l1 = min(k.l for k in fx1.klines)
        h2 = max(k.h for k in fx2.klines)
        l2 = min(k.l for k in fx2.klines)

        # 规则3: 分型区间不能有包含关系
        if (h1 >= h2 and l1 <= l2) or (h2 >= h1 and l2 <= l1):
            LogUtil.info(f"无效笔尝试 (分型间包含): K线索引 {fx1.k.index} to {fx2.k.index}")
            return False

        # 规则 5: 新笔的高低点必须突破前一分型
        if fx1.type == 'ding':  # fx2 必须是底分型
            if l2 >= l1:
                LogUtil.info(f"无效笔尝试 (向下笔但fx2.low >= fx1.low): K线索引 {fx1.k.index} to {fx2.k.index}")
                return False
        else:  # fx1.type == 'di', fx2 必须是顶分型
            if h2 <= h1:
                LogUtil.info(f"无效笔尝试 (向上笔但fx2.high <= fx1.high): K线索引 {fx1.k.index} to {fx2.k.index}")
                return False

        return True

    def calculate(self, cl_klines: List[CLKline]) -> List[BI]:
        """
        在识别分型的过程中同步判断笔的形成。
        支持全量和增量数据。当传入增量数据时，返回增量结果。
        """
        # --- 1. 数据和状态初始化 ---
        if not cl_klines:
            return []

        is_incremental = bool(self.cl_klines)
        if is_incremental and cl_klines[-1].index <= self.cl_klines[-1].index:
            return []

        new_bis: List[BI] = []
        new_fxs: List[FX] = []
        self.cl_klines = cl_klines

        start_index = 1
        if is_incremental:
            LogUtil.info("开始笔计算 (增量)...")
            if self.bis and not self.bis[-1].end.done:
                self.pending_bi = self.bis.pop()
                self.bi_index -= 1
                start_index = self.pending_bi.start.k.index
                self.fxs = [fx for fx in self.fxs if fx.k.index < start_index]
            elif self.bis:
                start_index = self.bis[-1].end.k.index
            elif self.fxs:
                start_index = self.fxs[-1].k.index
            start_index = max(1, start_index)
        else:
            LogUtil.info(f"开始笔计算 (全量)，共 {len(self.cl_klines)} 根处理后K线。")
            self.bis, self.fxs, self.pending_bi, self.bi_index = [], [], None, 0

        # --- 2. 核心计算循环 ---
        i = start_index
        while i < len(self.cl_klines) - 1:
            current_fx = self._find_fractal(self.cl_klines[i - 1], self.cl_klines[i], self.cl_klines[i + 1])

            if current_fx:
                if self.fxs and self.fxs[-1].k.index >= current_fx.k.index:
                    i += 1
                    continue

                LogUtil.info(f"在K线索引 {current_fx.k.index} 找到潜在分型: {current_fx.type} (值: {current_fx.val})")
                self.fxs.append(current_fx)
                new_fxs.append(current_fx)

                if not self.pending_bi:
                    if len(self.fxs) >= 2:
                        start_fx = self.fxs[-2]
                        if self._check_stroke_validity(start_fx, current_fx):
                            bi_type = 'up' if start_fx.type == 'di' else 'down'
                            self.pending_bi = BI(start=start_fx, end=current_fx, _type=bi_type, index=self.bi_index)
                            self.bi_index += 1
                            LogUtil.info(f"创建新待定笔 ({self.pending_bi.type}): {start_fx.k.index} -> {current_fx.k.index}")
                            i += 3
                        else:
                            i += 1
                    else:
                        i += 3
                else:
                    end_fx_of_pending = self.pending_bi.end
                    if current_fx.type == end_fx_of_pending.type:
                        if (end_fx_of_pending.type == 'ding' and current_fx.val > end_fx_of_pending.val) or \
                           (end_fx_of_pending.type == 'di' and current_fx.val < end_fx_of_pending.val):
                            LogUtil.info(f"延伸待定笔 ({self.pending_bi.type}) 至新分型在 {current_fx.k.index}")
                            self.pending_bi.end = current_fx
                            i += 3
                        else:
                            i += 1
                    elif self._check_stroke_validity(end_fx_of_pending, current_fx):
                        LogUtil.info(f"完成笔 #{self.pending_bi.index}，结束于 {end_fx_of_pending.k.index}")
                        self.pending_bi.end.done = True
                        self.bis.append(self.pending_bi)
                        new_bis.append(self.pending_bi)

                        bi_type = 'up' if end_fx_of_pending.type == 'di' else 'down'
                        self.pending_bi = BI(start=end_fx_of_pending, end=current_fx, _type=bi_type, index=self.bi_index)
                        self.bi_index += 1
                        LogUtil.info(f"开启新待定笔 #{self.pending_bi.index} ({self.pending_bi.type})")
                        i += 3
                    else:
                        i += 1
            i += 1
        # --- 3. 循环结束后，处理最后一个待定笔 ---
        if self.pending_bi:
            self.pending_bi.end.done = False
            self.bis.append(self.pending_bi)
            if any(fx for fx in new_fxs if fx == self.pending_bi.end):
                new_bis.append(self.pending_bi)
            LogUtil.info(f"将最后一笔 (待定) #{self.pending_bi.index} 添加到列表。")

        # --- 4. 返回结果 ---
        if is_incremental:
            LogUtil.info(f"笔计算完成 (增量)。新增 {len(new_bis)} 笔。")
            return new_bis
        else:
            LogUtil.info(f"笔计算完成 (全量)。共找到 {len(self.bis)} 笔。")
            return self.bis

    def _find_fractal(self, k1: CLKline, k2: CLKline, k3: CLKline) -> Optional[FX]:
        """
        根据三根K线判断是否构成顶或底分型。
        """
        is_ding = k2.h > k1.h and k2.h > k3.h and k2.l > k1.l and k2.l > k3.l
        if is_ding:
            return FX(_type='ding', k=k2, klines=[k1, k2, k3], val=k2.h)

        is_di = k2.l < k1.l and k2.l < k3.l and k2.h < k1.h and k2.h < k3.h
        if is_di:
            return FX(_type='di', k=k2, klines=[k1, k2, k3], val=k2.l)

        return None
