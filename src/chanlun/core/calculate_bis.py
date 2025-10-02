# -*- coding: utf-8 -*-
"""
笔计算模块
负责根据有效分型列表，连接成笔。
"""
from typing import List, Tuple, Optional

from chanlun.core.cl_interface import FX, BI, CLKline

# ==============================================================================
# 2. 辅助函数 (验证笔)
#    Helper Functions (Validate Stroke)
# ==============================================================================

def check_stroke_validity(fx1: FX, fx2: FX) -> bool:
    """
    检查两个分型是否能构成有效的一笔。
    Check if two fractals can form a valid stroke.
    """
    # 规则1: 必须是顶底分型相连
    # Rule 1: Must connect a top fractal and a bottom fractal.
    if fx1.type == fx2.type:
        return False

    # 规则2: 分型之间必须至少有1根不属于分型的K线
    # 这里的实现方式是，两个分型的中间K线之间至少隔了3根K线，确保中间至少有1根独立K线
    if abs(fx2.k.index - fx1.k.index) < 4:
        return False

    # 获取分型区间的最高点和最低点.
    h1 = max(k.h for k in fx1.klines)
    l1 = min(k.l for k in fx1.klines)
    h2 = max(k.h for k in fx2.klines)
    l2 = min(k.l for k in fx2.klines)

    # 规则3: 分型区间不能有包含关系
    # Rule 3: The fractal intervals cannot have an inclusion relationship.
    if (h1 >= h2 and l1 <= l2) or (h2 >= h1 and l2 <= l1):
        return False

    # 新增规则 5: 根据您的要求进行检查
    # New Rule 5: Check based on your requirements.
    if fx1.type == 'ding':  # 那么 fx2 必须是底分型 (then fx2 must be a bottom fractal)
        # 如果fx2是底分型，其最低点必须低于前一个顶分型(fx1)的最低点
        # If fx2 is a bottom fractal, its lowest point must be lower than the lowest point of the previous top fractal (fx1).
        if l2 >= l1:
            return False
    else:  # fx1.type == 'di', 那么 fx2 必须是顶分型 (fx1.type is 'di', so fx2 must be a top fractal)
        # 如果fx2是顶分型，其最高点必须高于前一个底分型(fx1)的最高点
        # If fx2 is a top fractal, its highest point must be higher than the highest point of the previous bottom fractal (fx1).
        if h2 <= h1:
            return False

    return True

# ==============================================================================
# 3. 核心函数 (计算笔)
#    Core Function (Calculate Strokes)
# ==============================================================================

def calculate_bis(cl_klines: List[CLKline]) -> Tuple[List[BI], List[FX]]:
    """
    在识别分型的过程中同步判断笔的形成，而不是先找到所有分型。

    :param cl_klines: List[CLKline] - 处理过包含关系的K线列表
    :return: Tuple[List[BI], List[FX]] - (完成的笔列表, 找到的所有分型列表)
    """
    bis: List[BI] = []
    fxs: List[FX] = []
    pending_bi: BI = None
    bi_index = 0

    i = 1
    while i < len(cl_klines) - 1:
        k1, k2, k3 = cl_klines[i - 1], cl_klines[i], cl_klines[i + 1]

        # 步骤 1: 实时寻找下一个有效分型
        current_fx = None
        is_ding = k2.h > k1.h and k2.h > k3.h and k2.l > k1.l and k2.l > k3.l
        if is_ding:
            current_fx = FX(_type='ding', k=k2, klines=[k1, k2, k3], val=k2.h)

        is_di = k2.l < k1.l and k2.l < k3.l and k2.h < k1.h and k2.h < k3.h
        if not is_ding and is_di:
            current_fx = FX(_type='di', k=k2, klines=[k1, k2, k3], val=k2.l)

        if not current_fx:
            i += 1
            continue  # 未找到分型，继续遍历

        fxs.append(current_fx)

        # 步骤 2: 处理找到的分型，判断笔的形成、延伸或完成
        if not pending_bi:
            # 如果还没有待定笔，尝试用最近的两个分型形成第一笔
            if len(fxs) >= 2:
                start_fx = fxs[-2]
                if check_stroke_validity(start_fx, current_fx):
                    bi_type = 'up' if start_fx.type == 'di' else 'down'
                    pending_bi = BI(start=start_fx, end=current_fx, _type=bi_type, index=bi_index)
                    bi_index += 1
                    i += 3
                else:
                    i += 1
            else:
                i += 3
        else:
            # 如果已有待定笔，判断新分型
            end_fx_of_pending = pending_bi.end
            # 情况1: 新分型与待定笔终点同向 (可延伸)
            if current_fx.type == end_fx_of_pending.type:
                if end_fx_of_pending.type == 'ding' and current_fx.val > end_fx_of_pending.val:
                    pending_bi.end = current_fx  # 向上笔延伸
                    i += 3
                elif end_fx_of_pending.type == 'di' and current_fx.val < end_fx_of_pending.val:
                    pending_bi.end = current_fx  # 向下笔延伸
                    i += 3
                else:
                    i += 1
            # 情况2: 新分型与待定笔终点反向 (可完成旧笔，开启新笔)
            else:
                if check_stroke_validity(end_fx_of_pending, current_fx):
                    # 待定笔完成
                    pending_bi.end.done = True
                    bis.append(pending_bi)
                    # 开启新的待定笔
                    bi_type = 'up' if end_fx_of_pending.type == 'di' else 'down'
                    pending_bi = BI(start=end_fx_of_pending, end=current_fx, _type=bi_type, index=bi_index)
                    bi_index += 1
                    i += 3
                else:
                    i += 1

    # 循环结束后，最后一个待定笔也需要加入列表 (但标记为未完成)
    if pending_bi:
        pending_bi.end.done = False
        bis.append(pending_bi)

    return bis, fxs
