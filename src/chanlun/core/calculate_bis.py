# -*- coding: utf-8 -*-
"""
笔计算模块
负责根据有效分型列表，连接成笔。
"""
from typing import List, Tuple, Optional

from chanlun.core.cl_interface import FX, BI, CLKline


#
# def _check_bi_requirement(fx1: FX, fx2: FX) -> bool:
#     """
#     检查两个分型之间是否满足形成一笔的最低要求。
#     规则：相邻的顶分型和底分型必须要有一根及以上除了分型K线之外的其他K线
#
#     参数:
#         fx1: 第一个分型
#         fx2: 第二个分型
#
#     返回:
#         bool: True表示满足要求，False表示不满足
#     """
#     # 分型类型必须不同
#     if fx1.type == fx2.type:
#         return False
#
#     # 获取两个分型包含的K线范围
#     # 注意：分型的klines列表包含3个缠论K线
#     # 需要检查这些K线之间是否有足够的间隔
#
#     # 获取fx1的最后一个缠论K线
#     fx1_last_ck = None
#     for ck in reversed(fx1.klines):
#         if ck is not None:
#             fx1_last_ck = ck
#             break
#
#     # 获取fx2的第一个缠论K线
#     fx2_first_ck = fx2.klines[0]
#
#     if fx1_last_ck is None or fx2_first_ck is None:
#         return False
#
#     # 计算两个分型之间的K线数量
#     # 使用k_index来判断（缠论K线的索引）
#     k_count_between = fx2_first_ck.index - fx1_last_ck.index - 1
#
#     # 至少需要1根K线间隔
#     has_enough_gap = k_count_between >= 1
#
#     if not has_enough_gap:
#         print(f"  分型间隔不足：fx1_last_index={fx1_last_ck.index}, "
#               f"fx2_first_index={fx2_first_ck.index}, gap={k_count_between}")
#
#     return has_enough_gap
#
# def calculate_bis(fxs: List[FX]) -> List[BI]:
#     """
#     根据缠论规则计算笔，严格遵循用户定义的筛选和连接逻辑。
#     """
#     if len(fxs) < 2:
#         return []
#
#     # 步骤 1: 筛选点（分型包含处理）
#     # 规则:
#     #   - 顶1 -> 底1 -> 顶2, 且 顶2 > 顶1 => 删除 顶1, 底1
#     #   - 底1 -> 顶1 -> 底2, 且 底2 < 底1 => 删除 底1, 顶1
#
#     # 创建一个副本进行操作
#     processed_fxs = list(fxs)
#
#     while True:
#         removed = False
#         if len(processed_fxs) < 3:
#             break
#
#         i = 0
#         while i <= len(processed_fxs) - 3:
#             fx1, fx2, fx3 = processed_fxs[i], processed_fxs[i + 1], processed_fxs[i + 2]
#             # 检查连续顶分型中的包含关系
#             if fx1.type == 'ding' and fx2.type == 'di' and fx3.type == 'ding':
#                 if fx3.val > fx1.val:
#                     # 顶2更高，顶1和中间的底1被“X掉”
#                     processed_fxs.pop(i)  # remove fx1
#                     processed_fxs.pop(i)  # remove fx2
#                     removed = True
#                     break  # 重启外层循环 (Restart the outer loop)
#             # 检查连续底分型中的包含关系
#             if fx1.type == 'di' and fx2.type == 'ding' and fx3.type == 'di':
#                 if fx3.val < fx1.val:
#                     # 底2更低，底1和中间的顶1被“X掉”
#                     processed_fxs.pop(i)  # remove fx1
#                     processed_fxs.pop(i)  # remove fx2
#                     removed = True
#                     break  # 重启外层循环
#             i += 1
#
#         if not removed:
#             # 如果此轮循环没有删除任何元素，说明处理完毕
#             break
#
#     # 步骤 2 & 3: 连接笔并处理剩余的同类分型
#     if len(processed_fxs) < 2:
#         return []
#
#     final_fxs = []
#     if processed_fxs:
#         final_fxs.append(processed_fxs[0])
#
#     i = 1
#     while i < len(processed_fxs):
#         last_fx = final_fxs[-1]
#         current_fx = processed_fxs[i]
#
#         # 情况 A: 分型类型不同
#         if last_fx.type != current_fx.type:
#             # 检查是否满足成笔的两个关键条件
#             # 条件1: K线间隔
#             has_gap = _check_bi_requirement(last_fx, current_fx)
#             # 条件2: 高低点
#             is_valid_stroke = (last_fx.type == 'ding' and last_fx.val > current_fx.val) or \
#                               (last_fx.type == 'di' and last_fx.val < current_fx.val)
#
#             if has_gap and is_valid_stroke:
#                 # 满足条件，连接成笔，当前分型成为新的笔端点
#                 final_fxs.append(current_fx)
#             else:
#                 # 不满足条件，发生笔的延续
#                 # 规则：用更极端的值替换掉前一个不成立的转折点
#                 if (last_fx.type == 'ding' and current_fx.val > last_fx.val) or \
#                         (last_fx.type == 'di' and current_fx.val < last_fx.val):
#                     final_fxs[-1] = current_fx
#
#         # 情况 B: 分型类型相同
#         # 这是处理步骤1中留下的 底A -> 顶1 -> 顶2 -> 底B (顶1>顶2) 这类情况
#         else:
#             if last_fx.type == 'ding' and current_fx.val > last_fx.val:
#                 # 一串连续顶中，取更高的那个
#                 final_fxs[-1] = current_fx
#             elif last_fx.type == 'di' and current_fx.val < last_fx.val:
#                 # 一串连续底中，取更低的那个
#                 final_fxs[-1] = current_fx
#             # 如果 current_fx 不够极端 (例如 顶1 > 顶2), 它就会被自然忽略
#
#         i += 1
#
#     # 步骤 4: 根据最终确认的分型点构建笔列表
#     bis = []
#     if len(final_fxs) >= 2:
#         for j in range(len(final_fxs) - 1):
#             start_fx = final_fxs[j]
#             end_fx = final_fxs[j + 1]
#
#             if start_fx.type == end_fx.type:
#                 continue
#
#             bi_type = 'up' if start_fx.type == 'di' else 'down'
#             new_bi = BI(start=start_fx, end=end_fx, _type=bi_type, index=len(bis))
#             bis.append(new_bi)
#
#     # 步骤 5: 处理待定笔
#     # 待定笔是指从最后一个确认的分型点到最新一个分型点之间可能形成的笔。
#     if final_fxs and processed_fxs:
#         last_confirmed_fx = final_fxs[-1]
#         last_processed_fx = processed_fxs[-1]
#
#         # 如果最后一个处理过的分型点不是最后一个确认的笔端点，
#         # 那么它们之间就存在一根待定笔。
#         if last_confirmed_fx is not last_processed_fx:
#             pending_bi_type = 'up' if last_confirmed_fx.type == 'di' else 'down'
#             pending_bi = BI(
#                 start=last_confirmed_fx,
#                 end=last_processed_fx,
#                 _type=pending_bi_type,
#                 index=len(bis)
#             )
#             bis.append(pending_bi)
#
#     return bis

# def calculate_bis(fxs: List[FX]) -> List[BI]:
#     """
#     根据给定的分型列表计算笔。
#
#     规则:
#     1. 分型预处理：
#        - 如果一个顶分型后面紧跟着一个更高的顶分型，则忽略前者。
#        - 如果一个底分型后面紧跟着一个更低的底分型，则忽略前者。
#        - 其他情况（如顶后跟更低的顶，或底后跟更高的底）则都保留。
#        - 此过程需迭代进行，直到分型列表稳定。
#     2. 划笔：
#        - 从一个分型开始，找到其后第一个类型相反的分型，两者相连成一笔。
#        - 中间所有同类型的分型都将被跳过。
#
#     :param fxs: 分型对象列表
#     :return: 笔对象列表
#     """
#     if len(fxs) < 2:
#         return []
#
#     # 步骤 1: 根据规则预处理分型列表 (迭代处理，直到稳定)
#     # 这个过程需要迭代，因为一次剔除后可能产生新的需要比较的相邻分型。
#     processed_fxs: List[FX] = list(fxs)
#     while True:
#         before_count = len(processed_fxs)
#         if before_count < 2:
#             break
#
#         fxs_after_pass: List[FX] = []
#         i = 0
#         while i < len(processed_fxs):
#             # 检查是否和下一个分型类型一致
#             if i + 1 < len(processed_fxs) and processed_fxs[i].type == processed_fxs[i + 1].type:
#                 f1 = processed_fxs[i]
#                 f2 = processed_fxs[i + 1]
#                 # 顶分型，前者低于后者，则跳过前者(f1)
#                 if f1.type == "ding" and f1.val < f2.val:
#                     i += 1
#                     continue
#                 # 底分型，前者高于后者，则跳过前者(f1)
#                 if f1.type == "di" and f1.val > f2.val:
#                     i += 1
#                     continue
#
#             fxs_after_pass.append(processed_fxs[i])
#             i += 1
#
#         processed_fxs = fxs_after_pass
#         if len(processed_fxs) == before_count:
#             # 如果一轮处理后，列表长度没有变化，说明已经稳定
#             break
#
#     # 步骤 2: 从处理后的分型列表中识别笔
#     # 规则: 一笔的结束是下一笔的开始
#     bis: List[BI] = []
#     if len(processed_fxs) < 2:
#         return []
#
#     bi_index = 0
#     start_fx_idx = 0
#     while start_fx_idx < len(processed_fxs) - 1:
#         start_fx = processed_fxs[start_fx_idx]
#
#         # 寻找结束点：从当前起点的下一个开始，找到第一个类型相反的分型
#         end_fx_idx = -1
#         for j in range(start_fx_idx + 1, len(processed_fxs)):
#             if processed_fxs[j].type != start_fx.type:
#                 end_fx_idx = j
#                 break
#
#         # 如果找到了结束点
#         if end_fx_idx != -1:
#             end_fx = processed_fxs[end_fx_idx]
#
#             # 判断笔的方向
#             bi_type = "up" if start_fx.type == "di" else "down"
#
#             # 创建笔对象
#             new_bi = BI(start=start_fx, end=end_fx, _type=bi_type, index=bi_index)
#             bis.append(new_bi)
#F
#             bi_index += 1
#             # 下一笔的起点是上一笔的终点
#             start_fx_idx = end_fx_idx
#         else:
#             # 如果从当前起点找不到任何后续的笔，则结束循环
#             break
#
#     return bis


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
