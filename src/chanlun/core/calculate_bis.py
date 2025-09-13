# -*- coding: utf-8 -*-
"""
笔计算模块
负责根据有效分型列表，连接成笔。
"""
from typing import List

from chanlun.core.cl_interface import FX, BI


def _check_bi_requirement(fx1: FX, fx2: FX) -> bool:
    """
    检查两个分型之间是否满足形成一笔的最低要求。
    规则：相邻的顶分型和底分型必须要有一根及以上除了分型K线之外的其他K线

    参数:
        fx1: 第一个分型
        fx2: 第二个分型

    返回:
        bool: True表示满足要求，False表示不满足
    """
    # 分型类型必须不同
    if fx1.type == fx2.type:
        return False

    # 获取两个分型包含的K线范围
    # 注意：分型的klines列表包含3个缠论K线
    # 需要检查这些K线之间是否有足够的间隔

    # 获取fx1的最后一个缠论K线
    fx1_last_ck = None
    for ck in reversed(fx1.klines):
        if ck is not None:
            fx1_last_ck = ck
            break

    # 获取fx2的第一个缠论K线
    fx2_first_ck = fx2.klines[0]

    if fx1_last_ck is None or fx2_first_ck is None:
        return False

    # 计算两个分型之间的K线数量
    # 使用k_index来判断（缠论K线的索引）
    k_count_between = fx2_first_ck.index - fx1_last_ck.index - 1

    # 至少需要1根K线间隔
    has_enough_gap = k_count_between >= 1

    if not has_enough_gap:
        print(f"  分型间隔不足：fx1_last_index={fx1_last_ck.index}, "
              f"fx2_first_index={fx2_first_ck.index}, gap={k_count_between}")

    return has_enough_gap

def calculate_bis(fxs: List[FX]) -> List[BI]:
    """
    根据缠论规则计算笔，严格遵循用户定义的筛选和连接逻辑。
    """
    if len(fxs) < 2:
        return []

    # 步骤 1: 筛选点（分型包含处理）
    # 规则:
    #   - 顶1 -> 底1 -> 顶2, 且 顶2 > 顶1 => 删除 顶1, 底1
    #   - 底1 -> 顶1 -> 底2, 且 底2 < 底1 => 删除 底1, 顶1

    # 创建一个副本进行操作
    processed_fxs = list(fxs)

    while True:
        removed = False
        if len(processed_fxs) < 3:
            break

        i = 0
        while i <= len(processed_fxs) - 3:
            fx1, fx2, fx3 = processed_fxs[i], processed_fxs[i + 1], processed_fxs[i + 2]
            # 检查连续顶分型中的包含关系
            if fx1.type == 'ding' and fx2.type == 'di' and fx3.type == 'ding':
                if fx3.val > fx1.val:
                    # 顶2更高，顶1和中间的底1被“X掉”
                    processed_fxs.pop(i)  # remove fx1
                    processed_fxs.pop(i)  # remove fx2
                    removed = True
                    break  # 重启外层循环 (Restart the outer loop)
            # 检查连续底分型中的包含关系
            if fx1.type == 'di' and fx2.type == 'ding' and fx3.type == 'di':
                if fx3.val < fx1.val:
                    # 底2更低，底1和中间的顶1被“X掉”
                    processed_fxs.pop(i)  # remove fx1
                    processed_fxs.pop(i)  # remove fx2
                    removed = True
                    break  # 重启外层循环
            i += 1

        if not removed:
            # 如果此轮循环没有删除任何元素，说明处理完毕
            break

    # 步骤 2 & 3: 连接笔并处理剩余的同类分型
    if len(processed_fxs) < 2:
        return []

    final_fxs = []
    if processed_fxs:
        final_fxs.append(processed_fxs[0])

    i = 1
    while i < len(processed_fxs):
        last_fx = final_fxs[-1]
        current_fx = processed_fxs[i]

        # 情况 A: 分型类型不同
        if last_fx.type != current_fx.type:
            # 检查是否满足成笔的两个关键条件
            # 条件1: K线间隔
            has_gap = _check_bi_requirement(last_fx, current_fx)
            # 条件2: 高低点
            is_valid_stroke = (last_fx.type == 'ding' and last_fx.val > current_fx.val) or \
                              (last_fx.type == 'di' and last_fx.val < current_fx.val)

            if has_gap and is_valid_stroke:
                # 满足条件，连接成笔，当前分型成为新的笔端点
                final_fxs.append(current_fx)
            else:
                # 不满足条件，发生笔的延续
                # 规则：用更极端的值替换掉前一个不成立的转折点
                if (last_fx.type == 'ding' and current_fx.val > last_fx.val) or \
                        (last_fx.type == 'di' and current_fx.val < last_fx.val):
                    final_fxs[-1] = current_fx

        # 情况 B: 分型类型相同
        # 这是处理步骤1中留下的 底A -> 顶1 -> 顶2 -> 底B (顶1>顶2) 这类情况
        else:
            if last_fx.type == 'ding' and current_fx.val > last_fx.val:
                # 一串连续顶中，取更高的那个
                final_fxs[-1] = current_fx
            elif last_fx.type == 'di' and current_fx.val < last_fx.val:
                # 一串连续底中，取更低的那个
                final_fxs[-1] = current_fx
            # 如果 current_fx 不够极端 (例如 顶1 > 顶2), 它就会被自然忽略

        i += 1

    # 步骤 4: 根据最终确认的分型点构建笔列表
    bis = []
    if len(final_fxs) >= 2:
        for j in range(len(final_fxs) - 1):
            start_fx = final_fxs[j]
            end_fx = final_fxs[j + 1]

            if start_fx.type == end_fx.type:
                continue

            bi_type = 'up' if start_fx.type == 'di' else 'down'
            new_bi = BI(start=start_fx, end=end_fx, _type=bi_type, index=len(bis))
            bis.append(new_bi)

    # 步骤 5: 处理待定笔
    # 待定笔是指从最后一个确认的分型点到最新一个分型点之间可能形成的笔。
    if final_fxs and processed_fxs:
        last_confirmed_fx = final_fxs[-1]
        last_processed_fx = processed_fxs[-1]

        # 如果最后一个处理过的分型点不是最后一个确认的笔端点，
        # 那么它们之间就存在一根待定笔。
        if last_confirmed_fx is not last_processed_fx:
            pending_bi_type = 'up' if last_confirmed_fx.type == 'di' else 'down'
            pending_bi = BI(
                start=last_confirmed_fx,
                end=last_processed_fx,
                _type=pending_bi_type,
                index=len(bis)
            )
            bis.append(pending_bi)

    return bis
