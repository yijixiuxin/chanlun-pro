# -*- coding: utf-8 -*-
"""
分型识别模块
负责从处理过的K线中识别顶分型和底分型。
"""
from typing import List

from chanlun.core.cl_interface import CLKline, FX


def _find_all_potential_fractals(cl_klines: List[CLKline]) -> List[FX]:
    """扫描所有K线，找出所有理论上成立的潜在分型。"""
    potential_fxs = []
    if len(cl_klines) < 3:
        return potential_fxs

    for i in range(1, len(cl_klines) - 1):
        prev_k, curr_k, next_k = cl_klines[i - 1], cl_klines[i], cl_klines[i + 1]

        is_top_fractal = (curr_k.h > prev_k.h and curr_k.h > next_k.h) and \
                         (curr_k.l > prev_k.l and curr_k.l > next_k.l)
        is_bottom_fractal = (curr_k.l < prev_k.l and curr_k.l < next_k.l) and \
                            (curr_k.h < prev_k.h and curr_k.h < next_k.h)

        if is_top_fractal:
            potential_fxs.append(FX(_type='ding', k=curr_k, klines=[prev_k, curr_k, next_k], val=curr_k.h))
        elif is_bottom_fractal:
            potential_fxs.append(FX(_type='di', k=curr_k, klines=[prev_k, curr_k, next_k], val=curr_k.l))

    return potential_fxs


def identify_fractals(cl_klines: List[CLKline]) -> List[FX]:
    """
    根据“重叠冲突处理”规则，筛选最终有效分型。
    最新版逻辑:
    1.  一个分型要成立，其首要条件是与上一个已确认的分型不重叠。
    2.  在满足条件1后，它将成为一个“候选分型”。
    3.  这个“候选分型”还需要与它后面的、且与它重叠的“潜在分型”进行比较，形成一个“冲突组”。
    4.  在冲突组内，根据第一个版本提供的比较逻辑（顶比顶、底比底、一顶一底看前一个分型）决出唯一的胜出者。
    5.  这个胜出者最终被确认为新的有效分型。
    """
    potential_fxs = _find_all_potential_fractals(cl_klines)

    if not potential_fxs:
        print("K线数量不足，未找到任何分型。")
        return []

    final_fxs = [potential_fxs[0]]

    i = 1
    while i < len(potential_fxs):
        last_confirmed_fx = final_fxs[-1]

        # 步骤 1: 寻找第一个不与 last_confirmed_fx 重叠的潜在分型
        start_candidate_index = -1
        for j in range(i, len(potential_fxs)):
            is_overlapping_with_last = (potential_fxs[j].k.index - 1) <= (last_confirmed_fx.k.index + 1)
            if not is_overlapping_with_last:
                start_candidate_index = j
                break

        # 如果找不到任何不重叠的分型，说明处理结束
        if start_candidate_index == -1:
            break

        # 步骤 2 & 3: 找到了一个候选分型，现在检查它与后续分型的重叠情况
        candidate_fx = potential_fxs[start_candidate_index]

        # 建立冲突组，从候选者的下一个开始看
        conflict_group_end_index = start_candidate_index
        for k in range(start_candidate_index + 1, len(potential_fxs)):
            next_potential_fx = potential_fxs[k]
            is_overlapping_in_group = (next_potential_fx.k.index - 1) <= (candidate_fx.k.index + 1)

            if is_overlapping_in_group:
                # --- 步骤 4: 重叠，进入冲突解决模式 (采用用户最初的逻辑) ---
                c_type = candidate_fx.type
                n_type = next_potential_fx.type

                if c_type == 'ding' and n_type == 'ding':
                    if next_potential_fx.val > candidate_fx.val:
                        candidate_fx = next_potential_fx
                elif c_type == 'di' and n_type == 'di':
                    if next_potential_fx.val < candidate_fx.val:
                        candidate_fx = next_potential_fx
                else:  # 一顶一底
                    competing_top = candidate_fx if c_type == 'ding' else next_potential_fx
                    competing_bottom = candidate_fx if c_type == 'di' else next_potential_fx

                    # 这里的比较基准是 last_confirmed_fx，即冲突组外的最后一个确认分型
                    if last_confirmed_fx.type == 'ding':
                        if competing_top.val > last_confirmed_fx.val:
                            candidate_fx = competing_top
                        else:
                            candidate_fx = competing_bottom
                    else:  # last_confirmed_fx.type == 'di'
                        if competing_bottom.val < last_confirmed_fx.val:
                            candidate_fx = competing_bottom
                        else:
                            candidate_fx = competing_top

                # 记录冲突组处理到的最后位置
                conflict_group_end_index = k
            else:
                # 冲突组结束
                break

        # 步骤 5: 冲突组处理完毕，将胜出者加入最终列表
        final_fxs.append(candidate_fx)

        # 更新主循环的索引，跳过整个处理过的冲突组
        i = conflict_group_end_index + 1

    print(f"分型识别完成，共找到 {len(final_fxs)} 个有效分型。")
    return final_fxs