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
    新规则:
    1. 将连续的、K线重叠的潜在分型视为一个“冲突组”。
    2. 在冲突组内，根据前一个已确认的分型，通过比较保留一个最优分型作为临时的“候选分型”。
    3. 当冲突组结束（即下一个分型不再重叠时），才将这个从冲突中胜出的“候选分型”加入最终列表。
    """
    potential_fxs = _find_all_potential_fractals(cl_klines)

    if not potential_fxs:
        print("K线数量不足，未找到任何分型。")
        return []

    final_fxs = [potential_fxs[0]]
    
    if len(potential_fxs) < 2:
        print(f"分型识别完成，共找到 {len(final_fxs)} 个有效分型。")
        return final_fxs

    # 从第二个潜在分型开始，它成为我们的第一个候选者
    candidate_fx = potential_fxs[1]
    i = 2  # 指针从第三个潜在分型开始

    while i < len(potential_fxs):
        last_confirmed_fx = final_fxs[-1]
        next_potential_fx = potential_fxs[i]

        # 检查当前候选分型与下一个潜在分型是否重叠
        is_overlapping = (next_potential_fx.k.index - 1) <= (candidate_fx.k.index + 1)


        if is_overlapping:
            # --- 情况 A: 存在重叠，进入冲突解决模式 ---
            c_type = candidate_fx.type
            n_type = next_potential_fx.type

            if c_type == 'ding' and n_type == 'ding':
                # 两个都是顶，取更高的顶
                if next_potential_fx.val > candidate_fx.val:
                    candidate_fx = next_potential_fx

            elif c_type == 'di' and n_type == 'di':
                # 两个都是底，取更低的底
                if next_potential_fx.val < candidate_fx.val:
                    candidate_fx = next_potential_fx

            else:
                # 一顶一底，应用原有的比较规则
                competing_top = candidate_fx if c_type == 'ding' else next_potential_fx
                competing_bottom = candidate_fx if c_type == 'di' else next_potential_fx

                if last_confirmed_fx.type == 'ding':
                    # 前一个是顶，比较新顶和旧顶
                    if competing_top.val > last_confirmed_fx.val:
                        # if c_type == 'di': # 待定的分型为底分型时
                        #     candidate_fx = candidate_fx
                        # else:
                        candidate_fx = competing_top  # 新顶更高，候选者变为顶
                    else:
                        candidate_fx = competing_bottom  # 否则，候选者变为底
                else:  # last_confirmed_fx.type == 'di'
                    # 前一个是底，比较新底和旧底
                    if competing_bottom.val < last_confirmed_fx.val:
                        # if c_type == 'ding': # 待定的分型为顶分型时
                        #     candidate_fx = candidate_fx
                        # else:
                        candidate_fx = competing_bottom  # 新底更低，候选者变为底
                    else:
                        candidate_fx = competing_top  # 否则，候选者变为顶

            # 解决完一个重叠，指针前进 1，继续用新的 candidate_fx 和下一个分型比较
            i += 1

        else:
            # --- 情况 B: 不重叠，冲突结束 ---
            final_fxs.append(candidate_fx)

            # 更新 candidate_fx 为当前这个不重叠的分型，它将成为下一轮比较的起点
            candidate_fx = next_potential_fx
            i += 1

    # 循环结束后，处理最后一个留下的 candidate_fx
    if candidate_fx and (not final_fxs or candidate_fx.type != final_fxs[-1].type):
        final_fxs.append(candidate_fx)

    print(f"分型识别完成，共找到 {len(final_fxs)} 个有效分型。")
    return final_fxs
