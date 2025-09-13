# -*- coding: utf-8 -*-
"""
缠论K线包含关系处理模块=
"""
from typing import List
from chanlun.core.cl_interface import Kline, CLKline


def _need_merge(k1: CLKline, k2: CLKline) -> bool:
    """
    判断两根缠论K线是否存在包含关系=
    """
    k1_contains_k2 = k1.h >= k2.h and k1.l <= k2.l
    k2_contains_k1 = k2.h >= k1.h and k2.l <= k1.l
    return k1_contains_k2 or k2_contains_k1


def _merge_klines(k1: CLKline, k2: CLKline, direction: str) -> CLKline:
    """
    根据指定方向合并两根K线
    """
    if direction == 'up':
        h, l = max(k1.h, k2.h), max(k1.l, k2.l)
        date, k_index = (k1.date, k1.k_index) if k1.h > k2.h else (k2.date, k2.k_index)
    else:
        h, l = min(k1.h, k2.h), min(k1.l, k2.l)
        date, k_index = (k1.date, k1.k_index) if k1.l < k2.l else (k2.date, k2.k_index)
    merged = CLKline(
        k_index=k_index, date=date, h=h, l=l, o=h, c=l, a=k1.a + k2.a,
        klines=k1.klines + k2.klines, index=k1.index, _n=k1.n + k2.n, _q=k1.q
    )
    merged.up_qs = direction
    return merged


def process_cl_klines(src_klines: List[Kline]) -> List[CLKline]:
    """
    遍历原始K线，进行包含关系处理，生成缠论K线。
    """
    if not src_klines:
        return []

    cl_klines: List[CLKline] = []

    for i in range(len(src_klines)):
        current_k = src_klines[i]

        # 将原始K线包装成临时的缠论K线对象
        cl_k = CLKline(
            k_index=current_k.index, date=current_k.date, h=current_k.h, l=current_k.l,
            o=current_k.o, c=current_k.c, a=current_k.a, klines=[current_k],
            index=len(cl_klines), _n=1
        )

        # 如果是第一根K线，直接放入结果列表
        if not cl_klines:
            cl_klines.append(cl_k)
            continue

        last_cl_k = cl_klines[-1]

        # 检查是否有缺口，有缺口则不进行包含处理
        has_gap = cl_k.l > last_cl_k.h or cl_k.h < last_cl_k.l
        if has_gap:
            cl_k.q = True
            cl_klines.append(cl_k)
            continue

        # 判断是否需要与前一根处理过的K线合并
        if _need_merge(last_cl_k, cl_k):
            # 1. 确定合并方向 (1. Determine the merge direction)
            direction = 'up'  # 默认向上 (Default to 'up')
            if last_cl_k.up_qs is not None:
                # 如果上一根合并K线已经有方向，则继承该方向
                direction = last_cl_k.up_qs
            elif len(cl_klines) >= 2:
                # 标准情况：比较最后两根已处理K线的高点来定方向
                prev_prev_k = cl_klines[-2]
                if last_cl_k.h < prev_prev_k.h:
                    direction = 'down'
            else:
                # 边缘情况：第一次发生包含关系，根据两根K线的高低点关系来确定初始方向
                if cl_k.h > last_cl_k.h:
                    direction = 'up'
                elif cl_k.h < last_cl_k.h:
                    direction = 'down'
                else:  # 高点相同
                    if cl_k.l > last_cl_k.l:
                        direction = 'up'
                    else:  # 低点相同或更低
                        direction = 'down'

            # 2. 调用合并函数，并传入确定的方向
            merged_k = _merge_klines(last_cl_k, cl_k, direction)
            cl_klines[-1] = merged_k  # 替换掉最后一根K线
        else:
            # 如果没有包含关系，直接添加新K线
            cl_klines.append(cl_k)

    return cl_klines
