# -*- coding: utf-8 -*-
"""
线段计算模块
负责将笔合并且划分为线段。这是缠论中较为复杂的部分。
"""
import logging
from typing import List, Optional, Union, Dict, Tuple

from chanlun.core.cl_interface import BI, XD


# --- 辅助函数 ---
def _get_bi_high(bi: BI) -> float:
    """获取笔的最高价"""
    if bi.high:
        return bi.high
    return max(bi.start.val, bi.end.val)

def _get_bi_low(bi: BI) -> float:
    """获取笔的最低价"""
    if bi.low:
        return bi.low
    return min(bi.start.val, bi.end.val)

def _check_bi_overlap(bi1: BI, bi2: BI) -> bool:
    """检查两笔的价格区间是否有重叠"""
    low1, high1 = _get_bi_low(bi1), _get_bi_high(bi1)
    low2, high2 = _get_bi_low(bi2), _get_bi_high(bi2)
    return max(low1, low2) < min(high1, high2)


def _find_critical_bi_and_truncate(all_bis: List[BI]) -> int:
    """找到一个关键的笔作为分析起点，并返回其索引"""
    if len(all_bis) < 5:
        return 0

    for i in range(len(all_bis) - 4):
        bi_i = all_bis[i]
        bi_i_plus_2 = all_bis[i + 2]
        bi_i_plus_4 = all_bis[i + 4]
        is_critical = False

        if bi_i.type == 'down':
            is_start_higher = (_get_bi_high(bi_i) > _get_bi_high(bi_i_plus_2) and
                               _get_bi_high(bi_i) > _get_bi_high(bi_i_plus_4))
            is_end_higher = (_get_bi_low(bi_i) > _get_bi_low(bi_i_plus_2) and
                             _get_bi_low(bi_i) > _get_bi_low(bi_i_plus_4))
            if is_start_higher and is_end_higher:
                is_critical = True
        elif bi_i.type == 'up':
            is_start_lower = (_get_bi_low(bi_i) < _get_bi_low(bi_i_plus_2) and
                              _get_bi_low(bi_i) < _get_bi_low(bi_i_plus_4))
            is_end_lower = (_get_bi_high(bi_i) < _get_bi_high(bi_i_plus_2) and
                            _get_bi_high(bi_i) < _get_bi_high(bi_i_plus_4))
            if is_start_lower and is_end_lower:
                is_critical = True

        if is_critical:
            logging.info(f"在索引 {i} 处找到关键笔。从此开始分析。")
            return i

    logging.warning("未找到关键笔。从索引 0 开始分析。")
    return 0


def _get_characteristic_sequence(segment_bis: List[BI], segment_type: str) -> List[BI]:
    """从线段的笔列表中获取其特征序列"""
    cs_type = 'down' if segment_type == 'up' else 'up'
    return [bi for bi in segment_bis if bi.type == cs_type]


def _bi_to_dict(bi: BI) -> dict:
    """将BI对象转换为字典格式"""
    return {
        'bi': bi,
        'high': _get_bi_high(bi),
        'low': _get_bi_low(bi),
        'type': bi.type
    }

def _check_inclusion_dict(bi1: dict, bi2: dict) -> bool:
    """检查字典格式的两笔是否存在包含关系"""
    high1, low1 = bi1['high'], bi1['low']
    high2, low2 = bi2['high'], bi2['low']
    bi1_contains_bi2 = high1 >= high2 and low1 <= low2
    bi2_contains_bi1 = high2 >= high1 and low2 <= low1
    return bi1_contains_bi2 or bi2_contains_bi1

def _process_inclusion(bis: Union[List[BI], List[dict]], direction: str) -> List[dict]:
    """对特征序列进行包含关系处理"""
    if not bis:
        return []

    # 判断是否需要转换
    if isinstance(bis[0], dict):
        processed = bis[:]
    else:
        processed = [_bi_to_dict(bi) for bi in bis]

    if len(processed) < 2:
        return processed

    i = 0
    while i < len(processed) - 1:
        bi1 = processed[i]
        bi2 = processed[i + 1]

        if _check_inclusion_dict(bi1, bi2):
            high1, low1 = bi1['high'], bi1['low']
            high2, low2 = bi2['high'], bi2['low']

            if direction == 'down':
                new_low = min(low1, low2)
                new_high = min(high1, high2)
            else:
                new_high = max(high1, high2)
                new_low = max(low1, low2)

            merged = {
                'bi': bi1['bi'],
                'high': new_high,
                'low': new_low,
                'type': bi1['type'],
                'is_merged': True,
                'original_bis': bi1.get('original_bis', [bi1['bi']]) + bi2.get('original_bis', [bi2['bi']])
            }

            processed[i] = merged
            del processed[i + 1]
        else:
            i += 1

    return processed

def _check_top_fractal(processed_cs: List[dict]) -> tuple:
    """在特征序列中检查顶分型"""
    if len(processed_cs) < 3:
        return False, None, None

    for i in range(len(processed_cs) - 2):
        cs1, cs2, cs3 = processed_cs[i:i + 3]
        h2 = cs2['high']
        is_high_highest = h2 >= cs1['high'] and h2 >= cs3['high']
        if is_high_highest:
            return True, cs2, cs3

    return False, None, None


def _check_bottom_fractal(processed_cs: List[dict]) -> tuple:
    """在特征序列中检查底分型"""
    if len(processed_cs) < 3:
        return False, None, None

    for i in range(len(processed_cs) - 2):
        cs1, cs2, cs3 = processed_cs[i:i + 3]
        l2 = cs2['low']
        is_low_lowest = l2 <= cs1['low'] and l2 <= cs3['low']
        if is_low_lowest:
            return True, cs2, cs3

    return False, None, None


def _get_segment_end_bi_from_middle_cs(middle_cs: dict, all_bis: List[BI]) -> Optional[BI]:
    """根据分型的中间笔确定线段的结束笔 - 优化版本"""
    target_bi = None

    # 检查中间笔是否为合并笔
    if middle_cs.get('is_merged') and 'original_bis' in middle_cs:
        original_bis = middle_cs.get('original_bis', [])
        if original_bis:
            target_bi = original_bis[0]
        else:
            logging.warning("中间笔为合并笔，但其 'original_bis' 为空。")
            return None
    else:
        target_bi = middle_cs['bi']

    if not target_bi:
        logging.error("无法确定用于定位的目标笔。")
        return None

    try:
        # 优化：使用列表的 index 方法查找目标笔
        idx = target_bi.index
        logging.info(f"根据分型的中间笔确定线段的结束笔，idx:{idx}")
        if idx > 0:
            return all_bis[idx - 1]
        else:
            logging.warning("目标笔是列表中的第一根笔，无法找到其前一笔。")
            return None
    except ValueError:
        logging.error("目标笔在 all_bis 列表中未找到。")
        return None

def _get_extremum_bi_from_cs(cs_bi: dict) -> BI:
    """从特征序列笔中获取关键的原始笔"""
    original_bis = cs_bi.get('original_bis', [cs_bi['bi']])
    return original_bis[0] if original_bis else cs_bi['bi']


def calculate_segment_high_low(current_segment: Dict) -> (float, float):
    """
    根据 current_segment 的类型计算其高点和低点
    :param current_segment: 包含 'type' 和 'bis' 键的字典
    :return: 一个包含 (segment_high, segment_low) 的元组
    """
    segment_high = 0.0
    segment_low = 0.0

    bis_objects = current_segment.get('bis', [])

    if not bis_objects:
        print("警告: 'bis' 列表为空, 无法计算高点和低点。")
        return segment_high, segment_low

    first_bi = bis_objects[0]
    last_bi = bis_objects[-1]
    segment_type = current_segment.get('type')

    if segment_type == 'up':
        # 当类型为 'up' 时:
        # 高点 = 最后一笔的终点值
        # 低点 = 第一笔的起点值
        segment_high = last_bi.end.val
        segment_low = first_bi.start.val
    elif segment_type == 'down':
        # 当类型为 'down' 时:
        # 高点 = 第一笔的起点值
        # 低点 = 最后一笔的终点值
        segment_high = first_bi.start.val
        segment_low = last_bi.end.val
    else:
        print(f"警告: 未知的 segment type '{segment_type}'。")

    return segment_high, segment_low

# --- 主函数 ---
def calculate_xds(bis: List[BI], config: Dict) -> List[XD]:
    """根据笔列表计算线段"""

    logging.info("开始划分线段 (优化版)...")

    xds: List[XD] = []

    all_bis = bis
    current_list_index = _find_critical_bi_and_truncate(all_bis)

    if len(all_bis) < 3:
        logging.warning("笔的数量少于3，无法形成线段。")
        return []

    next_segment_builder = None

    # 主循环
    while current_list_index <= len(all_bis) - 3:

        # 使用 builder 构建下一段
        if next_segment_builder:
            logging.debug(f"主循环: 使用 builder 构建新线段")
            start_bi = next_segment_builder['start_bi']
            end_bi = next_segment_builder['end_bi']

            # 使用安全的索引查找方法
            start_idx = start_bi.index
            end_idx = end_bi.index

            # 验证索引的有效性
            if start_idx < 0 or end_idx < 0:
                logging.error(f"Builder 中的笔索引无效: start_idx={start_idx}, end_idx={end_idx}")
                current_list_index += 1
                next_segment_builder = None
                continue

            if end_idx < start_idx + 2:
                logging.info("Builder 信息不足以构成三笔，退回标准模式。")
                current_list_index = start_idx + 1
                next_segment_builder = None
                continue

            current_segment_bis = all_bis[start_idx: end_idx + 1]
            current_segment = {'bis': current_segment_bis, 'type': next_segment_builder['next_segment_type']}
            current_list_index = start_idx
            next_segment_builder = None

        else:
            # 标准模式
            s1, s2, s3 = all_bis[current_list_index:current_list_index + 3]

            if not _check_bi_overlap(s1, s3):
                current_list_index += 1
                continue

            current_segment = {'bis': [s1, s2, s3], 'type': s1.type}

        # 检查线段的延伸和结束
        next_check_idx = current_list_index + len(current_segment['bis'])
        is_completed = False
        break_info = None

        while next_check_idx + 1 < len(all_bis):

            segment_high, segment_low = calculate_segment_high_low(current_segment)

            bi_for_fractal_check = all_bis[next_check_idx]
            bi_for_extension_check = all_bis[next_check_idx + 1]

            # 处理上升线段
            if current_segment['type'] == 'up':
                if _get_bi_high(bi_for_extension_check) >= segment_high:
                    current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                    next_check_idx += 2
                    continue
                else:
                    # 检查是否结束
                    cs_existing_raw = _get_characteristic_sequence(current_segment['bis'], 'up')
                    if not cs_existing_raw:
                        current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                        next_check_idx += 2
                        continue

                    last_cs_bi = _process_inclusion(cs_existing_raw, 'up')[-1]
                    last_cs_original_bi = last_cs_bi['bi']

                    # 查找边界
                    lookahead_bis = all_bis[next_check_idx:]
                    bounded_lookahead_bis = []
                    for bi in lookahead_bis:
                        bounded_lookahead_bis.append(bi)
                        if bi.type == 'up' and _get_bi_high(bi) > segment_high:
                            break

                    # 检查顶分型
                    if _check_bi_overlap(bi_for_fractal_check, last_cs_original_bi):
                        cs_existing_raw = _process_inclusion(cs_existing_raw, 'down')
                        processed_cs_existing = _process_inclusion(cs_existing_raw, 'up')
                        new_cs_down_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'down']
                        processed_cs_new = _process_inclusion(new_cs_down_raw, 'up')
                        final_processed_cs = ([processed_cs_existing[-1]] + processed_cs_new
                                              if processed_cs_existing else processed_cs_new)

                        check1_passes, cs_middle, cs_right = _check_top_fractal(final_processed_cs)
                        if check1_passes:
                            segment_end_bi = _get_segment_end_bi_from_middle_cs(cs_middle, all_bis)
                            if segment_end_bi:  # 确保不为None
                                is_completed = True
                                peak_bi = _get_extremum_bi_from_cs(cs_middle)
                                right_bi = _get_extremum_bi_from_cs(cs_right)
                                break_info = {'reason': 'top_fractal', 'next_segment_type': 'down',
                                              'start_bi': peak_bi, 'end_bi': right_bi,
                                              'segment_end_bi': segment_end_bi}
                        else:
                            current_segment['bis'].extend(bounded_lookahead_bis)
                            next_check_idx += len(bounded_lookahead_bis)
                            continue
                    else:
                        # 双重条件检查
                        processed_cs_existing = _process_inclusion(cs_existing_raw, 'up')
                        new_cs_down_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'down']
                        processed_cs_new = _process_inclusion(new_cs_down_raw, 'up')
                        cs_for_check1 = ([processed_cs_existing[-1]] + processed_cs_new
                                         if processed_cs_existing else processed_cs_new)

                        check1_passes, cs_middle_top, cs_right_top = _check_top_fractal(cs_for_check1)

                        next_segment_cs_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'up']
                        processed_cs2 = _process_inclusion(next_segment_cs_raw, 'down')
                        check2_passes, _, _ = _check_bottom_fractal(processed_cs2)

                        if check1_passes and check2_passes:
                            segment_end_bi = _get_segment_end_bi_from_middle_cs(cs_middle_top, all_bis)
                            if segment_end_bi:  # 确保不为None
                                is_completed = True
                                peak_bi = _get_extremum_bi_from_cs(cs_middle_top)
                                right_bi_for_builder = _get_extremum_bi_from_cs(cs_right_top)
                                break_info = {'reason': 'dual_condition_up_break', 'next_segment_type': 'down',
                                              'start_bi': peak_bi, 'end_bi': right_bi_for_builder,
                                              'segment_end_bi': segment_end_bi}
                        else:
                            current_segment['bis'].extend(bounded_lookahead_bis)
                            next_check_idx += len(bounded_lookahead_bis)
                            continue

            # 处理下降线段（逻辑对称）
            elif current_segment['type'] == 'down':
                if _get_bi_low(bi_for_extension_check) <= segment_low:
                    current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                    next_check_idx += 2
                    continue
                else:
                    cs_existing_raw = _get_characteristic_sequence(current_segment['bis'], 'down')
                    if not cs_existing_raw:
                        current_segment['bis'].extend([bi_for_fractal_check, bi_for_extension_check])
                        next_check_idx += 2
                        continue

                    last_cs_bi = _process_inclusion(cs_existing_raw, 'down')[-1]
                    last_cs_original_bi = last_cs_bi['bi']
                    lookahead_bis = all_bis[next_check_idx:]
                    bounded_lookahead_bis = []

                    for bi in lookahead_bis:
                        bounded_lookahead_bis.append(bi)
                        if bi.type == 'down' and _get_bi_low(bi) < segment_low:
                            break

                    if _check_bi_overlap(bi_for_fractal_check, last_cs_original_bi):
                        cs_existing_raw = _process_inclusion(cs_existing_raw, 'up')
                        processed_cs_existing = _process_inclusion(cs_existing_raw, 'down')
                        new_cs_up_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'up']
                        processed_cs_new = _process_inclusion(new_cs_up_raw, 'down')
                        final_processed_cs = ([processed_cs_existing[-1]] + processed_cs_new
                                              if processed_cs_existing else processed_cs_new)

                        check1_passes, cs_middle, cs_right = _check_bottom_fractal(final_processed_cs)
                        if check1_passes:
                            segment_end_bi = _get_segment_end_bi_from_middle_cs(cs_middle, all_bis)
                            if segment_end_bi:  # 确保不为None
                                is_completed = True
                                trough_bi = _get_extremum_bi_from_cs(cs_middle)
                                right_bi = _get_extremum_bi_from_cs(cs_right)
                                break_info = {'reason': 'bottom_fractal', 'next_segment_type': 'up',
                                              'start_bi': trough_bi, 'end_bi': right_bi,
                                              'segment_end_bi': segment_end_bi}
                        else:
                            current_segment['bis'].extend(bounded_lookahead_bis)
                            next_check_idx += len(bounded_lookahead_bis)
                            continue
                    else:
                        processed_cs_existing = _process_inclusion(cs_existing_raw, 'down')
                        new_cs_up_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'up']
                        processed_cs_new = _process_inclusion(new_cs_up_raw, 'down')
                        cs_for_check1 = ([processed_cs_existing[-1]] + processed_cs_new
                                         if processed_cs_existing else processed_cs_new)

                        check1_passes, cs_middle_bottom, cs_right_bottom = _check_bottom_fractal(
                            cs_for_check1)

                        next_segment_cs_raw = [bi for bi in bounded_lookahead_bis if bi.type == 'down']
                        processed_cs2 = _process_inclusion(next_segment_cs_raw, 'up')
                        check2_passes, _, _ = _check_top_fractal(processed_cs2)

                        if check1_passes and check2_passes:
                            segment_end_bi = _get_segment_end_bi_from_middle_cs(cs_middle_bottom, all_bis)
                            if segment_end_bi:  # 确保不为None
                                is_completed = True
                                trough_bi = _get_extremum_bi_from_cs(cs_middle_bottom)
                                right_bi_for_builder = _get_extremum_bi_from_cs(cs_right_bottom)
                                break_info = {'reason': 'dual_condition_down_break', 'next_segment_type': 'up',
                                              'start_bi': trough_bi, 'end_bi': right_bi_for_builder,
                                              'segment_end_bi': segment_end_bi}
                        else:
                            current_segment['bis'].extend(bounded_lookahead_bis)
                            next_check_idx += len(bounded_lookahead_bis)
                            continue

            # 处理完成的线段
            if is_completed and break_info:
                final_end_bi = break_info.get('segment_end_bi')

                if final_end_bi is None:
                    logging.error("segment_end_bi 为 None，跳过此线段")
                    current_list_index += 1
                    continue

                final_bi_index = final_end_bi.index

                if final_bi_index < 0 or final_bi_index < current_list_index:
                    logging.error(f"无效的索引范围: current={current_list_index}, final={final_bi_index}")
                    current_list_index += 1
                    continue

                final_segment_bis = all_bis[current_list_index: final_bi_index + 1]

                if not final_segment_bis:
                    logging.warning(f"线段的笔列表为空，跳过")
                    current_list_index = final_bi_index + 1
                    continue

                # 创建XD对象
                xd = XD(
                    start=final_segment_bis[0].start,
                    end=final_end_bi.end,
                    start_line=final_segment_bis[0],
                    end_line=final_end_bi,
                    _type=current_segment['type'],
                    index=len(xds),
                    default_zs_type=config.get('zs_type_xd', None)
                )

                xd.high = max(_get_bi_high(bi) for bi in final_segment_bis)
                xd.low = min(_get_bi_low(bi) for bi in final_segment_bis)
                xd.zs_high = xd.high
                xd.zs_low = xd.low
                xd.done = True
                xds.append(xd)

                logging.debug(f"完成 {current_segment['type']} 线段，结束于索引:{final_bi_index}")

                # 设置下一轮的起始位置
                if break_info.get('start_bi'):
                    next_segment_builder = break_info
                    current_list_index = break_info['start_bi'].index
                    if current_list_index < 0:
                        logging.error("无法找到下一段的起始位置")
                        break
                else:
                    current_list_index = final_bi_index + 1

                break  # 退出内层循环

            # 处理待定线段
        if not is_completed and next_check_idx >= len(all_bis) - 1:
                if current_segment and current_segment.get('bis'):
                    pending_xd = XD(
                        start=all_bis[current_list_index].start,
                        end=all_bis[-1].end,
                        start_line=all_bis[current_list_index],
                        end_line=all_bis[-1],
                        _type=current_segment['type'],
                        index=len(xds),
                        default_zs_type=config.get('zs_type_xd', None)
                    )
                    pending_xd.high = max(_get_bi_high(bi) for bi in current_segment['bis'])
                    pending_xd.low = min(_get_bi_low(bi) for bi in current_segment['bis'])
                    pending_xd.zs_high = pending_xd.high
                    pending_xd.zs_low = pending_xd.low
                    pending_xd.done = False
                    xds.append(pending_xd)
                break

    logging.info(f"线段划分结束，完成 {len(xds)} 个线段。")
    return xds
        
    #     
    #     current_segment = None
    #     if next_segment_builder:
    #         start_bi, end_bi = next_segment_builder['start_bi'], next_segment_builder['end_bi']
    #         start_idx, end_idx = start_bi.index, end_bi.index
    #         if start_idx < 0 or end_idx < 0 or end_idx < start_idx + 2:
    #             current_list_index = start_idx + 1 if start_idx >= 0 else current_list_index + 1
    #             next_segment_builder = None
    #             continue
    #         current_segment = {'bis': all_bis[start_idx: end_idx + 1],
    #                            'type': next_segment_builder['next_segment_type']}
    #         current_list_index = start_idx
    #         next_segment_builder = None
    #     else:
    #         s1, s2, s3 = all_bis[current_list_index:current_list_index + 3]
    #         if not _check_bi_overlap(s1, s3):
    #             current_list_index += 1
    #             continue
    #         current_segment = {'bis': [s1, s2, s3], 'type': s1.type}
    # 
    #     next_check_idx = current_list_index + len(current_segment['bis'])
    #     is_completed = False
    #     break_info = None
    # 
    #     while next_check_idx + 1 < len(all_bis):
    #         segment_high = max(_get_bi_high(b) for b in current_segment['bis'])
    #         segment_low = min(_get_bi_low(b) for b in current_segment['bis'])
    #         bi_ext_check = all_bis[next_check_idx + 1]
    # 
    #         if (current_segment['type'] == 'up' and _get_bi_high(bi_ext_check) > segment_high) or \
    #                 (current_segment['type'] == 'down' and _get_bi_low(bi_ext_check) < segment_low):
    #             current_segment['bis'].extend(all_bis[next_check_idx:next_check_idx + 2])
    #             next_check_idx += 2
    #             continue
    # 
    #         cs_existing_raw = _get_characteristic_sequence(current_segment['bis'], current_segment['type'])
    #         if not cs_existing_raw:
    #             current_segment['bis'].extend(all_bis[next_check_idx:next_check_idx + 2])
    #             next_check_idx += 2
    #             continue
    # 
    #         # ... (The rest of the complex logic for segment breaking and completion)
    #         # This logic is highly specific and long, so it is kept as close to original as possible
    #         # To avoid introducing new bugs during refactoring.
    #         is_completed = True  # Placeholder for actual completion logic
    #         break
    # 
    #     if is_completed and break_info:
    #         final_end_bi = break_info.get('segment_end_bi')
    #         if final_end_bi is None:
    #             current_list_index += 1;
    #             continue
    #         final_bi_index = final_end_bi.index
    #         if final_bi_index < current_list_index:
    #             current_list_index += 1;
    #             continue
    # 
    #         final_segment_bis = all_bis[current_list_index: final_bi_index + 1]
    #         if not final_segment_bis:
    #             current_list_index = final_bi_index + 1;
    #             continue
    # 
    #         xd = XD(
    #             start=final_segment_bis[0].start, end=final_end_bi.end, start_line=final_segment_bis[0],
    #             end_line=final_end_bi, _type=current_segment['type'], index=len(xds),
    #             default_zs_type=config.get('zs_type_xd', None)
    #         )
    #         xd.high = max(_get_bi_high(b) for b in final_segment_bis)
    #         xd.low = min(_get_bi_low(b) for b in final_segment_bis)
    #         xd.done = True;
    #         xds.append(xd)
    # 
    #         if break_info.get('start_bi'):
    #             next_segment_builder = break_info
    #             current_list_index = break_info['start_bi'].index
    #             if current_list_index < 0: break
    #         else:
    #             current_list_index = final_bi_index + 1
    #     else:  # Handle pending segment if loop finishes
    #         break
    # 
    # # Note: The original logic for segment calculation is extremely complex.
    # # This refactored version moves the logic but does not simplify it to preserve correctness.
    # # The full logic from the original `_calculate_xds` should be placed here.
    # # For brevity, only the skeleton is shown. A simple copy might be easier for a real-world scenario.
    # if not xds or (bis and xds[-1].end_line.end != bis[-1].end):
    #     # A simplified pending segment logic
    #     if len(bis) > current_list_index:
    #         pending_bis = bis[current_list_index:]
    #         if len(pending_bis) > 0:
    #             xd = XD(start=pending_bis[0].start, end=pending_bis[-1].end, start_line=pending_bis[0],
    #                     end_line=pending_bis[-1],
    #                     _type=pending_bis[0].type, index=len(xds), default_zs_type=config.get('zs_type_xd', None))
    #             xd.high = max(_get_bi_high(b) for b in pending_bis)
    #             xd.low = min(_get_bi_low(b) for b in pending_bis)
    #             xd.done = False;
    #             xds.append(xd)

    # return xds
