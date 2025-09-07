# chanlun/segment.py
# -*- coding: utf-8 -*-
"""
【线段划分模块】
- 实现从笔序列中划分线段的复杂逻辑。
- 包含特征序列分析、分型判断和线段结束的特殊处理规则。
"""
import logging
from copy import deepcopy
from typing import List, Dict, Any, Optional, Tuple


# --- 辅助函数 ---
def get_stroke_high(stroke: Dict[str, Any]) -> float:
    """获取笔的最高价。"""
    # 优先使用预计算的 'high' 字段
    if 'high' in stroke and stroke['high'] is not None:
        return stroke['high']
    # 如果笔未完成，其最高价就是起点的价格
    if stroke.get('end') is None:
        return stroke['start']['price']
    # 对于已完成的笔，返回起点和终点价格中的较大者
    return max(stroke['start']['price'], stroke['end']['price'])


def check_top_fractal(processed_cs: List[Dict[str, Any]]) -> Tuple[bool, Optional[Dict], Optional[Dict]]:
    """
    【修改】在特征序列中检查顶分型。
    如果找到，则返回 True 以及构成顶分型的中间笔和右侧笔。
    :return: (是否找到分型, 中间笔, 右侧笔)
    """
    # 检查特征序列的长度是否足以构成一个分型
    if len(processed_cs) < 3:
        return False, None, None

    # 遍历特征序列，寻找三笔构成的顶分型结构
    for i in range(len(processed_cs) - 2):
        cs1, cs2, cs3 = processed_cs[i:i + 3]
        try:
            # 获取中间笔的最高价
            h2 = get_stroke_high(cs2)
            # 判断中间笔的最高价是否是三笔中的最高点
            is_high_highest = h2 >= get_stroke_high(cs1) and h2 >= get_stroke_high(cs3)
            if is_high_highest:
                # 如果找到顶分型，返回成功标识以及相关的笔
                return True, cs2, cs3
        except KeyError:
            # 如果笔数据结构不完整，跳过此次循环
            continue
    # 如果遍历完成仍未找到，则返回失败
    return False, None, None


def check_bottom_fractal(processed_cs: List[Dict[str, Any]]) -> Tuple[bool, Optional[Dict], Optional[Dict]]:
    """
    【修改】在特征序列中检查底分型。
    如果找到，则返回 True 以及构成底分型的中间笔和右侧笔。
    :return: (是否找到分型, 中间笔, 右侧笔)
    """
    # 检查特征序列的长度是否足以构成一个分型
    if len(processed_cs) < 3:
        return False, None, None

    # 遍历特征序列，寻找三笔构成的底分型结构
    for i in range(len(processed_cs) - 2):
        cs1, cs2, cs3 = processed_cs[i:i + 3]
        try:
            # 获取中间笔的最低价
            l2 = get_stroke_low(cs2)
            # 判断中间笔的最低价是否是三笔中的最低点
            is_low_lowest = l2 <= get_stroke_low(cs1) and l2 <= get_stroke_low(cs3)
            if is_low_lowest:
                # 如果找到底分型，返回成功标识以及相关的笔
                return True, cs2, cs3
        except KeyError:
            # 如果笔数据结构不完整，跳过此次循环
            continue
    # 如果遍历完成仍未找到，则返回失败
    return False, None, None


def get_stroke_low(stroke: Dict[str, Any]) -> float:
    """获取笔的最低价。"""
    # 优先使用预计算的 'low' 字段
    if 'low' in stroke and stroke['low'] is not None:
        return stroke['low']
    # 如果笔未完成，其最低价就是起点的价格
    if stroke.get('end') is None:
        return stroke['start']['price']
    # 对于已完成的笔，返回起点和终点价格中的较小者
    return min(stroke['start']['price'], stroke['end']['price'])


def check_overlap(stroke1: Dict[str, Any], stroke2: Dict[str, Any]) -> bool:
    """检查两笔的价格区间是否有重叠。"""
    # 获取两笔的高低点
    low1, high1 = get_stroke_low(stroke1), get_stroke_high(stroke1)
    low2, high2 = get_stroke_low(stroke2), get_stroke_high(stroke2)
    # 判断是否存在重叠：两个区间中较低点的较大值小于较高点的较小值
    return max(low1, low2) < min(high1, high2)


def check_inclusion(stroke1: Dict[str, Any], stroke2: Dict[str, Any]) -> bool:
    """检查两笔是否存在包含关系。"""
    # 获取两笔的高低点
    high1, low1 = get_stroke_high(stroke1), get_stroke_low(stroke1)
    high2, low2 = get_stroke_high(stroke2), get_stroke_low(stroke2)
    # 判断第一笔是否包含第二笔
    s1_contains_s2 = high1 >= high2 and low1 <= low2
    # 判断第二笔是否包含第一笔
    s2_contains_s1 = high2 >= high1 and low2 <= low1
    # 只要满足其中一个条件，就存在包含关系
    return s1_contains_s2 or s2_contains_s1


def get_extremum_stroke_from_cs(cs_stroke: Dict[str, Any]) -> Dict[str, Any]:
    """从一个可能合并过的特征序列笔中，找到关键的原始笔。"""
    # 如果特征序列笔是合并过的，则从其包含的原始笔列表中获取第一笔
    original_strokes = cs_stroke.get('original_strokes', [cs_stroke])
    return original_strokes[0]


def check_fractal_in_cs(processed_cs: List[Dict[str, Any]], fractal_type: str) -> Tuple[
    bool, Optional[Dict], Optional[Dict]]:
    """在特征序列中检查顶/底分型。"""
    # 确保特征序列至少有三笔
    if len(processed_cs) < 3:
        return False, None, None

    # 遍历特征序列，寻找分型
    for i in range(len(processed_cs) - 2):
        cs1, cs2, cs3 = processed_cs[i:i + 3]
        try:
            # 根据需要检查的分型类型（顶分型或底分型）进行判断
            if fractal_type == 'top':
                val2 = get_stroke_high(cs2)
                is_extremum = val2 >= get_stroke_high(cs1) and val2 >= get_stroke_high(cs3)
            else:  # bottom
                val2 = get_stroke_low(cs2)
                is_extremum = val2 <= get_stroke_low(cs1) and val2 <= get_stroke_low(cs3)

            if is_extremum:
                # 如果找到分型，返回成功标识以及构成该分型的中间笔和右侧笔
                return True, cs2, cs3
        except KeyError:
            # 如果数据有误，跳过
            continue
    # 未找到分型
    return False, None, None


def get_characteristic_sequence(segment_strokes: List[Dict[str, Any]], segment_type: str) -> List[Dict[str, Any]]:
    """从线段的笔画列表中获取其特征序列。"""
    # 上升线段的特征序列由下降笔构成，反之亦然
    cs_type = 'down' if segment_type == 'up' else 'up'
    # 从线段的所有笔中筛选出符合特征序列类型的笔
    return [s for s in segment_strokes if s['type'] == cs_type]


# --- 线段划分相关函数 (与原版相同) ---
def find_critical_stroke_and_truncate(data):
    """找到一个关键的笔作为分析起点，并截断之前的数据。"""
    # 深拷贝输入数据，避免修改原始数据
    original_data = deepcopy(data)
    completed_strokes = original_data.get('completed_strokes', [])
    # 至少需要5笔才能判断
    if len(completed_strokes) < 5:
        return None, data
    # 遍历已完成的笔，寻找一个“关键笔”
    for i in range(len(completed_strokes) - 4):
        stroke_i = completed_strokes[i]
        stroke_i_plus_2 = completed_strokes[i + 2]
        stroke_i_plus_4 = completed_strokes[i + 4]
        is_critical = False
        # 判断下降笔是否为关键笔
        if stroke_i['type'] == 'down':
            is_start_higher = (stroke_i['start']['price'] > stroke_i_plus_2['start']['price'] and
                               stroke_i['start']['price'] > stroke_i_plus_4['start']['price'])
            is_end_higher = (stroke_i['end']['price'] > stroke_i_plus_2['end']['price'] and
                             stroke_i['end']['price'] > stroke_i_plus_4['end']['price'])
            if is_start_higher and is_end_higher:
                is_critical = True
        # 判断上升笔是否为关键笔
        elif stroke_i['type'] == 'up':
            is_start_lower = (stroke_i['start']['price'] < stroke_i_plus_2['start']['price'] and
                              stroke_i['start']['price'] < stroke_i_plus_4['start']['price'])
            is_end_lower = (stroke_i['end']['price'] < stroke_i_plus_2['end']['price'] and
                            stroke_i['end']['price'] < stroke_i_plus_4['end']['price'])
            if is_start_lower and is_end_lower:
                is_critical = True

        # 如果找到了关键笔
        if is_critical:
            found_point = stroke_i['start']
            # 从关键笔开始截断，作为新的笔序列
            new_completed_strokes = completed_strokes[i:]
            new_data = {
                'completed_strokes': new_completed_strokes,
                **{k: v for k, v in original_data.items() if k != 'completed_strokes'}
            }
            return found_point, new_data
    # 如果没有找到关键笔，返回原始数据
    return None, data


# --- 2. 线段划分核心逻辑函数 ---
def process_inclusion(strokes: List[Dict[str, Any]], direction: str) -> List[Dict[str, Any]]:
    """
    【修改】对特征序列（由笔构成的列表）进行标准的包含关系处理。
    此版本新增了对 'original_strokes' 的追踪，以便于从合并后的笔追溯到原始笔。
    """
    # 如果笔的数量少于2，无法进行包含处理
    if len(strokes) < 2:
        return strokes

    # 复制列表以进行处理
    processed = [s.copy() for s in strokes]

    i = 0
    while i < len(processed) - 1:
        s1 = processed[i]
        s2 = processed[i + 1]

        # 检查相邻两笔是否存在包含关系
        if check_inclusion(s1, s2):
            high1, low1 = get_stroke_high(s1), get_stroke_low(s1)
            high2, low2 = get_stroke_high(s2), get_stroke_low(s2)

            # 根据处理方向（向上或向下）合并笔
            if direction == 'down':  # 向下处理，高点取低，低点取低
                new_low = min(low1, low2)
                new_high = min(high1, high2)
            else:  # direction == 'up' # 向上处理，高点取高，低点取高
                new_high = max(high1, high2)
                new_low = max(low1, low2)

            # 创建合并后的新笔
            merged_stroke = {
                'start': s1['start'],
                'end': s2['end'],
                'type': s1['type'],
                'high': new_high,
                'low': new_low,
                'is_merged': True,
                # 记录被合并的原始笔
                'original_strokes': s1.get('original_strokes', [s1]) + s2.get('original_strokes', [s2])
            }

            # 替换并删除旧笔
            processed[i] = merged_stroke
            del processed[i + 1]
        else:
            # 如果没有包含关系，继续检查下一对
            i += 1

    return processed


def get_segment_end_stroke_from_middle_cs(middle_cs_stroke: Dict[str, Any], all_strokes: List[Dict[str, Any]]) -> \
Optional[Dict[str, Any]]:
    """
    【修改】根据分型的中间笔来确定线段的结束笔。
    - 如果中间笔包含 'original_strokes'，则目标笔是其 'original_strokes' 的第一笔。
    - 否则，目标笔就是中间笔自身。
    - 线段的结束笔是目标笔在总笔列表中的前一笔。
    """
    target_stroke = None
    # 检查中间笔是否为合并笔
    if middle_cs_stroke.get('is_merged') and 'original_strokes' in middle_cs_stroke:
        original_strokes = middle_cs_stroke.get('original_strokes', [])
        if original_strokes:
            # 目标笔是合并序列中的第一笔
            target_stroke = original_strokes[0]
        else:
            logging.warning(f"中间笔为合并笔，但其 'original_strokes' 为空。")
            return None
    else:
        # 对于非合并笔，目标笔就是它自己
        target_stroke = middle_cs_stroke

    if not target_stroke:
        logging.error("无法确定用于定位的目标笔。")
        return None

    try:
        # 在所有笔的列表中找到目标笔的索引
        idx = all_strokes.index(target_stroke)
        if idx > 0:
            # 线段的结束笔是目标笔的前一笔
            return all_strokes[idx - 1]
        else:
            logging.warning(
                f"目标笔 {target_stroke.get('start', {}).get('k_index')} 是列表中的第一根笔，无法找到其前一笔。")
            return target_stroke
    except ValueError:
        logging.error(f"严重错误: 目标笔 {target_stroke.get('start', {}).get('k_index')} 在 all_strokes 列表中未找到。")
        return None


def identify_segments(strokes_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    根据缠论线段定义，从笔序列中划分线段。
    这是一个简化的实现，主要演示逻辑，实际情况可能更复杂。
    """
    logging.info("开始划分线段 (新版逻辑)...")
    # 首先找到一个关键的分析起点，并截断之前的数据
    _, new_strokes_data = find_critical_stroke_and_truncate(strokes_data)
    all_strokes: list = new_strokes_data.get('completed_strokes', [])
    pending_stroke: Optional[Dict[str, Any]] = new_strokes_data.get('pending_stroke')

    # 笔的数量过少，无法形成线段
    if len(all_strokes) < 3:
        logging.warning("笔的数量少于3，无法形成线段。")
        return {"completed_segments": [], "pending_segment": None}

    completed_segments: List[Dict[str, Any]] = []
    pending_segment: Optional[Dict[str, Any]] = None
    current_list_index = 0
    # 【新增】用于接收上一段的结束信息，并构建下一段的起点
    next_segment_builder = None

    # 主循环，遍历所有笔
    while current_list_index <= len(all_strokes) - 3:

        # 【修改】主循环逻辑，优先使用 builder 构建下一段
        if next_segment_builder:
            logging.debug(f"主循环: 使用 builder 构建新线段")
            start_stroke = next_segment_builder['start_stroke']
            end_stroke = next_segment_builder['end_stroke']

            try:
                # 在总笔列表中找到 builder 提供的起止笔的索引
                start_idx = all_strokes.index(start_stroke)
                end_idx = all_strokes.index(end_stroke)
            except ValueError:
                logging.error("Builder 中的笔在 all_strokes 中未找到。终止。")
                break

            # 确保起止笔能构成至少三笔的线段
            if end_idx < start_idx or (end_idx - start_idx) < 2:
                logging.info("Builder 信息不足以构成三笔，退回标准模式。")
                current_list_index = start_idx + 1
                next_segment_builder = None
                continue

            # 从 builder 信息构建潜在线段
            current_segment_strokes = all_strokes[start_idx: end_idx + 1]
            current_segment = {'strokes': current_segment_strokes, 'type': next_segment_builder['next_segment_type']}
            current_list_index = start_idx
            logging.debug(
                f"从 builder 构建潜在线段。起点 k_index:{start_stroke['start']['k_index']}，方向: {current_segment['type']}")
            next_segment_builder = None
        else:
            # 标准模式：从当前位置开始寻找线段起点
            logging.debug(f"主循环: 检查列表索引位置: {current_list_index}")
            s1, s2, s3 = all_strokes[current_list_index:current_list_index + 3]

            # 线段成立的第一个条件：第一笔和第三笔必须有重叠
            if not check_overlap(s1, s3):
                logging.debug(
                    f"笔 {s1['start']['k_index']}-{s1['end']['k_index']} 与笔 {s3['start']['k_index']}-{s3['end']['k_index']} 不重叠。")
                current_list_index += 1
                continue

            # 发现一个潜在线段的起点
            current_segment = {'strokes': [s1, s2, s3], 'type': s1['type']}
            logging.debug(f"发现潜在线段起点于笔 k_index:{s1['start']['k_index']}。方向: {current_segment['type']}")

        # 开始检查这个潜在线段是否会延伸或结束
        next_check_idx = current_list_index + len(current_segment['strokes'])
        is_completed = False
        break_info = None

        # 内循环，检查线段的延伸和结束
        while next_check_idx + 1 < len(all_strokes):
            segment_high = max(get_stroke_high(s) for s in current_segment['strokes'])
            segment_low = min(get_stroke_low(s) for s in current_segment['strokes'])

            stroke_for_fractal_check = all_strokes[next_check_idx]
            stroke_for_extension_check = all_strokes[next_check_idx + 1]

            logging.debug(f"延伸/结束检查 at index {next_check_idx}")
            logging.debug(f"当前线段极值: High={segment_high:.2f}, Low={segment_low:.2f}")

            # --- 处理上升线段 ---
            if current_segment['type'] == 'up':
                # 情况1：线段延伸。后续同向笔创出新高
                if get_stroke_high(stroke_for_extension_check) > segment_high:
                    logging.debug("延伸: 创出新高，线段延伸。")
                    current_segment['strokes'].extend([stroke_for_fractal_check, stroke_for_extension_check])
                    next_check_idx += 2
                    continue
                else:
                    # 情况2：未创新高，需要用特征序列判断是否结束
                    logging.debug("结束判断: 未创新高，进入特殊判断逻辑。")
                    cs_existing_raw = get_characteristic_sequence(current_segment['strokes'], 'up')
                    if not cs_existing_raw:
                        logging.warning("当前线段无有效特征序列，无法判断，继续延伸。")
                        current_segment['strokes'].extend([stroke_for_fractal_check, stroke_for_extension_check])
                        next_check_idx += 2
                        continue

                    # 找到后续走势的边界（直到再次创出线段新高）
                    last_cs_stroke = process_inclusion(cs_existing_raw, 'up')[-1]
                    lookahead_strokes = all_strokes[next_check_idx:]
                    bounded_lookahead_strokes = []
                    break_point_found = False
                    for s in lookahead_strokes:
                        bounded_lookahead_strokes.append(s)
                        if s['type'] == 'up' and get_stroke_high(s) > segment_high:
                            logging.debug(
                                f"边界: 发现向上笔 k_index {s['start']['k_index']} 突破线段高点 {segment_high:.2f}。检查范围确定。")
                            break_point_found = True
                            break
                    if not break_point_found:
                        logging.debug("边界: 未发现突破线段高点的笔，检查范围为所有后续笔。")

                    # 线段结束的第一种情况：特征序列出现顶分型
                    if check_overlap(stroke_for_fractal_check, last_cs_stroke):
                        logging.debug("情况 1: 重叠, 检查顶分型...")
                        cs_existing_raw = process_inclusion(cs_existing_raw, 'down')
                        processed_cs_existing = process_inclusion(cs_existing_raw, 'up')
                        new_cs_down_raw = [s for s in bounded_lookahead_strokes if s['type'] == 'down']
                        processed_cs_new = process_inclusion(new_cs_down_raw, 'up')
                        final_processed_cs = [processed_cs_existing[
                                                  -1]] + processed_cs_new if processed_cs_existing else processed_cs_new

                        check1_passes, cs_middle, cs_right = check_top_fractal(final_processed_cs)
                        if check1_passes:
                            segment_end_stroke = get_segment_end_stroke_from_middle_cs(cs_middle, all_strokes)
                            logging.debug("确认: ** 顶分型确认 ** -> 向上线段结束。")
                            is_completed = True
                            peak_stroke = get_extremum_stroke_from_cs(cs_middle)
                            right_stroke = get_extremum_stroke_from_cs(cs_right)
                            break_info = {'reason': 'top_fractal', 'next_segment_type': 'down',
                                          'start_stroke': peak_stroke, 'end_stroke': right_stroke,
                                          'segment_end_stroke': segment_end_stroke}
                    else:
                        # 线段结束的第二种情况：特征序列出现顶分型，且后续出现一个向上的线段
                        logging.debug("情况 2: 不重叠, 检查 (顶分型 AND 后续底分型)...")
                        processed_cs_existing = process_inclusion(cs_existing_raw, 'up')
                        new_cs_down_raw = [s for s in bounded_lookahead_strokes if s['type'] == 'down']
                        processed_cs_new = process_inclusion(new_cs_down_raw, 'up')
                        cs_for_check1 = [processed_cs_existing[
                                             -1]] + processed_cs_new if processed_cs_existing else processed_cs_new

                        check1_passes, cs_middle_top, cs_right_top = check_top_fractal(cs_for_check1)
                        logging.debug(f"检查1 (顶分型): {'通过' if check1_passes else '失败'}")

                        next_segment_cs_raw = [s for s in bounded_lookahead_strokes if s['type'] == 'up']
                        processed_cs2 = process_inclusion(next_segment_cs_raw, 'down')
                        check2_passes, _, _ = check_bottom_fractal(processed_cs2)
                        logging.debug(f"检查2 (后续底分型): {'通过' if check2_passes else '失败'}")

                        if check1_passes and check2_passes:
                            segment_end_stroke = get_segment_end_stroke_from_middle_cs(cs_middle_top, all_strokes)
                            logging.debug("确认: ** 双重条件满足 ** -> 向上线段结束。")
                            is_completed = True
                            peak_stroke = get_extremum_stroke_from_cs(cs_middle_top)
                            right_stroke_for_builder = get_extremum_stroke_from_cs(cs_right_top)
                            break_info = {'reason': 'dual_condition_up_break', 'next_segment_type': 'down',
                                          'start_stroke': peak_stroke, 'end_stroke': right_stroke_for_builder,
                                          'segment_end_stroke': segment_end_stroke}
                        else:
                            # 如果双重条件不满足，线段延续到检查边界
                            logging.debug("延伸: 双重条件不满足，线段延伸至检查边界。")
                            current_segment['strokes'].extend(bounded_lookahead_strokes)
                            next_check_idx += len(bounded_lookahead_strokes)
                            continue

            # --- 处理下降线段 (逻辑与上升线段对称) ---
            elif current_segment['type'] == 'down':
                # 情况1：创出新低，线段延伸
                if get_stroke_low(stroke_for_extension_check) < segment_low:
                    logging.debug("延伸: 创出新低，线段延伸。")
                    current_segment['strokes'].extend([stroke_for_fractal_check, stroke_for_extension_check])
                    next_check_idx += 2
                    continue
                else:
                    # 情况2：未创新低，进入特殊判断
                    logging.debug("结束判断: 未创新低，进入特殊判断逻辑。")
                    cs_existing_raw = get_characteristic_sequence(current_segment['strokes'], 'down')
                    if not cs_existing_raw:
                        logging.warning("当前线段无有效特征序列，无法判断，继续延伸。")
                        current_segment['strokes'].extend([stroke_for_fractal_check, stroke_for_extension_check])
                        next_check_idx += 2
                        continue

                    last_cs_stroke = process_inclusion(cs_existing_raw, 'down')[-1]
                    lookahead_strokes = all_strokes[next_check_idx:]
                    bounded_lookahead_strokes = []
                    break_point_found = False
                    for s in lookahead_strokes:
                        bounded_lookahead_strokes.append(s)
                        if s['type'] == 'down' and get_stroke_low(s) < segment_low:
                            logging.debug(
                                f"边界: 发现向下笔 k_index {s['start']['k_index']} 突破线段低点 {segment_low:.2f}。检查范围确定。")
                            break_point_found = True
                            break
                    if not break_point_found:
                        logging.debug("边界: 未发现突破线段低点的笔，检查范围为所有后续笔。")

                    # 线段结束的第一种情况：特征序列出现底分型
                    if check_overlap(stroke_for_fractal_check, last_cs_stroke):
                        logging.debug("情况 1: 重叠, 检查底分型...")
                        cs_existing_raw = process_inclusion(cs_existing_raw, 'up')
                        processed_cs_existing = process_inclusion(cs_existing_raw, 'down')
                        new_cs_up_raw = [s for s in bounded_lookahead_strokes if s['type'] == 'up']
                        processed_cs_new = process_inclusion(new_cs_up_raw, 'down')
                        final_processed_cs = [processed_cs_existing[
                                                  -1]] + processed_cs_new if processed_cs_existing else processed_cs_new

                        check1_passes, cs_middle, cs_right = check_bottom_fractal(final_processed_cs)
                        if check1_passes:
                            segment_end_stroke = get_segment_end_stroke_from_middle_cs(cs_middle, all_strokes)
                            logging.debug("确认: ** 底分型确认 ** -> 向下线段结束。")
                            is_completed = True
                            trough_stroke = get_extremum_stroke_from_cs(cs_middle)
                            right_stroke = get_extremum_stroke_from_cs(cs_right)
                            break_info = {'reason': 'bottom_fractal', 'next_segment_type': 'up',
                                          'start_stroke': trough_stroke, 'end_stroke': right_stroke,
                                          'segment_end_stroke': segment_end_stroke}
                    else:
                        # 线段结束的第二种情况：特征序列出现底分型，且后续出现一个向下的线段
                        logging.debug("情况 2: 不重叠, 检查 (底分型 AND 后续顶分型)...")
                        processed_cs_existing = process_inclusion(cs_existing_raw, 'down')
                        new_cs_up_raw = [s for s in bounded_lookahead_strokes if s['type'] == 'up']
                        processed_cs_new = process_inclusion(new_cs_up_raw, 'down')
                        cs_for_check1 = [processed_cs_existing[
                                             -1]] + processed_cs_new if processed_cs_existing else processed_cs_new

                        check1_passes, cs_middle_bottom, cs_right_bottom = check_bottom_fractal(cs_for_check1)
                        logging.debug(f"检查1 (底分型): {'通过' if check1_passes else '失败'}")

                        next_segment_cs_raw = [s for s in bounded_lookahead_strokes if s['type'] == 'down']
                        processed_cs2 = process_inclusion(next_segment_cs_raw, 'up')
                        check2_passes, _, _ = check_top_fractal(processed_cs2)
                        logging.debug(f"检查2 (后续顶分型): {'通过' if check2_passes else '失败'}")

                        if check1_passes and check2_passes:
                            segment_end_stroke = get_segment_end_stroke_from_middle_cs(cs_middle_bottom, all_strokes)
                            logging.debug("确认: ** 双重条件满足 ** -> 向下线段结束。")
                            is_completed = True
                            trough_stroke = get_extremum_stroke_from_cs(cs_middle_bottom)
                            right_stroke_for_builder = get_extremum_stroke_from_cs(cs_right_bottom)
                            break_info = {'reason': 'dual_condition_down_break', 'next_segment_type': 'up',
                                          'start_stroke': trough_stroke, 'end_stroke': right_stroke_for_builder,
                                          'segment_end_stroke': segment_end_stroke}

                        else:
                            logging.debug("延伸: 双重条件不满足，线段延伸至检查边界。")
                            current_segment['strokes'].extend(bounded_lookahead_strokes)
                            next_check_idx += len(bounded_lookahead_strokes)
                            continue

            # --- 延伸/结束的最终处理 ---
            if is_completed:
                # 如果线段被确认完成
                final_end_stroke = break_info['segment_end_stroke']
                final_stroke_list_index = all_strokes.index(final_end_stroke)
                # 确定最终构成该线段的所有笔
                final_segment_strokes = all_strokes[current_list_index: final_stroke_list_index + 1]

                # 构建已完成线段的完整信息
                final_segment = {
                    'start_stroke': all_strokes[current_list_index],
                    'end_stroke': final_end_stroke,
                    'type': current_segment['type'],
                    'strokes': final_segment_strokes,
                    'break_info': break_info
                }
                completed_segments.append(final_segment)
                logging.debug(
                    f"完成: {final_segment['type']} 线段完成。结束于笔 k_index:{final_end_stroke['start']['k_index']}。")

                # 【修改】设置 builder 并让主循环处理下一段
                next_segment_builder = break_info
                current_list_index = all_strokes.index(break_info['start_stroke'])
                logging.debug(
                    f"推进: 下一轮将从索引 {current_list_index} (笔 k_index: {break_info['start_stroke']['start']['k_index']}) 开始构建。")
                break  # 退出内层 while 循环

            else:
                # 如果所有特殊判断都未导致线段结束，则线段正常延伸
                logging.debug("延伸: 所有特殊判断均未导致线段结束，继续延伸。")
                current_segment['strokes'].extend([stroke_for_fractal_check, stroke_for_extension_check])
                next_check_idx += 2

        # 如果内层循环是因为 is_completed==True 而中断的，外层循环将继续
        if is_completed:
            continue

        # 如果内层循环是正常结束的（所有笔都检查完了），则处理待定线段并结束外层循环
        if not is_completed:
            pending_segment = {
                'start_stroke': all_strokes[current_list_index],
                'end_stroke': all_strokes[-1],
                'type': current_segment['type'],
                'strokes': current_segment['strokes']
            }
            logging.debug("待定: 所有笔已处理完毕，当前线段成为待定线段。")
            break

    # 在所有已完成笔的线段划分结束后，检查是否可以附加未完成的笔
    if pending_segment and pending_stroke:
        last_completed_stroke_in_segment = pending_segment['strokes'][-1]

        # 【重要修正】从待定笔的起点类型推断其方向
        # 'top' (顶分型) 开始的笔是向下的, 'bottom' (底分型) 开始的笔是向上的
        pending_stroke_direction = None
        if pending_stroke.get('start') and pending_stroke['start'].get('type') == 'top':
            pending_stroke_direction = 'down'
        elif pending_stroke.get('start') and pending_stroke['start'].get('type') == 'bottom':
            pending_stroke_direction = 'up'

        # 只有当待定笔的方向与线段中最后一笔的方向相反时，它才是有效的延续
        if pending_stroke_direction and last_completed_stroke_in_segment['type'] != pending_stroke_direction:
            logging.debug(f"附加操作: 将未完成的笔 k_index:{pending_stroke['start']['k_index']} 附加到待定线段。")
            # 【重要修正】为待定笔添加 'type' 字段以保持数据结构一致性
            pending_stroke['type'] = pending_stroke_direction
            pending_segment['strokes'].append(pending_stroke)
            # 更新待定线段的终点信息为这个未完成的笔
            pending_segment['end_stroke'] = pending_stroke
        else:
            logging.debug("附加操作: 未完成的笔与线段最后一笔同向或方向未知，不附加。")

    logging.info(f"线段划分结束，完成 {len(completed_segments)} 个线段。")
    return {"completed_segments": completed_segments, "pending_segment": pending_segment}
