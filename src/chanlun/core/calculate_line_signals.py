# -*- coding: utf-8 -*-
"""
买卖点和背驰计算模块
负责根据线（笔或线段）与中枢的位置关系，判断买卖点和背驰。
"""
from typing import List, Union, Tuple

from chanlun.core.cl_interface import XD, BI, ZS, LINE, compare_ld_beichi, Config


def _identify_mmd(line: Union[BI, XD], zs: ZS, zs_type: str):
    """识别买卖点"""
    if line.type == 'down':
        if line.low < zs.zd:
            line.add_mmd('3buy', zs, zs_type, '跌破中枢下沿')
        elif line.low > zs.zg:
            line.add_mmd('1buy', zs, zs_type, '回调不破中枢上沿')
        else:
            line.add_mmd('2buy', zs, zs_type, '中枢内部买点')
    elif line.type == 'up':
        if line.high > zs.zg:
            line.add_mmd('3sell', zs, zs_type, '突破中枢上沿')
        elif line.high < zs.zd:
            line.add_mmd('1sell', zs, zs_type, '反弹不破中枢下沿')
        else:
            line.add_mmd('2sell', zs, zs_type, '中枢内部卖点')


def _identify_bc(cl_obj, line: Union[BI, XD], zs: ZS, zs_type: str):
    """识别背驰"""
    if len(zs.lines) < 2: return
    compare_line = next((l for l in reversed(zs.lines[:-1]) if l.type == line.type), None)
    if not compare_line: return

    line_ld = line.get_ld(cl_obj)
    compare_ld = compare_line.get_ld(cl_obj)
    is_bc = compare_ld_beichi(compare_ld, line_ld, line.type)
    if is_bc:
        bc_type = 'bi' if isinstance(line, BI) else 'xd'
        line.add_bc(bc_type, zs, compare_line, [compare_line], True, zs_type)


def calculate_line_signals(cl_obj, lines: List[Union[BI, XD]], zss_map: dict):
    """计算给定线（笔或线段）的买卖点和背驰"""
    if not zss_map: return
    for line in lines:
        for zs_type, zss in zss_map.items():
            if not zss: continue
            relevant_zs = next((zs for zs in reversed(zss) if line.index >= zs.lines[0].index), None)
            if not relevant_zs: continue
            _identify_mmd(line, relevant_zs, zs_type)
            _identify_bc(cl_obj, line, relevant_zs, zs_type)


def beichi_pz(cl_obj, zs: ZS, now_line: LINE) -> Tuple[bool, Union[LINE, None]]:
    """判断盘整背驰"""
    if len(zs.lines) < 2: return False, None
    compare_line = next((l for l in reversed(zs.lines[:-1]) if l.type == now_line.type), None)
    if not compare_line: return False, None
    is_bc = compare_ld_beichi(compare_line.get_ld(cl_obj), now_line.get_ld(cl_obj), now_line.type)
    return is_bc, compare_line


def beichi_qs(cl_obj, lines: List[LINE], zss: List[ZS], now_line: LINE) -> Tuple[bool, List[LINE]]:
    """判断趋势背驰"""
    if len(zss) < 2: return False, []
    qs_direction = zss_is_qs(cl_obj, zss[-2], zss[-1])
    if not qs_direction or qs_direction != now_line.type: return False, []

    compare_lines = [l for l in lines if l.type == now_line.type and l.end.k.k_index <= zss[-2].start.k.k_index]
    if not compare_lines: return False, []

    is_bc = compare_ld_beichi(compare_lines[-1].get_ld(cl_obj), now_line.get_ld(cl_obj), now_line.type)
    return is_bc, [compare_lines[-1]]


def zss_is_qs(cl_obj, one_zs: ZS, two_zs: ZS) -> Union[str, None]:
    """判断两个中枢是否形成趋势"""
    wzgx_config = cl_obj.config.get('zs_wzgx', Config.ZS_WZGX_ZGGDD.value)
    if wzgx_config == Config.ZS_WZGX_ZGD.value:
        if one_zs.zg < two_zs.zd: return 'up'
        if one_zs.zd > two_zs.zg: return 'down'
    elif wzgx_config == Config.ZS_WZGX_ZGGDD.value:
        if one_zs.zg < two_zs.dd: return 'up'
        if one_zs.zd > two_zs.gg: return 'down'
    elif wzgx_config == Config.ZS_WZGX_GD.value:
        if one_zs.gg < two_zs.dd: return 'up'
        if one_zs.dd > two_zs.gg: return 'down'
    return None
