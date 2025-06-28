import math
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd

from chanlun import fun
from chanlun.cl_interface import BI, FX, ICL, LINE, MACD_INFOS, ZS, Config, Kline
from chanlun.db import db
from chanlun.exchange import exchange
from chanlun.file_db import FileCacheDB


def web_batch_get_cl_datas(
    market: str, code: str, klines: Dict[str, pd.DataFrame], cl_config: dict = None
) -> List[ICL]:
    """
    WEB端批量计算并获取 缠论 数据
    内部使用文件缓存，只能进行增量更新，不可用来获取并计算历史k线数据
    :param market: 市场
    :param code: 计算的标的
    :param klines: 计算的 k线 数据，每个周期对应一个 k线DataFrame，例如 ：{'30m': klines_30m, '5m': klines_5m}
    :param cl_config: 缠论配置
    :return: 返回计算好的缠论数据对象，List 列表格式，按照传入的 klines.keys 顺序返回 如上调用：[0] 返回 30m 周期数据 [1] 返回 5m 数据
    """
    cls = []
    fdb = FileCacheDB()
    for f, k in klines.items():
        cls.append(fdb.get_web_cl_data(market, code, f, cl_config, k))
    return cls


def cal_klines_macd_infos(start_k: Kline, end_k: Kline, cd: ICL) -> MACD_INFOS:
    """
    计算线中macd信息
    """
    infos = MACD_INFOS()

    idx = cd.get_idx()
    dea = np.array(idx["macd"]["dea"][start_k.index : end_k.index + 1])
    dif = np.array(idx["macd"]["dif"][start_k.index : end_k.index + 1])
    if len(dea) < 2 or len(dif) < 2:
        return infos
    zero = np.zeros(len(dea))

    infos.dif_up_cross_num = len(up_cross(dif, zero))
    infos.dif_down_cross_num = len(down_cross(dif, zero))
    infos.dea_up_cross_num = len(up_cross(dea, zero))
    infos.dea_down_cross_num = len(down_cross(dea, zero))
    infos.gold_cross_num = len(up_cross(dif, dea))
    infos.die_cross_num = len(down_cross(dif, dea))
    infos.last_dif = dif[-1]
    infos.last_dea = dea[-1]
    return infos


def cal_line_macd_infos(line: LINE, cd: ICL) -> MACD_INFOS:
    """
    计算线中macd信息
    """
    infos = MACD_INFOS()

    idx = cd.get_idx()
    dea = np.array(idx["macd"]["dea"][line.start.k.k_index : line.end.k.k_index + 1])
    dif = np.array(idx["macd"]["dif"][line.start.k.k_index : line.end.k.k_index + 1])
    if len(dea) < 2 or len(dif) < 2:
        return infos
    zero = np.zeros(len(dea))

    infos.dif_up_cross_num = len(up_cross(dif, zero))
    infos.dif_down_cross_num = len(down_cross(dif, zero))
    infos.dea_up_cross_num = len(up_cross(dea, zero))
    infos.dea_down_cross_num = len(down_cross(dea, zero))
    infos.gold_cross_num = len(up_cross(dif, dea))
    infos.die_cross_num = len(down_cross(dif, dea))
    infos.last_dif = dif[-1]
    infos.last_dea = dea[-1]
    return infos


def cal_macd_bis_is_bc(bis: List[BI], cd: ICL) -> Tuple[bool, bool]:
    """
    给定一组笔列表，判断其 macd 是否出现背驰，柱子高度变小，黄白线也缩小

    条件：
        1. 结束笔是向下，则需要结束笔的低点是给定笔的最低点
        2. 结束笔是向上，则需要结束笔的高点是给定笔的最高点

    只获取最后两块 红绿柱子的情况进行对比

    返回：
        bool 第一个值是 红绿柱子是否背驰
        bool 第二个值是 黄白线是否背驰
    """
    if len(bis) < 3:
        return False, False

    # 最后一笔不是最高或最低
    direction = bis[-1].type
    if direction == "up":
        bi_max_val = max([_bi.high for _bi in bis])
        if bi_max_val != bis[-1].high:
            return False, False
    else:
        bi_min_val = min([_bi.low for _bi in bis])
        if bi_min_val != bis[-1].low:
            return False, False

    macd_idx = cd.get_idx()["macd"]
    # 如果最后一笔内部没有找到 红绿柱子，则直接返回 True
    if direction == "up":
        macd_up_hist_max_val = max(
            macd_idx["hist"][bis[-1].start.k.k_index : bis[-1].end.k.k_index + 1]
        )
        if macd_up_hist_max_val <= 0:
            return True, True
    elif direction == "down":
        macd_down_hist_min_val = min(
            macd_idx["hist"][bis[-1].start.k.k_index : bis[-1].end.k.k_index + 1]
        )
        if macd_down_hist_min_val >= 0:
            return True, True

    # 黄白线在给定的笔区间内部，至少有一次穿越零轴
    start_k = cd.get_klines()[bis[0].start.k.k_index]
    end_k = cd.get_klines()[bis[-1].end.k.k_index]
    bis_macd_infos = cal_klines_macd_infos(start_k, end_k, cd)
    if direction == "up" and (
        bis_macd_infos.dea_up_cross_num == 0 or bis_macd_infos.dif_up_cross_num == 0
    ):
        return False, False
    if direction == "down" and (
        bis_macd_infos.dif_down_cross_num == 0 or bis_macd_infos.dif_down_cross_num == 0
    ):
        return False, False

    def get_macd_dump_info(start_fx: FX, end_fx: FX):
        # 获取给定区间内，hist dif dea 最大值，hist 出现的驼峰值列表
        start_k_index = start_fx.klines[0].k_index
        end_k_index = (
            end_fx.klines[-1].k_index
            if end_fx.klines[-1] is not None
            else end_fx.klines[1].k_index
        )
        # 根据红绿柱子的边界获取
        while True:
            if direction == "up":
                if start_k_index > 0 and macd_idx["hist"][start_k_index] > 0:
                    start_k_index -= 1
                else:
                    break
            if direction == "down":
                if start_k_index > 0 and macd_idx["hist"][start_k_index] < 0:
                    start_k_index -= 1
                else:
                    break
        while True:
            if direction == "up":
                if (
                    end_k_index < len(macd_idx["hist"])
                    and macd_idx["hist"][end_k_index] > 0
                ):
                    end_k_index += 1
                else:
                    break
            if direction == "down":
                if (
                    end_k_index < len(macd_idx["hist"])
                    and macd_idx["hist"][end_k_index] < 0
                ):
                    end_k_index += 1
                else:
                    break

        macd_hists = macd_idx["hist"][start_k_index : end_k_index + 1]
        macd_difs = macd_idx["dif"][start_k_index : end_k_index + 1]
        macd_deas = macd_idx["dea"][start_k_index : end_k_index + 1]

        max_hist = max_dif = max_dea = 0
        hist_dumps = []
        _temp_dumps = []
        for i in range(len(macd_hists)):
            _hist = macd_hists[i]
            _dif = macd_difs[i]
            _dea = macd_deas[i]
            if direction == "up" and _hist > 0:
                _temp_dumps.append(_hist)
                max_hist = max(max_hist, _hist)
            if direction == "up" and _dif > 0:
                max_dif = max(max_dif, _dif)
            if direction == "up" and _dea > 0:
                max_dea = max(max_dea, _dea)
            if direction == "down" and _hist < 0:
                _temp_dumps.append(abs(_hist))
                max_hist = max(max_hist, abs(_hist))
            if direction == "down" and _dif < 0:
                max_dif = max(max_dif, abs(_dif))
            if direction == "down" and _dea < 0:
                max_dea = max(max_dea, abs(_dea))
            if (direction == "up" and _hist < 0) or (direction == "down" and _hist > 0):
                if len(_temp_dumps) > 0:
                    hist_dumps.append(_temp_dumps)
                    _temp_dumps = []
        if len(_temp_dumps) > 0:
            hist_dumps.append(_temp_dumps)

        return max_hist, max_dif, max_dea, hist_dumps

    # 计算最后一笔的 macd 信息
    (
        last_bi_max_hist,
        last_bi_max_dif,
        last_bi_max_dea,
        last_bi_hist_dumps,
    ) = get_macd_dump_info(bis[-1].start, bis[-1].end)
    last_bi_sum_hist = sum([sum(_hists) for _hists in last_bi_hist_dumps])
    # print(
    #     f'最后一笔macd 信息： max_hist {last_bi_max_hist} max_dif {last_bi_max_dif} max_dea {last_bi_max_dea} sum_hist {last_bi_sum_hist}')
    # 根据中枢数量，来获取要比较的部分
    zss = cd.create_dn_zs("bi", bis)
    if len(zss) == 0:
        # 没有中枢，就用上一笔
        compare_start_fx = bis[-3].start
        compare_end_fx = bis[-3].end
    else:
        # 有中枢，根据最后一个中枢获取，这里也分两种情况，中枢最后一笔是否为给定笔的最后一笔
        if zss[-1].lines[-1].index == bis[-1].index:
            # 是中枢最后一笔
            compare_start_fx = zss[-1].lines[0].start
            compare_end_fx = zss[-1].lines[-2].end
        else:
            # 不是中枢最后一笔
            compare_start_fx = zss[-1].lines[0].start
            compare_end_fx = zss[-1].lines[-1].end

    (
        compare_max_hist,
        compare_max_dif,
        compare_max_dea,
        compare_hist_dumps,
    ) = get_macd_dump_info(compare_start_fx, compare_end_fx)
    compare_max_sum_hist = max([sum(_hists) for _hists in compare_hist_dumps])
    # print(
    #     f'要比较的macd信息： max_hist {compare_max_hist} max_dif {compare_max_dif} max_dea {compare_max_dea} sum_hist {compare_max_sum_hist}')

    hist_bc = False
    deadif_bc = False
    if last_bi_max_hist < compare_max_hist and last_bi_sum_hist < compare_max_sum_hist:
        hist_bc = True
    if last_bi_max_dif < compare_max_dif and last_bi_max_dea < compare_max_dea:
        deadif_bc = True

    return hist_bc, deadif_bc


def cal_zs_macd_infos(zs: ZS, cd: ICL) -> MACD_INFOS:
    """
    计算中枢的macd信息
    """
    infos = MACD_INFOS()
    dea = np.array(
        cd.get_idx()["macd"]["dea"][zs.start.k.k_index : zs.end.k.k_index + 1]
    )
    dif = np.array(
        cd.get_idx()["macd"]["dif"][zs.start.k.k_index : zs.end.k.k_index + 1]
    )
    if len(dea) < 2 or len(dif) < 2:
        return infos
    zero = np.zeros(len(dea))

    infos.dif_up_cross_num = len(up_cross(dif, zero))
    infos.dif_down_cross_num = len(down_cross(dif, zero))
    infos.dea_up_cross_num = len(up_cross(dea, zero))
    infos.dea_down_cross_num = len(down_cross(dea, zero))
    infos.gold_cross_num = len(up_cross(dif, dea))
    infos.die_cross_num = len(down_cross(dif, dea))
    infos.last_dif = dif[-1]
    infos.last_dea = dea[-1]
    return infos


def query_cl_chart_config(
    market: str, code: str, suffix: str = ""
) -> Dict[str, object]:
    """
    查询指定市场和标的下的缠论和画图配置
    """
    # 如果是期货，代码进行特殊处理，只保留核心的交易所和品种信息，其他的去掉
    if market == "futures":
        code = code.upper().replace("KQ.M@", "")
        code = "".join([i for i in code if not i.isdigit()])

    config: dict = db.cache_get(f"cl_config_{market}_{code}{suffix}")
    if config is None:
        config: dict = db.cache_get(f"cl_config_{market}_common{suffix}")
    # 默认配置设置，用于在前台展示设置值
    default_config = {
        "config_use_type": "common",
        # 个人定制配置
        "kline_qk": Config.KLINE_QK_NONE.value,
        "judge_zs_qs_level": "1",
        # K线配置
        "kline_type": Config.KLINE_TYPE_DEFAULT.value,
        # 分型配置
        "fx_qy": Config.FX_QY_THREE.value,
        "fx_qj": Config.FX_QJ_K.value,
        "fx_bh": Config.FX_BH_YES.value,
        # 笔配置
        "bi_type": Config.BI_TYPE_OLD.value,
        "bi_bzh": Config.BI_BZH_YES.value,
        "bi_qj": Config.BI_QJ_DD.value,
        "bi_fx_cgd": Config.BI_FX_CHD_YES.value,
        "bi_split_k_cross_nums": "20,1",
        "fx_check_k_nums": 13,
        "allow_bi_fx_strict": "0",
        # 线段配置
        "xd_qj": Config.XD_QJ_DD.value,
        "zsd_qj": Config.ZSD_QJ_DD.value,
        "xd_zs_max_lines_split": 11,
        "xd_allow_bi_pohuai": Config.XD_BI_POHUAI_YES.value,
        "xd_allow_split_no_highlow": "1",
        "xd_allow_split_zs_kz": "0",
        "xd_allow_split_zs_more_line": "1",
        "xd_allow_split_zs_no_direction": "1",
        # 中枢配置
        "zs_bi_type": [Config.ZS_TYPE_BZ.value],
        "zs_xd_type": [Config.ZS_TYPE_BZ.value],
        "zs_qj": Config.ZS_QJ_DD.value,
        "zs_cd": Config.ZS_CD_THREE.value,
        "zs_wzgx": Config.ZS_WZGX_GD.value,
        "zs_optimize": "0",
        # MACD 配置（计算力度背驰）
        "idx_macd_fast": 12,
        "idx_macd_slow": 26,
        "idx_macd_signal": 9,
        # 买卖点配置
        # 两中枢及以上趋势背驰，产生一类买卖点
        "cl_mmd_cal_qs_1mmd": "1",
        # 非趋势，产生三类买卖点，后续创新高/新低且背驰，产生一类买卖点
        "cl_mmd_cal_not_qs_3mmd_1mmd": "1",
        # 趋势，产生三类买卖点，后续创新高/新低且背驰，产生一类买卖点
        "cl_mmd_cal_qs_3mmd_1mmd": "1",
        # 趋势，不创新高/新低，产生二类买卖点
        "cl_mmd_cal_qs_not_lh_2mmd": "1",
        # 趋势，新高/新低后，下一段与新高/新低段比较背驰后，产生二类买卖点
        "cl_mmd_cal_qs_bc_2mmd": "1",
        # 趋势，三类买卖点后，后续段不创新高/新低，或者有背驰，产生二类买卖点
        "cl_mmd_cal_3mmd_not_lh_bc_2mmd": "1",
        # 之前有一类买卖点，后续不创新高/新低，产生二类买卖点
        "cl_mmd_cal_1mmd_not_lh_2mmd": "1",
        # 三类买卖点后创新高/新低且不背驰，后续段不创新高/新低且背驰，产生二类买卖点
        "cl_mmd_cal_3mmd_xgxd_not_bc_2mmd": "1",
        # 回调不进入中枢的，产生三类买卖点
        "cl_mmd_cal_not_in_zs_3mmd": "1",
        # 回调不进入中枢的(中枢大于等于9段)，产生三类买卖点
        "cl_mmd_cal_not_in_zs_gt_9_3mmd": "1",
        # 缠论高级配置
        "enable_kchart_low_to_high": "0",
        # 画图默认配置
        "chart_show_infos": "0",
        "chart_show_fx": "0",
        "chart_show_bi": "1",
        "chart_show_xd": "1",
        "chart_show_zsd": "1",
        "chart_show_qsd": "0",
        "chart_show_bi_zs": "1",
        "chart_show_xd_zs": "1",
        "chart_show_zsd_zs": "0",
        "chart_show_qsd_zs": "0",
        "chart_show_bi_mmd": "1",
        "chart_show_xd_mmd": "1",
        "chart_show_zsd_mmd": "1",
        "chart_show_qsd_mmd": "1",
        "chart_show_bi_bc": "1",
        "chart_show_xd_bc": "1",
        "chart_show_zsd_bc": "1",
        "chart_show_qsd_bc": "1",
        "chart_show_ma": "0",
        "chart_show_boll": "0",
        "chart_show_futu": "macd",
        "chart_show_atr_stop_loss": False,
        "chart_show_ld": "xd",
        "chart_kline_nums": 500,
        "chart_idx_ma_period": "5,34",
        "chart_idx_vol_ma_period": "5,60",
        "chart_idx_boll_period": 20,
        "chart_idx_rsi_period": 14,
        "chart_idx_atr_period": 14,
        "chart_idx_atr_multiplier": 1.5,
        "chart_idx_cci_period": 14,
        "chart_idx_kdj_period": "9,3,3",
        "chart_qstd": "xd,0",
    }

    if config is None:
        return default_config
    for _key, _val in default_config.items():
        if _key not in config.keys():
            config[_key] = _val

    return config


def set_cl_chart_config(
    market: str, code: str, config: Dict[str, object], suffix: str = ""
) -> bool:
    """
    设置指定市场和标的下的缠论和画图配置
    """
    # 如果是期货，代码进行特殊处理，只保留核心的交易所和品种信息，其他的去掉
    if market == "futures":
        code = code.upper().replace("KQ.M@", "")
        code = "".join([i for i in code if not i.isdigit()])

    # 读取原来的配置，使用新的配置项进行覆盖，并保存
    old_config = query_cl_chart_config(market, code, suffix)
    if config["config_use_type"] == "custom" and code is None:
        return False
    elif config["config_use_type"] == "common":
        db.cache_del(f"cl_config_{market}_{code}{suffix}")

    for new_key, new_val in config.items():
        if new_key in old_config.keys():
            old_config[new_key] = new_val
        else:
            old_config[new_key] = new_val

    db.cache_set(
        f"cl_config_{market}_{code if config['config_use_type'] == 'custom' else 'common'}{suffix}",
        old_config,
    )
    return True


def del_cl_chart_config(market: str, code: str, suffix: str = "") -> bool:
    """
    删除指定市场标的的独立配置项
    """
    # 如果是期货，代码进行特殊处理，只保留核心的交易所和品种信息，其他的去掉
    if market == "futures":
        code = code.upper().replace("KQ.M@", "")
        code = "".join([i for i in code if not i.isdigit()])

    db.cache_del(f"cl_config_{market}_{code}{suffix}")
    return True


def kcharts_frequency_h_l_map(
    market: str, frequency
) -> Tuple[Union[None, str], Union[None, str]]:
    """
    将原周期，转换为新的周期进行图表展示
    按照设置好的对应关系进行返回

    返回两个值，第一个是需要获取的低级别周期值，第二个是 kcharts 画图指定的 to_frequency 值
    """
    # 高级别对应的低级别关系
    market_frequencs_map = {
        "a": {
            "m": "w",
            "w": "d",
            "d": "30m",
            "120m": "15m",
            "60m": "15m",
            "30m": "5m",
            "15m": "5m",
            "5m": "1m",
        },
        "futures": {
            "w": "d",
            "d": "60m",
            "60m": "10m",
            "30m": "5m",
            "15m": "3m",
            "10m": "2m",
            "6m": "1m",
            "5m": "1m",
            "3m": "1m",
        },
        # TODO 港美股没有写周期转换的方法，先不支持呢
        # 'us': {
        #     'y': 'q', 'q': 'm', 'm': 'w', 'w': 'd', 'd': '60m', '120m': '15m',
        #     '60m': '15m', '30m': '5m', '15m': '5m', '5m': '1m',
        # },
        # 'hk': {
        #     'y': 'm', 'm': 'w', 'w': 'd', 'd': '60m', '120m': '15m', '60m': '15m',
        #     '30m': '5m', '15m': '5m', '10m': '1m', '5m': '1m',
        # },
        "currency": {
            "w": "d",
            "d": "4h",
            "4h": "30m",
            "60m": "15m",
            "30m": "5m",
            "15m": "5m",
            "10m": "2m",
            "5m": "1m",
            "3m": "1m",
        },
    }

    try:
        return market_frequencs_map[market][frequency], f"{market}:{frequency}"
    except Exception:
        return None, None


def cl_qstd(cd: ICL, line_type="xd", line_num: int = 5):
    """
    缠论线段的趋势通道
    基于已完成的最后 n 条线段，线段最高两个点，线段最低两个点连线，作为趋势通道线指导交易（不一定精确）
    """
    lines = cd.get_xds() if line_type == "xd" else cd.get_bis()
    qs_lines = []
    for i in range(1, len(lines)):
        xd = lines[-i]
        if xd.is_done():
            qs_lines.append(xd)
        if len(qs_lines) == line_num:
            break

    if len(qs_lines) != line_num:
        return None

    # 获取线段的高低点并排序
    line_highs = [
        {"val": l.high, "index": l.end.k.k_index, "date": l.end.k.date}
        for l in qs_lines
        if l.type == "up"
    ]
    line_lows = [
        {"val": l.low, "index": l.end.k.k_index, "date": l.end.k.date}
        for l in qs_lines
        if l.type == "down"
    ]
    if len(line_highs) < 2 or len(line_lows) < 2:
        return None
    line_highs = sorted(line_highs, key=lambda v: v["val"], reverse=True)
    line_lows = sorted(line_lows, key=lambda v: v["val"], reverse=False)

    def xl(one, two):
        # 计算斜率
        k = (one["val"] - two["val"]) / (one["index"] - two["index"])
        return k

    qstd = {
        "up": {
            "one": line_highs[0],
            "two": line_highs[1],
            "xl": xl(line_highs[0], line_highs[1]),
        },
        "down": {
            "one": line_lows[0],
            "two": line_lows[1],
            "xl": xl(line_lows[0], line_lows[1]),
        },
    }
    # 图标上展示的坐标设置
    chart_up_start = {
        "val": line_highs[0]["val"]
        - qstd["up"]["xl"] * (line_highs[0]["index"] - qs_lines[-1].start.k.k_index),
        "index": qs_lines[-1].start.k.k_index,
        "date": qs_lines[-1].start.k.date,
    }
    chart_up_end = {
        "val": line_highs[0]["val"]
        - qstd["up"]["xl"] * (line_highs[0]["index"] - cd.get_klines()[-1].index),
        "index": cd.get_klines()[-1].index,
        "date": cd.get_klines()[-1].date,
    }
    chart_down_start = {
        "val": line_lows[0]["val"]
        - qstd["down"]["xl"] * (line_lows[0]["index"] - qs_lines[-1].start.k.k_index),
        "index": qs_lines[-1].start.k.k_index,
        "date": qs_lines[-1].start.k.date,
    }
    chart_down_end = {
        "val": line_lows[0]["val"]
        - qstd["down"]["xl"] * (line_lows[0]["index"] - cd.get_klines()[-1].index),
        "index": cd.get_klines()[-1].index,
        "date": cd.get_klines()[-1].date,
    }
    qstd["up"]["chart"] = {
        "x": [chart_up_start["date"], chart_up_end["date"]],
        "y": [chart_up_start["val"], chart_up_end["val"]],
        "index": [chart_up_start["index"], chart_up_end["index"]],
    }
    qstd["down"]["chart"] = {
        "x": [chart_down_start["date"], chart_down_end["date"]],
        "y": [chart_down_start["val"], chart_down_end["val"]],
        "index": [chart_down_start["index"], chart_down_start["index"]],
    }

    # 计算当前价格和趋势线的位置关系
    now_point = {
        "val": cd.get_klines()[-1].c,
        "index": cd.get_klines()[-1].index,
        "date": cd.get_klines()[-1].date,
    }
    qstd["up"]["now"] = (
        "up" if xl(chart_up_start, now_point) > qstd["up"]["xl"] else "down"
    )
    qstd["down"]["now"] = (
        "up" if xl(chart_down_start, now_point) > qstd["down"]["xl"] else "down"
    )

    return qstd


def prices_jiaodu(prices):
    """
    技术价格序列中，起始与终点的角度（正为上，负为下）

    弧度 = dy / dx
        dy = 终点与起点的差值
        dx = 固定位 100000
        dy 如果不足六位数，进行补位
    不同品种的标的价格有差异，这时计算的角度会有很大的不同，不利于量化，将 dy 固定，变相的将所有标的放在一个尺度进行对比
    """
    if prices[-1] == prices[0]:
        return 0
    dy = max(prices[-1], prices[0]) - min(prices[-1], prices[0])
    dx = 100000
    while True:
        dy_len = len(str(int(dy)))
        if dy_len < 6:
            dy = dy * (10 ** (6 - dy_len))
        elif dy_len > 6:
            dy = dy / (10 ** (dy_len - 6))
        else:
            break
    # 弧度
    k = math.atan2(dy, dx)
    # 弧度转角度
    j = math.degrees(k)
    return j if prices[-1] > prices[0] else -j


def cl_data_to_tv_chart(
    cd: ICL, config: dict, to_frequency: str = None
) -> Union[dict, None]:
    """
    将缠论数据，转换成 tv 画图的坐标数据
    """
    # K线
    klines = [
        {
            "date": k.date,
            "high": k.h,
            "low": k.l,
            "open": k.o,
            "close": k.c,
            "volume": k.a,
        }
        for k in cd.get_klines()
    ]
    klines = pd.DataFrame(klines)
    if len(klines) == 0:
        return None
    klines.loc[:, "code"] = cd.get_code()
    if to_frequency is not None:
        # 将数据转换成指定的周期数据
        market = to_frequency.split(":")[0]
        frequency = to_frequency.split(":")[1]
        if market == "a":
            klines = exchange.convert_stock_kline_frequency(klines, frequency)
        elif market == "futures":
            klines = exchange.convert_futures_kline_frequency(klines, frequency)
        elif market == "currency":
            klines = exchange.convert_currency_kline_frequency(klines, frequency)
        else:
            raise Exception(f"图表周期数据转换，不支持的市场 {market}")

    # K 线数据
    kline_ts = klines["date"].map(fun.datetime_to_int).tolist()
    kline_cs = klines["close"].tolist()
    kline_os = klines["open"].tolist()
    kline_hs = klines["high"].tolist()
    kline_ls = klines["low"].tolist()
    kline_vs = klines["volume"].tolist()

    fx_data = []
    if config["chart_show_fx"] == "1":
        for fx in cd.get_fxs():
            fx_data.append(
                {
                    "points": [
                        {"time": fun.datetime_to_int(fx.k.date), "price": fx.val},
                        {"time": fun.datetime_to_int(fx.k.date), "price": fx.val},
                    ],
                    "text": fx.type,
                }
            )

    bi_chart_data = []
    if config["chart_show_bi"] == "1":
        for bi in cd.get_bis():
            bi_chart_data.append(
                {
                    "points": [
                        {
                            "time": fun.datetime_to_int(bi.start.k.date),
                            "price": bi.start.val,
                        },
                        {
                            "time": fun.datetime_to_int(bi.end.k.date),
                            "price": bi.end.val,
                        },
                    ],
                    "linestyle": "0" if bi.is_done() else "1",
                }
            )

    xd_chart_data = []
    if config["chart_show_xd"] == "1":
        for xd in cd.get_xds():
            xd_chart_data.append(
                {
                    "points": [
                        {
                            "time": fun.datetime_to_int(xd.start.k.date),
                            "price": xd.start.val,
                        },
                        {
                            "time": fun.datetime_to_int(xd.end.k.date),
                            "price": xd.end.val,
                        },
                    ],
                    "linestyle": "0" if xd.is_done() else "1",
                }
            )

    zsd_chart_data = []
    if config["chart_show_zsd"] == "1":
        for zsd in cd.get_zsds():
            zsd_chart_data.append(
                {
                    "points": [
                        {
                            "time": fun.datetime_to_int(zsd.start.k.date),
                            "price": zsd.start.val,
                        },
                        {
                            "time": fun.datetime_to_int(zsd.end.k.date),
                            "price": zsd.end.val,
                        },
                    ],
                    "linestyle": "0" if zsd.is_done() else "1",
                }
            )

    bi_zs_chart_data = []
    if config["chart_show_bi_zs"] == "1":
        for zs_type in config["zs_bi_type"]:
            for zs in cd.get_bi_zss(zs_type):
                bi_zs_chart_data.append(
                    {
                        "points": [
                            {
                                "time": fun.datetime_to_int(zs.start.k.date),
                                "price": zs.zg,
                            },
                            {
                                "time": fun.datetime_to_int(zs.end.k.date),
                                "price": zs.zd,
                            },
                        ],
                        "linestyle": "0" if zs.done else "1",
                    }
                )

    xd_zs_chart_data = []
    if config["chart_show_xd_zs"] == "1":
        for zs_type in config["zs_xd_type"]:
            for zs in cd.get_xd_zss(zs_type):
                xd_zs_chart_data.append(
                    {
                        "points": [
                            {
                                "time": fun.datetime_to_int(zs.start.k.date),
                                "price": zs.zg,
                            },
                            {
                                "time": fun.datetime_to_int(zs.end.k.date),
                                "price": zs.zd,
                            },
                        ],
                        "linestyle": "0" if zs.done else "1",
                    }
                )

    zsd_zs_chart_data = []
    if config["chart_show_zsd_zs"] == "1":
        for zs in cd.get_zsd_zss():
            zsd_zs_chart_data.append(
                {
                    "points": [
                        {"time": fun.datetime_to_int(zs.start.k.date), "price": zs.zg},
                        {"time": fun.datetime_to_int(zs.end.k.date), "price": zs.zd},
                    ],
                    "linestyle": "0" if zs.done else "1",
                }
            )

    # 背驰信息
    bc_infos = {}
    # 买卖点信息
    mmd_infos = {}

    lines = {
        "bi": cd.get_bis(),
        "xd": cd.get_xds(),
        "zsd": cd.get_zsds(),
    }
    line_type_map = {"bi": "笔", "xd": "段", "zsd": "走", "qsd": "趋"}
    bc_type_map = {
        "bi": "BI",
        "xd": "XD",
        "zsd": "ZSD",
        "qsd": "QSD",
        "pz": "PZ",
        "qs": "QS",
    }
    mmd_type_map = {
        "1buy": "1B",
        "2buy": "2B",
        "l2buy": "L2B",
        "3buy": "3B",
        "l3buy": "L3B",
        "1sell": "1S",
        "2sell": "2S",
        "l2sell": "L2S",
        "3sell": "3S",
        "l3sell": "L3S",
    }
    for line_type, ls in lines.items():
        for l in ls:
            bcs = l.line_bcs("|")
            if len(bcs) != 0 and l.end.k.date not in bc_infos.keys():
                bc_infos[l.end.k.date] = {
                    "price": l.end.val,
                    "bc_infos": {_type: [] for _type in line_type_map.keys()},
                }
            if config[f"chart_show_{line_type}_bc"] == "1":
                for bc in bcs:
                    bc_infos[l.end.k.date]["bc_infos"][line_type].append(
                        bc_type_map[bc]
                    )

            mmds = l.line_mmds("|")
            if len(mmds) != 0 and l.end.k.date not in mmd_infos.keys():
                mmd_infos[l.end.k.date] = {
                    "price": l.end.val,
                    "mmd_infos": {_type: [] for _type in line_type_map.keys()},
                }
            if config[f"chart_show_{line_type}_mmd"] == "1":
                for mmd in mmds:
                    mmd_infos[l.end.k.date]["mmd_infos"][line_type].append(
                        mmd_type_map[mmd]
                    )

    bc_chart_data = []
    for dt, bc in bc_infos.items():
        bc_text = "/".join(
            [
                f"{line_type_map[_type]}:{','.join(list(set(_bcs)))}"
                for _type, _bcs in bc["bc_infos"].items()
                if len(_bcs) > 0
            ]
        )
        if len(bc_text) > 0:
            bc_chart_data.append(
                {
                    "points": {"time": fun.datetime_to_int(dt), "price": bc["price"]},
                    "text": bc_text,
                }
            )

    mmd_chart_data = []
    for dt, mmd in mmd_infos.items():
        mmd_text = "/".join(
            [
                f"{line_type_map[_type]}:{','.join(list(set(_mmds)))}"
                for _type, _mmds in mmd["mmd_infos"].items()
                if len(_mmds) > 0
            ]
        )
        if len(mmd_text) > 0:
            mmd_chart_data.append(
                {
                    "points": {"time": fun.datetime_to_int(dt), "price": mmd["price"]},
                    "text": mmd_text,
                }
            )

    fx_data.sort(key=lambda v: v["points"][0]["time"], reverse=False)
    bi_chart_data.sort(key=lambda v: v["points"][0]["time"], reverse=False)
    xd_chart_data.sort(key=lambda v: v["points"][0]["time"], reverse=False)
    zsd_chart_data.sort(key=lambda v: v["points"][0]["time"], reverse=False)
    bi_zs_chart_data.sort(key=lambda v: v["points"][0]["time"], reverse=False)
    xd_zs_chart_data.sort(key=lambda v: v["points"][0]["time"], reverse=False)
    zsd_zs_chart_data.sort(key=lambda v: v["points"][0]["time"], reverse=False)
    bc_chart_data.sort(key=lambda v: v["points"]["time"], reverse=False)
    mmd_chart_data.sort(key=lambda v: v["points"]["time"], reverse=False)

    return {
        "t": kline_ts,
        "c": kline_cs,
        "o": kline_os,
        "h": kline_hs,
        "l": kline_ls,
        "v": kline_vs,
        "fxs": fx_data,
        "bis": bi_chart_data,
        "xds": xd_chart_data,
        "zsds": zsd_chart_data,
        "bi_zss": bi_zs_chart_data,
        "xd_zss": xd_zs_chart_data,
        "zsd_zss": zsd_zs_chart_data,
        "bcs": bc_chart_data,
        "mmds": mmd_chart_data,
    }


def bi_td(bi: BI, cd: ICL):
    """
    判断是否笔停顿
    """
    if bi.is_done() is False:
        return False
    next_ks = cd.get_klines()[bi.end.klines[-1].k_index + 1 :]
    if len(next_ks) == 0:
        return False
    for _nk in next_ks:
        if bi.type == "up" and _nk.c < _nk.o and _nk.c < bi.end.klines[-1].l:
            return True
        elif bi.type == "down" and _nk.c > _nk.o and _nk.c > bi.end.klines[-1].h:
            return True

    return False


def up_cross(one_list: np.array, two_list: np.array):
    """
    获取上穿信号列表
    """
    assert len(one_list) == len(two_list), "信号输入维度不相等"
    if len(one_list) < 2:
        return []
    cross = []
    for i in range(1, len(two_list)):
        if one_list[i - 1] < two_list[i - 1] and one_list[i] > two_list[i]:
            cross.append(i)
    return cross


def down_cross(one_list: np.array, two_list: np.array):
    """
    获取下穿信号列表
    """
    assert len(one_list) == len(two_list), "信号输入维度不相等"
    if len(one_list) < 2:
        return []
    cross = []
    for i in range(1, len(two_list)):
        if one_list[i - 1] > two_list[i - 1] and one_list[i] < two_list[i]:
            cross.append(i)
    return cross


def last_done_bi(cd: ICL):
    """
    获取最后一个 完成笔
    """
    bis = cd.get_bis()
    if len(bis) == 0:
        return None
    for bi in bis[::-1]:
        if bi.is_done():
            return bi
    return None


def bi_qk_num(cd: ICL, bi: BI) -> Tuple[int, int]:
    """
    获取笔的缺口数量（分别是向上跳空，向下跳空数量）
    """
    up_qk_num = 0
    down_qk_num = 0
    _ks = cd.get_src_klines()[bi.start.k.k_index : bi.end.k.k_index + 1]
    for i in range(1, len(_ks)):
        pre_k = _ks[i - 1]
        now_k = _ks[i]
        if now_k.l > pre_k.h:
            up_qk_num += 1
        elif now_k.h < pre_k.l:
            down_qk_num += 1
    return up_qk_num, down_qk_num


def klines_to_heikin_ashi_klines(ks: pd.DataFrame) -> pd.DataFrame:
    """
    将缠论数据的普通K线，转换成平均K线数据，返回格式 pd.DataFrame
    """
    # s_time = time.time()
    cd_klines = ks.to_dict(orient="records")
    # print(f"转换成列表数据格式耗时: {time.time() - s_time:.2f}s")

    # s_time = time.time()
    mean_klines: list = []
    for i in range(len(cd_klines)):
        if i == 0:
            mean_klines.append(cd_klines[i])
            continue
        mk = mean_klines[i - 1]
        nk = cd_klines[i]
        # 开盘价 =（前一根烛台的开盘价+ 前一根烛台的收盘价）/2
        # 收盘价 =（当前烛台的开盘价 + 最高价 + 最低价 + 收盘价）/4
        # 最大值（或最高价）= 当前周期的最高价、当前周期的平均 K 线图开盘价或收盘价中的最大值。
        # 最小值（或最低价）= 当前周期的最低价、当前周期的平均 K 线图开盘价或收盘价中的最小值
        _open = (mk["open"] + mk["close"]) / 2
        _close = (nk["open"] + nk["high"] + nk["low"] + nk["close"]) / 4
        _high = max(nk["high"], _open, _close)
        _low = min(nk["low"], _open, _close)
        _volume = nk["volume"]
        mean_klines.append(
            {
                "code": nk["code"],
                "date": nk["date"],
                "high": _high,
                "open": _open,
                "low": _low,
                "close": _close,
                "volume": _volume,
            }
        )
    # print(f"转换成平均K线数据格式耗时: {time.time() - s_time:.2f}s")

    # s_time = time.time()
    df = pd.DataFrame(mean_klines)
    # print(f"转换成 pd.DataFrame 数据格式耗时: {time.time() - s_time:.2f}s")

    return df


if __name__ == "__main__":
    cl_config = query_cl_chart_config("a", "SH.000001")
    print(cl_config)
