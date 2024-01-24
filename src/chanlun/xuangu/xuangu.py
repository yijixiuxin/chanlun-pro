import itertools
import talib
from chanlun.cl_utils import *
from chanlun.cl_interface import Config

"""
根据缠论数据，选择自己所需要的形态方法集合
"""

direction_types = {"long": ["down"], "short": ["up"]}
mmd_types = {
    "long": ["1buy", "2buy", "3buy", "l2buy", "l3buy"],
    "short": ["1sell", "2sell", "3sell", "l2sell", "l3sell"],
}


def get_opt_types(opt_type: list = []):
    if len(opt_type) == 0:
        opt_type = ["long"]
    opt_direction = list(
        itertools.chain.from_iterable([direction_types[x] for x in opt_type])
    )
    opt_mmd = list(itertools.chain.from_iterable([mmd_types[x] for x in opt_type]))
    return opt_direction, opt_mmd


def xg_single_xd_and_bi_mmd(cl_datas: List[ICL], opt_type: list = []):
    """
    线段和笔都有出现买点
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)

    cd = cl_datas[0]
    if len(cd.get_xds()) == 0 or len(cd.get_bis()) == 0:
        return None
    xd = cd.get_xds()[-1]
    bi = cd.get_bis()[-1]

    if xd.mmd_exists(opt_mmd, "|") and bi.mmd_exists(opt_mmd, "|"):
        return {
            "code": cd.get_code(),
            "msg": f"线段买点 【{xd.line_mmds('|')}】 笔买点【{bi.line_mmds('|')}】",
        }
    return None


def xg_multiple_xd_bi_mmd(cl_datas: List[ICL], opt_type: list = []):
    """
    选择 高级别线段，低级别笔 都出现买点，或者 高级别线段和高级别笔 都出现 背驰 的条件
    高级别线段买点或背驰，并且次级别笔买点或背驰
    周期：两个周期
    适用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)

    high_data = cl_datas[0]
    low_data = cl_datas[1]
    if len(high_data.get_xds()) == 0 or len(high_data.get_bis()) == 0:
        return None
    if len(low_data.get_xds()) == 0 or len(low_data.get_bis()) == 0:
        return None

    high_xd = high_data.get_xds()[-1]
    low_bi = low_data.get_bis()[-1]
    if high_xd.type not in opt_direction or low_bi.type not in opt_direction:
        return None
    if (high_xd.mmd_exists(opt_mmd, "|") or high_xd.bc_exists(["pz", "qs"], "|")) and (
        low_bi.mmd_exists(opt_mmd, "|") or low_bi.bc_exists(["pz", "qs"], "|")
    ):
        return {
            "code": high_data.get_code,
            "msg": f"{high_data.get_frequency()} 线段买点【{high_xd.line_mmds('|')}】背驰【{high_xd.line_bcs('|')}】 {low_data.get_frequency()} 笔买点【{low_bi.line_mmds('|')}】背驰【{low_bi.line_bcs('|')}】",
        }

    return None


def xg_single_xd_bi_zs_zf_5(cl_datas: List[ICL]):
    """
    上涨线段的 第一个 笔中枢， 突破 笔中枢， 大涨 5% 以上的股票
    周期：单周期
    适用市场：沪深A股
    作者：Jiang Haoquan
    """
    cd = cl_datas[0]

    if len(cd.get_xds()) == 0 or len(cd.get_bi_zss()) == 0:
        return None
    xd = cd.get_xds()[-1]
    bi_zs = cd.get_bi_zss()[-1]
    kline = cd.get_klines()[-1]

    if (
        xd.type == "up"
        and xd.start.index == bi_zs.lines[0].start.index
        and kline.h > bi_zs.zg >= kline.l
        and (kline.c - kline.o) / kline.o > 0.05
    ):
        return {"code": cd.get_code(), "msg": "线段向上，当前K线突破中枢高点，并且涨幅大于 5% 涨幅"}

    return None


def xg_single_xd_bi_23_overlapped(cl_datas: List[ICL]):
    """
    上涨线段的 第一个 笔中枢， 突破 笔中枢后 23买重叠的股票
    周期：单周期
    适用市场：沪深A股
    作者：Jiang Haoquan
    """
    cd = cl_datas[0]
    if len(cd.get_xds()) == 0 or len(cd.get_bi_zss()) == 0:
        return None
    xd = cd.get_xds()[-1]
    bi_zs = cd.get_bi_zss()[-1]
    bi = cd.get_bis()[-1]
    bi_2 = cd.get_bis()[-2]
    bi_3 = cd.get_bis()[-3]

    overlapped_23_bi = bi.mmd_exists(["2buy"]) and bi.mmd_exists(["3buy"])
    overlapped_23_bi_2 = (
        bi_2.mmd_exists(["2buy"])
        and bi_2.mmd_exists(["3buy"])
        and bi_td(bi, cd) is True
    )
    overlapped_23_bi_3 = (
        bi_3.mmd_exists(["2buy"])
        and bi_3.mmd_exists(["3buy"])
        and bi.mmd_exists(["l3buy"])
    )

    if (
        xd.type == "up"
        and xd.start.index == bi_zs.lines[0].start.index
        and overlapped_23_bi
        or overlapped_23_bi_2
        or overlapped_23_bi_3
    ):
        return {"code": cd.get_code(), "msg": "线段向上，当前笔突破中枢高点后 2，3 买重叠"}

    return None


def xg_single_day_bc_and_up_jincha(cl_datas: List[ICL]):
    """
    日线级别，倒数第二个向下笔背驰（笔背驰、盘整背驰、趋势背驰），后续macd在水上金叉
    """
    cd = cl_datas[0]
    if len(cd.get_bis()) <= 5 or len(cd.get_xds()) == 0 or len(cd.get_bi_zss()) == 0:
        return None
    xd = cd.get_xds()[-1]
    bis = cd.get_bis()
    bi_zs = cd.get_bi_zss()[-1]
    # 获取所有下跌笔
    down_bis = [bi for bi in bis if bi.type == "down"]
    if len(down_bis) < 2:
        return None
    if xd.type != "down":
        return None

    # 下跌笔不能再创新低
    if down_bis[-1].low < down_bis[-2].low:
        return None

    # 当前黄白线要在零轴上方
    macd_dif = cd.get_idx()["macd"]["dif"][-1]
    macd_dea = cd.get_idx()["macd"]["dea"][-1]
    if macd_dif < 0 or macd_dea < 0:
        return None

    # 倒数第二下跌笔要出背驰
    if down_bis[-2].bc_exists(["pz", "qs"]) is False:
        return None

    # 最后一个中枢 黄白线要上穿零轴
    zs_macd_info = cal_zs_macd_infos(bi_zs, cd)
    if zs_macd_info.dif_up_cross_num == 0 and zs_macd_info.dea_up_cross_num == 0:
        return None

    macd_infos = cal_klines_macd_infos(
        down_bis[-1].start.k.klines[0], cd.get_klines()[-1], cd
    )
    if macd_infos.gold_cross_num > 0:
        return {
            "code": cd.get_code(),
            "msg": f"前down笔背驰 {down_bis[-2].line_bcs()} macd 在零轴之上，后续又出现金叉，可关注",
        }
    return None


def xg_multiple_low_level_12mmd(cl_datas: List[ICL], opt_type: list = []):
    """
    选择 高级别出现背驰or买卖点，并且低级别出现一二类买卖点
    周期：三个周期
    适用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)

    high_data = cl_datas[0]
    low_data_1 = cl_datas[1]
    low_data_2 = cl_datas[2]
    if len(high_data.get_bis()) == 0:
        return None
    if len(low_data_1.get_bis()) == 0 or len(low_data_2.get_bis()) == 0:
        return None

    # 高级别向下，并且有背驰or买卖点
    high_bi = high_data.get_bis()[-1]
    if high_bi.type not in opt_direction:
        return None
    if len(high_bi.line_bcs("|")) == 0 and len(high_bi.line_mmds("|")) == 0:
        return None

    # 获取高级别底分型后的低级别笔
    start_datetime = high_bi.end.klines[0].date
    low_bis: List[BI] = []
    for _bi in low_data_1.get_bis():
        if _bi.end.k.date > start_datetime:
            low_bis.append(_bi)
    for _bi in low_data_2.get_bis():
        if _bi.end.k.date > start_datetime:
            low_bis.append(_bi)

    # 遍历低级别的笔，找是否有一二类买点
    exists_12_mmd = False
    for _bi in low_bis:
        if _bi.mmd_exists(
            ["1buy", "2buy"] if high_bi.type == "down" else ["1sell", "2sell"], "|"
        ):
            exists_12_mmd = True
            break

    if exists_12_mmd:
        return {
            "code": high_data.get_code(),
            "msg": f"{high_data.get_frequency()} 背驰 {high_bi.line_bcs('|')} 买点 {high_bi.line_mmds('|')} 并且低级别出现12类买卖点",
        }

    return None


def xg_single_bi_1mmd(cl_datas: List[ICL], opt_type: list = []):
    """
    获取笔的一类买卖点
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)

    cd = cl_datas[0]
    if len(cd.get_bis()) == 0:
        return None
    bi = cd.get_bis()[-1]
    if bi.type not in opt_direction:
        return None
    for _zs_type, _mmds in bi.zs_type_mmds.items():
        for _m in _mmds:
            if _m.name == "1buy" and _m.zs.line_num < 9:
                return {"code": cd.get_code(), "msg": f"{cd.get_frequency()} 出现本级别笔一买"}

    return None


def xg_single_bi_2mmd(cl_datas: List[ICL], opt_type: list = []):
    """
    获取笔的二类买卖点
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)
    cd = cl_datas[0]
    if len(cd.get_bis()) == 0:
        return None
    bi = cd.get_bis()[-1]
    if bi.type not in opt_direction:
        return None
    for _zs_type, _mmds in bi.zs_type_mmds.items():
        for _m in _mmds:
            if _m.name == "2buy" and _m.zs.line_num < 9:
                return {"code": cd.get_code(), "msg": f"{cd.get_frequency()} 出现本级别笔二买"}

    return None


def xg_single_bi_3mmd(cl_datas: List[ICL], opt_type: list = []):
    """
    获取笔的三类买卖点
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)
    cd = cl_datas[0]
    if len(cd.get_bis()) == 0:
        return None
    bi = cd.get_bis()[-1]
    if bi.type not in opt_direction:
        return None
    for _zs_type, _mmds in bi.zs_type_mmds.items():
        for _m in _mmds:
            if _m.name == "3buy" and _m.zs.line_num < 9:
                return {"code": cd.get_code(), "msg": f"{cd.get_frequency()} 出现本级别笔三买"}

    return None


def xg_single_bcmmd_next_di_fx_verif(cl_datas: List[ICL]):
    """
    笔出现买点或下跌背驰，并且后续出现底分型验证，则提示
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    cd = cl_datas[0]
    if len(cd.get_bis()) == 0:
        return None
    bi = last_done_bi(cd)
    if bi.type != "down":
        return None

    for bc in bi.bcs:
        if bc.type in ["pz", "qs"] and bc.zs.line_num <= 9:
            zs_macd_info = cal_zs_macd_infos(bc.zs, cd)
            if zs_macd_info.dif_up_cross_num > 0 or zs_macd_info.dea_up_cross_num > 0:
                end_di_fx = [
                    _fx
                    for _fx in cd.get_fxs()
                    if (_fx.type == "di" and _fx.index > bi.end.index and _fx.done)
                ]
                if len(end_di_fx) == 0:
                    return None
                end_fx = end_di_fx[0]
                if (
                    cd.get_cl_klines()[-1].index - end_fx.k.index <= 3
                    and end_fx.val > bi.end.val
                ):
                    return {
                        "code": cd.get_code(),
                        "msg": f"{cd.get_frequency()} 出现背驰 {bi.line_bcs()}，并且后续出现验证底分型，可关注",
                    }

    return None


def xg_multiple_zs_tupo_low_3buy(cl_datas: List[ICL]):
    """
    高级别中枢突破，在低级别有三买
    所谓横有多长竖有多长
    找一个高级别（比如日线）大级别中枢窄幅震荡（大于9笔的中枢），在 macd 零轴上方，低一级别出现三类买点的股票
    周期：双周期
    适用市场：沪深A股
    作者：WX
    """
    high_cd = cl_datas[0]
    if len(high_cd.get_bi_zss()) == 0:
        return None
    high_last_bi_zs = high_cd.get_bi_zss()[-1]
    if (
        high_last_bi_zs.done is True
        or high_last_bi_zs.line_num < 9
        or high_last_bi_zs.zf() <= 50
    ):
        return None
    # macd 黄白线要在上方
    high_dif = high_cd.get_idx()["macd"]["dif"][-1]
    high_dea = high_cd.get_idx()["macd"]["dea"][-1]
    if high_dif < 0 or high_dea < 0:
        return None

    # 低级别笔三买
    low_cd = cl_datas[-1]
    if len(low_cd.get_bis()) == 0:
        return None

    low_last_bi = low_cd.get_bis()[-1]
    if low_last_bi.mmd_exists(["3buy"]):
        return {
            "code": high_cd.get_code(),
            "msg": f"{high_cd.get_frequency()} 中枢有可能突破，低级别出现三买，进行关注",
        }

    return None


def xg_single_pre_bi_tk_and_3buy(cl_datas: List[ICL]):
    """
    在三买点前一笔，有跳空缺口
    说明突破中枢的力度比较大，可以重点关注
    周期：单周期
    使用市场：沪深A股
    作者：WX
    """
    cd = cl_datas[0]
    if len(cd.get_bis()) < 5:
        return None
    pre_bi = cd.get_bis()[-2]
    now_bi = cd.get_bis()[-1]
    # 之前一笔要出现向上跳空缺口
    up_qk_num, _ = bi_qk_num(cd, pre_bi)
    if up_qk_num <= 0:
        return None
    # 出现三类买点，并且前笔的高点大于等于中枢的 gg 点
    for mmd in now_bi.mmds:
        if mmd.name == "3buy" and pre_bi.high >= mmd.zs.gg:
            return {"code": cd.get_code(), "msg": f"三买前一笔出现 {up_qk_num} 缺口，可重点关注"}
    return None


def xg_single_find_3buy_by_1buy(cl_datas: List[ICL], opt_type: list = []):
    """
    找三买点，前提是前面中枢内有一类买卖点
    （不同的中枢配置，筛选的条件会有差异）
    周期：单周期
    使用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)

    cd = cl_datas[0]
    if len(cd.get_bis()) <= 5:
        return None

    if len(cd.get_bi_zss()) < 2:
        return None

    # 前面有一买有以下几种情况
    # 三买出现在一个大的中枢上方，在中枢内部有一买 （标准中枢情况会出现）
    # 在三买中枢与前一个中枢之间，有个一买（段内中枢有可能出现）
    # ......

    bi = cd.get_bis()[-1]
    if bi.type not in opt_direction:
        return None
    for _zs_type, _mmds in bi.zs_type_mmds.items():
        for _m in _mmds:
            if _m.name not in ["3buy", "3sell"]:
                continue
            for _l in _m.zs.lines:
                if _l.mmd_exists(
                    ["1buy"] if bi.type == "down" else ["1sell"], _zs_type
                ):
                    return {"code": cd.get_code(), "msg": f"出现三买，并且之前有出现一买"}
    return None


def xg_single_find_3buy_by_zhuanzhe(cl_datas: List[ICL], opt_type: list = []):
    """
    找三买点，之前段内要有是一个下跌趋势，后续下跌趋势结束，出现转折中枢的三买
    （缠论的笔中枢配置要是段内中枢）
    周期：单周期
    使用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)
    cd = cl_datas[0]
    if (
        len(cd.get_bis()) <= 5
        or len(cd.get_xds()) < 2
        or len(cd.get_bi_zss(Config.ZS_TYPE_DN.value)) < 3
    ):
        return None
    # 在三买中枢之前的两个中枢，要是趋势下跌
    bi = cd.get_bis()[-1]
    if bi.type not in opt_direction:
        return None
    if bi.mmd_exists(["3buy"] if bi.type == "down" else ["3sell"], "|") is False:
        return None
    dn_zss = cd.get_bi_zss(Config.ZS_TYPE_DN.value)
    if (
        cd.zss_is_qs(dn_zss[-2], dn_zss[-1]) == "down"
        or cd.zss_is_qs(dn_zss[-3], dn_zss[-2]) == "down"
    ):
        return {"code": cd.get_code(), "msg": "出现三买，并且之前有下跌趋势"}
    return None


def xg_single_ma_250(cl_datas: List[ICL], opt_type: list = []):
    """
    找最新价格在 ma 250 线的上下
    周期：单周期
    使用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)
    cd = cl_datas[0]
    closes = np.array([_k.c for _k in cd.get_src_klines()])
    ma250 = talib.MA(closes, timeperiod=250)
    close = cd.get_src_klines()[-1].c
    if "up" in opt_direction and close > ma250[-1]:
        return {"code": cd.get_code(), "msg": "最新价格高于250日均线"}
    if "down" in opt_direction and close < ma250[-1]:
        return {"code": cd.get_code(), "msg": "最新价格低于250日均线"}
    return None


def xg_single_bi_1buy_next_l3buy_mmd(cl_datas: List[ICL], opt_type: list = []):
    """
    笔1buy后的中枢[类三买/类二买]
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    opt_direction, opt_mmd = get_opt_types(opt_type)

    cd = cl_datas[0]
    if len(cd.get_bis()) == 0:
        return None
    bi = cd.get_bis()[-1]

    if bi.type == "up":  # 笔向上，跳过
        return None

    # 找寻前面的一买笔
    bi_by_1buy = None
    for _bi in cd.get_bis()[::-1]:
        if _bi.mmd_exists(["1buy"], "|"):
            bi_by_1buy = _bi
            break

    if bi_by_1buy is None:  # 没有找到有一买
        return None

    # 一买后的笔创建一个中枢，如果没有中枢或多余1个中枢都是不符合条件的
    zs_lines = cd.get_bis()[bi_by_1buy.index + 1 :]
    # 一买后续有大于9笔，那就不找了
    if len(zs_lines) > 9:
        return None
    zs = cd.create_dn_zs("bi", zs_lines)
    if len(zs) != 1:
        return None

    zs = zs[0]
    if bi.index not in [_l.index for _l in zs.lines]:  # 当前笔不在创建的中枢内
        return None

    # 过滤中枢的起始笔不是一买后的第一个笔
    if zs.lines[0].index != zs_lines[0].index:
        return None

    # 中枢线段的最低点
    zs_min_price = min([_l.low for _l in zs.lines[1:]])

    zss_by_1buy = []  # 一买的中枢，根据配置的不用，可能会有多个
    for _zs_type, _mmds in bi_by_1buy.zs_type_mmds.items():
        for _m in _mmds:
            if _m.name == "1buy":
                zss_by_1buy.append(_m.zs)
    for _zs_1buy in zss_by_1buy:
        # 一买后续中枢低点不能低于一买中枢的中心
        _zs_1buy_mid_price = _zs_1buy.zg - (_zs_1buy.zg - _zs_1buy.zd) / 2
        if zs_min_price > _zs_1buy_mid_price:
            return {
                "code": cd.get_code(),
                "msg": f"一买后形成中枢，且中枢低点 {zs_min_price} 高于 一买中枢的中心 {_zs_1buy_mid_price}",
            }

    return None


if __name__ == "__main__":
    from chanlun.exchange.exchange_tdx import ExchangeTDX
    from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas

    market = "a"
    code = "SZ.000551"
    freq = "d"

    ex = ExchangeTDX()
    cl_config = query_cl_chart_config(market, code)

    klines = ex.klines(code, freq)
    cds = web_batch_get_cl_datas(market, market, {freq: klines}, cl_config)

    res = xg_single_bi_1buy_next_l3buy_mmd(cds)
    print(res)
