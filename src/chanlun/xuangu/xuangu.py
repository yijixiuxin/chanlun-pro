from chanlun.cl_utils import *

"""
根据缠论数据，选择自己所需要的形态方法集合
"""


def xg_single_xd_and_bi_mmd(cl_datas: List[ICL]):
    """
    线段和笔都有出现买卖点 或 笔出现类三买 的条件
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    cd = cl_datas[0]
    if len(cd.get_xds()) == 0 or len(cd.get_bis()) == 0:
        return None
    xd = cd.get_xds()[-1]
    bi = cd.get_bis()[-1]
    if xd.mmd_exists(['1buy', '2buy', '3buy', 'l2buy', 'l3buy']) and bi.mmd_exists(
            ['1buy', '2buy', '3buy', 'l2buy', 'l3buy']):
        return f'线段买点 【{xd.line_mmds()}】 笔买点【{bi.line_mmds()}】'
    return next(
        (f'笔出现线段买点【{bi.line_mmds()}】' for mmd in bi.mmds if mmd.zs.zs_type == 'xd' and 'buy' in mmd.name), None)


def xg_multiple_xd_bi_mmd(cl_datas: List[ICL]):
    """
    选择 高级别线段，低级别笔 都出现买点，或者 高级别线段和高级别笔 都出现 背驰 的条件
    周期：两个周期
    适用市场：沪深A股
    作者：WX
    """
    high_data = cl_datas[0]
    low_data = cl_datas[1]
    if len(high_data.get_xds()) == 0 or len(high_data.get_bis()) == 0:
        return None
    if len(low_data.get_xds()) == 0 or len(low_data.get_bis()) == 0:
        return None

    high_xd = high_data.get_xds()[-1]
    high_bi = high_data.get_bis()[-1]
    low_bi = low_data.get_bis()[-1]
    if high_xd.mmd_exists(['1buy', '2buy', '3buy', 'l2buy', 'l3buy']) and \
            low_bi.mmd_exists(['1buy', '2buy', '3buy', 'l2buy', 'l3buy']):
        return f'{high_data.get_frequency()} 线段买点【{high_xd.line_mmds()}】 {low_data.get_frequency()} 笔买点【{low_bi.line_mmds()}】'

    if high_xd.bc_exists(['pz', 'qs']) and high_bi.bc_exists(['pz', 'qs']):
        return f'{high_data.get_frequency()} 线段背驰【{high_xd.line_bcs()}】 笔背驰【{high_bi.line_bcs()}】'

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

    if xd.type == 'up' \
            and xd.start.index == bi_zs.lines[0].start.index \
            and kline.h > bi_zs.zg >= kline.l and (kline.c - kline.o) / kline.o > 0.05:
        return '线段向上，当前K线突破中枢高点，并且涨幅大于 5% 涨幅'

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

    overlapped_23_bi = bi.mmd_exists(['2buy']) and bi.mmd_exists(['3buy'])
    overlapped_23_bi_2 = bi_2.mmd_exists(['2buy']) and bi_2.mmd_exists(['3buy']) and bi_td(bi, cd) is True
    overlapped_23_bi_3 = bi_3.mmd_exists(['2buy']) and bi_3.mmd_exists(['3buy']) and bi.mmd_exists(['l3buy'])

    if xd.type == 'up' \
            and xd.start.index == bi_zs.lines[0].start.index \
            and overlapped_23_bi or overlapped_23_bi_2 or overlapped_23_bi_3:
        return '线段向上，当前笔突破中枢高点后 2，3 买重叠'

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
    down_bis = [bi for bi in bis if bi.type == 'down']
    if len(down_bis) < 2:
        return None
    if xd.type != 'down':
        return None

    # 下跌笔不能再创新低
    if down_bis[-1].low < down_bis[-2].low:
        return None

    # 当前黄白线要在零轴上方
    macd_dif = cd.get_idx()['macd']['dif'][-1]
    macd_dea = cd.get_idx()['macd']['dea'][-1]
    if macd_dif < 0 or macd_dea < 0:
        return None

    # 倒数第二下跌笔要出背驰
    if down_bis[-2].bc_exists(['pz', 'qs']) is False:
        return None

    # 最后一个中枢 黄白线要上穿零轴
    zs_macd_info = cal_zs_macd_infos(bi_zs, cd)
    if zs_macd_info.dif_up_cross_num == 0 and zs_macd_info.dea_up_cross_num == 0:
        return None

    macd_infos = cal_klines_macd_infos(down_bis[-1].start.k.klines[0], cd.get_klines()[-1], cd)
    if macd_infos.gold_cross_num > 0:
        return f'前down笔背驰 {down_bis[-2].line_bcs()} macd 在零轴之上，后续又出现金叉，可关注'
    return None


def xg_multiple_low_level_1mmd(cl_datas: List[ICL]):
    """
    选择 高级别出现背驰or买卖点，并且低级别出现一二类买卖点
    周期：三个周期
    适用市场：沪深A股
    作者：WX
    """
    high_data = cl_datas[0]
    low_data_1 = cl_datas[1]
    low_data_2 = cl_datas[2]
    if len(high_data.get_bis()) == 0:
        return None
    if len(low_data_1.get_bis()) == 0 or len(low_data_2.get_bis()) == 0:
        return None

    # 高级别向下，并且有背驰or买卖点
    high_bi = high_data.get_bis()[-1]
    if high_bi.type == 'up':
        return None
    if len(high_bi.line_bcs()) == 0 and len(high_bi.line_mmds()) == 0:
        return None
    if high_data.get_cl_klines()[-1].index - high_bi.end.klines[-1].index > 3:
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
    exists_12buy_mmd = False
    for _bi in low_bis:
        if _bi.mmd_exists(['1buy', '2buy']):
            exists_12buy_mmd = True
            break

    if exists_12buy_mmd:
        return f'{high_data.get_frequency()} 背驰 {high_bi.line_bcs()} 买点 {high_bi.line_mmds()} 并且低级别出现12类买点'

    return None


def xg_single_bi_2mmd(cl_datas: List[ICL]):
    """
    获取笔的二类买卖点
    周期：单周期
    适用市场：沪深A股
    作者：WX
    """
    cd = cl_datas[0]
    if len(cd.get_bis()) == 0:
        return None
    bi = cd.get_bis()[-1]
    for mmd in bi.mmds:
        if mmd.name == '2buy' and mmd.zs.line_num < 9:
            zs_macd_info = cal_zs_macd_infos(mmd.zs, cd)
            if zs_macd_info.dif_up_cross_num > 0 or zs_macd_info.dea_up_cross_num > 0:
                return f'{cd.get_frequency()} 出现本级别笔二买'

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
    if bi.type != 'down':
        return None

    for bc in bi.bcs:
        if bc.type in ['pz', 'qs'] and bc.zs.line_num <= 9:
            zs_macd_info = cal_zs_macd_infos(bc.zs, cd)
            if zs_macd_info.dif_up_cross_num > 0 or zs_macd_info.dea_up_cross_num > 0:
                end_di_fx = [
                    _fx for _fx in cd.get_fxs() if
                    (_fx.type == 'di' and _fx.index > bi.end.index and _fx.done)
                ]
                if len(end_di_fx) == 0:
                    return None
                end_fx = end_di_fx[0]
                if cd.get_cl_klines()[-1].index - end_fx.k.index <= 3 and end_fx.val > bi.end.val:
                    return f'{cd.get_frequency()} 出现背驰 {bi.line_bcs()}，并且后续出现验证底分型，可关注'

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
    if high_last_bi_zs.done is True or high_last_bi_zs.line_num < 9 or high_last_bi_zs.zf() <= 50:
        return None
    # macd 黄白线要在上方
    high_dif = high_cd.get_idx()['macd']['dif'][-1]
    high_dea = high_cd.get_idx()['macd']['dea'][-1]
    if high_dif < 0 or high_dea < 0:
        return None

    # 低级别笔三买
    low_cd = cl_datas[-1]
    if len(low_cd.get_bis()) == 0:
        return None

    low_last_bi = low_cd.get_bis()[-1]
    if low_last_bi.mmd_exists(['3buy']):
        return f'{high_cd.get_frequency()} 中枢有可能突破，低级别出现三买，进行关注'

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
        if mmd.name == '3buy' and pre_bi.high >= mmd.zs.gg:
            return f'三买前一笔出现 {up_qk_num} 缺口，可重点关注'
    return None


def xg_single_find_3buy_by_1buy(cl_datas: List[ICL]):
    """
    找三买点，前提是前面中枢内有一类买卖点
    （不同的中枢配置，筛选的条件会有差异）
    周期：单周期
    使用市场：沪深A股
    作者：WX
    """
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
    if bi.mmd_exists(['3buy']) is False:
        return None
    zss = cd.get_bi_zss()
    exists_1buy = False
    # 第一种情况，中枢内部有一买
    for _zsbi in zss[-1].lines:
        if _zsbi.mmd_exists(['1buy']):
            exists_1buy = True
            break
    # 第二种情况，与前一个中枢之间有一买
    zs_qj_bis = [_bi for _bi in cd.get_bis() if zss[-2].lines[-1].index <= _bi.index <= zss[-1].lines[0].index]
    for _zsbi in zs_qj_bis:
        if _zsbi.mmd_exists(['1buy']):
            exists_1buy = True
            break
    if exists_1buy:
        return f'出现三买，并且之前有出现一买'


def xg_single_find_3buy_by_zhuanzhe(cl_datas: List[ICL]):
    """
    找三买点，之前段内要有是一个下跌趋势，后续下跌趋势结束，出现转折中枢的三买
    （缠论的笔中枢配置要是段内中枢）
    周期：单周期
    使用市场：沪深A股
    作者：WX
    """
    cd = cl_datas[0]
    if len(cd.get_bis()) <= 5 or len(cd.get_xds()) <= 2 or len(cd.get_bi_zss()) < 3:
        return None
    # 在三买中枢之前的两个中枢，要是趋势下跌
    bi = cd.get_bis()[-1]
    if bi.mmd_exists(['3buy']) is False:
        return None
    # 倒数第二个线段是下跌线段
    xd = cd.get_xds()[-2]
    if xd.type != 'down':
        return None
    zss = cd.get_bi_zss()
    zs1 = zss[-3]
    zs2 = zss[-2]
    # 两个中枢都在线段内部 (这个比较严格，去掉会有比较多的不太合适的)
    if xd.start_line.index <= zs1.lines[0].index and zs2.lines[-1].index <= xd.end_line.index:
        pass
    else:
        return None
    # 非严格的趋势比较，用 zg/zd，严格的趋势比较用 gg/dd
    if zs2.level == zs1.level and zs2.zg < zs1.zd:
        return '出现三买，并且之前有下跌趋势'


if __name__ == '__main__':
    from chanlun.exchange.exchange_tdx import ExchangeTDX
    from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas

    market = 'a'
    code = 'SZ.000001'
    freq = 'd'

    ex = ExchangeTDX()
    cl_config = query_cl_chart_config(market, code)

    klines = ex.klines(code, freq)
    cds = web_batch_get_cl_datas(market, market, {freq: klines}, cl_config)

    res = xg_single_find_3buy_by_zhuanzhe(cds)
    print(res)
