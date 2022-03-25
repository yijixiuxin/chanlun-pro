import datetime
import os

# 画图配置
from pyecharts import options as opts
from pyecharts.charts import Kline, Line, Bar, Grid, Scatter
from pyecharts.commons.utils import JsCode

from chanlun import cl

if "JPY_PARENT_PID" in os.environ:
    from pyecharts.globals import CurrentConfig, NotebookType

    CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB
    Kline().load_javascript()
    Line().load_javascript()
    Bar().load_javascript()
    Grid().load_javascript()
    Scatter().load_javascript()


def render_charts(title, cl_data: cl.CL, show_num=500, orders=[], config=None):
    """
    缠论数据图表化展示
    :param title:
    :param cl_data:
    :param show_num:
    :param orders:
    :param config: 画图配置
    :return:
    """

    if config is None:
        config = {}

    if 'show_bi_zs' not in config.keys():
        config['show_bi_zs'] = True
    if 'show_xd_zs' not in config.keys():
        config['show_xd_zs'] = True
    if 'show_bi_mmd' not in config.keys():
        config['show_bi_mmd'] = True
    if 'show_xd_mmd' not in config.keys():
        config['show_xd_mmd'] = True
    if 'show_bi_bc' not in config.keys():
        config['show_bi_bc'] = True
    if 'show_xd_bc' not in config.keys():
        config['show_xd_bc'] = True

    if 'show_ma' not in config.keys():
        config['show_ma'] = True
    if 'show_boll' not in config.keys():
        config['show_boll'] = True

    # 颜色配置
    color_k_up = '#FD1050'
    color_k_down = '#0CF49B'
    color_bi = '#FDDD60'
    color_bi_zs = '#FFFFFF'
    color_bi_zs_up = '#993333'
    color_bi_zs_down = '#99CC99'

    color_xd = '#FFA710'
    color_xd_zs = '#e9967a'
    color_xd_zs_up = '#CC0033'
    color_xd_zs_down = '#66CC99'

    color_qs = '#A1C0FC'

    klines = cl_data.klines
    cl_klines = cl_data.cl_klines
    fxs = cl_data.fxs
    bis = cl_data.bis
    xds = cl_data.xds
    qss = cl_data.qss
    bi_zss = cl_data.bi_zss
    xd_zss = cl_data.xd_zss

    idx = cl_data.idx

    range_start = 0
    if len(klines) > show_num:
        range_start = 100 - (show_num / len(klines)) * 100

    # 重新整理Kline数据
    klines_yaxis = []
    klines_xaxis = []
    klines_vols = []

    cl_klines_yaxis = []
    cl_klines_xaxis = []

    # 找到顶和底的坐标
    point_ding = {'index': [], 'val': [], 'text': []}
    point_di = {'index': [], 'val': [], 'text': []}

    for k in klines:
        klines_xaxis.append(k.date)
        # 开/收/低/高
        klines_yaxis.append([k.o, k.c, k.l, k.h])
        klines_vols.append(k.a)

    for clk in cl_klines:
        cl_klines_xaxis.append(clk.date)
        # 开/收/低/高
        cl_klines_yaxis.append([clk.l, clk.h, clk.l, clk.h])

    for fx in fxs:
        if fx.type == 'ding':
            point_ding['index'].append(fx.k.date)
            point_ding['val'].append(fx.val)
            point_ding['text'].append('力度：%s' % fx.ld())
        else:
            point_di['index'].append(fx.k.date)
            point_di['val'].append(fx.val)
            point_di['text'].append('力度：%s' % fx.ld())

    # 画 笔
    line_bis = {'index': [], 'val': []}
    line_xu_bis = {'index': [], 'val': []}
    bis_done = [_bi for _bi in bis if _bi.done]
    bis_no_done = [_bi for _bi in bis if _bi.done is False]
    if len(bis_done) > 0:
        line_bis['index'].append(bis_done[0].start.k.date)
        line_bis['val'].append(bis_done[0].start.val)
    for b in bis_done:
        line_bis['index'].append(b.end.k.date)
        line_bis['val'].append(b.end.val)
    if len(bis_no_done) > 0:
        line_xu_bis['index'].append(bis_no_done[0].start.k.date)
        line_xu_bis['val'].append(bis_no_done[0].start.val)
    for b in bis_no_done:
        line_xu_bis['index'].append(b.end.k.date)
        line_xu_bis['val'].append(b.end.val)

    # 画 线段
    line_xds = {'index': [], 'val': []}
    line_xu_xds = {'index': [], 'val': []}
    xds_done = [_xd for _xd in xds if _xd.done]
    xds_no_done = [_xd for _xd in xds if _xd.done is False]
    if len(xds_done) > 0:
        line_xds['index'].append(xds_done[0].start.k.date)
        line_xds['val'].append(xds_done[0].start.val)
    for x in xds_done:
        line_xds['index'].append(x.end.k.date)
        line_xds['val'].append(x.end.val)
    if len(xds_no_done) > 0:
        line_xu_xds['index'].append(xds_no_done[0].start.k.date)
        line_xu_xds['val'].append(xds_no_done[0].start.val)
    for x in xds_no_done:
        line_xu_xds['index'].append(x.end.k.date)
        line_xu_xds['val'].append(x.end.val)

    # 画 大趋势
    line_qss = {'index': [], 'val': []}
    line_xu_qss = {'index': [], 'val': []}
    qss_done = [_qs for _qs in qss if _qs.done]
    qss_no_done = [_qs for _qs in qss if _qs.done is False]
    if len(qss_done) > 0:
        line_qss['index'].append(qss_done[0].start.k.date)
        line_qss['val'].append(qss_done[0].start.val)
    for x in qss_done:
        line_qss['index'].append(x.end.k.date)
        line_qss['val'].append(x.end.val)
    if len(qss_no_done) > 0:
        line_xu_qss['index'].append(qss_no_done[0].start.k.date)
        line_xu_qss['val'].append(qss_no_done[0].start.val)
    for x in qss_no_done:
        line_xu_qss['index'].append(x.end.k.date)
        line_xu_qss['val'].append(x.end.val)

    # 画 笔 中枢
    line_bi_zss = []
    for zs in bi_zss:
        if config['show_bi_zs'] is False:
            break
        if zs.real is False:
            continue
        if cl_data.config['zs_type'] == 'bl' and zs.level > 0:
            continue
        start_index = zs.start.k.date
        end_index = zs.end.k.date
        l_zs = [
            # 两竖，两横，5个点，转一圈
            [start_index, start_index, end_index, end_index, start_index],
            [zs.zg, zs.zd, zs.zd, zs.zg, zs.zg],
        ]
        if zs.type == 'up':
            l_zs.append(color_bi_zs_up)
        elif zs.type == 'down':
            l_zs.append(color_bi_zs_down)
        else:
            l_zs.append(color_bi_zs)

        l_zs.append(zs.level + 1)

        line_bi_zss.append(l_zs)

    # 画 线段 中枢
    line_xd_zss = []
    for zs in xd_zss:
        if config['show_xd_zs'] is False:
            break
        if zs.real is False:
            continue
        if cl_data.config['zs_type'] == 'bl' and zs.level > 0:
            continue
        start_index = zs.start.k.date
        end_index = zs.end.k.date
        l_zs = [
            # 两竖，两横，5个点，转一圈
            [start_index, start_index, end_index, end_index, start_index],
            [zs.zg, zs.zd, zs.zd, zs.zg, zs.zg],
        ]
        if zs.type == 'up':
            l_zs.append(color_xd_zs_up)
        elif zs.type == 'down':
            l_zs.append(color_xd_zs_down)
        else:
            l_zs.append(color_xd_zs)

        l_zs.append(zs.level + 2)

        line_xd_zss.append(l_zs)

    # 分型中的 背驰 和 买卖点信息，归类，一起显示
    fx_bcs_mmds = {}
    for _bi in bis:
        _fx = _bi.end
        if _fx.index not in fx_bcs_mmds.keys():
            fx_bcs_mmds[_fx.index] = {'fx': _fx, 'bcs': [], 'mmds': []}
        for _bc in _bi.bcs:
            if config['show_bi_bc'] is False:
                break
            if _bc.bc:
                fx_bcs_mmds[_fx.index]['bcs'].append(_bc)
        for _mmd in _bi.mmds:
            if config['show_bi_mmd'] is False:
                break
            fx_bcs_mmds[_fx.index]['mmds'].append(_mmd)
    for _xd in xds:
        _fx = _xd.end
        if _fx.index not in fx_bcs_mmds.keys():
            fx_bcs_mmds[_fx.index] = {'fx': _fx, 'bcs': [], 'mmds': []}
        for _bc in _xd.bcs:
            if config['show_xd_bc'] is False:
                break
            if _bc.bc:
                fx_bcs_mmds[_fx.index]['bcs'].append(_bc)
        for _mmd in _xd.mmds:
            if config['show_xd_mmd'] is False:
                break
            fx_bcs_mmds[_fx.index]['mmds'].append(_mmd)

    # 画 背驰
    scatter_bc = {'i': [], 'val': []}  # 背驰
    bc_maps = {'bi': '笔背驰', 'xd': '线段背驰', 'pz': '盘整背驰', 'qs': '趋势背驰'}
    for fx_index in fx_bcs_mmds.keys():
        fx_bc_info = fx_bcs_mmds[fx_index]
        bc_label = ''
        fx = fx_bc_info['fx']
        for bc in fx_bc_info['bcs']:
            if bc.zs is not None:
                bc_label += '笔：' if bc.zs.zs_type == 'bi' else '线段：'
            bc_label += bc_maps[bc.type] + ' '
            if bc.zs is not None:
                bc_label += ' ZS[%s (%.2f - %.2f)]' % (bc.zs.type, bc.zs.zg, bc.zs.zd)
            bc_label += ' / '

        if bc_label != '':
            scatter_bc['i'].append(fx.k.date)
            scatter_bc['val'].append([fx.val, bc_label])

    # 画买卖点
    mmd_maps = {'1buy': '一买', '2buy': '二买', 'l2buy': '类二买', '3buy': '三买', 'l3buy': '类三买',
                '1sell': '一卖', '2sell': '二卖', 'l2sell': '类二卖', '3sell': '三卖', 'l3sell': '类三卖'}
    scatter_buy = {'i': [], 'val': []}
    scatter_sell = {'i': [], 'val': []}
    for fx_index in fx_bcs_mmds.keys():
        fx = fx_bcs_mmds[fx_index]['fx']
        mmds = fx_bcs_mmds[fx_index]['mmds']
        if len(mmds) == 0:
            continue
        buy_label = ''
        sell_label = ''
        for mmd in mmds:
            if mmd.name in ['1buy', '2buy', '3buy', 'l2buy', 'l3buy']:
                buy_label += '%s %s ZS[%s (%.2f - %.2f)] /' % (
                    '笔' if mmd.zs.zs_type == 'bi' else '线段', mmd_maps[mmd.name], mmd.zs.type, mmd.zs.zg, mmd.zs.zd)
            if mmd.name in ['1sell', '2sell', '3sell', 'l2sell', 'l3sell']:
                sell_label += '%s %s ZS[%s (%.2f - %.2f)] /' % (
                    '笔' if mmd.zs.zs_type == 'bi' else '线段', mmd_maps[mmd.name], mmd.zs.type, mmd.zs.zg, mmd.zs.zd)

        if buy_label != '':
            scatter_buy['i'].append(fx.k.date)
            scatter_buy['val'].append([fx.val, buy_label])
        if sell_label != '':
            scatter_sell['i'].append(fx.k.date)
            scatter_sell['val'].append([fx.val, sell_label])

    # 画订单记录
    scatter_buy_orders = {'i': [], 'val': []}
    scatter_sell_orders = {'i': [], 'val': []}
    if orders:
        for o in orders:
            if type(o['datetime']) == 'str':
                odt = o['datetime']
            else:
                odt = datetime.datetime.strptime(o['datetime'], '%Y-%m-%d %H:%M:%S')
            if odt < klines[0].date:
                continue
            if o['type'] == 'buy':
                scatter_buy_orders['i'].append(odt)
                scatter_buy_orders['val'].append(
                    [o['price'], str(o['price']) + ' - 买入:' + ('' if 'info' not in o else o['info'])])
            elif o['type'] == 'sell':
                scatter_sell_orders['i'].append(odt)
                scatter_sell_orders['val'].append(
                    [o['price'], str(o['price']) + ' - 卖出:' + ('' if 'info' not in o else o['info'])])

    klines = (Kline().add_xaxis(xaxis_data=klines_xaxis).add_yaxis(
        series_name="",
        y_axis=klines_yaxis,
        itemstyle_opts=opts.ItemStyleOpts(
            color=color_k_up,
            color0=color_k_down,
            border_color=color_k_up,
            border_color0=color_k_down,
        ),
    ).set_global_opts(
        title_opts=opts.TitleOpts(title=title, pos_left="0"),
        xaxis_opts=opts.AxisOpts(
            type_="category",
            is_scale=True,
            axisline_opts=opts.AxisLineOpts(is_on_zero=False),
            splitline_opts=opts.SplitLineOpts(is_show=False),
            axislabel_opts=opts.LabelOpts(is_show=False),
            split_number=20,
            min_="dataMin",
            max_="dataMax",
        ),
        yaxis_opts=opts.AxisOpts(
            is_scale=True,
            splitline_opts=opts.SplitLineOpts(is_show=False),
            position="right",
            axislabel_opts=opts.LabelOpts(is_show=False),
            axisline_opts=opts.AxisLineOpts(is_show=False),
            axistick_opts=opts.AxisTickOpts(is_show=False),
        ),
        tooltip_opts=opts.TooltipOpts(
            trigger="axis", axis_pointer_type="line"),
        datazoom_opts=[
            opts.DataZoomOpts(is_show=False, type_="inside", xaxis_index=[0, 0], range_start=range_start,
                              range_end=100),
            opts.DataZoomOpts(is_show=True, xaxis_index=[0, 1], pos_top="97%", range_start=range_start,
                              range_end=100),
            opts.DataZoomOpts(is_show=False, xaxis_index=[0, 2], range_start=range_start, range_end=100),
        ],
    )
    )

    # 画顶底分型
    fenxing_ding = (
        Scatter().add_xaxis(point_ding['index']).add_yaxis(
            "分型",
            point_ding['val'],
            itemstyle_opts=opts.ItemStyleOpts(color='red'),
            symbol_size=2,
            label_opts=opts.LabelOpts(is_show=False)
        )
    )
    fenxing_di = (
        Scatter().add_xaxis(point_di['index']).add_yaxis(
            "分型",
            point_di['val'],
            itemstyle_opts=opts.ItemStyleOpts(color='green'),
            symbol_size=2,
            label_opts=opts.LabelOpts(is_show=False)
        )
    )
    overlap_kline = klines.overlap(fenxing_ding)
    overlap_kline = overlap_kline.overlap(fenxing_di)

    # 画 完成笔
    line_bi = (
        Line().add_xaxis(line_bis['index']).add_yaxis(
            "笔",
            line_bis['val'],
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=1, color=color_bi),
        )
    )
    overlap_kline = overlap_kline.overlap(line_bi)
    # 画 未完成笔
    line_xu_bi = (
        Line().add_xaxis(line_xu_bis['index']).add_yaxis(
            "笔",
            line_xu_bis['val'],
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=1, type_='dashed', color=color_bi),
        )
    )
    overlap_kline = overlap_kline.overlap(line_xu_bi)

    # 画 完成线段
    line_xd = (
        Line().add_xaxis(line_xds['index']).add_yaxis(
            "线段",
            line_xds['val'],
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2, color=color_xd)
        )
    )
    overlap_kline = overlap_kline.overlap(line_xd)
    # 画 未完成线段
    line_xu_xd = (
        Line().add_xaxis(line_xu_xds['index']).add_yaxis(
            "线段",
            line_xu_xds['val'],
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2, type_='dashed', color=color_xd)
        )
    )
    overlap_kline = overlap_kline.overlap(line_xu_xd)

    # 画 完成大趋势
    line_qs = (
        Line().add_xaxis(line_qss['index']).add_yaxis(
            "趋势",
            line_qss['val'],
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2, color=color_qs)
        )
    )
    overlap_kline = overlap_kline.overlap(line_qs)
    # 画 未完成大趋势
    line_xu_qs = (
        Line().add_xaxis(line_xu_qss['index']).add_yaxis(
            "趋势",
            line_xu_qss['val'],
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2, type_='dashed', color=color_qs)
        )
    )
    overlap_kline = overlap_kline.overlap(line_xu_qs)

    if config['show_boll']:
        # 画 指标线
        line_idx_boll = (
            Line().add_xaxis(xaxis_data=klines_xaxis).add_yaxis(
                series_name="BOLL",
                is_symbol_show=False,
                y_axis=idx['boll']['up'],
                linestyle_opts=opts.LineStyleOpts(width=1, color='#99CC99'),
                label_opts=opts.LabelOpts(is_show=False),
            ).add_yaxis(
                series_name="BOLL",
                is_symbol_show=False,
                y_axis=idx['boll']['mid'],
                linestyle_opts=opts.LineStyleOpts(width=1, color='#FF6D00'),
                label_opts=opts.LabelOpts(is_show=False),
            ).add_yaxis(
                series_name="BOLL",
                is_symbol_show=False,
                y_axis=idx['boll']['low'],
                linestyle_opts=opts.LineStyleOpts(width=1, color='#99CC99'),
                label_opts=opts.LabelOpts(is_show=False),
            ).set_global_opts()
        )
        overlap_kline = overlap_kline.overlap(line_idx_boll)
    if config['show_ma']:
        line_idx_ma = (
            Line().add_xaxis(xaxis_data=klines_xaxis).add_yaxis(
                series_name="MA",
                is_symbol_show=False,
                y_axis=idx['ma'],
                linestyle_opts=opts.LineStyleOpts(width=1, color='red'),
                label_opts=opts.LabelOpts(is_show=False),
            ).set_global_opts()
        )
        overlap_kline = overlap_kline.overlap(line_idx_ma)

    # 画 笔中枢
    for zs in line_bi_zss:
        bi_zs = (
            Line().add_xaxis(zs[0]).add_yaxis(
                "笔中枢",
                zs[1],
                symbol=None,
                label_opts=opts.LabelOpts(is_show=False),
                linestyle_opts=opts.LineStyleOpts(width=zs[3], color=zs[2]),
                areastyle_opts=opts.AreaStyleOpts(opacity=0.1, color=zs[2]),
                tooltip_opts=opts.TooltipOpts(is_show=False),
            )
        )
        overlap_kline = overlap_kline.overlap(bi_zs)
    # 画 线段 中枢
    for zs in line_xd_zss:
        xd_zs = (
            Line().add_xaxis(zs[0]).add_yaxis(
                "线段中枢",
                zs[1],
                symbol=None,
                label_opts=opts.LabelOpts(is_show=False),
                linestyle_opts=opts.LineStyleOpts(width=zs[3], color=zs[2]),
                tooltip_opts=opts.TooltipOpts(is_show=False),
            )
        )
        overlap_kline = overlap_kline.overlap(xd_zs)

    # 画背驰
    scatter_bc_tu = (
        Scatter().add_xaxis(xaxis_data=scatter_bc['i']).add_yaxis(
            series_name="背驰",
            y_axis=scatter_bc['val'],
            symbol_size=10,
            symbol='circle',
            itemstyle_opts=opts.ItemStyleOpts(color='rgba(223,148,100,0.7)'),
            label_opts=opts.LabelOpts(is_show=False),
            tooltip_opts=opts.TooltipOpts(
                textstyle_opts=opts.TextStyleOpts(font_size=12),
                formatter=JsCode(
                    "function (params) {return params.value[2];}"
                )
            ),
        )
    )
    overlap_kline = overlap_kline.overlap(scatter_bc_tu)

    # 画买卖点
    scatter_buy_tu = (
        Scatter().add_xaxis(xaxis_data=scatter_buy['i']).add_yaxis(
            series_name="买卖点",
            y_axis=scatter_buy['val'],
            symbol_size=15,
            symbol='arrow',
            itemstyle_opts=opts.ItemStyleOpts(color='rgba(250,128,114,0.8)'),
            tooltip_opts=opts.TooltipOpts(
                textstyle_opts=opts.TextStyleOpts(font_size=12),
                formatter=JsCode(
                    "function (params) {return params.value[2];}"
                )
            ),
        )
    )
    scatter_sell_tu = (
        Scatter().add_xaxis(xaxis_data=scatter_sell['i']).add_yaxis(
            series_name="买卖点",
            y_axis=scatter_sell['val'],
            symbol_size=15,
            symbol='arrow',
            symbol_rotate=180,
            itemstyle_opts=opts.ItemStyleOpts(color='rgba(30,144,255,0.8)'),
            tooltip_opts=opts.TooltipOpts(
                textstyle_opts=opts.TextStyleOpts(font_size=12),
                formatter=JsCode(
                    "function (params) {return params.value[2];}"
                )
            ),
        )
    )
    overlap_kline = overlap_kline.overlap(scatter_buy_tu)
    overlap_kline = overlap_kline.overlap(scatter_sell_tu)

    # 画订单记录
    if orders and len(orders) > 0:
        scatter_buy_orders_tu = (
            Scatter().add_xaxis(xaxis_data=scatter_buy_orders['i']).add_yaxis(
                series_name="订单",
                y_axis=scatter_buy_orders['val'],
                symbol_size=15,
                symbol='diamond',
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color='rgba(255,20,147,0.5)'),
                tooltip_opts=opts.TooltipOpts(
                    textstyle_opts=opts.TextStyleOpts(font_size=12),
                    formatter=JsCode(
                        "function (params) {return params.value[2];}"
                    )
                ),
            )
        )
        overlap_kline = overlap_kline.overlap(scatter_buy_orders_tu)
        scatter_sell_orders_tu = (
            Scatter().add_xaxis(xaxis_data=scatter_sell_orders['i']).add_yaxis(
                series_name="订单",
                y_axis=scatter_sell_orders['val'],
                symbol_size=15,
                symbol='diamond',
                label_opts=opts.LabelOpts(is_show=False),
                itemstyle_opts=opts.ItemStyleOpts(color='rgba(0,191,255,0.5)'),
                tooltip_opts=opts.TooltipOpts(
                    textstyle_opts=opts.TextStyleOpts(font_size=12),
                    formatter=JsCode(
                        "function (params) {return params.value[2];}"
                    )
                ),
            )
        )
        overlap_kline = overlap_kline.overlap(scatter_sell_orders_tu)

    # 成交量
    bar_vols = (
        Bar().add_xaxis(xaxis_data=klines_xaxis).add_yaxis(
            series_name="",
            y_axis=klines_vols,
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(color='rgba(236,55,59,0.5)'),
        )
        #     .set_global_opts(
        #     legend_opts=opts.LegendOpts(is_show=True),
        #     xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False), ),
        #     yaxis_opts=opts.AxisOpts(
        #         position="right",
        #         axislabel_opts=opts.LabelOpts(is_show=False),
        #         axisline_opts=opts.AxisLineOpts(is_show=False),
        #         axistick_opts=opts.AxisTickOpts(is_show=False),
        #     ),
        # )
    )

    # MACD
    bar_macd = (
        Bar().add_xaxis(xaxis_data=klines_xaxis).add_yaxis(
            series_name="MACD",
            y_axis=list(idx['macd']['hist']),
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(
                color=JsCode('function(p){var c;if (p.data >= 0) {c = \'#ef232a\';} else {c = \'#14b143\';}return c;}')
            ),
        ).set_global_opts(
            legend_opts=opts.LegendOpts(is_show=False),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False), ),
            yaxis_opts=opts.AxisOpts(
                position="right",
                axislabel_opts=opts.LabelOpts(is_show=False),
                axisline_opts=opts.AxisLineOpts(is_show=False),
                axistick_opts=opts.AxisTickOpts(is_show=False),
            ),
        )
    )

    line_macd_dif = (
        Line().add_xaxis(xaxis_data=klines_xaxis).add_yaxis(
            series_name="DIF",
            y_axis=idx['macd']['dif'],
            is_symbol_show=False,
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(color='#fe832d'),
        ).add_yaxis(
            series_name="DEA",
            y_axis=idx['macd']['dea'],
            is_symbol_show=False,
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(color='#f5a4df'),
        ).set_global_opts(
            legend_opts=opts.LegendOpts(is_show=True),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False), ),
            yaxis_opts=opts.AxisOpts(position="right",
                                     axislabel_opts=opts.LabelOpts(is_show=False),
                                     axisline_opts=opts.AxisLineOpts(is_show=False),
                                     axistick_opts=opts.AxisTickOpts(is_show=False)),
        )
    )

    # 最下面的柱状图和折线图
    macd_bar_line = bar_macd.overlap(line_macd_dif)

    # 最后的 Grid
    grid_chart = Grid(init_opts=opts.InitOpts(width="100%", height="800px", theme='dark'))

    grid_chart.add(
        overlap_kline,
        grid_opts=opts.GridOpts(width="96%", height="75%", pos_left='1%', pos_right='3%'),
    )

    # Volumn 柱状图
    grid_chart.add(
        bar_vols,
        grid_opts=opts.GridOpts(
            pos_bottom="15%", height="10%", width="96%", pos_left='1%', pos_right='3%'
        ),
    )

    # MACD 柱状图
    grid_chart.add(
        macd_bar_line,
        grid_opts=opts.GridOpts(
            pos_bottom="0", height="15%", width="96%", pos_left='1%', pos_right='3%'
        ),
    )
    if "JPY_PARENT_PID" in os.environ:
        return grid_chart.render_notebook()
    else:
        return grid_chart.dump_options()

