#:  -*- coding: utf-8 -*-

import datetime

from chanlun import fun, zixuan  # noqa: F401
from chanlun.base import Market
from chanlun.cl_utils import query_cl_chart_config
from chanlun.db import db
from chanlun.exchange import get_exchange
from chanlun.xuangu.xuangu_by_same import XuanguBySame

"""
选相同的股票
根据给定的一个股票信息，提取特征，选取相似度排名高的 n 只股票，加入自选组
"""

if __name__ == "__main__":
    # 市场设置
    market = "a"
    # 这里设置选股缠论计算的参数，要与前台展示的配置一致，不然这里选出的股票符合条件，前台页面有可能看不到
    cl_config = query_cl_chart_config(market, "SH.000001")
    # 自选组，选择的股票，放入的自选组
    zx = zixuan.ZiXuan(market)
    zx_group = "相似选股"
    # 选股标记，展示在前端图表中
    xg_mark = "SG"

    # 清空自选组中的股票，删除所有选股标记
    zx.clear_zx_stocks(zx_group)
    db.marks_del(market=market, mark_label=xg_mark)

    # 目标股票信息
    target_code = "SZ.002082"
    # 目标周期
    target_frequency = "d"
    # 目标截至时间 (可以设置目标最后的日期，不设置则使用最新的)
    target_end_datetime = None
    # target_end_datetime = fun.str_to_datetime("2025-06-25 15:00:00")

    # 设置从那些标的中找相似的
    ex = get_exchange(Market(market))
    all_stocks = ex.all_stocks()
    find_codes = [
        _s["code"]
        for _s in all_stocks
        if _s["code"].startswith("SH.60")
        # or _s["code"].startswith("SZ.00")
        # or _s["code"].startswith("SZ.30")
    ]  # 找上海 60 / 深圳 00 30 开头的股票

    # 实例化并运行
    xg_by_same = XuanguBySame(market)
    # 相似度检查配置
    xg_by_same.k_num = 200  # K线相似度检查数量（不能为0）
    xg_by_same.bi_num = 9  # 相似度比较的 笔的数量（不能为0）
    xg_by_same.xd_num = 3  # 相似度比较的 线段的数量（不能为0）
    # 各项的权重设置，总和要等于 1，如果不计算某一项的相似度，权重设置为 0.0 即可
    xg_by_same.k_weight = 0.3  # K线相似度占比权重
    xg_by_same.bi_weight = 0.6  # 笔相似度占比权重
    xg_by_same.xd_weight = 0.1  # 线段相似度占比权重

    same_codes = xg_by_same.run(
        target_code=target_code,
        frequency=target_frequency,
        target_end_datetime=target_end_datetime,
        find_codes=find_codes,
        cl_config=cl_config,
        run_type="process",
    )  # 使用多进行运行相似度选股

    # 按照相似度进行排序
    same_codes = sorted(same_codes, key=lambda x: x["similarity"], reverse=True)
    # 打印并添加自选，相似度最高的 20 只股票
    for _s in same_codes[:20]:
        print(_s)
        # 添加自选
        zx.add_stock(zx_group, _s["code"], None, memo=_s["similarity"])
        # 添加标记
        db.marks_add(
            market=market,
            stock_code=_s["code"],
            stock_name=_s["code"],
            frequency="",
            mark_time=fun.datetime_to_int(datetime.datetime.now()),
            mark_label=xg_mark,
            mark_tooltip=f"相似度：{_s['similarity']}",
            mark_shape="circle",
            mark_color="red",
        )

    print("Done")
