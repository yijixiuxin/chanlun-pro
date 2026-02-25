"""
监控相关代码
"""

import os
import pathlib
import time
import traceback
from typing import List

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateImageRequest,
    CreateImageRequestBody,
    CreateImageResponse,
)
from playwright.sync_api import sync_playwright

from chanlun import config, fun, kcharts
from chanlun.backtesting.base import Strategy
from chanlun.cl_interface import ICL
from chanlun.cl_utils import bi_td, web_batch_get_cl_datas
from chanlun.db import db
from chanlun.exchange import Market, get_exchange
from chanlun.utils import send_fs_msg


def monitoring_code(
    task_name: str,
    market: str,
    code: str,
    name: str,
    frequencys: list,
    check_cl_types: dict = None,
    check_idx_types: dict = None,
    is_send_msg: bool = False,
    is_send_img: bool = None,
    cl_config=None,
):
    """
    监控指定股票是否出现指定的信号
    :param market: 市场
    :param code: 代码
    :param name: 名称
    :param frequencys: 检查周期
    :param check_cl_types: 监控的缠论项目
    :param check_idx_types: 监控的指标项目
    :param is_send_msg: 是否发送消息
    :param is_send_img: 是否发送图片（None 则读取全局配置）
    :param cl_config: 缠论配置
    :return:
    """
    if check_cl_types is None:
        check_cl_types = {
            "bi_types": [],
            "bi_beichi": [],
            "bi_mmd": [],
            "xd_types": [],
            "xd_beichi": [],
            "xd_mmd": [],
        }

    if check_idx_types is None:
        check_idx_types = {
            "idx_ma": {
                "enable": 0,
                "slow": 10,
                "fast": 5,
                "cross_up": 0,
                "cross_down": 0,
            },
            "idx_macd": {
                "enable": 0,
                "cross_up": 0,
                "cross_down": 0,
            },
            "idx_zhixing": {
                "enable": 0,
                "cross_up": 0,
                "cross_down": 0,
            },
        }

    ex = get_exchange(Market(market))

    klines = {f: ex.klines(code, f) for f in frequencys}
    try:
        cl_datas: List[ICL] = web_batch_get_cl_datas(market, code, klines, cl_config)

        jh_cl_msgs = []  # 这里保存缠论触发的机会信息
        jh_idx_msgs = []  # 这里保存指标触发的机会信息
    except Exception as e:
        print(f'{market} {code} {name} 监控异常 {e}')
        traceback.print_exc()
        return []

    bc_maps = {"xd": "线段背驰", "bi": "笔背驰", "pz": "盘整背驰", "qs": "趋势背驰"}
    mmd_maps = {
        "1buy": "一买点",
        "2buy": "二买点",
        "l2buy": "类二买点",
        "3buy": "三买点",
        "l3buy": "类三买点",
        "1sell": "一卖点",
        "2sell": "二卖点",
        "l2sell": "类二卖点",
        "3sell": "三卖点",
        "l3sell": "类三卖点",
    }
    for cd in cl_datas:
        bis = cd.get_bis()
        frequency = cd.get_frequency()
        if len(bis) == 0:
            continue
        end_bi = bis[-1]
        end_xd = cd.get_xds()[-1] if len(cd.get_xds()) > 0 else None
        # 检查背驰和买卖点
        if end_bi.type in check_cl_types["bi_types"]:
            jh_cl_msgs.extend(
                {
                    "type": f"笔 {end_bi.type} {bc_maps[bc_type]}",
                    "frequency": frequency,
                    "bi": end_bi,
                    "bi_td": bi_td(end_bi, cd),
                    "fx_ld": end_bi.end.ld(),
                    "line_dt": end_bi.start.k.date,
                    "mark_dt": end_bi.end.k.date,
                    "k_date": cd.get_src_klines()[-1].date,
                    "k_index": cd.get_src_klines()[-1].index,
                    "line_type": end_bi.type,
                }
                for bc_type in check_cl_types["bi_beichi"]
                if end_bi.bc_exists([bc_type], "|")
            )

            jh_cl_msgs.extend(
                {
                    "type": f"笔 {mmd_maps[mmd]}",
                    "frequency": frequency,
                    "bi": end_bi,
                    "bi_td": bi_td(end_bi, cd),
                    "fx_ld": end_bi.end.ld(),
                    "line_dt": end_bi.start.k.date,
                    "mark_dt": end_bi.end.k.date,
                    "k_date": cd.get_src_klines()[-1].date,
                    "k_index": cd.get_src_klines()[-1].index,
                    "line_type": end_bi.type,
                    "cd": cd, # 传递 cd 对象以便判断是否是最后一笔
                }
                for mmd in check_cl_types["bi_mmd"]
                if end_bi.mmd_exists([mmd], "|")
            )

        if end_xd:
            # 检查背驰和买卖点
            if end_xd.type in check_cl_types["xd_types"]:
                jh_cl_msgs.extend(
                    {
                        "type": f"线段 {end_xd.type} {bc_maps[bc_type]}",
                        "frequency": frequency,
                        "xd": end_xd,
                        "line_dt": end_xd.start.k.date,
                        "mark_dt": end_xd.end.k.date,
                        "k_date": cd.get_src_klines()[-1].date,
                        "k_index": cd.get_src_klines()[-1].index,
                        "line_type": end_xd.type,
                        "cd": cd,
                    }
                    for bc_type in check_cl_types["xd_beichi"]
                    if end_xd.bc_exists([bc_type], "|")
                )

                jh_cl_msgs.extend(
                    {
                        "type": f"线段 {mmd_maps[mmd]}",
                        "frequency": frequency,
                        "xd": end_xd,
                        "line_dt": end_xd.start.k.date,
                        "mark_dt": end_xd.end.k.date,
                        "k_date": cd.get_src_klines()[-1].date,
                        "k_index": cd.get_src_klines()[-1].index,
                        "line_type": end_xd.type,
                        "cd": cd,
                    }
                    for mmd in check_cl_types["xd_mmd"]
                    if end_xd.mmd_exists([mmd], "|")
                )

        # 指标监测
        if (
            check_idx_types["idx_ma"]["enable"]
            and len(cd.get_src_klines()) > check_idx_types["idx_ma"]["slow"]
        ):
            idx_ma_slow = Strategy.idx_ma(cd, period=check_idx_types["idx_ma"]["slow"])
            idx_ma_fast = Strategy.idx_ma(cd, period=check_idx_types["idx_ma"]["fast"])
            if (
                check_idx_types["idx_ma"]["cross_up"]
                and idx_ma_fast[-1] > idx_ma_slow[-1]
                and idx_ma_fast[-2] < idx_ma_slow[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "ma",
                        "msg": f"均线上穿[{check_idx_types['idx_ma']['slow']},{check_idx_types['idx_ma']['fast']}]",
                        "frequency": frequency,
                        "cross": "up",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "down",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )
            if (
                check_idx_types["idx_ma"]["cross_down"]
                and idx_ma_fast[-1] < idx_ma_slow[-1]
                and idx_ma_fast[-2] > idx_ma_slow[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "ma",
                        "msg": f"均线下穿[{check_idx_types['idx_ma']['slow']},{check_idx_types['idx_ma']['fast']}]",
                        "frequency": frequency,
                        "cross": "down",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "up",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )
        if check_idx_types["idx_macd"]["enable"]:
            idx_macd_dif = cd.get_idx()["macd"]["dif"]
            idx_macd_dea = cd.get_idx()["macd"]["dea"]
            if (
                check_idx_types["idx_macd"]["cross_up"]
                and idx_macd_dif[-1] > idx_macd_dea[-1]
                and idx_macd_dif[-2] < idx_macd_dea[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "macd",
                        "msg": "MACD上穿",
                        "frequency": frequency,
                        "cross": "up",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "down",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )
            if (
                check_idx_types["idx_macd"]["cross_down"]
                and idx_macd_dif[-1] < idx_macd_dea[-1]
                and idx_macd_dif[-2] > idx_macd_dea[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "macd",
                        "msg": "MACD下穿",
                        "frequency": frequency,
                        "cross": "down",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "up",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )

        if check_idx_types.get("idx_zhixing", {}).get("enable", 0):
            # DEBUG
            # print("DEBUG: Checking idx_zhixing")
            zhixing_data = Strategy.idx_zhixing(cd)
            short_trend = zhixing_data["short_trend"]
            long_short = zhixing_data["long_short"]
            close_prices = [k.c for k in cd.get_src_klines()]

            if (
                check_idx_types["idx_zhixing"]["cross_up"]
                and short_trend[-1] > long_short[-1]
                and short_trend[-2] < long_short[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "zhixing",
                        "msg": "知行短期趋势线上穿多空线",
                        "frequency": frequency,
                        "cross": "up",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "down",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )
            if (
                check_idx_types["idx_zhixing"]["cross_down"]
                and short_trend[-1] < long_short[-1]
                and short_trend[-2] > long_short[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "zhixing",
                        "msg": "知行短期趋势线下穿多空线",
                        "frequency": frequency,
                        "cross": "down",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "up",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )

            if (
                check_idx_types["idx_zhixing"].get("price_cross_short_up", 0)
                and close_prices[-1] > short_trend[-1]
                and close_prices[-2] < short_trend[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "zhixing",
                        "msg": "收盘价上穿知行短期趋势线",
                        "frequency": frequency,
                        "cross": "up",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "down",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )
            if (
                check_idx_types["idx_zhixing"].get("price_cross_short_down", 0)
                and close_prices[-1] < short_trend[-1]
                and close_prices[-2] > short_trend[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "zhixing",
                        "msg": "收盘价下穿知行短期趋势线",
                        "frequency": frequency,
                        "cross": "down",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "up",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )
            if (
                check_idx_types["idx_zhixing"].get("price_cross_long_up", 0)
                and close_prices[-1] > long_short[-1]
                and close_prices[-2] < long_short[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "zhixing",
                        "msg": "收盘价上穿知行多空线",
                        "frequency": frequency,
                        "cross": "up",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "down",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )
            if (
                check_idx_types["idx_zhixing"].get("price_cross_long_down", 0)
                and close_prices[-1] < long_short[-1]
                and close_prices[-2] > long_short[-2]
            ):
                jh_idx_msgs.append(
                    {
                        "type": "zhixing",
                        "msg": "收盘价下穿知行多空线",
                        "frequency": frequency,
                        "cross": "down",
                        "k_date": cd.get_src_klines()[-1].date,
                        "line_type": "up",
                        "price": cd.get_src_klines()[-1].c,
                    }
                )

    send_msgs = []
    # 记录缠论提醒信息
    for jh in jh_cl_msgs:
        line_type = "bi"
        if "bi" in jh.keys():
            is_done = "笔完成" if jh["bi"].is_done() else "笔未完成"
            is_td = "停顿:" + ("Yes" if jh["bi_td"] else "No")
        else:
            is_done = "线段完成" if jh["xd"].is_done() else "线段未完成"
            is_td = ""
            line_type = "xd"

        is_exists = db.alert_record_query_by_code(
            market, code, jh["frequency"], line_type, jh["line_dt"].replace(tzinfo=None)
        )

        if (
            is_exists is None
            or is_exists.bi_is_done != is_done
            or is_exists.bi_is_td != is_td
        ):
            fx_ld = f" FX:{jh['fx_ld']}" if "fx_ld" in jh.keys() else ""  # 分型力度
            
            # 增加价格信息
            price_info = ""
            if "bi" in jh.keys():
                price_info = f" 价格:{jh['bi'].end.val}"
            elif "xd" in jh.keys():
                price_info = f" 价格:{jh['xd'].end.val}"

            msg = f"触发 {jh['type']} ({is_done} - {is_td}{fx_ld}{price_info})"
            
            # 检查信号是否新鲜，如果是很久之前的信号（比如初始化加载时），则不发送消息，只记录
            is_fresh = True
            if "bi" in jh.keys():
                # 笔信号
                bi = jh["bi"]
                # 规则1: 只提示未完成笔的信号，或者如果是已完成的笔，必须是最后一笔
                # 用户反馈：is_done 过滤掉了刚完成但还没被新笔替代的信号，导致提醒不出来
                # 逻辑修正：
                # 1. 如果笔未完成 (is_done=False)，必然是最新信号 -> 提醒
                # 2. 如果笔已完成 (is_done=True)，但它是最后一笔 (index == last_bi.index) -> 提醒
                # 3. 只有当笔已完成且不是最后一笔时 -> 不提醒 (历史信号)
                
                # 获取数据对象
                cd = jh.get("cd")
                if cd and is_fresh:
                    last_bi = cd.get_bis()[-1]
                    
                    if bi.is_done():
                        # 如果已完成，必须是最后一笔才提醒
                        # 2025-01-11 Fix: 用户反馈部分信号仍未提醒
                        # 原因可能是 last_bi 获取到了后续还未成笔的分型结构（如果有虚拟笔逻辑的话，但现在虚拟笔逻辑已移除）
                        # 或者是数据更新不及时
                        # 这里放宽条件：只要 index 大于等于倒数第二笔，就尝试提醒（防止数据更新时恰好生成了新的一笔但还没完全确认）
                        # 还是严格一点：index == last_bi.index
                        if bi.index != last_bi.index:
                            is_fresh = False
                    else:
                        # 如果未完成，肯定是最后一笔（或者倒数第二笔被虚拟笔顶了一下，但虚拟笔不参与计算）
                        # 只要后面没有更晚的有效笔结构，就是新鲜的
                        # 简单起见，未完成的笔我们都认为是新鲜的（配合距离检查）
                        pass

                    # 额外检查：如果当前笔虽然是最后一笔，但后面已经有新的分型（虚拟笔或正在生成的笔）
                    # 导致当前笔不再是“最前沿”的信号？
                    # 实际上，如果后面有新笔生成，当前笔的 index 就不会等于 last_bi.index
                    # 所以上述逻辑已经覆盖了。
                
                # 规则2: 如果未完成笔之后已经出现了反向的一笔（哪怕未完成，只要有分型），则不再提示当前笔的信号
                # 这个逻辑其实已经被 "bi.index != last_bi.index" 覆盖了。
                # 如果后面出了反向一笔，last_bi 就是那个反向笔，bi.index != last_bi.index 成立，is_fresh = False
                
                # 规则3: 距离限制 (防止信号过旧)
                if cd and is_fresh:
                    current_k_index = cd.get_src_klines()[-1].index
                    # 信号产生于笔的结束分型处
                    signal_k_index = bi.end.k.index
                    # 阈值设为 9 (可根据需求调整)
                    if current_k_index - signal_k_index > 9:
                        is_fresh = False

            elif "xd" in jh.keys():
                # 线段信号
                xd = jh["xd"]
                # 规则1: 只提示未完成线段的信号，或者如果是已完成的线段，必须是最后一段
                # 逻辑同笔

                # 获取数据对象
                cd = jh.get("cd")
                if cd and is_fresh:
                    last_xd = cd.get_xds()[-1]

                    if xd.is_done():
                        if xd.index != last_xd.index:
                            is_fresh = False
                    else:
                        pass

                # 规则3: 距离限制
                if cd and is_fresh:
                   current_k_index = cd.get_src_klines()[-1].index
                   signal_k_index = xd.end.k.index
                   # 阈值设为 20 (可根据需求调整)
                   if current_k_index - signal_k_index > 20:
                       is_fresh = False
            
            if is_fresh:
                send_msgs.append(f"【{name} - {jh['frequency']}】{msg}")
            
            # 添加数据库记录
            db.alert_record_save(
                market,
                task_name,
                code,
                name,
                jh["frequency"],
                msg,
                is_done,
                is_td,
                line_type,
                jh["line_dt"],
            )
            # 添加图表标记
            db.marks_add_by_price(
                market,
                code,
                name,
                jh["frequency"],
                fun.datetime_to_int(jh["mark_dt"]),
                "A",
                msg,
                "green" if jh["line_type"] == "down" else "red",
                "red" if jh["line_type"] == "down" else "green",
            )
    # 记录指标提醒信息
    for jh in jh_idx_msgs:
        is_exists = db.alert_record_query_by_code(
            market, code, jh["frequency"], jh["type"], jh["k_date"]
        )
        if is_exists is None:
            # 之前没有，进行记录
            price_info = f" 价格:{jh['price']}" if "price" in jh else ""
            msg = f"触发 {jh['msg']}{price_info}"
            send_msgs.append(f"【{name} - {jh['frequency']}】{msg}")
            db.alert_record_save(
                market,
                task_name,
                code,
                name,
                jh["frequency"],
                msg,
                "--",
                "--",
                jh["type"],
                jh["k_date"],
            )

    # 沪深A股，增加行业概念信息
    if market == "a" and len(send_msgs) > 0:
        hygn = ex.stock_owner_plate(code)
        if len(hygn["HY"]) > 0:
            send_msgs.append("行业 : " + "/".join([_["name"] for _ in hygn["HY"]]))
        if len(hygn["GN"]) > 0:
            send_msgs.append("概念 : " + "/".join([_["name"] for _ in hygn["GN"]]))

    # 添加图片
    # 如果 is_send_img 为 None，则使用全局配置
    send_img = is_send_img if is_send_img is not None else config.FEISHU_KEYS["enable_img"]
    if is_send_msg and send_img and len(send_msgs) > 0:
        for cd in cl_datas:
            title = f"{name} - {cd.get_frequency()}"
            image_key = kchart_to_png(market, title, cd, cl_config, force=True)
            if image_key != "":
                send_msgs.append(image_key)
    # 发送消息
    if is_send_msg and len(send_msgs) > 0:
        send_fs_msg(market, f"{task_name} 监控提醒", send_msgs)

    return jh_cl_msgs


def kchart_to_png(market: str, title: str, cd: ICL, cl_config: dict, force: bool = False) -> str:
    """
    缠论数据保存图表并上传网络，返回访问地址
    """
    # 没有启用图片则不生产图片
    if force is False and config.FEISHU_KEYS["enable_img"] is False:
        return ""

    fs_keys = (
        config.FEISHU_KEYS[market]
        if market in config.FEISHU_KEYS.keys()
        else config.FEISHU_KEYS["default"]
    )

    png_path = config.get_data_path() / "png"
    if png_path.is_dir() is False:
        png_path.mkdir(parents=True)
    cl_config["chart_width"] = "100%"
    cl_config["chart_high"] = "400px"
    cl_config["chart_show_ma"] = False
    cl_config["chart_show_ama"] = False
    cl_config["chart_show_boll"] = False
    cl_config["chart_show_infos"] = False
    cl_config["chart_kline_nums"] = 600
    file_name = (
        cd.get_code().replace(".", "_").replace("/", "_").replace("@", "_")
        + "_"
        + cd.get_frequency()
    )
    cl_config["to_file"] = f"{str(png_path)}/{file_name}_{int(time.time())}.html"
    png_file = f"{str(png_path)}/{file_name}_{int(time.time())}.png"

    try:
        # 渲染并保存图片
        render_file = kcharts.render_charts(title, cd, config=cl_config)

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            # 设置页面的视口大小
            page.set_viewport_size({"width": 1000, "height": 400})
            page.goto(f"file://{render_file}")
            # 等待页面加载完成
            page.wait_for_load_state("domcontentloaded")
            # 截图
            page.screenshot(path=png_file, type="png", full_page=True)
            browser.close()

        # 上传图片
        # 创建client
        client = (
            lark.Client.builder()
            .app_id(fs_keys["app_id"])
            .app_secret(fs_keys["app_secret"])
            .log_level(lark.LogLevel.INFO)
            .build()
        )
        # 构造请求对象
        with open(png_file, "rb") as img_fp:
            request: CreateImageRequest = (
                CreateImageRequest.builder()
                .request_body(
                    CreateImageRequestBody.builder()
                    .image_type("message")
                    .image(img_fp)
                    .build()
                )
                .build()
            )
            # 发起请求
            response: CreateImageResponse = client.im.v1.image.create(request)
        return response.data.image_key
    except Exception as e:
        print(f"{title} 生成并上传图片异常：{e}")
        traceback.print_exc()
        return ""
    finally:
        # 删除本地图片
        if pathlib.Path(png_file).is_file():
            os.remove(png_file)


if __name__ == "__main__":
    from chanlun import cl
    from chanlun.cl_utils import query_cl_chart_config
    from chanlun.exchange.exchange_tdx import ExchangeTDX

    ex = ExchangeTDX()
    cl_config = query_cl_chart_config("a", "SH.000852")
    klines = ex.klines("SH.000852", "5m")
    cd = cl.CL("SH.000852", "5m", cl_config)
    cd.process_klines(klines)

    image_key = kchart_to_png("a", "缠论数据", cd, cl_config)
    print(image_key)

    send_fs_msg("a", "测试", ["测试消息", image_key])
