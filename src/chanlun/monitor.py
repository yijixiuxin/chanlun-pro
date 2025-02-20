"""
监控相关代码
"""

import os
import pathlib
import time
from typing import List

from pyecharts.render import make_snapshot
from snapshot_selenium import snapshot

import lark_oapi as lark
from lark_oapi.api.im.v1 import *

from chanlun import kcharts
from chanlun.cl_interface import ICL
from chanlun.cl_utils import web_batch_get_cl_datas, bi_td
from chanlun.exchange import get_exchange, Market
from chanlun.utils import send_fs_msg
from chanlun import config
from chanlun.db import db
import traceback


def monitoring_code(
    task_name: str,
    market: str,
    code: str,
    name: str,
    frequencys: list,
    check_types: dict = None,
    is_send_msg: bool = False,
    cl_config=None,
):
    """
    监控指定股票是否出现指定的信号
    :param market: 市场
    :param code: 代码
    :param name: 名称
    :param frequencys: 检查周期
    :param check_types: 监控项目
    :param is_send_msg: 是否发送消息
    :param cl_config: 缠论配置
    :return:
    """
    if check_types is None:
        check_types = {
            "bi_types": ["up", "down"],
            "bi_beichi": [],
            "bi_mmd": [],
            "xd_types": ["up", "down"],
            "xd_beichi": [],
            "xd_mmd": [],
        }

    if (
        len(check_types["bi_beichi"]) == 0
        and len(check_types["bi_mmd"]) == 0
        and len(check_types["xd_beichi"]) == 0
        and len(check_types["xd_mmd"]) == 0
    ):
        return ""

    ex = get_exchange(Market(market))

    klines = {f: ex.klines(code, f) for f in frequencys}
    cl_datas: List[ICL] = web_batch_get_cl_datas(market, code, klines, cl_config)

    jh_msgs = []  # 这里保存当前触发的所有机会信息
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
        if end_bi.type in check_types["bi_types"]:
            jh_msgs.extend(
                {
                    "type": f"笔 {end_bi.type} {bc_maps[bc_type]}",
                    "frequency": frequency,
                    "bi": end_bi,
                    "bi_td": bi_td(end_bi, cd),
                    "fx_ld": end_bi.end.ld(),
                    "line_dt": end_bi.start.k.date,
                }
                for bc_type in check_types["bi_beichi"]
                if end_bi.bc_exists([bc_type], "|")
            )

            jh_msgs.extend(
                {
                    "type": f"笔 {mmd_maps[mmd]}",
                    "frequency": frequency,
                    "bi": end_bi,
                    "bi_td": bi_td(end_bi, cd),
                    "fx_ld": end_bi.end.ld(),
                    "line_dt": end_bi.start.k.date,
                }
                for mmd in check_types["bi_mmd"]
                if end_bi.mmd_exists([mmd], "|")
            )

        if end_xd:
            # 检查背驰和买卖点
            if end_xd.type in check_types["xd_types"]:
                jh_msgs.extend(
                    {
                        "type": f"线段 {end_xd.type} {bc_maps[bc_type]}",
                        "frequency": frequency,
                        "xd": end_xd,
                        "line_dt": end_xd.start.k.date,
                    }
                    for bc_type in check_types["xd_beichi"]
                    if end_xd.bc_exists([bc_type], "|")
                )

                jh_msgs.extend(
                    {
                        "type": f"线段 {mmd_maps[mmd]}",
                        "frequency": frequency,
                        "xd": end_xd,
                        "line_dt": end_xd.start.k.date,
                    }
                    for mmd in check_types["xd_mmd"]
                    if end_xd.mmd_exists([mmd], "|")
                )

    send_msgs = []
    for jh in jh_msgs:
        line_type = "bi"
        if "bi" in jh.keys():
            is_done = "笔完成" if jh["bi"].is_done() else "笔未完成"
            is_td = "停顿:" + ("Yes" if jh["bi_td"] else "No")
        else:
            is_done = "线段完成" if jh["xd"].is_done() else "线段未完成"
            is_td = ""
            line_type = "xd"

        is_exists = db.alert_record_query_by_code(
            market, code, jh["frequency"], line_type, jh["line_dt"]
        )

        if (
            is_exists is None
            or is_exists.bi_is_done != is_done
            or is_exists.bi_is_td != is_td
        ) and is_send_msg:
            fx_ld = f" FX:{jh['fx_ld']}" if "fx_ld" in jh.keys() else ""  # 分型力度
            msg = f"【{name} - {jh['frequency']}】触发 {jh['type']} ({is_done} - {is_td}{fx_ld})"
            send_msgs.append(msg)
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

    # 沪深A股，增加行业概念信息
    if market == "a" and len(send_msgs) > 0:
        hygn = ex.stock_owner_plate(code)
        if len(hygn["HY"]) > 0:
            send_msgs.append("行业 : " + "/".join([_["name"] for _ in hygn["HY"]]))
        if len(hygn["GN"]) > 0:
            send_msgs.append("概念 : " + "/".join([_["name"] for _ in hygn["GN"]]))

    # 添加图片
    if len(send_msgs) > 0:
        for cd in cl_datas:
            title = f"{name} - {cd.get_frequency()}"
            image_key = kchart_to_png(market, title, cd, cl_config)
            if image_key != "":
                send_msgs.append(image_key)
    # 发送消息
    if len(send_msgs) > 0:
        send_fs_msg(market, f"{task_name} 监控提醒", send_msgs)

    return jh_msgs


def kchart_to_png(market: str, title: str, cd: ICL, cl_config: dict) -> str:
    """
    缠论数据保存图表并上传网络，返回访问地址
    """
    # 没有启用图片则不生产图片
    if config.FEISHU_KEYS["enable_img"] == False:
        return ""

    fs_keys = (
        config.FEISHU_KEYS[market]
        if market in config.FEISHU_KEYS.keys()
        else config.FEISHU_KEYS["default"]
    )

    png_path = config.get_data_path() / "png"
    if png_path.is_dir() is False:
        png_path.mkdir(parents=True)
    cl_config["chart_width"] = "1000px"
    cl_config["chart_heigh"] = "800px"
    cl_config["chart_kline_nums"] = 600
    file_name = (
        cd.get_code().replace(".", "_").replace("/", "_").replace("@", "_")
        + "_"
        + cd.get_frequency()
    )
    cl_config["to_file"] = f"{file_name}_{int(time.time())}.html"
    png_file = f"{str(png_path)}/{file_name}_{int(time.time())}.png"

    try:
        # 渲染并保存图片
        render_file = kcharts.render_charts(title, cd, config=cl_config)
        make_snapshot(snapshot, render_file, png_file, is_remove_html=True, delay=4)

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
    from chanlun.exchange.exchange_tdx import ExchangeTDX
    from chanlun.cl_utils import query_cl_chart_config
    from chanlun import cl

    ex = ExchangeTDX()
    cl_config = query_cl_chart_config("a", "SH.000001")
    klines = ex.klines("SH.600519", "d")
    cd = cl.CL("SH.600519", "d", cl_config).process_klines(klines)

    image_key = kchart_to_png("a", "缠论数据", cd, cl_config)
    print(image_key)

    send_fs_msg("a", "测试", ["测试消息", image_key])
