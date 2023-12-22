"""
监控相关代码
"""
import os
import time
from typing import List

from pyecharts.render import make_snapshot
from qiniu import Auth, put_file
from snapshot_selenium import snapshot

from chanlun import kcharts
from chanlun.cl_interface import ICL
from chanlun.cl_utils import web_batch_get_cl_datas, bi_td
from chanlun.exchange import get_exchange, Market
from chanlun.utils import send_dd_msg
from chanlun import config
from chanlun.db import db


def monitoring_code(
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
                    "line_dt": end_bi.end.k.date,
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
                    "line_dt": end_bi.end.k.date,
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
                        "line_dt": end_xd.end.k.date,
                    }
                    for bc_type in check_types["xd_beichi"]
                    if end_xd.bc_exists([bc_type], "|")
                )

                jh_msgs.extend(
                    {
                        "type": f"线段 {mmd_maps[mmd]}",
                        "frequency": frequency,
                        "xd": end_xd,
                        "line_dt": end_xd.end.k.date,
                    }
                    for mmd in check_types["xd_mmd"]
                    if end_xd.mmd_exists([mmd], "|")
                )

    send_msgs = ""
    for jh in jh_msgs:
        if "bi" in jh.keys():
            is_done = "笔完成" if jh["bi"].is_done() else "笔未完成"
            is_td = "停顿:" + ("Yes" if jh["bi_td"] else "No")
        else:
            is_done = "线段完成" if jh["xd"].is_done() else "线段未完成"
            is_td = ""

        is_exists = db.alert_record_query_by_code(
            market, code, jh["frequency"], jh["line_dt"]
        )

        if (
            is_exists is None
            or is_exists.bi_is_done != is_done
            or is_exists.bi_is_td != is_td
        ) and is_send_msg:
            msg = "【%s - %s】触发 %s (%s - %s) \n" % (
                name,
                jh["frequency"],
                jh["type"],
                is_done,
                is_td,
            )
            send_msgs += msg
            db.alert_record_save(
                market, code, name, jh["frequency"], msg, is_done, is_td, jh["line_dt"]
            )

    # 沪深A股，增加行业概念信息
    if market == "a" and send_msgs != "":
        hygn = ex.stock_owner_plate(code)
        if len(hygn["HY"]) > 0:
            send_msgs += "\n行业 : " + "/".join([_["name"] for _ in hygn["HY"]])
        if len(hygn["GN"]) > 0:
            send_msgs += "\n概念 : " + "/".join([_["name"] for _ in hygn["GN"]])
    # print('Send_msgs: ', send_msgs)

    # 添加图片
    if send_msgs != "":
        pics = []
        for cd in cl_datas:
            title = f"{name} - {cd.get_frequency()}"
            pic = kchart_to_png(title, cd, cl_config)
            if pic != "":
                pics.append(pic)
        if len(pics) > 0:
            # 有图片，将 text 转换成 markdown 类型
            for pic in pics:
                send_msgs += f"\n![pic]({pic})"
            send_msgs = {
                "title": send_msgs.split("\n")[0],
                "text": send_msgs.replace("\n", "\n\n"),
            }
    if len(send_msgs) > 0:
        send_dd_msg(market, send_msgs)

    return jh_msgs


def kchart_to_png(title: str, cd: ICL, cl_config: dict) -> str:
    """
    缠论数据保存图表并上传网络，返回访问地址
    """
    # 如果没有设置七牛云的 key，则不使用生成图片的功能
    if config.QINIU_AK == "":
        return ""

    try:
        cl_config["chart_width"] = "1000px"
        cl_config["chart_heigh"] = "800px"

        file_name = (
            cd.get_code().replace(".", "_").replace("/", "_").replace("@", "_")
            + "_"
            + cd.get_frequency()
        )
        cl_config["to_file"] = f"{file_name}_{int(time.time())}.html"
        png_file = f"{file_name}_{int(time.time())}.png"

        # 渲染并保存图片
        render_file = kcharts.render_charts(title, cd, config=cl_config)
        make_snapshot(snapshot, render_file, png_file, is_remove_html=True, delay=4)

        # 上传图片
        q = Auth(config.QINIU_AK, config.QINIU_SK)
        file_key = f"{config.QINIU_PATH}/{file_name}_{int(time.time())}.png"
        token = q.upload_token(config.QINIU_BUCKET_NAME, file_key, 3600)
        ret, info = put_file(token, file_key, png_file, version="v2")

        # 删除本地图片
        os.remove(png_file)

        return config.QINIU_URL + "/" + ret["key"]
    except Exception as e:
        print(f"{title} 生成并上传图片异常：{e}")
        return ""
