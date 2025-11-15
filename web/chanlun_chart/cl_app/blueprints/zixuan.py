"""
自选（自选组与股票）相关接口蓝图。

  - `/get_zixuan_groups/<market>`
  - `/get_zixuan_stocks/<market>/<group_name>`
  - `/get_stock_zixuan/<market>/<code>`
  - `/zixuan_group/<market>`
  - `/opt_zixuan_group/<market>`
  - `/zixuan_opt_export`
  - `/zixuan_opt_import`
  - `/set_stock_zixuan`
"""

import json
import os

from flask import Blueprint, render_template, request, send_file
from flask_login import login_required

from chanlun.base import Market
from chanlun.config import get_data_path
from chanlun.exchange import get_exchange
from chanlun.zixuan import ZiXuan


zixuan_bp = Blueprint("zixuan", __name__)


@zixuan_bp.route("/get_zixuan_groups/<market>")
@login_required
def get_zixuan_groups(market):
    zx = ZiXuan(market)
    groups = zx.get_zx_groups()
    return groups


@zixuan_bp.route("/get_zixuan_stocks/<market>/<group_name>")
@login_required
def get_zixuan_stocks(market, group_name):
    zx = ZiXuan(market)
    stock_list = zx.zx_stocks(group_name)
    return {"code": 0, "msg": "", "count": len(stock_list), "data": stock_list}


@zixuan_bp.route("/get_stock_zixuan/<market>/<code>")
@login_required
def get_stock_zixuan(market, code: str):
    code = code.replace("__", "/")  # 数字货币特殊处理
    zx = ZiXuan(market)
    zx_groups = zx.query_code_zx_names(code)
    return zx_groups


@zixuan_bp.route("/zixuan_group/<market>", methods=["GET"])
@login_required
def zixuan_group_view(market):
    zx = ZiXuan(market)
    zx_groups = zx.get_zx_groups()
    return render_template("zixuan.html", market=market, zx_groups=zx_groups)


@zixuan_bp.route("/opt_zixuan_group/<market>", methods=["POST"])
@login_required
def opt_zixuan_group(market):
    """
    操作自选组
    """
    opt = request.form["opt"]
    zx_group = request.form["zx_group"]
    zx = ZiXuan(market)
    if opt == "DEL":
        return {"ok": zx.del_zx_group(zx_group)}
    else:
        return {"ok": zx.add_zx_group(zx_group)}


@zixuan_bp.route("/zixuan_opt_export", methods=["GET"])
@login_required
def opt_zixuan_export():
    """
    导出自选组
    """
    market = request.args.get("market")
    zx_group = request.args.get("zx_group")
    zx = ZiXuan(market)
    stock_list = zx.zx_stocks(zx_group)
    output = ""
    for s in stock_list:
        output += f"{s['code']},{s['name']}\n"
    try:
        down_file = get_data_path() / "zx.txt"
        down_file.write_text(output, encoding="utf-8")
        return send_file(
            down_file, as_attachment=True, download_name=f"zixuan_{zx_group}.txt"
        )
    finally:
        try:
            os.remove(down_file)
        except Exception:
            pass


@zixuan_bp.route("/zixuan_opt_import", methods=["POST"])
@login_required
def opt_zixuan_import():
    """
    导入自选
    """
    market = request.form["market"]
    zx_group = request.form["zx_group"]
    file = request.files["file"]
    import_file = get_data_path() / "zx.txt"
    file.save(import_file)
    zx = ZiXuan(market)
    ex = get_exchange(Market(market))
    import_nums = 0
    market_all_stocks = ex.all_stocks()
    market_all_codes = [s["code"] for s in market_all_stocks]
    with open(import_file, "r", encoding="utf-8") as fp:
        for line in fp.readlines():
            try:
                import_infos = line.strip().split(",")
                if len(import_infos) >= 2:
                    code = import_infos[0].strip()
                    name = import_infos[1].strip()
                else:
                    code = import_infos[0].strip()
                    name = None

                # 股票代码兼容性处理
                if market == "a":
                    code = code.replace("SHSE.", "SH.").replace("SZSE.", "SZ.")

                if code not in market_all_codes:
                    same_codes = [_c for _c in market_all_codes if code in _c]
                    if len(same_codes) == 1:
                        code = same_codes[0]
                    else:
                        continue

                zx.add_stock(zx_group, code, name)
                import_nums += 1
            except Exception as e:
                print(line, e)

    try:
        os.remove(import_file)
    except Exception:
        pass

    return {"ok": True, "msg": f"成功导入 {import_nums} 条记录"}


@zixuan_bp.route("/set_stock_zixuan", methods=["POST"])
@login_required
def set_stock_zixuan():
    market = request.form["market"]
    opt = request.form["opt"]
    group_name = request.form["group_name"]
    code = request.form["code"]
    zx = ZiXuan(market)
    if opt == "DEL":
        res = zx.del_stock(group_name, code)
    elif opt == "ADD":
        res = zx.add_stock(group_name, code, None)
    elif opt == "COLOR":
        color = request.form["color"]
        res = zx.color_stock(group_name, code, color)
    elif opt == "SORT":
        direction = request.form["direction"]
        if direction == "top":
            res = zx.sort_top_stock(group_name, code)
        else:
            res = zx.sort_bottom_stock(group_name, code)
    else:
        res = False

    return {"ok": res}