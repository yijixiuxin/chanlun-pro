"""
系统设置相关接口蓝图。

  - `/setting`
  - `/setting/save`
"""

from flask import Blueprint, render_template, request
from flask_login import login_required

from chanlun.db import db


setting_bp = Blueprint("setting", __name__)


@setting_bp.route("/setting", methods=["GET"])
@login_required
def setting():
    proxy = db.cache_get("req_proxy")
    fs_setting = db.cache_get("fs_keys")
    set_config = {
        "fs_app_id": fs_setting["fs_app_id"] if fs_setting is not None else "",
        "fs_app_secret": (fs_setting["fs_app_secret"] if fs_setting is not None else ""),
        "fs_user_id": fs_setting["fs_user_id"] if fs_setting is not None else "",
        "proxy_host": proxy["host"] if proxy is not None else "",
        "proxy_port": proxy["port"] if proxy is not None else "",
    }
    return render_template("setting.html", **set_config)


@setting_bp.route("/setting/save", methods=["POST"])
@login_required
def setting_save():
    proxy = {"host": request.form["proxy_host"], "port": request.form["proxy_port"]}
    fs_keys = {
        "fs_app_id": request.form["fs_app_id"],
        "fs_app_secret": request.form["fs_app_secret"],
        "fs_user_id": request.form["fs_user_id"],
    }
    db.cache_set("req_proxy", proxy)
    db.cache_set("fs_keys", fs_keys)
    return {"ok": True}