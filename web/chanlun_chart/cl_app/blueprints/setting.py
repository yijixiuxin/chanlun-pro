"""
系统设置相关接口蓝图。

  - `/setting`
  - `/setting/save`
"""

from flask import Blueprint, render_template, request
from flask_login import login_required

from chanlun.db import db
from chanlun.security import decrypt_str, encrypt_str, mask_secret


setting_bp = Blueprint("setting", __name__)

# 前端"未修改"占位符：保存时若提交值等于该占位符则保留原密文不更新。
_SECRET_PLACEHOLDER = "__keep__"


@setting_bp.route("/setting", methods=["GET"])
@login_required
def setting():
    proxy = db.cache_get("req_proxy")
    fs_setting = db.cache_get("fs_keys") or {}
    # 读取时解密；历史明文记录会被原样返回（首次保存后改写为密文）。
    fs_app_secret_plain = decrypt_str(fs_setting.get("fs_app_secret"))
    set_config = {
        "fs_app_id": fs_setting.get("fs_app_id", ""),
        # 仅向前端回显遮蔽后的值，避免页面源码泄露完整 secret。
        "fs_app_secret_mask": mask_secret(fs_app_secret_plain),
        "fs_app_secret_placeholder": _SECRET_PLACEHOLDER,
        "fs_app_secret_set": bool(fs_app_secret_plain),
        "fs_user_id": fs_setting.get("fs_user_id", ""),
        "proxy_host": proxy["host"] if proxy is not None else "",
        "proxy_port": proxy["port"] if proxy is not None else "",
    }
    return render_template("setting.html", **set_config)


@setting_bp.route("/setting/save", methods=["POST"])
@login_required
def setting_save():
    proxy = {
        "host": request.form["proxy_host"],
        "port": request.form["proxy_port"],
    }

    submitted_secret = request.form.get("fs_app_secret", "")
    existing = db.cache_get("fs_keys") or {}
    if submitted_secret == _SECRET_PLACEHOLDER:
        # 用户未修改：保留原密文（若历史是明文则在这里完成一次升级写入）。
        existing_secret_plain = decrypt_str(existing.get("fs_app_secret"))
        encrypted_secret = encrypt_str(existing_secret_plain)
    else:
        encrypted_secret = encrypt_str(submitted_secret)

    fs_keys = {
        "fs_app_id": request.form["fs_app_id"],
        "fs_app_secret": encrypted_secret,
        "fs_user_id": request.form["fs_user_id"],
    }
    db.cache_set("req_proxy", proxy)
    db.cache_set("fs_keys", fs_keys)

    return {"ok": True}
