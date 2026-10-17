"""TradingView 独立存储 API（Flask Blueprint）。

- 统一响应信封：成功 ``{"status": "ok", "data": ...}``；
  失败 ``{"status": "error", "message": "..."}`` + 4xx 状态码。
- 所有接口 ``@login_required``。
- 布局/绘图按 ``user_id + client_id`` 过滤；模板只按 ``user_id`` 过滤。
"""

import json

from flask import Blueprint, jsonify, request
from flask_login import login_required

from chanlun.db import TV_STORAGE_USER_ID, db

tv_storage_bp = Blueprint("tv_storage", __name__, url_prefix="/tv/storage")

# SharingMode 字符串枚举 -> 数字（库内运行时是数字）
_SHARING_MODE_MAP = {
    "NotShared": 0,
    "SharedInLayout": 1,
    "GloballyShared": 2,
}


def _ok(data=None):
    return jsonify({"status": "ok", "data": data})


def _error(message, status_code=400):
    return jsonify({"status": "error", "message": message}), status_code


def _client_id():
    """图表实例作用域参数（布局/绘图/迁移状态接口必填）"""
    return str(request.args.get("client") or "").strip()


def _int_value(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _bool_value(value):
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _normalize_sharing_mode(value):
    """SharingMode 数字/字符串统一归一化为 0/1/2"""
    if isinstance(value, str):
        value = value.strip()
        if value in _SHARING_MODE_MAP:
            return _SHARING_MODE_MAP[value]
    return _int_value(value, 0)


# ----------------------------------------------------------------------
# 布局（charts）
# ----------------------------------------------------------------------
@tv_storage_bp.route("/charts", methods=["GET"])
@login_required
def list_charts():
    client_id = _client_id()
    if not client_id:
        return _error("client 不能为空")

    charts = db.tv_storage_list_charts(client_id, TV_STORAGE_USER_ID)
    return _ok(
        [
            {
                "id": c.id,
                "name": c.name,
                "symbol": c.symbol,
                "resolution": c.resolution,
                "timestamp": c.timestamp,
                "drawings_migrated": bool(c.drawings_migrated),
            }
            for c in charts
        ]
    )


@tv_storage_bp.route("/charts", methods=["POST"])
@login_required
def save_chart():
    client_id = _client_id()
    if not client_id:
        return _error("client 不能为空")

    body = request.get_json(silent=True) or {}

    raw_id = body.get("id")
    chart_id = None if raw_id in (None, "") else _int_value(raw_id, None)

    raw_timestamp = body.get("timestamp")
    timestamp = None if raw_timestamp is None else _int_value(raw_timestamp, None)

    content = body.get("content")
    if not isinstance(content, str):
        content = "" if content is None else json.dumps(content, ensure_ascii=False)

    saved_id = db.tv_storage_save_chart(
        client_id,
        TV_STORAGE_USER_ID,
        chart_id,
        body.get("name") or "",
        body.get("symbol") or "",
        body.get("resolution") or "",
        content,
        timestamp=timestamp,
    )
    return _ok({"id": saved_id})


@tv_storage_bp.route("/charts/<int:chart_id>/content", methods=["GET"])
@login_required
def get_chart_content(chart_id):
    client_id = _client_id()
    if not client_id:
        return _error("client 不能为空")

    content = db.tv_storage_get_chart_content(client_id, TV_STORAGE_USER_ID, chart_id)
    if content is None:
        return _error("布局不存在", 404)
    return _ok({"content": content})


@tv_storage_bp.route("/charts/<int:chart_id>", methods=["DELETE"])
@login_required
def delete_chart(chart_id):
    client_id = _client_id()
    if not client_id:
        return _error("client 不能为空")

    # 不存在也返回 ok（幂等），并连带删除该布局绘图
    db.tv_storage_delete_chart(client_id, TV_STORAGE_USER_ID, chart_id)
    return _ok(None)


@tv_storage_bp.route("/migration_status", methods=["GET"])
@login_required
def migration_status():
    client_id = _client_id()
    if not client_id:
        return _error("client 不能为空")

    return _ok(db.tv_storage_migration_status(client_id, TV_STORAGE_USER_ID))


# ----------------------------------------------------------------------
# 指标模板（复用 cl_tv_charts chart_type='template'）
# ----------------------------------------------------------------------
@tv_storage_bp.route("/study_templates", methods=["GET"])
@login_required
def list_study_templates():
    names = db.tv_storage_list_study_templates(TV_STORAGE_USER_ID)
    return _ok([{"name": name} for name in names])


@tv_storage_bp.route("/study_templates/content", methods=["GET"])
@login_required
def get_study_template_content():
    name = str(request.args.get("name") or "")
    if not name:
        return _error("name 不能为空")

    content = db.tv_storage_get_study_template_content(TV_STORAGE_USER_ID, name)
    if content is None:
        return _error("模板不存在", 404)
    return _ok({"name": name, "content": content})


@tv_storage_bp.route("/study_templates", methods=["POST"])
@login_required
def save_study_template():
    body = request.get_json(silent=True) or {}
    name = str(body.get("name") or "")
    if not name:
        return _error("name 不能为空")

    db.tv_storage_save_study_template(TV_STORAGE_USER_ID, name, body.get("content"))
    return _ok(None)


@tv_storage_bp.route("/study_templates", methods=["DELETE"])
@login_required
def delete_study_template():
    name = str(request.args.get("name") or "")
    if not name:
        return _error("name 不能为空")

    if not db.tv_storage_delete_study_template(TV_STORAGE_USER_ID, name):
        return _error("模板不存在", 404)
    return _ok(None)


# ----------------------------------------------------------------------
# 画图模板
# ----------------------------------------------------------------------
@tv_storage_bp.route("/drawing_templates", methods=["GET"])
@login_required
def list_drawing_templates():
    tool = str(request.args.get("tool") or "")
    if not tool:
        return _error("tool 不能为空")

    names = db.tv_storage_list_drawing_templates(TV_STORAGE_USER_ID, tool)
    return _ok([{"name": name} for name in names])


@tv_storage_bp.route("/drawing_templates/content", methods=["GET"])
@login_required
def get_drawing_template_content():
    tool = str(request.args.get("tool") or "")
    name = str(request.args.get("name") or "")
    if not tool or not name:
        return _error("tool/name 不能为空")

    content = db.tv_storage_get_drawing_template_content(TV_STORAGE_USER_ID, tool, name)
    if content is None:
        return _error("模板不存在", 404)
    return _ok({"name": name, "content": content})


@tv_storage_bp.route("/drawing_templates", methods=["POST"])
@login_required
def save_drawing_template():
    body = request.get_json(silent=True) or {}
    tool = str(body.get("tool") or "")
    name = str(body.get("name") or "")
    if not tool or not name:
        return _error("tool/name 不能为空")

    db.tv_storage_save_drawing_template(
        TV_STORAGE_USER_ID, tool, name, body.get("content")
    )
    return _ok(None)


@tv_storage_bp.route("/drawing_templates", methods=["DELETE"])
@login_required
def delete_drawing_template():
    tool = str(request.args.get("tool") or "")
    name = str(request.args.get("name") or "")
    if not tool or not name:
        return _error("tool/name 不能为空")

    if not db.tv_storage_delete_drawing_template(TV_STORAGE_USER_ID, tool, name):
        return _error("模板不存在", 404)
    return _ok(None)


# ----------------------------------------------------------------------
# 图表模板（content 为任意 JSON）
# ----------------------------------------------------------------------
@tv_storage_bp.route("/chart_templates", methods=["GET"])
@login_required
def list_chart_templates():
    names = db.tv_storage_list_chart_templates(TV_STORAGE_USER_ID)
    return _ok([{"name": name} for name in names])


@tv_storage_bp.route("/chart_templates/content", methods=["GET"])
@login_required
def get_chart_template_content():
    name = str(request.args.get("name") or "")
    if not name:
        return _error("name 不能为空")

    content = db.tv_storage_get_chart_template_content(TV_STORAGE_USER_ID, name)
    if content is None:
        return _error("模板不存在", 404)
    return _ok({"name": name, "content": content})


@tv_storage_bp.route("/chart_templates", methods=["POST"])
@login_required
def save_chart_template():
    body = request.get_json(silent=True) or {}
    name = str(body.get("name") or "")
    if not name:
        return _error("name 不能为空")

    db.tv_storage_save_chart_template(TV_STORAGE_USER_ID, name, body.get("content"))
    return _ok(None)


@tv_storage_bp.route("/chart_templates", methods=["DELETE"])
@login_required
def delete_chart_template():
    name = str(request.args.get("name") or "")
    if not name:
        return _error("name 不能为空")

    if not db.tv_storage_delete_chart_template(TV_STORAGE_USER_ID, name):
        return _error("模板不存在", 404)
    return _ok(None)


# ----------------------------------------------------------------------
# 绘图（line tools，独立存储）
# ----------------------------------------------------------------------
@tv_storage_bp.route("/line_tools", methods=["GET"])
@login_required
def load_line_tools():
    client_id = _client_id()
    if not client_id:
        return _error("client 不能为空")

    layout_id = str(request.args.get("layout_id") or "").strip()
    if not layout_id:
        return _error("layout_id 不能为空")

    data = db.tv_storage_load_line_tools(
        client_id,
        TV_STORAGE_USER_ID,
        layout_id,
        chart_id=_int_value(request.args.get("chart_id"), 0),
        symbol=request.args.get("symbol") or None,
        all_symbols=_bool_value(request.args.get("all_symbols")),
    )
    return _ok(data)


@tv_storage_bp.route("/line_tools", methods=["POST"])
@login_required
def save_line_tools():
    client_id = _client_id()
    if not client_id:
        return _error("client 不能为空")

    body = request.get_json(silent=True) or {}
    layout_id = body.get("layout_id")
    if layout_id is None or not str(layout_id).strip():
        return _error("layout_id 不能为空")

    sources = body.get("sources") or {}
    groups = body.get("groups") or {}
    if not isinstance(sources, dict):
        sources = {}
    if not isinstance(groups, dict):
        groups = {}

    data = db.tv_storage_save_line_tools(
        client_id,
        TV_STORAGE_USER_ID,
        str(layout_id),
        _int_value(body.get("chart_id"), 0),
        _normalize_sharing_mode(body.get("sharing_mode")),
        body.get("symbol"),
        sources,
        groups,
    )
    return _ok(data)
