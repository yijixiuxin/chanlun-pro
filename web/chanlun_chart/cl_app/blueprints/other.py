"""
杂项接口蓝图。

  - `/ticks`
"""

import json

from flask import Blueprint, request
from flask_login import login_required

from chanlun.base import Market
from chanlun.exchange import get_exchange
from chanlun.tools.log_util import LogUtil


other_bp = Blueprint("other", __name__)

# /ticks 单次请求的代码数量上限：保护后端不被超大列表打爆，
# 也覆盖正常前端使用场景（自选页面通常 < 200 个）。
_MAX_TICK_CODES = 500

_VALID_MARKETS = {m.value for m in Market}


@other_bp.route("/ticks", methods=["POST"])
@login_required
def ticks():
    market = request.form.get("market", "")
    codes_raw = request.form.get("codes", "")

    if market not in _VALID_MARKETS:
        return {"now_trading": False, "ticks": [], "errmsg": "invalid_market"}

    try:
        codes = json.loads(codes_raw)
    except (json.JSONDecodeError, TypeError):
        return {"now_trading": False, "ticks": [], "errmsg": "invalid_codes_json"}

    if not isinstance(codes, list):
        return {"now_trading": False, "ticks": [], "errmsg": "codes_not_list"}
    if len(codes) > _MAX_TICK_CODES:
        return {"now_trading": False, "ticks": [], "errmsg": "too_many_codes"}
    # 元素必须是字符串，避免下游交易所 SDK 收到非法类型崩溃。
    if not all(isinstance(c, str) for c in codes):
        return {"now_trading": False, "ticks": [], "errmsg": "code_must_be_string"}

    try:
        ex = get_exchange(Market(market))
        stock_ticks = ex.ticks(codes)
        now_trading = ex.now_trading()
        res_ticks = [
            {"code": _c, "price": _t.last, "rate": round(float(_t.rate), 2)}
            for _c, _t in stock_ticks.items()
        ]
        return {"now_trading": now_trading, "ticks": res_ticks}
    except Exception:
        # 完整堆栈仅写日志，避免直接暴露给前端调用方。
        LogUtil.exception(f"/ticks failed market={market} codes_len={len(codes)}")
        return {"now_trading": False, "ticks": [], "errmsg": "internal_error"}
