"""
杂项接口蓝图。

  - `/ticks`
"""

import json
import traceback

from flask import Blueprint, request
from flask_login import login_required

from chanlun.base import Market
from chanlun.exchange import get_exchange


other_bp = Blueprint("other", __name__)


@other_bp.route("/ticks", methods=["POST"])
@login_required
def ticks():
    market = request.form["market"]
    codes = request.form["codes"]
    codes = json.loads(codes)
    ex = get_exchange(Market(market))
    stock_ticks = ex.ticks(codes)
    try:
        now_trading = ex.now_trading()
        res_ticks = [
            {"code": _c, "price": _t.last, "rate": round(float(_t.rate), 2)}
            for _c, _t in stock_ticks.items()
        ]
        return {"now_trading": now_trading, "ticks": res_ticks}
    except Exception:
        traceback.print_exc()
    return {"now_trading": False, "ticks": []}