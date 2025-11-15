"""
AI 分析相关接口蓝图。

  - `/ai/analyse`
  - `/ai/analyse_records/<market>`
"""

from flask import Blueprint, request
from flask_login import login_required

from chanlun.tools.ai_analyse import AIAnalyse


ai_bp = Blueprint("ai", __name__)


@ai_bp.route("/ai/analyse", methods=["POST"])
@login_required
def ai_analyse():
    market = request.form["market"]
    code = request.form["code"]
    frequency = request.form["frequency"]

    ai_analyse_obj = AIAnalyse(market)
    ai_res = ai_analyse_obj.analyse(code, frequency)
    return ai_res


@ai_bp.route("/ai/analyse_records/<market>", methods=["GET"])
@login_required
def ai_analyse_records(market: str = "a"):
    ai_analyse_records = AIAnalyse(market=market).analyse_records(30)
    return {"code": 0, "msg": "", "count": len(ai_analyse_records), "data": ai_analyse_records}