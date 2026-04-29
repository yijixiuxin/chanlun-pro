import datetime
import json
import re
from typing import Any, Sequence

from chanlun import fun
from chanlun.cl_interface import ICL
from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from chanlun.exchange import Market, get_exchange
from chanlun.tools.ai_client import request_ai_model


def format_recent_kline_ma_data(
    klines: Sequence, precision: int, limit: int = 20
) -> list[dict[str, Any]]:
    """
    Format recent raw K lines with MA5/MA10 values for AI prompt context.
    """
    rows = []
    start = max(0, len(klines) - limit)
    for idx in range(start, len(klines)):
        k = klines[idx]
        ma5 = None
        ma10 = None
        if idx >= 4:
            ma5 = round(sum(_k.c for _k in klines[idx - 4 : idx + 1]) / 5, precision)
        if idx >= 9:
            ma10 = round(
                sum(_k.c for _k in klines[idx - 9 : idx + 1]) / 10, precision
            )
        rows.append(
            {
                "time": fun.datetime_to_str(k.date),
                "open": round(k.o, precision),
                "high": round(k.h, precision),
                "low": round(k.l, precision),
                "close": round(k.c, precision),
                "volume": round(k.a, precision),
                "ma5": ma5,
                "ma10": ma10,
            }
        )
    return rows


def format_recent_kline_ma_table(
    klines: Sequence, precision: int, limit: int = 20
) -> str:
    def fmt(value: Any) -> Any:
        if value is None:
            return "-"
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    rows = format_recent_kline_ma_data(klines, precision, limit)
    lines = [
        "| 时间 | 开盘 | 最高 | 最低 | 收盘 | 成交量 | MA5 | MA10 |",
        "|:---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['time']} | {fmt(row['open'])} | {fmt(row['high'])} | {fmt(row['low'])} | "
            f"{fmt(row['close'])} | {fmt(row['volume'])} | {fmt(row['ma5'])} | {fmt(row['ma10'])} |"
        )
    return "\n".join(lines)


class AITrendPredict:
    """
    AI 缠论走势预测。

    模型只负责输出严格 JSON 的走势假设，后端负责校验、补齐时间坐标并保存。
    """

    FREQUENCY_SECONDS = {
        "10s": 10,
        "30s": 30,
        "1m": 60,
        "2m": 120,
        "3m": 180,
        "5m": 300,
        "10m": 600,
        "15m": 900,
        "30m": 1800,
        "60m": 3600,
        "120m": 7200,
        "3h": 10800,
        "4h": 14400,
        "d": 86400,
        "2d": 172800,
        "w": 604800,
        "m": 2592000,
        "y": 31536000,
    }

    def __init__(self, market: str):
        self.market = market
        self.ex = get_exchange(market=Market(self.market))

    def predict(self, code: str, frequency: str) -> dict:
        stock = self.ex.stock_info(code)
        if stock is None:
            return {"ok": False, "msg": f"标的信息获取失败：{code}"}

        try:
            cd = self._load_cl_data(code, frequency)
            prompt = self.prompt(cd)
        except Exception as e:
            return {"ok": False, "msg": f"生成预测提示词异常：{e}"}

        ai_res = self.req_llm_ai_model(prompt)
        if ai_res["ok"] is False:
            return {"ok": False, "msg": ai_res["msg"]}

        try:
            raw_data = self.extract_response_json(ai_res["msg"])
            normalized = self.normalize_response(raw_data)
            normalized = self.fill_missing_times(normalized, cd, frequency)
        except Exception as e:
            return {"ok": False, "msg": f"AI 返回格式错误：{e}", "raw": ai_res["msg"]}

        record_id = self.save_prediction(
            code=code,
            stock_name=stock["name"],
            frequency=frequency,
            model=ai_res["model"],
            prompt=prompt,
            msg=normalized.get("msg", ""),
            predictions=normalized["predictions"],
            raw_response=ai_res["msg"],
        )
        return {
            "ok": True,
            "msg": normalized.get("msg", ""),
            "id": record_id,
            "model": ai_res["model"],
            "predictions": normalized["predictions"],
        }

    def _load_cl_data(self, code: str, frequency: str) -> ICL:
        cl_config = query_cl_chart_config(self.market, code)
        klines = self.ex.klines(code, frequency)
        return web_batch_get_cl_datas(
            self.market, code, {frequency: klines}, cl_config
        )[0]

    def prompt(self, cd: ICL) -> str:
        stock_info = self.ex.stock_info(cd.get_code())
        stock_name = stock_info["name"] if stock_info else cd.get_code()
        precision = (
            len(str(stock_info["precision"])) - 1
            if stock_info and "precision" in stock_info.keys()
            else 2
        )
        k = cd.get_src_klines()[-1]

        lines = []
        lines.append("# 角色与任务")
        lines.append(
            "你是缠论行情分析助手。请根据给定的笔、线段、中枢数据，推演后续最可能的笔走势。"
        )
        lines.append("预测必须是概率化假设，不得写成确定性结论。")
        lines.append("")
        lines.append("# 当前品种")
        lines.append(f"- 代码/名称：{cd.get_code()} - {stock_name}")
        lines.append(f"- 周期：{cd.get_frequency()}")
        lines.append(f"- 当前时间：{fun.datetime_to_str(k.date)}")
        lines.append(f"- 最新价格：{round(k.c, precision)}")
        lines.append("")
        lines.append("# 最近20根K线与均线")
        lines.append("以下表格按时间升序排列，字段为时间、开高低收、成交量、5日均线、10日均线。")
        lines.append(format_recent_kline_ma_table(cd.get_src_klines(), precision))
        lines.append("")
        lines.append("# 最新笔")
        for bi in cd.get_bis()[-9:]:
            lines.append(
                "- "
                f"{fun.datetime_to_str(bi.start.k.date)}->{fun.datetime_to_str(bi.end.k.date)} "
                f"{bi.type} {round(bi.start.val, precision)}->{round(bi.end.val, precision)} "
                f"done={bi.is_done()} mmd={bi.line_mmds('|')} bc={bi.line_bcs('|')}"
            )
        lines.append("")
        lines.append("# 最新线段")
        for xd in cd.get_xds()[-3:]:
            lines.append(
                "- "
                f"{fun.datetime_to_str(xd.start.k.date)}->{fun.datetime_to_str(xd.end.k.date)} "
                f"{xd.type} {round(xd.start.val, precision)}->{round(xd.end.val, precision)} "
                f"done={xd.is_done()} mmd={xd.line_mmds('|')} bc={xd.line_bcs('|')}"
            )
        lines.append("")
        lines.append("# 中枢")
        for zs_type in cd.get_config()["zs_bi_type"]:
            zss = cd.get_bi_zss(zs_type)
            for zs in zss[-2:]:
                lines.append(
                    "- "
                    f"{zs_type} {fun.datetime_to_str(zs.start.k.date)}->{fun.datetime_to_str(zs.end.k.date)} "
                    f"type={zs.type} gg={round(zs.gg, precision)} dd={round(zs.dd, precision)} "
                    f"zg={round(zs.zg, precision)} zd={round(zs.zd, precision)} level={zs.level}"
                )
        lines.append("")
        lines.append("# 严格输出要求")
        lines.append("只返回 JSON，不要 Markdown，不要代码块，不要额外解释。")
        lines.append("必须返回 1 到 3 个 predictions，并按 probability 从高到低排序。")
        lines.append("probability 必须是 0 到 1 的数字，所有候选概率之和不要求等于 1。")
        lines.append("每个 prediction 至少包含 1 条 bis，每条 bi 必须有两个点。")
        lines.append(
            "points 中优先使用 bar_offset 表示从当前 K 线之后第几根 K 线，bar_offset 必须为正整数；price 必须为数字。"
        )
        lines.append("如果给出 invalid_price，必须是数字；basis 用一句话说明缠论依据。")
        lines.append("")
        lines.append(
            json.dumps(
                {
                    "msg": "一句总述",
                    "predictions": [
                        {
                            "name": "候选走势名称",
                            "probability": 0.45,
                            "basis": "缠论推断依据",
                            "invalid_price": 0,
                            "bis": [
                                {
                                    "points": [
                                        {"bar_offset": 1, "price": 0},
                                        {"bar_offset": 5, "price": 0},
                                    ],
                                    "linestyle": "1",
                                    "text": "AI 45%",
                                }
                            ],
                        }
                    ],
                },
                ensure_ascii=False,
            )
        )
        return "\n".join(lines)

    @staticmethod
    def extract_response_json(content: str) -> dict:
        text = content.strip()
        fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.S | re.I)
        if fenced:
            text = fenced.group(1).strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise
            return json.loads(text[start : end + 1])

    @staticmethod
    def normalize_response(data: dict) -> dict:
        if not isinstance(data, dict):
            raise ValueError("根节点必须是 JSON object")
        raw_predictions = data.get("predictions")
        if not isinstance(raw_predictions, list):
            raise ValueError("predictions 必须是数组")

        predictions = []
        for raw_prediction in raw_predictions[:3]:
            try:
                probability = float(raw_prediction.get("probability"))
            except (TypeError, ValueError):
                continue
            if probability < 0 or probability > 1:
                continue

            bis = []
            for raw_bi in raw_prediction.get("bis", []):
                points = []
                for raw_point in raw_bi.get("points", []):
                    point = {"price": float(raw_point["price"])}
                    if "time" in raw_point:
                        point["time"] = int(raw_point["time"])
                    if "bar_offset" in raw_point:
                        point["bar_offset"] = max(1, int(raw_point["bar_offset"]))
                    points.append(point)
                if len(points) != 2:
                    continue
                bi = {
                    "points": points,
                    "linestyle": str(raw_bi.get("linestyle", "1")),
                    "text": str(raw_bi.get("text", f"AI {round(probability * 100)}%")),
                }
                bis.append(bi)

            if not bis:
                continue
            invalid_price = raw_prediction.get("invalid_price")
            predictions.append(
                {
                    "name": str(raw_prediction.get("name", "AI预测走势")),
                    "probability": probability,
                    "basis": str(raw_prediction.get("basis", "")),
                    "invalid_price": (
                        None if invalid_price in [None, ""] else float(invalid_price)
                    ),
                    "bis": bis,
                }
            )

        if not predictions:
            raise ValueError("没有有效的预测笔数据")
        predictions.sort(key=lambda item: item["probability"], reverse=True)
        return {"msg": str(data.get("msg", "")), "predictions": predictions}

    @classmethod
    def fill_missing_times(cls, data: dict, cd: ICL, frequency: str) -> dict:
        current_time = fun.datetime_to_int(cd.get_src_klines()[-1].date)
        step_seconds = cls.FREQUENCY_SECONDS.get(frequency, 86400)
        for prediction in data["predictions"]:
            for bi in prediction["bis"]:
                for point in bi["points"]:
                    if "time" not in point:
                        point["time"] = (
                            current_time + point["bar_offset"] * step_seconds
                        )
                    point.pop("bar_offset", None)
        return data

    def save_prediction(
        self,
        code: str,
        stock_name: str,
        frequency: str,
        model: str,
        prompt: str,
        msg: str,
        predictions: list[dict[str, Any]],
        raw_response: str,
    ) -> int:
        from chanlun.db import TableByAIPrediction, db

        with db.Session() as session:
            record = TableByAIPrediction(
                market=self.market,
                stock_code=code,
                stock_name=stock_name,
                frequency=frequency,
                dt=datetime.datetime.now(),
                model=model,
                prompt=prompt,
                msg=msg,
                predictions=json.dumps(predictions, ensure_ascii=False),
                raw_response=raw_response,
            )
            session.add(record)
            session.commit()
            return record.id

    def prediction_records(
        self, code: str = None, frequency: str = None, page: int = 1, limit: int = 20
    ) -> tuple[list[dict], int]:
        from chanlun.db import TableByAIPrediction, db

        with db.Session() as session:
            query = session.query(TableByAIPrediction).filter(
                TableByAIPrediction.market == self.market
            )
            if code:
                query = query.filter(TableByAIPrediction.stock_code == code)
            if frequency:
                query = query.filter(TableByAIPrediction.frequency == frequency)

            total = query.count()
            records = (
                query.order_by(TableByAIPrediction.dt.desc())
                .offset((page - 1) * limit)
                .limit(limit)
                .all()
            )

            data = []
            for record in records:
                item = record.__dict__.copy()
                item.pop("_sa_instance_state", None)
                item["dt"] = fun.datetime_to_str(item["dt"])
                item["predictions"] = json.loads(item["predictions"])
                data.append(item)
            return data, total

    def delete_prediction(self, record_id: int) -> bool:
        from chanlun.db import TableByAIPrediction, db

        with db.Session() as session:
            deleted = (
                session.query(TableByAIPrediction)
                .filter(
                    TableByAIPrediction.market == self.market,
                    TableByAIPrediction.id == record_id,
                )
                .delete()
            )
            session.commit()
        return deleted > 0

    def req_llm_ai_model(self, prompt: str) -> dict:
        return request_ai_model(
            prompt,
            response_format={"type": "json_object"},
        )
