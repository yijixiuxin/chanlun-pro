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
            ma10 = round(sum(_k.c for _k in klines[idx - 9 : idx + 1]) / 10, precision)
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
            prediction_payload=normalized,
            raw_response=ai_res["msg"],
        )
        return {
            "ok": True,
            "msg": normalized.get("msg", ""),
            "id": record_id,
            "model": ai_res["model"],
            "complete_classification": normalized["complete_classification"],
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
            "你是缠论行情分析助手。请根据给定的笔、线段、中枢数据，基于“走势必完美”和“完全分类”推演后续走势。"
        )
        lines.append(
            "任务不是猜唯一未来，而是穷尽当前结构后续可能进入的有限分类，并给出每类的触发、边界、失效与应对。"
        )
        lines.append(
            "预测必须是概率化假设，不得写成确定性结论；每个分类必须有明确边界条件，避免含糊表述。"
        )
        lines.append("")
        lines.append("# 当前品种")
        lines.append(f"- 代码/名称：{cd.get_code()} - {stock_name}")
        lines.append(f"- 周期：{cd.get_frequency()}")
        lines.append(f"- 当前时间：{fun.datetime_to_str(k.date)}")
        lines.append(f"- 最新价格：{round(k.c, precision)}")
        lines.append("")
        lines.append("# 最近20根K线与均线")
        lines.append(
            "以下表格按时间升序排列，字段为时间、开高低收、成交量、5日均线、10日均线。"
        )
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
        lines.append(
            "必须返回 complete_classification.classes，分类应覆盖当前结构之后的全部主要演化，不允许只给单一路径。"
        )
        lines.append(
            "通常至少包含：向上离开/突破、向下离开/破坏、继续围绕中枢震荡或延伸；若当前结构不适用，可按实际结构重命名，但要穷尽。"
        )
        lines.append(
            "每个 class 的 probability 必须是 0 到 1 的数字，并按 probability 从高到低排序。"
        )
        lines.append("每个 class 至少包含 1 条 bis，每条 bi 必须有两个点。")
        lines.append(
            "每个 class 必须包含 trigger、boundary、action、basis；levels 可给触发线/失效线/中枢边界。"
        )
        lines.append(
            "points 中优先使用 bar_offset 表示从当前 K 线之后第几根 K 线，bar_offset 必须为正整数；price 必须为数字。"
        )
        lines.append("如果给出 invalid_price，必须是数字；basis 用一句话说明缠论依据。")
        lines.append("")
        lines.append(
            json.dumps(
                {
                    "msg": "一句总述：当前结构的完全分类结论",
                    "complete_classification": {
                        "summary": "完全分类总述",
                        "current_structure": "当前缠论结构，例如：30分钟中枢震荡末端/上升线段未完成",
                        "classes": [
                            {
                                "key": "up_break",
                                "name": "向上突破/离开",
                                "direction": "up",
                                "probability": 0.4,
                                "trigger": "触发条件，例如有效站上ZG并回抽不入中枢",
                                "boundary": "分类边界，例如ZG=0，ZD=0",
                                "action": "应对，例如等待三买确认后跟随",
                                "basis": "缠论依据",
                                "invalid_price": 0,
                                "bis": [
                                    {
                                        "points": [
                                            {"bar_offset": 0, "price": 0},
                                            {"bar_offset": 5, "price": 0},
                                        ],
                                        "linestyle": "1",
                                        "text": "上破 40%",
                                    }
                                ],
                                "levels": [
                                    {
                                        "price": 0,
                                        "type": "trigger",
                                        "text": "触发线",
                                    },
                                    {
                                        "price": 0,
                                        "type": "invalid",
                                        "text": "失效线",
                                    },
                                ],
                            }
                        ],
                    },
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

        classification = AITrendPredict.normalize_complete_classification(data)
        if not classification["classes"]:
            raise ValueError("没有有效的完全分类数据")
        classification["classes"].sort(
            key=lambda item: item["probability"], reverse=True
        )
        return {
            "msg": str(data.get("msg", "")),
            "complete_classification": classification,
        }

    @staticmethod
    def normalize_bis(raw_bis: list, probability: float) -> list[dict]:
        bis = []
        if not isinstance(raw_bis, list):
            return bis
        for raw_bi in raw_bis:
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
        return bis

    @staticmethod
    def normalize_complete_classification(data: dict) -> dict:
        raw_classification = data.get("complete_classification")
        if not isinstance(raw_classification, dict):
            raise ValueError("complete_classification 必须是 object")
        raw_classes = raw_classification.get("classes", [])
        if not isinstance(raw_classes, list):
            raise ValueError("complete_classification.classes 必须是数组")

        classes = []
        for index, raw_class in enumerate(raw_classes):
            try:
                probability = float(raw_class.get("probability"))
            except (TypeError, ValueError):
                continue
            if probability < 0 or probability > 1:
                continue
            bis = AITrendPredict.normalize_bis(raw_class.get("bis", []), probability)
            if not bis:
                continue

            invalid_price = raw_class.get("invalid_price")
            levels = []
            for raw_level in raw_class.get("levels", []):
                try:
                    level = {
                        "price": float(raw_level["price"]),
                        "type": str(raw_level.get("type", "boundary")),
                        "text": str(raw_level.get("text", "")),
                    }
                except (KeyError, TypeError, ValueError):
                    continue
                if "time" in raw_level:
                    level["time"] = int(raw_level["time"])
                if "bar_offset" in raw_level:
                    level["bar_offset"] = max(1, int(raw_level["bar_offset"]))
                levels.append(level)

            classes.append(
                {
                    "key": str(raw_class.get("key", f"class_{index + 1}")),
                    "name": str(raw_class.get("name", "完全分类")),
                    "direction": str(raw_class.get("direction", "range")),
                    "probability": probability,
                    "trigger": str(raw_class.get("trigger", "")),
                    "boundary": str(raw_class.get("boundary", "")),
                    "action": str(raw_class.get("action", "")),
                    "basis": str(raw_class.get("basis", "")),
                    "invalid_price": (
                        None if invalid_price in [None, ""] else float(invalid_price)
                    ),
                    "bis": bis,
                    "levels": levels,
                }
            )

        return {
            "summary": str(raw_classification.get("summary", data.get("msg", ""))),
            "current_structure": str(raw_classification.get("current_structure", "")),
            "classes": classes,
        }

    @classmethod
    def fill_missing_times(cls, data: dict, cd: ICL, frequency: str) -> dict:
        current_time = fun.datetime_to_int(cd.get_src_klines()[-1].date)
        step_seconds = cls.FREQUENCY_SECONDS.get(frequency, 86400)
        for item in data.get("complete_classification", {}).get("classes", []):
            cls.fill_item_times(item, current_time, step_seconds)
            for level in item.get("levels", []):
                if "time" not in level:
                    level["time"] = (
                        current_time + level.get("bar_offset", 1) * step_seconds
                    )
                level.pop("bar_offset", None)
        return data

    @staticmethod
    def fill_item_times(item: dict, current_time: int, step_seconds: int) -> None:
        prev_end_time = current_time
        for bi in item.get("bis", []):
            pts = bi["points"]
            if "time" not in pts[0]:
                calc_start = current_time + pts[0]["bar_offset"] * step_seconds
                pts[0]["time"] = max(calc_start, prev_end_time + step_seconds)
            if "time" not in pts[1]:
                stretched_end = current_time + pts[1]["bar_offset"] * (step_seconds * 5)
                pts[1]["time"] = max(stretched_end, pts[0]["time"] + step_seconds)
            for pt in pts:
                pt.pop("bar_offset", None)
            prev_end_time = pts[1]["time"]
        for level in item.get("levels", []):
            if "time" not in level:
                level["time"] = current_time + level.get("bar_offset", 1) * step_seconds
            level.pop("bar_offset", None)

    def save_prediction(
        self,
        code: str,
        stock_name: str,
        frequency: str,
        model: str,
        prompt: str,
        msg: str,
        prediction_payload: dict[str, Any],
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
                predictions=json.dumps(prediction_payload, ensure_ascii=False),
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
                prediction_payload = json.loads(item["predictions"])
                item["complete_classification"] = prediction_payload[
                    "complete_classification"
                ]
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
