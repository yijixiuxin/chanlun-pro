import json
import datetime

from chanlun.cl_interface import Kline
from chanlun.tools.ai_predict import format_recent_kline_ma_table
from chanlun.tools.ai_predict import AITrendPredict


def test_extract_response_json_from_markdown_block():
    content = """```json
{"msg":"ok","predictions":[{"name":"up","probability":0.6,"bis":[]}]}
```"""

    data = AITrendPredict.extract_response_json(content)

    assert data["msg"] == "ok"
    assert data["predictions"][0]["probability"] == 0.6


def test_normalize_predictions_rejects_invalid_and_formats_points():
    raw = {
        "msg": "走势推演",
        "predictions": [
            {
                "name": "向上延伸",
                "probability": "0.45",
                "basis": "未完成上升笔延续",
                "invalid_price": "3120.5",
                "bis": [
                    {
                        "points": [
                            {"time": "1776998400", "price": "3200.1"},
                            {"time": 1777084800, "price": 3350},
                        ],
                        "linestyle": "1",
                    }
                ],
            },
            {
                "name": "无效概率",
                "probability": 1.5,
                "bis": [],
            },
        ],
    }

    normalized = AITrendPredict.normalize_response(raw)

    assert normalized["msg"] == "走势推演"
    assert len(normalized["predictions"]) == 1
    prediction = normalized["predictions"][0]
    assert prediction["invalid_price"] == 3120.5
    assert prediction["bis"][0]["points"] == [
        {"time": 1776998400, "price": 3200.1},
        {"time": 1777084800, "price": 3350.0},
    ]
    json.dumps(normalized, ensure_ascii=False)


def test_format_recent_kline_ma_table_includes_last_20_ohlcv_and_ma_values():
    klines = [
        Kline(
            index=i,
            date=datetime.datetime(2026, 1, i + 1),
            o=10 + i,
            h=11 + i,
            l=9 + i,
            c=10 + i,
            a=1000 + i,
        )
        for i in range(25)
    ]

    table = format_recent_kline_ma_table(klines, precision=2)

    lines = table.splitlines()
    assert lines[0] == "| 时间 | 开盘 | 最高 | 最低 | 收盘 | 成交量 | MA5 | MA10 |"
    assert lines[1] == "|:---:|---:|---:|---:|---:|---:|---:|---:|"
    assert len(lines) == 22
    assert lines[2] == "| 2026-01-06 00:00:00 | 15 | 16 | 14 | 15 | 1005 | 13 | - |"
    assert lines[-1] == "| 2026-01-25 00:00:00 | 34 | 35 | 33 | 34 | 1024 | 32 | 29.5 |"
