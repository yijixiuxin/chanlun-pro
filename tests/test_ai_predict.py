import json
import datetime

from chanlun.cl_interface import Kline
from chanlun.tools.ai_predict import format_recent_kline_ma_table
from chanlun.tools.ai_predict import AITrendPredict


def test_extract_response_json_from_markdown_block():
    content = """```json
{"msg":"ok","complete_classification":{"classes":[{"name":"up","probability":0.6,"bis":[]}]}}
```"""

    data = AITrendPredict.extract_response_json(content)

    assert data["msg"] == "ok"
    assert data["complete_classification"]["classes"][0]["probability"] == 0.6


def test_normalize_response_requires_complete_classification():
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

    try:
        AITrendPredict.normalize_response(raw)
    except ValueError as exc:
        assert "complete_classification 必须是 object" in str(exc)
    else:
        raise AssertionError("normalize_response accepted legacy predictions")


def test_normalize_response_preserves_complete_classification():
    raw = {
        "msg": "基于走势必完美进行完全分类",
        "complete_classification": {
            "summary": "当前30分钟中枢后只有向上离开、向下离开、继续震荡三类。",
            "current_structure": "30分钟中枢震荡末端",
            "classes": [
                {
                    "key": "up_break",
                    "name": "向上突破形成三买",
                    "direction": "up",
                    "probability": "0.4",
                    "trigger": "有效站上中枢ZG",
                    "boundary": "ZG=3310",
                    "action": "等待回踩不破后的三买",
                    "invalid_price": "3260",
                    "basis": "中枢上沿突破并回抽不入中枢",
                    "bis": [
                        {
                            "points": [
                                {"bar_offset": 1, "price": "3280"},
                                {"bar_offset": 5, "price": "3360"},
                            ],
                            "linestyle": "1",
                        }
                    ],
                    "levels": [
                        {
                            "price": "3310",
                            "type": "trigger",
                            "text": "上破确认",
                        },
                        {
                            "price": "3260",
                            "type": "invalid",
                            "text": "跌破失效",
                        },
                    ],
                },
                {
                    "key": "invalid_probability",
                    "name": "无效分类",
                    "direction": "down",
                    "probability": 1.2,
                    "bis": [],
                },
            ],
        },
    }

    normalized = AITrendPredict.normalize_response(raw)

    assert (
        normalized["complete_classification"]["summary"]
        == raw["complete_classification"]["summary"]
    )
    assert (
        normalized["complete_classification"]["current_structure"]
        == "30分钟中枢震荡末端"
    )
    assert len(normalized["complete_classification"]["classes"]) == 1
    klass = normalized["complete_classification"]["classes"][0]
    assert klass["key"] == "up_break"
    assert klass["probability"] == 0.4
    assert klass["invalid_price"] == 3260.0
    assert klass["bis"][0]["points"] == [
        {"bar_offset": 1, "price": 3280.0},
        {"bar_offset": 5, "price": 3360.0},
    ]
    assert klass["levels"] == [
        {"price": 3310.0, "type": "trigger", "text": "上破确认"},
        {"price": 3260.0, "type": "invalid", "text": "跌破失效"},
    ]
    assert "predictions" not in normalized
    json.dumps(normalized, ensure_ascii=False)


def test_fill_missing_times_updates_classification_paths_and_levels():
    class FakeKline:
        date = datetime.datetime(2026, 1, 1, 9, 30)

    class FakeCD:
        def get_src_klines(self):
            return [FakeKline()]

    data = {
        "msg": "",
        "complete_classification": {
            "summary": "",
            "current_structure": "",
            "classes": [
                {
                    "key": "up_break",
                    "name": "向上突破",
                    "direction": "up",
                    "probability": 0.4,
                    "trigger": "",
                    "boundary": "",
                    "action": "",
                    "invalid_price": None,
                    "basis": "",
                    "bis": [
                        {
                            "points": [
                                {"bar_offset": 2, "price": 11.0},
                                {"bar_offset": 4, "price": 13.0},
                            ],
                            "linestyle": "1",
                            "text": "上破",
                        }
                    ],
                    "levels": [{"price": 9.8, "type": "invalid", "text": "失效"}],
                }
            ],
        },
    }

    filled = AITrendPredict.fill_missing_times(data, FakeCD(), "1m")

    assert filled["complete_classification"]["classes"][0]["bis"][0]["points"] == [
        {"price": 11.0, "time": 1767231120},
        {"price": 13.0, "time": 1767231240},
    ]
    assert (
        filled["complete_classification"]["classes"][0]["levels"][0]["time"]
        == 1767231060
    )


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
