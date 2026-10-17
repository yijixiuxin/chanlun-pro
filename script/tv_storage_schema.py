"""TradingView 独立存储 —— 幂等数据库结构迁移脚本。

用途（契约：docs/tv_storage_refactor.md 第 3.5 节）：
1. 创建新表 ``cl_tv_drawing_items`` / ``cl_tv_drawing_templates`` / ``cl_tv_chart_templates``
   （``Base.metadata.create_all``，已存在则跳过）。
2. 为已存在的 ``cl_tv_charts`` 幂等补齐两列：
   - ``drawings_migrated``：是否已按独立存储（绘图分离）模式保存过（默认 0）
   - ``legacy_content``：迁移前布局 content 的一次性备份（可空）

``create_all`` 不会为已存在的表补列，因此缺失列通过 ``ALTER TABLE`` 补齐。
可重复执行：列已存在时不会重复 ALTER，也不会修改任何业务数据。

运行（项目根目录）：
    .venv/Scripts/python.exe script/tv_storage_schema.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sqlalchemy import inspect

from chanlun.db import (
    TV_STORAGE_CHART_LEGACY_COLUMNS,
    Base,
    TableByTVCharts,
    TableByTVChartTemplates,
    TableByTVDrawingItems,
    TableByTVDrawingTemplates,
    db,
    migrate_tv_storage_schema,
)

TARGET_TABLE = "cl_tv_charts"
TARGET_TABLES = [
    TableByTVCharts.__table__,
    TableByTVDrawingItems.__table__,
    TableByTVDrawingTemplates.__table__,
    TableByTVChartTemplates.__table__,
]
NEW_TABLES = (
    "cl_tv_drawing_items",
    "cl_tv_drawing_templates",
    "cl_tv_chart_templates",
)


def get_existing_columns(engine, table_name):
    """读取当前库中某表的列名集合（表不存在时返回空集合）"""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def table_exists(engine, table_name):
    return inspect(engine).has_table(table_name)


def create_missing_tables(engine):
    """按模型建表（已存在的表会被 create_all 跳过），返回本次新建的表名"""
    before = {t.name for t in TARGET_TABLES if table_exists(engine, t.name)}
    Base.metadata.create_all(bind=engine, tables=TARGET_TABLES)
    return [
        t.name
        for t in TARGET_TABLES
        if t.name not in before and table_exists(engine, t.name)
    ]


def main():
    engine = db.engine
    print("== TradingView storage schema migration ==")
    print(f"[db] dialect={engine.dialect.name}")

    created = create_missing_tables(engine)
    if created:
        for name in created:
            print(f"[create] new table {name}")
    else:
        print("[skip] all target tables already exist")

    executed = migrate_tv_storage_schema(engine)
    if executed:
        for column_name in executed:
            print(f"[alter] ADD COLUMN {column_name} -> {TARGET_TABLE}")
    else:
        print(f"[skip] {TARGET_TABLE} columns already present")

    columns = get_existing_columns(engine, TARGET_TABLE)
    missing_columns = [
        column_name
        for column_name in TV_STORAGE_CHART_LEGACY_COLUMNS
        if column_name not in columns
    ]
    if missing_columns:
        print(f"[error] columns still missing after migration: {missing_columns}")
        return 1
    print(f"[ok] {TARGET_TABLE} columns: {sorted(columns)}")

    missing_tables = []
    for name in NEW_TABLES:
        exists = table_exists(engine, name)
        print(f"[ok] table {name} exists: {exists}")
        if not exists:
            missing_tables.append(name)
    if missing_tables:
        print(f"[error] tables still missing after migration: {missing_tables}")
        return 1

    print("== migration done ==")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
