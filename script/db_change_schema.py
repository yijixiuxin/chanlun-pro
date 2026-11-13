"""统一数据库结构同步脚本（项目后续数据库变动的统一入口）。

用途：
1. 建表：按 ``src/chanlun/db.py`` 中的模型定义创建缺失的表（已存在的表跳过）。
2. 补列：按下方 ``COLUMN_CHANGES`` 登记的历史变动，为已存在的表补齐新列。
3. 兜底比对：与模型定义逐表比对，自动补齐「模型里有、库表里没有」的列，
   防止有人只改了模型而忘记在 ``COLUMN_CHANGES`` 登记。

以上三步都是「先检查、再修改」：表或列已存在则直接跳过，因此脚本可以重复执行，
也不会修改任何业务数据。

后续新增数据库变动时：
1. 先在 ``src/chanlun/db.py`` 对应模型中加好列（兜底比对会自动补列）；
2. 再把 DDL 追加到 ``COLUMN_CHANGES``（推荐，保留中文说明与精确的列位置）。

运行（项目根目录，``windows_run.bat`` 启动项目前会自动执行）：
    uv run script/db_change_schema.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sqlalchemy import inspect, text
from sqlalchemy.schema import CreateColumn

from chanlun.db import Base, db

# ======================================================================
# 数据库变动登记表（新增变动请追加到列表末尾）
#
# 每条变动包含：
#   table  / column：用于检查是否已经同步（表存在且列不存在时才执行 ALTER）
#   desc            ：中文说明，方便回看每次变动的原因
#   mysql / sqlite  ：对应数据库的 DDL 语句（按要求写的完整语句）
# ======================================================================
COLUMN_CHANGES = [
    {
        "table": "cl_tv_charts",
        "column": "drawings_migrated",
        "desc": "TV 图表布局改为独立存储（绘图分离）后，标记该布局是否已按新方式保存过",
        "mysql": (
            "ALTER TABLE `cl_tv_charts` ADD COLUMN `drawings_migrated` TINYINT(1) "
            "NOT NULL DEFAULT 0 COMMENT '是否已按独立存储（绘图分离）模式保存过'"
        ),
        "sqlite": (
            "ALTER TABLE cl_tv_charts ADD COLUMN drawings_migrated "
            "BOOLEAN NOT NULL DEFAULT 0"
        ),
    },
    {
        "table": "cl_tv_charts",
        "column": "legacy_content",
        "desc": "绘图分离前的布局 content 备份，仅在首次保存新格式时写入一次，可空",
        "mysql": (
            "ALTER TABLE `cl_tv_charts` ADD COLUMN `legacy_content` TEXT NULL "
            "COMMENT '迁移前布局 content 的一次性备份'"
        ),
        "sqlite": "ALTER TABLE cl_tv_charts ADD COLUMN legacy_content TEXT",
    },
    {
        "table": "cl_alert_task",
        "column": "check_bi_status",
        "desc": "报警任务新增「笔状态」过滤，可选择未完成 / 已完成 / 停顿三种状态进行提醒",
        "mysql": (
            "ALTER TABLE `cl_alert_task`\n"
            "\tADD COLUMN `check_bi_status` VARCHAR(50) NULL DEFAULT NULL "
            "COMMENT '检查笔的状态' AFTER `check_bi_mmd`"
        ),
        "sqlite": (
            "ALTER TABLE cl_alert_task ADD COLUMN check_bi_status "
            "VARCHAR(50) DEFAULT NULL"
        ),
    },
]


def get_table_names(engine):
    """当前库中已存在的表名集合"""
    return set(inspect(engine).get_table_names())


def get_columns(engine, table_name):
    """当前库中某表的列名集合（表不存在时返回空集合）"""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def create_missing_tables(engine):
    """按模型建表（已存在的表会被 create_all 跳过），返回本次新建的表名"""
    before = get_table_names(engine)
    Base.metadata.create_all(bind=engine)
    return sorted(get_table_names(engine) - before)


def apply_registered_changes(engine):
    """按登记表补齐列，返回 (本次执行的变动, 本次跳过的变动)"""
    applied, skipped = [], []
    existing_tables = get_table_names(engine)
    with engine.begin() as conn:
        for change in COLUMN_CHANGES:
            table, column = change["table"], change["column"]
            if table not in existing_tables:
                # 表还没有，会由建表步骤按最新模型创建，无需 ALTER
                skipped.append(f"{table}.{column}（表未创建，由模型建表）")
                continue
            if column in get_columns(engine, table):
                skipped.append(f"{table}.{column}")
                continue
            ddl = (
                change["mysql"]
                if engine.dialect.name == "mysql"
                else change["sqlite"]
            )
            conn.execute(text(ddl))
            applied.append(change)
    return applied, skipped


def build_add_column_ddl(engine, table, column):
    """按模型列定义生成 ADD COLUMN 语句（用于兜底补齐未登记的新列）"""
    column_ddl = str(CreateColumn(column).compile(dialect=engine.dialect))
    if (
        engine.dialect.name == "sqlite"
        and column.nullable is False
        and column.server_default is None
        and column.default is not None
        and column.default.is_scalar
    ):
        # SQLite 不支持给已有数据的表加「无默认值的非空列」，这里用模型默认值兜底
        value = column.default.arg
        if isinstance(value, bool):
            column_ddl += f" DEFAULT {int(value)}"
        elif isinstance(value, (int, float)):
            column_ddl += f" DEFAULT {value}"
        elif isinstance(value, str):
            column_ddl += " DEFAULT '" + value.replace("'", "''") + "'"
    table_name = engine.dialect.identifier_preparer.format_table(table)
    return f"ALTER TABLE {table_name} ADD COLUMN {column_ddl}"


def sync_model_columns(engine):
    """与模型定义比对，自动补齐缺失的列（未登记的变动也能同步），返回 (已补的列, 异常列)"""
    applied, errors = [], []
    existing_tables = get_table_names(engine)
    with engine.begin() as conn:
        for table in Base.metadata.sorted_tables:
            if table.name not in existing_tables:
                continue
            db_columns = get_columns(engine, table.name)
            for column in table.columns:
                if column.name in db_columns:
                    continue
                if column.primary_key:
                    # 主键列丢失属于表结构异常，无法用 ADD COLUMN 修复
                    errors.append(f"{table.name}.{column.name}（主键列缺失，需手工处理）")
                    continue
                conn.execute(text(build_add_column_ddl(engine, table, column)))
                applied.append(f"{table.name}.{column.name}")
    return applied, errors


def find_missing_columns(engine):
    """同步后再次比对，返回仍然缺失的列清单（用于最终校验）"""
    missing = []
    existing_tables = get_table_names(engine)
    for table in Base.metadata.sorted_tables:
        if table.name not in existing_tables:
            missing.append(f"{table.name}（表缺失）")
            continue
        db_columns = get_columns(engine, table.name)
        for column in table.columns:
            if column.name not in db_columns:
                missing.append(f"{table.name}.{column.name}")
    return missing


def main():
    engine = db.engine
    print("== 数据库结构同步 ==")
    print(f"[db] dialect={engine.dialect.name} database={engine.url.database}")

    created = create_missing_tables(engine)
    if created:
        for name in created:
            print(f"[create] 新建表 {name}")
    else:
        print("[skip] 数据表已齐备")

    applied, skipped = apply_registered_changes(engine)
    for change in applied:
        print(f"[alter] {change['table']}.{change['column']} <- {change['desc']}")
    if not applied:
        print(f"[skip] 登记的 {len(skipped)} 个变动列均已存在")

    auto_applied, auto_errors = sync_model_columns(engine)
    for column_name in auto_applied:
        print(f"[alter] {column_name}（模型中有、库中缺失，已自动补齐）")
    if not auto_applied:
        print("[skip] 模型列与库表结构一致")

    for error in auto_errors:
        print(f"[error] {error}")

    missing = find_missing_columns(engine)
    if missing:
        for column_name in missing:
            print(f"[error] 同步后仍缺失：{column_name}")
        return 1

    print("== 数据库结构同步完成 ==")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
