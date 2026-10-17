import datetime
import json
import re
import time
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    inspect,
    text,
)
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from chanlun import config, fun
from chanlun.base import Market
from chanlun.config import get_data_path

warnings.filterwarnings("ignore")

# https://docs.sqlalchemy.org/en/20/core/types.html

Base = declarative_base()

# ----------------------------------------------------------------------
# TradingView 独立存储（布局与绘图分离）常量与工具
# ----------------------------------------------------------------------

# 存储身份：单用户应用沿用历史前端写死的 user_id
TV_STORAGE_USER_ID = 999
# 模板类数据统一写入的 client_id（仅作冗余，读路径只按 user_id 过滤）
TV_STORAGE_CLIENT_ID = "chanlun_pro"
# 绘图条目类型
TV_STORAGE_ITEM_SOURCE = "source"
TV_STORAGE_ITEM_GROUP = "group"
# SharingMode.GloballyShared 的数值（NotShared=0 / SharedInLayout=1 / GloballyShared=2）
TV_STORAGE_SHARING_GLOBAL = 2
# 判定旧布局 content 中是否含“真正的绘图 source 类型”。
# 旧 content 里 chartProperties.axisLineToolLabelBackgroundColor* 也会命中裸子串
# LineTool，因此不能用 `"LineTool" in content`；历史布局 content 可能被二次序列化，
# 引号可能带反斜杠（\"type\":\"LineToolTrendLine\"），正则需同时兼容。
LEGACY_DRAWING_PATTERN = re.compile(r'\\?"type\\?"\s*:\s*\\?"LineTool')


def _tv_storage_content_text(content) -> str:
    """模板 content 统一转字符串：str 原样、None -> ""、其它 json 序列化"""
    if isinstance(content, str):
        return content
    if content is None:
        return ""
    return json.dumps(content, ensure_ascii=False)


def _tv_storage_clean_symbol(symbol):
    """空字符串归一为 None（数据库存 NULL）"""
    if symbol is None:
        return None
    symbol = str(symbol).strip()
    return symbol or None


def _tv_storage_decode_state(state):
    """绘图条目 state 反序列化，异常时原样返回"""
    try:
        return json.loads(state)
    except (TypeError, ValueError):
        return state


# cl_tv_charts 需要幂等补齐的列（MySQL / SQLite 分别给出 DDL）
TV_STORAGE_CHART_LEGACY_COLUMNS = ("drawings_migrated", "legacy_content")
_MYSQL_CHART_LEGACY_DDL = {
    "drawings_migrated": (
        "ALTER TABLE `cl_tv_charts` ADD COLUMN `drawings_migrated` TINYINT(1) "
        "NOT NULL DEFAULT 0 COMMENT '是否已按独立存储（绘图分离）模式保存过'"
    ),
    "legacy_content": (
        "ALTER TABLE `cl_tv_charts` ADD COLUMN `legacy_content` TEXT NULL "
        "COMMENT '迁移前布局 content 的一次性备份'"
    ),
}
_SQLITE_CHART_LEGACY_DDL = {
    "drawings_migrated": (
        "ALTER TABLE cl_tv_charts ADD COLUMN drawings_migrated "
        "BOOLEAN NOT NULL DEFAULT 0"
    ),
    "legacy_content": "ALTER TABLE cl_tv_charts ADD COLUMN legacy_content TEXT",
}


def migrate_tv_storage_schema(engine) -> list:
    """幂等补齐 cl_tv_charts 的独立存储迁移列，返回本次实际执行的列名。

    create_all 不会为已存在的表补列，因此这里检查现有列后按需 ALTER。
    MySQL 与 SQLite 均可重复执行；已存在列直接跳过。
    """
    inspector = inspect(engine)
    if not inspector.has_table("cl_tv_charts"):
        return []

    existing = {col["name"] for col in inspector.get_columns("cl_tv_charts")}
    if engine.dialect.name == "mysql":
        ddl_map = _MYSQL_CHART_LEGACY_DDL
    else:
        ddl_map = _SQLITE_CHART_LEGACY_DDL

    executed = []
    with engine.begin() as conn:
        for column_name in TV_STORAGE_CHART_LEGACY_COLUMNS:
            if column_name in existing:
                continue
            conn.execute(text(ddl_map[column_name]))
            executed.append(column_name)
    return executed


class TableByCache(Base):
    # 各种乱七八杂的信息
    __tablename__ = "cl_cache"
    k = Column(String(100), unique=True, primary_key=True)  # 唯一值
    v = Column(Text, comment="存储内容")  # 存储内容
    expire = Column(
        Integer, default=0, comment="过期时间戳，0为永不过期"
    )  # 过期时间戳，0为永不过期
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByZxGroup(Base):
    # 自选组列表
    __tablename__ = "cl_zixuan_groups"
    __table_args__ = (
        UniqueConstraint("market", "zx_group", name="table_market_group_unique"),
    )
    market = Column(String(20), primary_key=True, comment="市场")
    zx_group = Column(String(20), primary_key=True, comment="自选组名称")
    add_dt = Column(DateTime, comment="添加时间")
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByZixuan(Base):
    # 自选表
    __tablename__ = "cl_zixuan_watchlist"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")  # 市场
    zx_group = Column(String(20), comment="自选组")  # 自选组
    stock_code = Column(String(20), comment="标的代码")  # 标的代码
    stock_name = Column(String(100), comment="标的名称")  # 标的名称
    position = Column(Integer, comment="位置")  # 位置
    add_datetime = Column(DateTime, comment="添加时间")  # 添加时间
    stock_color = Column(String(20), comment="自选颜色")  # 自选颜色
    stock_memo = Column(String(100), comment="附加信息")  # 附加信息
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByAlertTask(Base):
    # 提醒任务
    __tablename__ = "cl_alert_task"
    __table_args__ = (
        UniqueConstraint("market", "task_name", name="table_market_task_name_unique"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")  # 市场
    task_name = Column(String(100), comment="任务名称")  # 任务名称
    zx_group = Column(String(20), comment="自选组")  # 自选组
    frequency = Column(String(20), comment="检查周期")  # 检查周期
    interval_minutes = Column(Integer, comment="检查间隔分钟")  # 检查间隔分钟
    check_bi_type = Column(String(20), comment="检查笔的类型")  # 检查笔的类型
    check_bi_beichi = Column(String(200), comment="检查笔的背驰")  # 检查笔的背驰
    check_bi_mmd = Column(String(200), comment="检查笔的买卖点")  # 检查笔的买卖点
    check_xd_type = Column(String(20), comment="检查线段的类型")  # 检查线段的类型
    check_xd_beichi = Column(String(200), comment="检查线段的背驰")  # 检查线段的背驰
    check_xd_mmd = Column(String(200), comment="检查线段的买卖点")  # 检查线段的买卖点
    check_bi_status = Column(String(50), comment="检查笔的状态")  # 检查笔的状态
    check_idx_ma_info = Column(String(200), comment="检查指数的均线")
    check_idx_macd_info = Column(String(200), comment="检查指数的MACD")
    is_run = Column(Integer, comment="是否运行")  # 是否运行
    is_send_msg = Column(Integer, comment="是否发送消息")  # 是否发送消息
    dt = Column(DateTime, comment="任务添加、修改时间")  # 任务添加、修改时间
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByAlertRecord(Base):
    # 提醒记录
    __tablename__ = "cl_alert_record"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")  # 市场
    task_name = Column(String(100), comment="任务名称")  # 任务名称
    stock_code = Column(String(20), comment="标的")  # 标的
    stock_name = Column(String(100), comment="标的名称")  # 标的名称
    frequency = Column(String(10), comment="提醒周期")  # 提醒周期
    line_type = Column(String(5), comment="提醒线段的类型")  # 提醒线段的类型
    alert_msg = Column(Text, comment="提醒消息")  # 提醒消息
    bi_is_done = Column(
        String(10), comment="笔是否完成,如果是指标，则记录上穿或下穿"
    )  # 笔是否完成
    bi_is_td = Column(String(10), comment="笔是否停顿")  # 笔是否停顿
    line_dt = Column(DateTime, comment="提醒线段的开始时间")  # 提醒线段的开始时间
    alert_dt = Column(DateTime, comment="提醒时间")  # 提醒时间
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByTVMarks(Base):
    # TV 图表的 mark 标记 (在时间轴上的标记)
    __tablename__ = "cl_tv_marks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")  # 市场
    stock_code = Column(String(20), comment="标的代码")  # 标的代码
    stock_name = Column(String(100), comment="标的名称")  # 标的名称
    frequency = Column(String(10), default="", comment="展示周期")  # 展示周期
    mark_time = Column(Integer, comment="标签时间戳")  # 标签时间戳
    mark_label = Column(String(2), comment="标签")  # 标签
    mark_tooltip = Column(String(100), comment="提示")  # 提示
    mark_shape = Column(String(20), comment="形状")  # 形状
    mark_color = Column(String(20), comment="颜色")  # 颜色
    dt = Column(DateTime, comment="添加时间")
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByTVMarksPrice(Base):
    # TV 图表的 mark 标记 (在价格主图的标记)
    __tablename__ = "cl_tv_marks_price"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")  # 市场
    stock_code = Column(String(20), comment="标的代码")  # 标的代码
    stock_name = Column(String(100), comment="标的名称")  # 标的名称
    frequency = Column(String(10), default="", comment="展示周期")  # 展示周期
    mark_time = Column(Integer, comment="标签时间戳")  # 标签时间戳
    mark_color = Column(String(20), comment="颜色")  # 颜色
    mark_text = Column(String(100), comment="提示")  # 提示
    mark_label = Column(String(2), comment="标签")  # 标签
    mark_label_font_color = Column(String(20), comment="标签字体颜色")  # 标签字体颜色
    mark_min_size = Column(Integer, comment="最小尺寸")  # 最小尺寸

    dt = Column(DateTime, comment="添加时间")
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByOrder(Base):
    # 订单
    __tablename__ = "cl_order"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")  # 市场
    stock_code = Column(String(20), comment="标的代码")  # 标的代码
    stock_name = Column(String(100), comment="标的名称")  # 标的名称
    order_type = Column(String(20), comment="订单类型")  # 订单类型
    order_price = Column(Float, comment="订单价格")  # 订单价格
    order_amount = Column(Float, comment="订单数量")  # 订单数量
    order_memo = Column(String(200), comment="订单备注")  # 订单备注
    dt = Column(DateTime, comment="添加时间")  # 添加时间
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByTVCharts(Base):
    # TV 图表的布局（chart_type='chart'）与指标模板（chart_type='template'）
    __tablename__ = "cl_tv_charts"
    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    client_id = Column(String(50), comment="客户端id")
    user_id = Column(Integer, comment="用户id")
    chart_type = Column(String(20), comment="布局类型")
    symbol = Column(String(50), comment="标的")
    resolution = Column(String(20), comment="周期")
    content = Column(Text, comment="布局内容")
    timestamp = Column(Integer, comment="时间戳")
    name = Column(String(50), comment="布局名称")
    drawings_migrated = Column(
        Boolean, default=False, comment="是否已按独立存储（绘图分离）模式保存过"
    )
    legacy_content = Column(
        Text, nullable=True, comment="迁移前布局 content 的一次性备份"
    )
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByTVDrawingItems(Base):
    # TV 图表的绘图条目（布局与绘图分离存储，按条增量读写）
    __tablename__ = "cl_tv_drawing_items"
    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    client_id = Column(String(50), comment="客户端id")
    user_id = Column(Integer, comment="用户id")
    layout_id = Column(String(50), default="", comment="布局id")
    chart_id = Column(Integer, default=0, comment="图表id")
    sharing_mode = Column(Integer, default=0, comment="共享模式 0/1/2")
    item_type = Column(String(16), comment="条目类型 source/group")
    item_id = Column(String(64), comment="条目id")
    symbol = Column(String(64), nullable=True, comment="标的")
    owner_source = Column(String(64), nullable=True, comment="所属绘图源")
    state = Column(Text, comment="绘图状态 JSON")
    updated_at = Column(DateTime, comment="更新时间")
    # 添加配置设置编码
    __table_args__ = (
        UniqueConstraint(
            "client_id",
            "user_id",
            "layout_id",
            "chart_id",
            "sharing_mode",
            "item_type",
            "item_id",
            name="table_tv_drawing_item_unique",
        ),
        Index(
            "table_tv_drawing_items_scope_index",
            "client_id",
            "user_id",
            "layout_id",
            "chart_id",
        ),
        Index(
            "table_tv_drawing_items_global_index",
            "client_id",
            "user_id",
            "sharing_mode",
            "symbol",
        ),
        {"mysql_collate": "utf8mb4_general_ci"},
    )


class TableByTVDrawingTemplates(Base):
    # TV 图表的画图模板
    __tablename__ = "cl_tv_drawing_templates"
    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    client_id = Column(String(50), comment="客户端id")
    user_id = Column(Integer, comment="用户id")
    tool = Column(String(50), comment="画图工具名称")
    name = Column(String(100), comment="模板名称")
    content = Column(Text, comment="模板内容")
    updated_at = Column(DateTime, comment="更新时间")
    # 添加配置设置编码
    __table_args__ = (
        UniqueConstraint(
            "user_id", "tool", "name", name="table_tv_drawing_template_unique"
        ),
        {"mysql_collate": "utf8mb4_general_ci"},
    )


class TableByTVChartTemplates(Base):
    # TV 图表的图表模板
    __tablename__ = "cl_tv_chart_templates"
    id = Column(Integer, primary_key=True, autoincrement=True, comment="id")
    client_id = Column(String(50), comment="客户端id")
    user_id = Column(Integer, comment="用户id")
    name = Column(String(100), comment="模板名称")
    content = Column(Text, comment="模板内容（任意 JSON）")
    updated_at = Column(DateTime, comment="更新时间")
    # 添加配置设置编码
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="table_tv_chart_template_unique"),
        {"mysql_collate": "utf8mb4_general_ci"},
    )


class TableByAIAnalyse(Base):
    # AI 分析结果记录
    __tablename__ = "cl_ai_analyses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")  # 市场
    stock_code = Column(String(20), comment="标的")  # 标的
    stock_name = Column(String(100), comment="标的名称")  # 标的名称
    frequency = Column(String(10), comment="分析周期")
    dt = Column(DateTime, comment="分析时间")
    model = Column(String(100), comment="分析模型")
    prompt = Column(Text, comment="缠论当下说明")
    msg = Column(Text, comment="分析结果")

    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


class TableByAIPrediction(Base):
    # AI 走势预测记录
    __tablename__ = "cl_ai_predictions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(20), comment="市场")
    stock_code = Column(String(20), comment="标的")
    stock_name = Column(String(100), comment="标的名称")
    frequency = Column(String(10), comment="预测周期")
    dt = Column(DateTime, comment="预测时间")
    model = Column(String(100), comment="预测模型")
    prompt = Column(Text, comment="预测提示词")
    msg = Column(Text, comment="预测结论")
    predictions = Column(Text, comment="结构化预测数据 JSON")
    raw_response = Column(Text, comment="AI 原始响应")

    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


@fun.singleton
class DB:
    global Base

    def __init__(self) -> None:
        if config.DB_TYPE == "sqlite":
            db_path = get_data_path() / "db"
            if db_path.is_dir() is False:
                db_path.mkdir(parents=True)
            self.engine = create_engine(
                f"sqlite:///{db_path / f'{config.DB_DATABASE}.sqlite'!s}",
                echo=False,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_timeout=10,
            )
        elif config.DB_TYPE == "mysql":
            self.engine = create_engine(
                f"mysql+pymysql://{config.DB_USER}:{config.DB_PWD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_DATABASE}?charset=utf8mb4",
                echo=False,
                poolclass=QueuePool,
                pool_recycle=3600,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20,
                pool_timeout=10,
            )
        else:
            raise Exception("DB_TYPE 配置错误")

        self.Session = sessionmaker(bind=self.engine)

        Base.metadata.create_all(self.engine)

        # 新表由 create_all 建，cl_tv_charts 的历史表靠幂等 ALTER 补齐新列
        migrate_tv_storage_schema(self.engine)

        self.__cache_tables = {}

    def klines_tables(self, market: str, stock_code: str):
        stock_code = (
            stock_code.replace(".", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace("@", "_")
            .lower()
        )
        if market == Market.HK.value:
            table_name = f"{market}_klines_{stock_code[-3:]}"
        elif market == Market.A.value:
            table_name = f"{market}_klines_{stock_code[:7]}"
        elif market == Market.US.value:
            table_name = f"{market}_klines_{stock_code[0]}"
        elif (
            market == Market.FX.value
            or market == Market.CURRENCY.value
            or market == Market.CURRENCY_SPOT.value
            or market == Market.FUTURES.value
        ):
            table_name = f"{market}_klines_{stock_code}"
        else:
            raise Exception(f"市场错误：{market}")

        if table_name in self.__cache_tables:
            return self.__cache_tables[table_name]

        class TableByKlines(Base):
            # 表名
            __tablename__ = table_name
            __table_args__ = (
                UniqueConstraint("code", "dt", "f", name="table_code_dt_f_unique"),
            )
            # 表结构
            code = Column(String(20), primary_key=True, comment="标的代码")
            dt = Column(DateTime, primary_key=True, comment="日期")
            f = Column(String(5), primary_key=True, comment="周期")
            o = Column(Float)
            c = Column(Float)
            h = Column(Float)
            l = Column(Float)
            v = Column(Float)
            # 添加配置设置编码
            __table_args__ = {
                "mysql_collate": "utf8mb4_general_ci",
            }

        if market == Market.FUTURES.value:
            # 期货市场，添加持仓列
            TableByKlines.p = Column(Float, comment="持仓量")

        self.__cache_tables[table_name] = TableByKlines
        Base.metadata.create_all(self.engine)
        return TableByKlines

    def klines_query(
        self,
        market: str,
        code: str,
        frequency: str,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        limit: int = 5000,
        order: str = "desc",
    ) -> list:
        """
        获取k线数据
        :param market:
        :param code:
        :param frequency:
        :param start_date:
        :param end_date:
        :param limit:
        :param order:
        :return:
        """
        with self.Session() as session:
            table = self.klines_tables(market, code)
            # 查询数据库
            filter = (table.code == code, table.f == frequency)
            if start_date is not None:
                filter += (table.dt >= start_date,)
            if end_date is not None:
                filter += (table.dt <= end_date,)
            query = session.query(table).filter(*filter)
            if order == "desc":
                query = query.order_by(table.dt.desc())
            else:
                query = query.order_by(table.dt.asc())
            if limit is not None:
                query = query.limit(limit)
            return query.all()

    def klines_last_datetime(self, market, code, frequency):
        """
        查询k线表中最后一条记录的日期
        :param market:
        :param code:
        :param frequency:
        :return:
        """
        with self.Session() as session:
            table = self.klines_tables(market, code)
            last_date = (
                session.query(table.dt)
                .filter(table.code == code)
                .filter(table.f == frequency)
                .order_by(table.dt.desc())
                .first()
            )
            if last_date is None:
                return None
            if market == "a":
                return last_date[0].strftime("%Y-%m-%d")
            else:
                return last_date[0].strftime("%Y-%m-%d %H:%M:%S")

    def klines_insert(
        self, market: str, code: str, frequency: str, klines: pd.DataFrame
    ):
        """
        插入k线
        :param market:
        :param code:
        :param frequency:
        :param klines:
        :return:
        """
        with self.Session() as session:
            table = self.klines_tables(market, code)

            # 如果是 sqlite ，则慢慢更新吧
            if config.DB_TYPE == "sqlite":
                for _, _k in klines.iterrows():
                    _in_k = {
                        "code": code,
                        "f": frequency,
                        "dt": _k["date"].replace(tzinfo=None),  # 去除时区信息
                        "o": _k["open"],
                        "c": _k["close"],
                        "h": _k["high"],
                        "l": _k["low"],
                        "v": _k["volume"],
                    }
                    if "position" in _k.keys():
                        _in_k["p"] = _k["position"]
                    db_k = (
                        session.query(table)
                        .filter(
                            table.code == code,
                            table.f == frequency,
                            table.dt == _in_k["dt"],
                        )
                        .first()
                    )
                    if db_k is None:
                        session.add(table(**_in_k))
                    else:
                        session.query(table).filter(
                            table.code == code,
                            table.f == frequency,
                            table.dt == _in_k["dt"],
                        ).update(_in_k)
                session.commit()
                return True

            # 将 klines 数据拆分为每 500 条一组，批量插入
            group = np.arange(len(klines)) // 500
            groups = [
                group.reset_index(drop=True) for _, group in klines.groupby(group)
            ]
            in_position = "position" in klines.columns
            for g_klines in groups:
                insert_klines = []
                for _, _k in g_klines.iterrows():
                    _insert_k = {
                        "code": code,
                        "dt": _k["date"].replace(tzinfo=None),  # 去除时区信息
                        "f": frequency,
                        "o": _k["open"],
                        "c": _k["close"],
                        "h": _k["high"],
                        "l": _k["low"],
                        "v": _k["volume"],
                    }
                    if in_position:
                        _insert_k["p"] = _k["position"]
                    insert_klines.append(_insert_k)
                insert_stmt = insert(table).values(insert_klines)
                update_keys = ["o", "c", "h", "l", "v"]
                if in_position:
                    update_keys.append("p")
                update_columns = {
                    x.name: x for x in insert_stmt.inserted if x.name in update_keys
                }
                upsert_stmt = insert_stmt.on_duplicate_key_update(**update_columns)
                session.execute(upsert_stmt)
                session.commit()

        return True

    def klines_delete(
        self,
        market: str,
        code: str,
        frequency: str = None,
        dt: datetime.datetime = None,
    ):
        """
        删除k线
        :param market:
        :param code:
        :param frequency:
        :param dt:
        :return:
        """
        with self.Session() as session:
            table = self.klines_tables(market, code)
            q = session.query(table).filter(table.code == code)
            if frequency is not None:
                q = q.filter(table.f == frequency)
            if dt is not None:
                q = q.filter(table.dt == dt)
            q.delete()
            session.commit()

        return True

    def zx_get_groups(self, market: str) -> list[TableByZxGroup]:
        """
        获取自选分组
        """
        with self.Session() as session:
            return (
                session.query(TableByZxGroup)
                .filter(TableByZxGroup.market == market)
                .order_by(TableByZxGroup.add_dt.asc())
                .all()
            )

    def zx_add_group(self, market: str, zx_group: str) -> bool:
        """
        添加自选分组
        """
        with self.Session() as session:
            session.add(
                TableByZxGroup(
                    market=market, zx_group=zx_group, add_dt=datetime.datetime.now()
                )
            )
            session.commit()

        return True

    def zx_del_group(self, market: str, zx_group: str) -> bool:
        """
        删除自选分组
        """
        with self.Session() as session:
            session.query(TableByZxGroup).filter(
                TableByZxGroup.market == market, TableByZxGroup.zx_group == zx_group
            ).delete()
            session.commit()

        return True

    def zx_get_group_stocks(self, market: str, zx_group: str) -> list[TableByZixuan]:
        """
        获取自选组下的股票列表
        """
        with self.Session() as session:
            stocks = (
                session.query(TableByZixuan)
                .filter(TableByZixuan.zx_group == zx_group)
                .filter(TableByZixuan.market == market)
                .order_by(TableByZixuan.position.asc())
                .all()
            )
        return stocks

    def zx_add_group_stock(
        self,
        market: str,
        zx_group: str,
        stock_code: str,
        stock_name: str,
        memo: str = "",
        color: str = "",
        location: str = "bottom",
    ):
        with self.Session() as session:
            # 添加前，统一删除在自选组下的股票信息
            session.query(TableByZixuan).filter(
                TableByZixuan.market == market,
                TableByZixuan.zx_group == zx_group,
                TableByZixuan.stock_code == stock_code,
            ).delete()

            position = 0
            if location == "top":
                # 自选组的股票位置+1
                session.query(TableByZixuan).filter(
                    TableByZixuan.zx_group == zx_group
                ).update(
                    {TableByZixuan.position: TableByZixuan.position + 1},
                    synchronize_session=False,
                )
            else:
                # 获取自选组的 position 最大值
                max_position = (
                    session.query(func.max(TableByZixuan.position))
                    .filter(TableByZixuan.market == market)
                    .filter(TableByZixuan.zx_group == zx_group)
                    .scalar()
                )
                position = max_position + 1 if max_position is not None else 0
            zx_stock = TableByZixuan(
                market=market,
                zx_group=zx_group,
                stock_code=stock_code,
                stock_name=stock_name,
                stock_color=color,
                position=position,
                stock_memo=memo,
                add_datetime=datetime.datetime.now(),
            )
            session.add(zx_stock)
            session.commit()

        return True

    def zx_del_group_stock(self, market: str, zx_group: str, stock_code: str):
        with self.Session() as session:
            session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                TableByZixuan.zx_group == zx_group
            ).filter(TableByZixuan.stock_code == stock_code).delete()
            session.commit()

        return True

    def zx_update_stock_color(
        self, market: str, zx_group: str, stock_code: str, color: str
    ):
        with self.Session() as session:
            session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                TableByZixuan.zx_group == zx_group
            ).filter(TableByZixuan.stock_code == stock_code).update(
                {"stock_color": color}, synchronize_session=False
            )
            session.commit()

        return True

    def zx_update_stock_name(
        self, market: str, zx_group: str, stock_code: str, stock_name: str
    ):
        with self.Session() as session:
            session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                TableByZixuan.zx_group == zx_group
            ).filter(TableByZixuan.stock_code == stock_code).update(
                {"stock_name": stock_name}, synchronize_session=False
            )
            session.commit()

        return True

    def zx_stock_sort_top(self, market: str, zx_group: str, stock_code: str):
        with self.Session() as session:
            # market、zx_group 结果下的 position + 1
            session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                TableByZixuan.zx_group == zx_group
            ).update(
                {"position": TableByZixuan.position + 1}, synchronize_session=False
            )
            # 再将指定的股票 postition 更新为 0
            session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                TableByZixuan.zx_group == zx_group
            ).filter(TableByZixuan.stock_code == stock_code).update(
                {"position": 0}, synchronize_session=False
            )
            session.commit()

        return True

    def zx_stock_sort_bottom(self, market: str, zx_group: str, stock_code: str):
        with self.Session() as session:
            # 获取 market zx_group 结果下最大的position
            max_position = (
                session.query(func.max(TableByZixuan.position))
                .filter(TableByZixuan.market == market)
                .filter(TableByZixuan.zx_group == zx_group)
                .scalar()
            )
            # 将 market zx_group stock_code 结果下的 position 更新为 max_position + 1
            session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                TableByZixuan.zx_group == zx_group
            ).filter(TableByZixuan.stock_code == stock_code).update(
                {"position": max_position + 1}, synchronize_session=False
            )
            session.commit()

        return True

    def zx_clear_by_group(self, market: str, zx_group: str):
        with self.Session() as session:
            # 删除 market、zx_group 下所有的记录
            session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                TableByZixuan.zx_group == zx_group
            ).delete(synchronize_session=False)
            session.commit()

        return True

    def zx_query_group_by_code(self, market: str, stock_code: str) -> list[str]:
        with self.Session() as session:
            # 查询 market 下 stock_code 的所有去重的 zx_group 记录
            return [
                _[0]
                for _ in (
                    session.query(TableByZixuan.zx_group)
                    .filter(TableByZixuan.market == market)
                    .filter(TableByZixuan.stock_code == stock_code)
                    .distinct()
                    .all()
                )
            ]

    def order_save(
        self,
        market: str,
        stock_code: str,
        stock_name: str,
        order_type: str,
        order_price: float,
        order_amount: float,
        order_memo: str,
        order_time: str | datetime.datetime,
    ):
        with self.Session() as session:
            # 保存订单
            order = TableByOrder(
                market=market,
                stock_code=stock_code,
                stock_name=stock_name,
                order_type=order_type,
                order_price=order_price,
                order_amount=order_amount,
                order_memo=order_memo,
                dt=order_time,
            )
            session.add(order)
            session.commit()

        return True

    def order_query_by_code(self, market: str, stock_code: str) -> list[TableByOrder]:
        with self.Session() as session:
            # 查询 market 下 stock_code 的所有订单
            orders = (
                session.query(TableByOrder)
                .filter(TableByOrder.market == market)
                .filter(TableByOrder.stock_code == stock_code)
                .all()
            )

        # {
        #     "code": "SH.000001",
        #     "datetime": "2021-10-19 10:09:51",
        #     "type": "buy", (允许的值：buy 买入 sell 卖出  open_long 开多  close_long 平多 open_short 开空 close_short 平空)
        #     "price": 205.8,
        #     "amount": 300.0,
        #     "info": "涨涨涨"
        # }
        return [  # 兼容之前的
            {
                "code": _o.stock_code,
                "name": _o.stock_name,
                "datetime": _o.dt,
                "type": _o.order_type,
                "price": _o.order_price,
                "amount": _o.order_amount,
                "info": _o.order_memo,
            }
            for _o in orders
        ]

    def order_clear_by_code(self, market: str, stock_code: str):
        with self.Session() as session:
            # 清除 market 下 stock_code 的所有订单
            session.query(TableByOrder).filter(TableByOrder.market == market).filter(
                TableByOrder.stock_code == stock_code
            ).delete()
            session.commit()

        return True

    def task_save(
        self,
        market: str,
        task_name: str,
        zx_group: str,
        frequency: str,
        interval_minutes: int,
        check_bi_type: str,
        check_bi_beichi: str,
        check_bi_mmd: str,
        check_xd_type: str,
        check_xd_beichi: str,
        check_xd_mmd: str,
        check_bi_status: str,
        check_idx_ma_info: str,
        check_idx_macd_info: str,
        is_run: int,
        is_send_msg: int,
    ):
        with self.Session() as session:
            # 保存任务
            session.add(
                TableByAlertTask(
                    market=market,
                    task_name=task_name,
                    zx_group=zx_group,
                    frequency=frequency,
                    interval_minutes=interval_minutes,
                    check_bi_type=check_bi_type,
                    check_bi_beichi=check_bi_beichi,
                    check_bi_mmd=check_bi_mmd,
                    check_xd_type=check_xd_type,
                    check_xd_beichi=check_xd_beichi,
                    check_xd_mmd=check_xd_mmd,
                    check_bi_status=check_bi_status,
                    check_idx_ma_info=check_idx_ma_info,
                    check_idx_macd_info=check_idx_macd_info,
                    is_run=is_run,
                    is_send_msg=is_send_msg,
                    dt=datetime.datetime.now(),
                )
            )
            session.commit()

        return True

    def task_query(self, market: str = None, id: int = None) -> list[TableByAlertTask]:
        with self.Session() as session:
            # 查询任务
            query = session.query(TableByAlertTask)
            filter = ()
            if market is not None:
                filter += (TableByAlertTask.market == market,)
            if id is not None:
                filter += (TableByAlertTask.id == id,)
            if len(filter) > 0:
                return query.filter(*filter).all()
            return query.all()

    def task_delete(self, id: int):
        with self.Session() as session:
            # 删除任务
            session.query(TableByAlertTask).filter(TableByAlertTask.id == id).delete()
            session.commit()

        return True

    def task_update(
        self,
        id: int,
        market: str,
        task_name: str,
        zx_group: str,
        frequency: str,
        interval_minutes: int,
        check_bi_type: str,
        check_bi_beichi: str,
        check_bi_mmd: str,
        check_xd_type: str,
        check_xd_beichi: str,
        check_xd_mmd: str,
        check_bi_status: str,
        check_idx_ma_info: str,
        check_idx_macd_info: str,
        is_run: int,
        is_send_msg: int,
    ):
        with self.Session() as session:
            session.query(TableByAlertTask).filter(
                TableByAlertTask.market == market,
                TableByAlertTask.id == id,
            ).update(
                {
                    TableByAlertTask.task_name: task_name,
                    TableByAlertTask.zx_group: zx_group,
                    TableByAlertTask.frequency: frequency,
                    TableByAlertTask.interval_minutes: interval_minutes,
                    TableByAlertTask.check_bi_type: check_bi_type,
                    TableByAlertTask.check_bi_beichi: check_bi_beichi,
                    TableByAlertTask.check_bi_mmd: check_bi_mmd,
                    TableByAlertTask.check_xd_type: check_xd_type,
                    TableByAlertTask.check_xd_beichi: check_xd_beichi,
                    TableByAlertTask.check_xd_mmd: check_xd_mmd,
                    TableByAlertTask.check_bi_status: check_bi_status,
                    TableByAlertTask.check_idx_ma_info: check_idx_ma_info,
                    TableByAlertTask.check_idx_macd_info: check_idx_macd_info,
                    TableByAlertTask.is_run: is_run,
                    TableByAlertTask.is_send_msg: is_send_msg,
                    TableByAlertTask.dt: datetime.datetime.now(),
                }
            )
            session.commit()
        return True

    def alert_record_save(
        self,
        market: str,
        task_name: str,
        stock_code: str,
        stock_name: str,
        frequency: str,
        alert_msg: str,
        bi_is_done: str,
        bi_is_td: str,
        line_type: str,
        line_dt: datetime.datetime,
    ):
        """
        保存预警记录
        :param market:
        :param stock_code:
        :param stock_name:
        :param frequency:
        :param alert_msg:
        :param bi_is_down:
        :param bi_is_td:
        :param line_dt:
        :return:
        """
        with self.Session() as session:
            recored = TableByAlertRecord(
                market=market,
                task_name=task_name,
                stock_code=stock_code,
                stock_name=stock_name,
                frequency=frequency,
                alert_msg=alert_msg,
                bi_is_done=bi_is_done,
                bi_is_td=bi_is_td,
                line_type=line_type,
                line_dt=line_dt.replace(tzinfo=None),
                alert_dt=datetime.datetime.now(),
            )
            session.add(recored)
            session.commit()

        return True

    def alert_record_query_by_code(
        self,
        market: str,
        stock_code: str,
        frequency: str,
        line_type: str,
        line_dt: datetime.datetime,
    ) -> TableByAlertRecord:
        """
        查询预警记录
        :param market:
        :param stock_code:
        :param frequency:
        :param dt:
        :return:
        """
        with self.Session() as session:
            return (
                session.query(TableByAlertRecord)
                .filter(
                    TableByAlertRecord.market == market,
                    TableByAlertRecord.stock_code == stock_code,
                    TableByAlertRecord.frequency == frequency,
                    TableByAlertRecord.line_type == line_type,
                    TableByAlertRecord.line_dt == line_dt,
                )
                .order_by(TableByAlertRecord.alert_dt.desc())
                .first()
            )

    def alert_record_query(
        self, market: str, task_name: str = None
    ) -> list[TableByAlertRecord]:
        """
        查询预警记录
        :param market:
        :param stock_code:
        :param frequency:
        :param dt:
        :return:
        """
        with self.Session() as session:
            query = session.query(TableByAlertRecord)
            query = query.filter(TableByAlertRecord.market == market)
            if task_name:
                query = query.filter(TableByAlertRecord.task_name == task_name)
            return query.order_by(TableByAlertRecord.alert_dt.desc()).limit(100)

    def marks_add(
        self,
        market: str,
        stock_code: str,
        stock_name: str,
        frequency: str,
        mark_time: int,
        mark_label: str,
        mark_tooltip: str,
        mark_shape: str,
        mark_color: str,
    ):
        """
        添加代码在 tv 时间轴显示的信息
        :param market:
        :param stock_code:
        :param stock_name:
        :param frequency:   需要在什么周期显示，默认 ‘’，所有周期，可以是 'd', '30m', '5m' 这样之下指定周期下展示
        :param mark_time:   int 时间戳
        :param mark_label:  时间刻度标记的标签，英文字母，最大 两位
        :param mark_tooltip:    工具提示内容
        :param mark_shape:  "circle" | "earningUp" | "earningDown" | "earning" 形状
        :param mark_color: 颜色 rgb，比如 'red'  '#FF0000'
        :return:
        """
        with self.Session() as session:
            # 相同的 market,code/mark_time/mark_label 只能又一个，先删除一下
            session.query(TableByTVMarks).filter(
                TableByTVMarks.market == market,
                TableByTVMarks.stock_code == stock_code,
                TableByTVMarks.mark_time == mark_time,
                TableByTVMarks.mark_label == mark_label,
            ).delete()

            mark = TableByTVMarks(
                market=market,
                stock_code=stock_code,
                stock_name=stock_name,
                frequency=frequency,
                mark_time=mark_time,
                mark_label=mark_label,
                mark_tooltip=mark_tooltip,
                mark_shape=mark_shape,
                mark_color=mark_color,
                dt=datetime.datetime.now(),
            )
            session.add(mark)
            session.commit()

        return True

    def marks_query(
        self, market: str, stock_code: str, start_date: int = None
    ) -> list[TableByTVMarks]:
        """
        查询图表标记
        :param market:
        :param stock_code:
        :return:
        """
        with self.Session() as session:
            query = session.query(TableByTVMarks).filter(
                TableByTVMarks.market == market,
                TableByTVMarks.stock_code == stock_code,
            )
            if start_date is not None:
                query = query.filter(TableByTVMarks.mark_time >= start_date)

            return query.order_by(TableByTVMarks.mark_time.asc()).all()

    def marks_del(self, market: str, mark_label: str):
        with self.Session() as session:
            session.query(TableByTVMarks).filter(
                TableByTVMarks.market == market, TableByTVMarks.mark_label == mark_label
            ).delete()
            session.commit()

        return True

    def marks_add_by_price(
        self,
        market: str,
        stock_code: str,
        stock_name: str,
        frequency: str,
        mark_time: int,
        mark_label: str,
        mark_text: str,
        mark_label_color: str,
        mark_color: str,
    ):
        """
        添加代码在 tv 价格主图显示的信息
        """
        with self.Session() as session:
            # 相同的 market,code/mark_time/mark_label 只能有一个，先删除一下
            session.query(TableByTVMarks).filter(
                TableByTVMarks.market == market,
                TableByTVMarks.stock_code == stock_code,
                TableByTVMarks.mark_time == mark_time,
                TableByTVMarks.mark_label == mark_label,
            ).delete()

            mark = TableByTVMarksPrice(
                market=market,
                stock_code=stock_code,
                stock_name=stock_name,
                frequency=frequency,
                mark_time=mark_time,
                mark_color=mark_color,
                mark_text=mark_text,
                mark_label=mark_label,
                mark_label_font_color=mark_label_color,
                mark_min_size=1,
                dt=datetime.datetime.now(),
            )
            session.add(mark)
            session.commit()

        return True

    def marks_query_by_price(
        self, market: str, stock_code: str, start_date: int = None
    ) -> list[TableByTVMarksPrice]:
        """
        查询图表标记
        :param market:
        :param stock_code:
        :return:
        """
        with self.Session() as session:
            query = session.query(TableByTVMarksPrice).filter(
                TableByTVMarksPrice.market == market,
                TableByTVMarksPrice.stock_code == stock_code,
            )
            if start_date is not None:
                query = query.filter(TableByTVMarksPrice.mark_time >= start_date)
            return query.order_by(TableByTVMarksPrice.mark_time.asc()).all()

    def marks_del_by_price(self, market: str, mark_label: str):
        with self.Session() as session:
            session.query(TableByTVMarksPrice).filter(
                TableByTVMarks.market == market,
                TableByTVMarksPrice.mark_label == mark_label,
            ).delete(synchronize_session=False)
            session.commit()

        return True

    def marks_del_all_by_code(self, market: str, code: str):
        """
        删除代码的所有标记
        """
        with self.Session() as session:
            session.query(TableByTVMarks).filter(
                TableByTVMarks.market == market,
                TableByTVMarks.stock_code == code,
            ).delete()
            session.query(TableByTVMarksPrice).filter(
                TableByTVMarksPrice.market == market,
                TableByTVMarksPrice.stock_code == code,
            ).delete()
            session.commit()
        return True

    # ------------------------------------------------------------------
    # TradingView 独立存储：布局（charts）
    # ------------------------------------------------------------------
    def tv_storage_list_charts(self, client_id, user_id=TV_STORAGE_USER_ID):
        # 布局列表：只取 chart_type='chart'，按 user_id + client_id 过滤
        with self.Session() as session:
            return (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.chart_type == "chart",
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                )
                .order_by(TableByTVCharts.timestamp.desc(), TableByTVCharts.id.desc())
                .all()
            )

    def tv_storage_save_chart(
        self,
        client_id,
        user_id,
        chart_id,
        name,
        symbol,
        resolution,
        content,
        timestamp=None,
    ):
        """保存/更新布局，返回布局 id；更新时做迁移记账（一次性备份旧 content）"""
        if timestamp is None:
            timestamp = int(time.time())
        timestamp = int(timestamp)

        with self.Session() as session:
            chart = None
            if chart_id:
                chart = (
                    session.query(TableByTVCharts)
                    .filter(
                        TableByTVCharts.id == chart_id,
                        TableByTVCharts.chart_type == "chart",
                        TableByTVCharts.client_id == client_id,
                        TableByTVCharts.user_id == user_id,
                    )
                    .first()
                )

            if chart is None:
                # 新增分支：新布局天然是独立存储
                chart = TableByTVCharts(
                    client_id=client_id,
                    user_id=user_id,
                    chart_type="chart",
                    name=name,
                    symbol=symbol,
                    resolution=resolution,
                    content=content,
                    timestamp=timestamp,
                    drawings_migrated=True,
                    legacy_content=None,
                )
                session.add(chart)
            else:
                # 更新分支：迁移记账必须在覆盖 content 之前完成
                if not chart.drawings_migrated:
                    old_content = chart.content or ""
                    if (
                        LEGACY_DRAWING_PATTERN.search(old_content)
                        and not chart.legacy_content
                    ):
                        chart.legacy_content = old_content
                    chart.drawings_migrated = True
                chart.name = name
                chart.symbol = symbol
                chart.resolution = resolution
                chart.content = content
                chart.timestamp = timestamp

            session.commit()
            return int(chart.id)

    def tv_storage_get_chart_content(self, client_id, user_id, chart_id):
        # 获取布局 content，不存在返回 None
        with self.Session() as session:
            chart = (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.id == chart_id,
                    TableByTVCharts.chart_type == "chart",
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                )
                .first()
            )
            return chart.content if chart is not None else None

    def tv_storage_delete_chart(self, client_id, user_id, chart_id):
        # 删除布局，并连带删除该布局的绘图条目
        with self.Session() as session:
            chart = (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.id == chart_id,
                    TableByTVCharts.chart_type == "chart",
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                )
                .first()
            )
            if chart is None:
                return False

            session.query(TableByTVDrawingItems).filter(
                TableByTVDrawingItems.client_id == client_id,
                TableByTVDrawingItems.user_id == user_id,
                TableByTVDrawingItems.layout_id == str(chart_id),
            ).delete(synchronize_session=False)

            session.delete(chart)
            session.commit()
            return True

    def tv_storage_migration_status(self, client_id, user_id):
        # 迁移进度：总数 / 已迁移 / 待迁移列表
        charts = self.tv_storage_list_charts(client_id, user_id)
        migrated = [c for c in charts if c.drawings_migrated]
        pending = [c for c in charts if not c.drawings_migrated]
        return {
            "total": len(charts),
            "migrated": len(migrated),
            "pending": [
                {
                    "id": c.id,
                    "name": c.name,
                    "symbol": c.symbol,
                    "timestamp": c.timestamp,
                }
                for c in pending
            ],
        }

    # ------------------------------------------------------------------
    # TradingView 独立存储：指标模板（复用 cl_tv_charts chart_type='template'）
    # 模板类数据只按 user_id 过滤（旧数据分散在各 client 下，必须都能看到）
    # ------------------------------------------------------------------
    def tv_storage_list_study_templates(self, user_id):
        # 指标模板名称列表（按 name 去重）
        with self.Session() as session:
            rows = (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.chart_type == "template",
                    TableByTVCharts.user_id == user_id,
                )
                .order_by(TableByTVCharts.id.asc())
                .all()
            )

        names = []
        seen = set()
        for row in rows:
            if row.name in seen:
                continue
            seen.add(row.name)
            names.append(row.name)
        return names

    def tv_storage_get_study_template_content(self, user_id, name):
        # 获取指标模板 content（同名多份时取最新一条），不存在返回 None
        with self.Session() as session:
            row = (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.chart_type == "template",
                    TableByTVCharts.user_id == user_id,
                    TableByTVCharts.name == name,
                )
                .order_by(TableByTVCharts.id.desc())
                .first()
            )
            return row.content if row is not None else None

    def tv_storage_save_study_template(self, user_id, name, content):
        # upsert 指标模板（chart_type='template'）
        content_text = _tv_storage_content_text(content)
        with self.Session() as session:
            updated = (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.chart_type == "template",
                    TableByTVCharts.user_id == user_id,
                    TableByTVCharts.name == name,
                )
                .update(
                    {TableByTVCharts.content: content_text},
                    synchronize_session=False,
                )
            )
            if updated == 0:
                session.add(
                    TableByTVCharts(
                        client_id=TV_STORAGE_CLIENT_ID,
                        user_id=user_id,
                        chart_type="template",
                        name=name,
                        content=content_text,
                        symbol="",
                        resolution="",
                        timestamp=int(time.time()),
                    )
                )
            session.commit()

    def tv_storage_delete_study_template(self, user_id, name):
        # 删除指标模板（同名多份一并删除）
        with self.Session() as session:
            deleted = (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.chart_type == "template",
                    TableByTVCharts.user_id == user_id,
                    TableByTVCharts.name == name,
                )
                .delete(synchronize_session=False)
            )
            session.commit()
            return deleted > 0

    # ------------------------------------------------------------------
    # TradingView 独立存储：画图模板
    # ------------------------------------------------------------------
    def tv_storage_list_drawing_templates(self, user_id, tool):
        # 画图模板名称列表
        with self.Session() as session:
            rows = (
                session.query(TableByTVDrawingTemplates)
                .filter(
                    TableByTVDrawingTemplates.user_id == user_id,
                    TableByTVDrawingTemplates.tool == tool,
                )
                .order_by(TableByTVDrawingTemplates.id.asc())
                .all()
            )
            return [row.name for row in rows]

    def tv_storage_get_drawing_template_content(self, user_id, tool, name):
        # 获取画图模板 content，不存在返回 None
        with self.Session() as session:
            row = (
                session.query(TableByTVDrawingTemplates)
                .filter(
                    TableByTVDrawingTemplates.user_id == user_id,
                    TableByTVDrawingTemplates.tool == tool,
                    TableByTVDrawingTemplates.name == name,
                )
                .first()
            )
            return row.content if row is not None else None

    def tv_storage_save_drawing_template(self, user_id, tool, name, content):
        # upsert 画图模板
        content_text = _tv_storage_content_text(content)
        with self.Session() as session:
            row = (
                session.query(TableByTVDrawingTemplates)
                .filter(
                    TableByTVDrawingTemplates.user_id == user_id,
                    TableByTVDrawingTemplates.tool == tool,
                    TableByTVDrawingTemplates.name == name,
                )
                .first()
            )
            if row is not None:
                row.content = content_text
                row.updated_at = datetime.datetime.now()
            else:
                session.add(
                    TableByTVDrawingTemplates(
                        client_id=TV_STORAGE_CLIENT_ID,
                        user_id=user_id,
                        tool=tool,
                        name=name,
                        content=content_text,
                        updated_at=datetime.datetime.now(),
                    )
                )
            session.commit()

    def tv_storage_delete_drawing_template(self, user_id, tool, name):
        # 删除画图模板
        with self.Session() as session:
            deleted = (
                session.query(TableByTVDrawingTemplates)
                .filter(
                    TableByTVDrawingTemplates.user_id == user_id,
                    TableByTVDrawingTemplates.tool == tool,
                    TableByTVDrawingTemplates.name == name,
                )
                .delete(synchronize_session=False)
            )
            session.commit()
            return deleted > 0

    # ------------------------------------------------------------------
    # TradingView 独立存储：图表模板（content 为任意 JSON）
    # ------------------------------------------------------------------
    def tv_storage_list_chart_templates(self, user_id):
        # 图表模板名称列表
        with self.Session() as session:
            rows = (
                session.query(TableByTVChartTemplates)
                .filter(TableByTVChartTemplates.user_id == user_id)
                .order_by(TableByTVChartTemplates.name.asc())
                .all()
            )
            return [row.name for row in rows]

    def tv_storage_get_chart_template_content(self, user_id, name):
        # 获取图表模板 content（解析为任意 JSON），不存在返回 None
        with self.Session() as session:
            row = (
                session.query(TableByTVChartTemplates)
                .filter(
                    TableByTVChartTemplates.user_id == user_id,
                    TableByTVChartTemplates.name == name,
                )
                .first()
            )
            if row is None:
                return None
            try:
                return json.loads(row.content)
            except (TypeError, ValueError):
                return row.content

    def tv_storage_save_chart_template(self, user_id, name, content):
        # upsert 图表模板（content 可以是任意 JSON）
        content_text = _tv_storage_content_text(content)
        with self.Session() as session:
            row = (
                session.query(TableByTVChartTemplates)
                .filter(
                    TableByTVChartTemplates.user_id == user_id,
                    TableByTVChartTemplates.name == name,
                )
                .first()
            )
            if row is not None:
                row.content = content_text
                row.updated_at = datetime.datetime.now()
            else:
                session.add(
                    TableByTVChartTemplates(
                        client_id=TV_STORAGE_CLIENT_ID,
                        user_id=user_id,
                        name=name,
                        content=content_text,
                        updated_at=datetime.datetime.now(),
                    )
                )
            session.commit()

    def tv_storage_delete_chart_template(self, user_id, name):
        # 删除图表模板
        with self.Session() as session:
            deleted = (
                session.query(TableByTVChartTemplates)
                .filter(
                    TableByTVChartTemplates.user_id == user_id,
                    TableByTVChartTemplates.name == name,
                )
                .delete(synchronize_session=False)
            )
            session.commit()
            return deleted > 0

    # ------------------------------------------------------------------
    # TradingView 独立存储：绘图（line tools，按条增量读写）
    # ------------------------------------------------------------------
    def tv_storage_load_line_tools(
        self,
        client_id,
        user_id,
        layout_id,
        chart_id=0,
        symbol=None,
        all_symbols=False,
    ):
        """加载绘图：作用域桶（layout_id + chart_id，非全局）+ 全局桶（sharing_mode=2）

        合并顺序：先全局、后作用域（作用域覆盖全局）。
        """
        layout_id = str(layout_id)
        chart_id = int(chart_id or 0)
        symbol = _tv_storage_clean_symbol(symbol)

        with self.Session() as session:
            # 作用域桶：该布局 + 图表下的非全局条目（不按 symbol 过滤）
            scope_rows = (
                session.query(TableByTVDrawingItems)
                .filter(
                    TableByTVDrawingItems.client_id == client_id,
                    TableByTVDrawingItems.user_id == user_id,
                    TableByTVDrawingItems.layout_id == layout_id,
                    TableByTVDrawingItems.chart_id == chart_id,
                    TableByTVDrawingItems.sharing_mode != TV_STORAGE_SHARING_GLOBAL,
                )
                .all()
            )

            # 全局桶：sharing_mode=2（跨布局），symbol 过滤大小写不敏感
            global_query = session.query(TableByTVDrawingItems).filter(
                TableByTVDrawingItems.client_id == client_id,
                TableByTVDrawingItems.user_id == user_id,
                TableByTVDrawingItems.sharing_mode == TV_STORAGE_SHARING_GLOBAL,
            )
            if not all_symbols and symbol is not None:
                global_query = global_query.filter(
                    func.lower(TableByTVDrawingItems.symbol) == symbol.lower()
                )
            global_rows = global_query.all()

            sources = {}
            groups = {}
            for row in list(global_rows) + list(scope_rows):
                target = sources if row.item_type == TV_STORAGE_ITEM_SOURCE else groups
                target[row.item_id] = _tv_storage_decode_state(row.state)

        return {"sources": sources, "groups": groups, "symbol": symbol or ""}

    def tv_storage_save_line_tools(
        self,
        client_id,
        user_id,
        layout_id,
        chart_id,
        sharing_mode,
        symbol,
        sources,
        groups,
    ):
        """保存绘图（含墓碑删除 / 全局脏副本清理），返回本次 saved/deleted 计数"""
        layout_id = str(layout_id)
        chart_id = int(chart_id or 0)
        sharing_mode = int(sharing_mode or 0)
        body_symbol = _tv_storage_clean_symbol(symbol)

        saved = 0
        deleted = 0

        with self.Session() as session:
            for item_type, items in (
                (TV_STORAGE_ITEM_SOURCE, sources or {}),
                (TV_STORAGE_ITEM_GROUP, groups or {}),
            ):
                for raw_item_id, item in items.items():
                    item_id = str(raw_item_id)
                    if not item_id:
                        continue

                    if item is None:
                        # 墓碑：删除对应行
                        deleted += (
                            session.query(TableByTVDrawingItems)
                            .filter(
                                TableByTVDrawingItems.client_id == client_id,
                                TableByTVDrawingItems.user_id == user_id,
                                TableByTVDrawingItems.layout_id == layout_id,
                                TableByTVDrawingItems.chart_id == chart_id,
                                TableByTVDrawingItems.sharing_mode == sharing_mode,
                                TableByTVDrawingItems.item_type == item_type,
                                TableByTVDrawingItems.item_id == item_id,
                            )
                            .delete(synchronize_session=False)
                        )
                    else:
                        if not isinstance(item, dict):
                            # 非对象状态无法存储，跳过
                            continue
                        item_symbol = (
                            _tv_storage_clean_symbol(item.get("symbol")) or body_symbol
                        )
                        owner_source = (
                            _tv_storage_clean_symbol(item.get("ownerSource"))
                            if item_type == TV_STORAGE_ITEM_SOURCE
                            else None
                        )
                        state = json.dumps(item, ensure_ascii=False)

                        row = (
                            session.query(TableByTVDrawingItems)
                            .filter(
                                TableByTVDrawingItems.client_id == client_id,
                                TableByTVDrawingItems.user_id == user_id,
                                TableByTVDrawingItems.layout_id == layout_id,
                                TableByTVDrawingItems.chart_id == chart_id,
                                TableByTVDrawingItems.sharing_mode == sharing_mode,
                                TableByTVDrawingItems.item_type == item_type,
                                TableByTVDrawingItems.item_id == item_id,
                            )
                            .first()
                        )
                        if row is not None:
                            row.symbol = item_symbol
                            row.owner_source = owner_source
                            row.state = state
                            row.updated_at = datetime.datetime.now()
                        else:
                            session.add(
                                TableByTVDrawingItems(
                                    client_id=client_id,
                                    user_id=user_id,
                                    layout_id=layout_id,
                                    chart_id=chart_id,
                                    sharing_mode=sharing_mode,
                                    item_type=item_type,
                                    item_id=item_id,
                                    symbol=item_symbol,
                                    owner_source=owner_source,
                                    state=state,
                                    updated_at=datetime.datetime.now(),
                                )
                            )
                        saved += 1

                    # 官方要求：全局共享保存时清理同 item_id 的非全局脏副本
                    if sharing_mode == TV_STORAGE_SHARING_GLOBAL:
                        session.query(TableByTVDrawingItems).filter(
                            TableByTVDrawingItems.client_id == client_id,
                            TableByTVDrawingItems.user_id == user_id,
                            TableByTVDrawingItems.item_type == item_type,
                            TableByTVDrawingItems.item_id == item_id,
                            TableByTVDrawingItems.sharing_mode
                            != TV_STORAGE_SHARING_GLOBAL,
                        ).delete(synchronize_session=False)

            session.commit()

        return {"saved": saved, "deleted": deleted}

    def cache_get(self, key: str):
        with self.Session() as session:
            # 获取当前时间戳
            now = int(time.time())
            # 获取缓存数据
            cache = session.query(TableByCache).filter(TableByCache.k == key).first()
            # 缓存数据存在，且缓存数据未过期
            if cache and (cache.expire == 0 or cache.expire > now):
                return json.loads(cache.v)
            # 缓存数据不存在，或缓存数据已过期
            # 删除过期缓存数据，expire_time != 0 and expire_time < now
            session.query(TableByCache).filter(
                TableByCache.expire != 0, TableByCache.expire < now
            ).delete()
            session.commit()

        return None

    def cache_set(self, key: str, val: dict, expire: int = 0):
        with self.Session() as session:
            session.query(TableByCache).filter(TableByCache.k == key).delete()
            cache = TableByCache(k=key, v=json.dumps(val), expire=expire)
            session.add(cache)
            session.commit()

        return True

    def cache_del(self, key: str):
        with self.Session() as session:
            session.query(TableByCache).filter(TableByCache.k == key).delete()
            session.commit()

        return True


db: DB = DB()

if __name__ == "__main__":
    db = DB()

    # db.klines_tables("a", "SH.111111")
    # print("Done")

    # # 增加自选股票
    # db.zx_add_group_stock("a", "我的持仓", "SH.000001", "上证指数", "", "red", location="top")
    # db.zx_add_group_stock("a", "我的持仓", "SH.600519", "贵州茅台", "", "green", location="top")

    # # 获取自选股下的股票代码
    # stocks = db.zx_get_group_stocks("a", "我的持仓")
    # for s in stocks:
    #     print(s.stock_code, s.stock_name, s.stock_color, s.position)

    # db.zx_update_stock_color("a", "我的持仓", "SH.600519", "yellow")
    # db.zx_update_stock_name("a", "我的持仓", "SH.600519", "贵州茅台[yyds]")
    # db.zx_stock_sort_top("a", "我的持仓", "SH.000001")
    # db.zx_stock_sort_bottom("a", "我的持仓", "SH.000001")

    # # 获取自选股下的股票代码
    # stocks = db.zx_get_group_stocks("a", "我的持仓")
    # for s in stocks:
    #     print(s.stock_code, s.stock_name, s.stock_color, s.position)

    # group = db.zx_query_group_by_code("a", "SH.000001")
    # print(group)

    # 订单测试
    # db.order_save(
    #     "a", "SH.000001", "上证指数", "buy", 3100.0, 100, "测试订单", datetime.datetime.now()
    # )
    # orders = db.order_query_by_code("a", "SH.000001")
    # for o in orders:
    #     print(
    #         o.market,
    #         o.stock_code,
    #         o.stock_name,
    #         o.order_type,
    #         o.order_price,
    #         o.order_amount,
    #         o.order_memo,
    #         o.dt,
    #     )
    # db.order_clear_by_code("a", "SH.000001")

    # 提醒任务测试
    # db.task_save(
    #     "a",
    #     "测试提醒任务",
    #     "我的持仓",
    #     "5m,30m",
    #     5,
    #     "up,down",
    #     "1buy,1sell",
    #     "1buy,2sell",
    #     "",
    #     "",
    #     "",
    #     1,
    #     1,
    # )
    # db.task_update(
    #     "a",
    #     "测试提醒任务",
    #     "我的持仓",
    #     "5m,15m",
    #     10,
    #     "up,down",
    #     "1buy,1sell",
    #     "1buy,2sell",
    #     "up",
    #     "xd,pz,qs",
    #     "1buy,",
    #     1,
    #     1,
    # )
    # db.task_delete('a', '测试提醒任务')
    # tasks = db.task_query("a")
    # for t in tasks:
    #     print(t.market, t.task_name, t.zx_group, t.frequencys, t.interval_minutes, t.dt)

    # 警报提醒
    # db.alert_record_save(
    #     "a",
    #     "SH.000001",
    #     "上证指数",
    #     "5m",
    #     "触发提醒",
    #     "笔完成",
    #     "TD",
    #     fun.str_to_datetime("2024-12-16 00:12:00"),
    # )

    # records = db.alert_record_query("a")
    # for r in records:
    #     print(
    #         r.market, r.stock_code, r.stock_name, r.frequency, r.alert_msg, r.alert_dt
    #     )

    # record = db.alert_record_query_by_code(
    #     "a", "SH.000001", "5m", fun.str_to_datetime("2024-12-16 00:12:00")
    # )
    # print(record)
    # print(record.alert_msg)

    # 图表标记
    # db.marks_add("a", "SH.000001", "上证", "", 1234567890, "AB", "测试标记", "cire")
    # marks = db.marks_query("a", "SH.000002")
    # for m in marks:
    #     print(
    #         m.market,
    #         m.stock_code,
    #         m.stock_name,
    #         m.mark_label,
    #         m.mark_tooltip,
    #         m.mark_shape,
    #     )

    # 添加图表标记
    # db.marks_add_by_price(
    #     "a",
    #     "SH.000001",
    #     "上证指数",
    #     "30m",
    #     fun.str_to_timeint("2026-05-22 14:30:00"),
    #     "A",
    #     "测试标记2",
    #     "green",
    #     "red",
    # )
    db.marks_del_by_price("a", "B")

    # 缓存
    # db.cache_set("test", "12312312312312", int(time.time()) + 5)
    # v = db.cache_get("test")
    # print(v)

    # from chanlun.exchange.exchange_tdx import ExchangeTDX
    # ex = ExchangeTDX()
    # ex_klines = ex.klines("SH.000001", "d")
    # print(ex_klines)

    # db.klines_insert("a", "SH.000001", "d", ex_klines)

    # klines = db.klines_query("a", "SH.000001", "d", end_date=datetime.datetime.now())
    # for k in klines[-10:]:
    #     print(k.code, k.f, k.dt, k.o, k.c, k.v)

    # last_dt = db.query_klines_last_datetime("a", "SH.000002", "d")
    # print(last_dt)

    # db.delete_klines("a", "SH.000001", "d")

    # insp = sqlalchemy.inspect(db.engine)
    # codes = ['SHFE.ao', 'DCE.jm', 'DCE.rr', 'DCE.j', 'DCE.v', 'DCE.fb', 'DCE.l', 'CZCE.PM', 'DCE.bb', 'CFFEX.TF', 'SHFE.ss', 'CZCE.RS', 'SHFE.au', 'CZCE.TC', 'DCE.c', 'SHFE.fu', 'CZCE.PF', 'SHFE.al', 'CFFEX.TS', 'DCE.cs', 'SHFE.wr', 'DCE.y', 'INE.sc', 'CZCE.WH', 'CZCE.WS', 'CZCE.PK', 'CZCE.WT', 'CZCE.OI', 'SHFE.ru', 'DCE.eg', 'SHFE.ag', 'INE.bc', 'SHFE.zn', 'CZCE.RI', 'CZCE.ME', 'SHFE.br', 'CZCE.UR', 'INE.lu', 'CZCE.JR', 'CZCE.RM', 'CZCE.SA', 'DCE.lh', 'INE.nr', 'CZCE.SR', 'CZCE.MA', 'SHFE.hc', 'DCE.b', 'CFFEX.TL', 'CFFEX.IH', 'CZCE.ZC', 'CZCE.PX', 'DCE.jd', 'GFEX.si', 'SHFE.sn', 'CZCE.AP', 'CZCE.ER', 'CZCE.RO', 'CFFEX.IM', 'CZCE.FG', 'SHFE.bu', 'CFFEX.IF', 'INE.ec', 'DCE.m', 'CZCE.LR', 'SHFE.cu', 'DCE.a', 'CZCE.TA', 'DCE.pp', 'CZCE.CY', 'SHFE.ni', 'DCE.i', 'SHFE.sp', 'CZCE.SM', 'DCE.pg', 'CZCE.CJ', 'SHFE.pb', 'CFFEX.T', 'CZCE.SH', 'CZCE.SF', 'CFFEX.IC', 'CZCE.CF', 'DCE.eb', 'GFEX.lc', 'DCE.p', 'SHFE.rb']
    # for table in insp.get_table_names():
    #     if table.startswith("futures_"):
    #         print(f"DROP TABLE `{table}`;")

    # record = db.alert_record_query_by_code(
    #     "a", "SZ.300014", "5m", "bi", fun.str_to_datetime("2023-12-25 13:55:00")
    # )
    # print(record)
