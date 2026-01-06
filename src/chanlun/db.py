import datetime
import json
import time
import warnings
from typing import List, Union

import numpy as np
import pandas as pd
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
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
    # TV 图表的布局
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
    # 添加配置设置编码
    __table_args__ = {"mysql_collate": "utf8mb4_general_ci"}


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


@fun.singleton
class DB(object):
    global Base

    def __init__(self) -> None:
        if config.DB_TYPE == "sqlite":
            db_path = get_data_path() / "db"
            if db_path.is_dir() is False:
                db_path.mkdir(parents=True)
            self.engine = create_engine(
                f"sqlite:///{str(db_path / f'{config.DB_DATABASE}.sqlite')}",
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
        elif market == Market.FX.value:
            table_name = f"{market}_klines_{stock_code}"
        elif market == Market.CURRENCY.value:
            table_name = f"{market}_klines_{stock_code}"
        elif market == Market.CURRENCY_SPOT.value:
            table_name = f"{market}_klines_{stock_code}"
        elif market == Market.FUTURES.value:
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
    ) -> List:
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

    def zx_get_groups(self, market: str) -> List[TableByZxGroup]:
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

    def zx_get_group_stocks(self, market: str, zx_group: str) -> List[TableByZixuan]:
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

    def zx_query_group_by_code(self, market: str, stock_code: str) -> List[str]:
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
        order_time: Union[str, datetime.datetime],
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

    def order_query_by_code(self, market: str, stock_code: str) -> List[TableByOrder]:
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
                    check_idx_ma_info=check_idx_ma_info,
                    check_idx_macd_info=check_idx_macd_info,
                    is_run=is_run,
                    is_send_msg=is_send_msg,
                    dt=datetime.datetime.now(),
                )
            )
            session.commit()

        return True

    def task_query(self, market: str = None, id: int = None) -> List[TableByAlertTask]:
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
    ) -> List[TableByAlertRecord]:
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
    ) -> List[TableByTVMarks]:
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
    ) -> List[TableByTVMarksPrice]:
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
            ).delete()
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

    def tv_chart_list(self, chart_type, client_id, user_id):
        with self.Session() as session:
            return (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.chart_type == chart_type,
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                )
                .all()
            )

    def tv_chart_save(
        self, chart_type, client_id, user_id, name, content, symbol, resolution
    ):
        # 保存图表布局，并返回 id
        with self.Session() as session:
            chart = TableByTVCharts(
                chart_type=chart_type,
                client_id=client_id,
                user_id=user_id,
                name=name,
                content=content,
                symbol=symbol,
                resolution=resolution,
                timestamp=int(time.time()),
            )
            session.add(chart)
            session.commit()
            return chart.id

    def tv_chart_update(
        self, chart_type, id, client_id, user_id, name, content, symbol, resolution
    ):
        # 更新图表布局
        with self.Session() as session:
            session.query(TableByTVCharts).filter(
                TableByTVCharts.id == id,
                TableByTVCharts.client_id == client_id,
                TableByTVCharts.user_id == user_id,
                TableByTVCharts.chart_type == chart_type,
            ).update(
                {
                    TableByTVCharts.name: name,
                    TableByTVCharts.content: content,
                    TableByTVCharts.symbol: symbol,
                    TableByTVCharts.resolution: resolution,
                    TableByTVCharts.timestamp: int(time.time()),
                }
            )
            session.commit()
        return True

    def tv_chart_get(self, chart_type, id, client_id, user_id):
        # 获取图表布局
        with self.Session() as session:
            return (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.id == id,
                    TableByTVCharts.chart_type == chart_type,
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                )
                .first()
            )

    def tv_chart_get_by_name(self, chart_type, name, client_id, user_id):
        # 获取图表布局
        with self.Session() as session:
            return (
                session.query(TableByTVCharts)
                .filter(
                    TableByTVCharts.name == name,
                    TableByTVCharts.chart_type == chart_type,
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                )
                .first()
            )

    def tv_chart_del(self, chart_type, id, client_id, user_id):
        # 删除图表布局
        with self.Session() as session:
            session.query(TableByTVCharts).filter(
                TableByTVCharts.id == id,
                TableByTVCharts.chart_type == chart_type,
                TableByTVCharts.client_id == client_id,
                TableByTVCharts.user_id == user_id,
            ).delete()
            session.commit()
        return True

    def tv_chart_del_by_name(self, chart_type, name, client_id, user_id):
        # 根据名称删除图表布局
        with self.Session() as session:
            session.query(TableByTVCharts).filter(
                TableByTVCharts.name == name,
                TableByTVCharts.chart_type == chart_type,
                TableByTVCharts.client_id == client_id,
                TableByTVCharts.user_id == user_id,
            ).delete()
            session.commit()
        return True

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
    db.marks_add_by_price(
        "a",
        "SH.600378",
        "昊华科技",
        "30m",
        fun.str_to_timeint("2025-07-03 14:00:00"),
        "A",
        "测试标记2",
        "green",
        "red",
    )

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
