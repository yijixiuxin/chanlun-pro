import datetime
import json
import threading
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
    Index,
    create_engine,
    func,
)
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from chanlun import config, fun
from chanlun.base import Market
from chanlun.config import get_data_path
from chanlun.tools.log_util import LogUtil

warnings.filterwarnings("ignore")

# https://docs.sqlalchemy.org/en/20/core/types.html

from chanlun.db_models.base import Base
from chanlun.db_models.alert_record import TableByAlertRecord
from chanlun.db_models.alert_task import TableByAlertTask
from chanlun.db_models.cache import TableByCache
from chanlun.db_models.order import TableByOrder
from chanlun.db_models.tv_charts import TableByTVCharts
from chanlun.db_models.tv_marks import TableByTVMarks
from chanlun.db_models.tv_marks_price import TableByTVMarksPrice
from chanlun.db_models.zixuan import TableByZixuan
from chanlun.db_models.zixuan_group import TableByZxGroup
@fun.singleton
class DB(object):

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
                connect_args={"check_same_thread": False},
            )
        elif config.DB_TYPE == "mysql":
            self.engine = create_engine(
                f"mysql+pymysql://{config.DB_USER}:{config.DB_PWD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_DATABASE}?charset=utf8mb4",
                echo=False,
                poolclass=QueuePool,
                pool_recycle=1800,
                pool_pre_ping=True,
                pool_use_lifo=True,
                pool_reset_on_return="rollback",
                pool_size=10,
                max_overflow=20,
                pool_timeout=10,
                connect_args={
                    "connect_timeout": 5,
                    "read_timeout": 10,
                    "write_timeout": 10,
                },
            )
        else:
            raise Exception("DB_TYPE 配置错误")

        # 避免提交后对象过期导致二次加载，提高查询性能
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

        Base.metadata.create_all(self.engine)

        self.__cache_tables = {}
        # 轻量级缓存：最后一根K线时间，降低重复查询成本。
        # 为避免多线程并发读写出现可见性问题（写入新 K 线时缓存与 DB 不一致），
        # 使用一个独立的锁保护 _last_dt_cache 的所有读写。
        self._last_dt_cache: dict = {}
        self._last_dt_cache_lock = threading.Lock()

    def _get_last_dt_cache(self, market: str, code: str, frequency: str):
        """加锁读 _last_dt_cache。"""
        with self._last_dt_cache_lock:
            return self._last_dt_cache.get((market, code, frequency))

    def _set_last_dt_cache(self, market: str, code: str, frequency: str, value):
        """加锁写 _last_dt_cache。"""
        with self._last_dt_cache_lock:
            self._last_dt_cache[(market, code, frequency)] = value

    def _invalidate_last_dt_cache(self, market: str, code: str, frequency: str):
        """加锁失效 _last_dt_cache 中指定 key。"""
        with self._last_dt_cache_lock:
            self._last_dt_cache.pop((market, code, frequency), None)

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
            table_name = f"{market}_klines_{stock_code}"
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
                # 频繁的 (code,f,dt) 查询与排序，建立复合索引
                Index("idx_code_f_dt", "code", "f", "dt"),
                {"mysql_collate": "utf8mb4_general_ci"},
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
            # 注意：__table_args__ 已在上方统一声明，避免覆盖

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
        auto_reverse: bool = False,
    ) -> List:
        """
        获取k线数据。

        ⚠️ 注意返回方向：
        - ``order='desc'``（默认）+ ``limit`` 是为了"取最近 N 根"而设计的，
          因此返回结果是 **按 dt 降序** 的（最新的在 [0] 位置）。
        - 缠论计算 / 大多数业务消费方期望的是 **升序**（最早的在 [0] 位置）。
        - 调用方如果需要升序，可以：
            a) 显式传 ``order='asc'``（但当 limit 生效时会取到"最早 N 根"，语义不同）；
            b) 传 ``auto_reverse=True``：保持 desc+limit 的"取最近 N 根"语义，
               但在返回前自动反转为升序，同时兼顾"最近 N 根 + 升序"两个诉求。

        :param market:
        :param code:
        :param frequency:
        :param start_date:
        :param end_date:
        :param limit:
        :param order: ``'desc'`` 或 ``'asc'``。
        :param auto_reverse: 仅在 ``order='desc'`` 时生效，将结果反转为升序返回。
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
            rows = query.all()
            if auto_reverse and order == "desc":
                rows = list(reversed(rows))
            return rows

    def klines_last_datetime(self, market, code, frequency):
        """
        查询k线表中最后一条记录的日期
        :param market:
        :param code:
        :param frequency:
        :return:
        """
        # 命中轻量级缓存（加锁），减少数据库查询
        cached = self._get_last_dt_cache(market, code, frequency)
        if cached is not None:
            return cached

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
                _res = last_date[0].strftime("%Y-%m-%d")
            else:
                _res = last_date[0].strftime("%Y-%m-%d %H:%M:%S")

        # 加锁写缓存
        self._set_last_dt_cache(market, code, frequency, _res)
        return _res

    # MySQL upsert 的批大小：默认 1000，受 max_allowed_packet 限制建议 500~5000。
    # 之前直接调到 20000 在大列宽 K 线表上很容易触发 "MySQL server has gone away"。
    KLINES_INSERT_BATCH_SIZE = 1000

    def klines_insert(
        self, market: str, code: str, frequency: str, klines: pd.DataFrame
    ):
        """
        插入k线 (性能优化版)
        """
        if klines.empty:
            return True

        # 1. 数据预处理 (Pandas 向量化操作，替代 iterrows)
        df = klines.copy()

        # 统一处理时间：去除时区信息。鲁棒化处理：
        # - 列已经是 tz-aware datetime 时，做 tz_localize(None)
        # - 列是 tz-naive datetime 时，原样返回
        # - 列是 object/混合类型时，用 pd.to_datetime 统一标准化
        date_col = df["date"]
        if not pd.api.types.is_datetime64_any_dtype(date_col):
            date_col = pd.to_datetime(date_col, errors="coerce", utc=False)
        if getattr(date_col.dt, "tz", None) is not None:
            date_col = date_col.dt.tz_localize(None)
        df["dt"] = date_col
        # 异常 date 值不能写入 DB（主键），直接抛出让调用方处理。
        if df["dt"].isna().any():
            raise ValueError(
                f"klines_insert({market},{code},{frequency}) 中存在无法解析的 date 值"
            )

        # 映射列名：API返回的列名 -> 数据库列名
        rename_map = {
            "open": "o",
            "close": "c",
            "high": "h",
            "low": "l",
            "volume": "v",
            "position": "p",
        }
        df.rename(columns=rename_map, inplace=True)

        # 填充必要的索引列
        df["code"] = code
        df["f"] = frequency

        # 筛选需要入库的列
        db_columns = ["code", "dt", "f", "o", "c", "h", "l", "v"]
        if "p" in df.columns:
            db_columns.append("p")

        # 仅保留存在的列，防止 KeyError
        final_columns = [col for col in db_columns if col in df.columns]
        data_to_insert = df[final_columns].to_dict(orient="records")

        # 写入前先失效缓存（保证读侧不会拿到过期 last_dt）
        self._invalidate_last_dt_cache(market, code, frequency)

        with self.Session() as session:
            table = self.klines_tables(market, code)

            # SQLite 分支：逐行 upsert，必须有 try/except + rollback，
            # 否则一条记录失败会让前面已 add 的数据状态不一致。
            if config.DB_TYPE == "sqlite":
                try:
                    for _in_k in data_to_insert:
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
                except Exception:
                    session.rollback()
                    raise
                return True

            # MySQL 批量 upsert
            batch_size = self.KLINES_INSERT_BATCH_SIZE
            try:
                for i in range(0, len(data_to_insert), batch_size):
                    batch = data_to_insert[i : i + batch_size]
                    insert_stmt = insert(table).values(batch)
                    # 主键/唯一索引列不参与 update
                    update_columns = {
                        x.name: x
                        for x in insert_stmt.inserted
                        if x.name not in ("code", "dt", "f")
                    }
                    upsert_stmt = insert_stmt.on_duplicate_key_update(**update_columns)
                    session.execute(upsert_stmt)
                session.commit()
            except Exception as e:
                session.rollback()
                LogUtil.error(f"Batch Insert Error: {e}")
                raise

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
        # 删除前先失效缓存（无论后续是否成功），避免读到过期值。
        if frequency is not None:
            self._invalidate_last_dt_cache(market, code, frequency)
        with self.Session() as session:
            try:
                table = self.klines_tables(market, code)
                q = session.query(table).filter(table.code == code)
                if frequency is not None:
                    q = q.filter(table.f == frequency)
                if dt is not None:
                    q = q.filter(table.dt == dt)
                q.delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

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
            try:
                session.add(
                    TableByZxGroup(
                        market=market, zx_group=zx_group, add_dt=datetime.datetime.now()
                    )
                )
                session.commit()
            except Exception:
                session.rollback()
                raise

        return True

    def zx_del_group(self, market: str, zx_group: str) -> bool:
        """
        删除自选分组
        """
        with self.Session() as session:
            try:
                session.query(TableByZxGroup).filter(
                    TableByZxGroup.market == market, TableByZxGroup.zx_group == zx_group
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

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
            try:
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
            except Exception:
                session.rollback()
                raise

        return True

    def zx_del_group_stock(self, market: str, zx_group: str, stock_code: str):
        with self.Session() as session:
            try:
                session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                    TableByZixuan.zx_group == zx_group
                ).filter(TableByZixuan.stock_code == stock_code).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

        return True

    def zx_update_stock_color(
        self, market: str, zx_group: str, stock_code: str, color: str
    ):
        with self.Session() as session:
            try:
                session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                    TableByZixuan.zx_group == zx_group
                ).filter(TableByZixuan.stock_code == stock_code).update(
                    {"stock_color": color}, synchronize_session=False
                )
                session.commit()
            except Exception:
                session.rollback()
                raise

        return True

    def zx_update_stock_name(
        self, market: str, zx_group: str, stock_code: str, stock_name: str
    ):
        with self.Session() as session:
            try:
                session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                    TableByZixuan.zx_group == zx_group
                ).filter(TableByZixuan.stock_code == stock_code).update(
                    {"stock_name": stock_name}, synchronize_session=False
                )
                session.commit()
            except Exception:
                session.rollback()
                raise

        return True

    def zx_stock_sort_top(self, market: str, zx_group: str, stock_code: str):
        with self.Session() as session:
            try:
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
            except Exception:
                session.rollback()
                raise

        return True

    def zx_stock_sort_bottom(self, market: str, zx_group: str, stock_code: str):
        with self.Session() as session:
            try:
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
            except Exception:
                session.rollback()
                raise

        return True

    def zx_clear_by_group(self, market: str, zx_group: str):
        with self.Session() as session:
            try:
                # 删除 market、zx_group 下所有的记录
                session.query(TableByZixuan).filter(TableByZixuan.market == market).filter(
                    TableByZixuan.zx_group == zx_group
                ).delete(synchronize_session=False)
                session.commit()
            except Exception:
                session.rollback()
                raise

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
            try:
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
            except Exception:
                session.rollback()
                raise

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
            try:
                # 清除 market 下 stock_code 的所有订单
                session.query(TableByOrder).filter(TableByOrder.market == market).filter(
                    TableByOrder.stock_code == stock_code
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

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
            try:
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
            except Exception:
                session.rollback()
                raise

        return True

    def task_query(self, market: str = None, id: int = None) -> List[TableByAlertTask]:
        """
        查询任务列表。

        签名为兼容历史调用方仍然返回 ``List``。

        约束：
        - 当传 ``id`` 时，``id`` 是主键，理论上只会有 0 或 1 条；
          ``.limit(1)`` 防御脏数据（极端情况存在多条同 id），
          避免上层静默丢弃多余记录。
        """
        with self.Session() as session:
            # 查询任务
            query = session.query(TableByAlertTask)
            filter = ()
            if market is not None:
                filter += (TableByAlertTask.market == market,)
            if id is not None:
                filter += (TableByAlertTask.id == id,)
            if len(filter) > 0:
                query = query.filter(*filter)
            if id is not None:
                # 主键查询只可能命中 1 条；显式 limit(1) 既加快查询，也防御脏数据。
                query = query.limit(1)
            return query.all()

    def task_delete(self, id: int):
        with self.Session() as session:
            try:
                # 删除任务
                session.query(TableByAlertTask).filter(TableByAlertTask.id == id).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

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
            try:
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
            except Exception:
                session.rollback()
                raise
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
            try:
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
            except Exception:
                session.rollback()
                raise

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
            try:
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
            except Exception:
                session.rollback()
                raise

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
            try:
                session.query(TableByTVMarks).filter(
                    TableByTVMarks.market == market, TableByTVMarks.mark_label == mark_label
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

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
            try:
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
            except Exception:
                session.rollback()
                raise

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
            try:
                # 修复原 bug：原代码这里把 TableByTVMarks.market 与 TableByTVMarksPrice 混用，
                # 实际期望删除的是 TableByTVMarksPrice，统一改为 TableByTVMarksPrice.market。
                session.query(TableByTVMarksPrice).filter(
                    TableByTVMarksPrice.market == market,
                    TableByTVMarksPrice.mark_label == mark_label,
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

        return True

    def marks_del_all_by_code(self, market: str, code: str):
        """
        删除代码的所有标记
        """
        with self.Session() as session:
            try:
                session.query(TableByTVMarks).filter(
                    TableByTVMarks.market == market,
                    TableByTVMarks.stock_code == code,
                ).delete()
                session.query(TableByTVMarksPrice).filter(
                    TableByTVMarksPrice.market == market,
                    TableByTVMarksPrice.stock_code == code,
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise
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
            # 如果是 drawing 或 study_template，先尝试根据名称查找并更新，实现覆盖保存
            if chart_type in ["drawing", "study_template"]:
                chart = (
                    session.query(TableByTVCharts)
                    .filter(
                        TableByTVCharts.name == name,
                        TableByTVCharts.chart_type == chart_type,
                        TableByTVCharts.client_id == client_id,
                        TableByTVCharts.user_id == user_id,
                    )
                    .first()
                )
                if chart:
                    chart.content = content
                    chart.symbol = symbol
                    chart.resolution = resolution
                    chart.timestamp = int(time.time())
                    session.commit()
                    return chart.id

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
                .order_by(TableByTVCharts.timestamp.desc())
                .first()
            )

    def tv_chart_del(self, chart_type, id, client_id, user_id):
        # 删除图表布局
        with self.Session() as session:
            try:
                session.query(TableByTVCharts).filter(
                    TableByTVCharts.id == id,
                    TableByTVCharts.chart_type == chart_type,
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise
        return True

    def tv_chart_del_by_name(self, chart_type, name, client_id, user_id):
        # 根据名称删除图表布局
        with self.Session() as session:
            try:
                session.query(TableByTVCharts).filter(
                    TableByTVCharts.name == name,
                    TableByTVCharts.chart_type == chart_type,
                    TableByTVCharts.client_id == client_id,
                    TableByTVCharts.user_id == user_id,
                ).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise
        return True

    # cache_get 过期清理节流：避免每次读 cache 都扫全表 delete，造成写放大。
    # 同进程内每 _CACHE_GC_INTERVAL_SEC 秒最多触发一次过期清理；
    # 用类属性 + 简单时间戳即可，不需要锁（多次执行也只是重复 delete，幂等无害）。
    _CACHE_GC_INTERVAL_SEC = 300
    _last_cache_gc_at = 0.0

    def cache_get(self, key: str):
        for attempt in range(2):
            try:
                with self.Session() as session:
                    now = int(time.time())
                    cache = (
                        session.query(TableByCache)
                        .filter(TableByCache.k == key)
                        .first()
                    )
                    if cache and (cache.expire == 0 or cache.expire > now):
                        return json.loads(cache.v)

                    # 过期清理节流：只有距离上次清理超过 _CACHE_GC_INTERVAL_SEC 秒才执行；
                    # 并且 delete + commit 必须自己包 try/except + rollback，
                    # 失败时不能让外层吞掉（外层 except 会把它当成可重试的连接异常）。
                    if (now - DB._last_cache_gc_at) >= self._CACHE_GC_INTERVAL_SEC:
                        try:
                            session.query(TableByCache).filter(
                                TableByCache.expire != 0,
                                TableByCache.expire < now,
                            ).delete()
                            session.commit()
                            DB._last_cache_gc_at = now
                        except Exception as gc_exc:
                            session.rollback()
                            LogUtil.warning(
                                f"[db.cache_get] gc expired cache failed: {gc_exc}"
                            )
                            # 仍然刷新时间戳，避免坏 case 下连续重试拖慢主流程。
                            DB._last_cache_gc_at = now
                return None
            except Exception as e:
                err = str(e)
                retryable = (
                    "Packet sequence number wrong" in err
                    or "MySQL server has gone away" in err
                    or "Lost connection to MySQL server" in err
                    or "server has gone away" in err
                )
                if attempt == 0 and retryable:
                    LogUtil.warning(
                        f"[db.cache_get] retry key={key} because db connection error: {e}"
                    )
                    # 注意：原实现这里调用了 self.engine.dispose()，
                    # 它会一次性关闭整个连接池里的所有连接，让其他线程下一次取连接
                    # 时全都要重建 —— 在缓存失效高频场景下容易引发雪崩。
                    # SQLAlchemy 的 pool_pre_ping=True（见 __init__）已经能在每次取连接时
                    # 检测并替换掉单条失效连接，足够应对 "MySQL gone away" / "Lost connection"，
                    # 因此这里不需要再 dispose 整个池。
                    time.sleep(0.05)
                    continue
                LogUtil.error(f"[db.cache_get] failed key={key}: {e}", exc_info=True)
                return None
        return None

    def cache_set(self, key: str, val: dict, expire: int = 0):
        with self.Session() as session:
            try:
                session.query(TableByCache).filter(TableByCache.k == key).delete()
                cache = TableByCache(k=key, v=json.dumps(val), expire=expire)
                session.add(cache)
                session.commit()
            except Exception:
                session.rollback()
                raise

        return True

    def cache_del(self, key: str):
        with self.Session() as session:
            try:
                session.query(TableByCache).filter(TableByCache.k == key).delete()
                session.commit()
            except Exception:
                session.rollback()
                raise

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