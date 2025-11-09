import datetime
import numpy as np
import pandas as pd
from sqlalchemy import Column, Integer, String, Float, DateTime, Index, func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import sessionmaker

from chanlun import config
from chanlun.base import Market


class KlinesDB:
    def __init__(self, session: sessionmaker, engine):
        self.Session = session
        self.engine = engine

    def klines_tables(self, market: str, code: str):
        """
        获取k线表对象
        :param market:
        :param code:
        :return:
        """
        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()

        table_name = f"{market.lower()}_{code.lower().replace('.', '_')}"
        if self.engine.dialect.has_table(self.engine.connect(), table_name) is True:
            # print(f'Table {table_name} is exists')
            return self.engine.classes[table_name]

        class TableByKlines(Base):
            __tablename__ = table_name
            code = Column(String(20), primary_key=True)
            f = Column(String(10), primary_key=True)
            dt = Column(DateTime, primary_key=True)
            o = Column(Float)
            c = Column(Float)
            h = Column(Float)
            l = Column(Float)
            v = Column(Float)
            p = Column(Float, default=None)

            __table_args__ = (Index("__table_args_code_f_dt_index", "code", "f", "dt"),)

        Base.metadata.create_all(self.engine)
        return TableByKlines

    def klines_query(
        self, market: str, code: str, frequency: str, start_date=None, end_date=None
    ) -> pd.DataFrame:
        """
        查询k线
        :param market:
        :param code:
        :param frequency:
        :param start_date:
        :param end_date:
        :return:
        """
        with self.Session() as session:
            table = self.klines_tables(market, code)
            query = session.query(table).filter(
                table.code == code, table.f == frequency
            )
            if start_date is not None:
                query = query.filter(table.dt >= start_date)
            if end_date is not None:
                query = query.filter(table.dt <= end_date)

            # order by dt asc
            query = query.order_by(table.dt.asc())
            klines = pd.read_sql(query.statement, self.engine)
            if klines.empty:
                return pd.DataFrame(
                    columns=[
                        "code",
                        "date",
                        "open",
                        "close",
                        "high",
                        "low",
                        "volume",
                        "position",
                    ]
                )
            klines = klines.rename(
                columns={
                    "dt": "date",
                    "o": "open",
                    "c": "close",
                    "h": "high",
                    "l": "low",
                    "v": "volume",
                    "p": "position",
                }
            )
            return klines[["code", "date", "open", "close", "high", "low", "volume", "position"]]

    def klines_last_datetime(self, market: str, code: str, frequency: str):
        """
        查询指定k线的最后时间
        :param market:
        :param code:
        :param frequency:
        :return:
        """
        with self.Session() as session:
            table = self.klines_tables(market, code)
            last_date = (
                session.query(func.max(table.dt))
                .filter(table.code == code, table.f == frequency)
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
                for _, _k in g_glines.iterrows():
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