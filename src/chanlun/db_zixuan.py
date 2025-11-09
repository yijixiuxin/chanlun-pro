import datetime
from typing import List

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from chanlun.db_models.zixuan import TableByZixuan
from chanlun.db_models.zixuan_group import TableByZxGroup


class ZixuanDB:
    def __init__(self, session: sessionmaker):
        self.Session = session

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