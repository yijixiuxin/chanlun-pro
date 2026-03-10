from sqlalchemy import (
    Column,
    DateTime,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


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
