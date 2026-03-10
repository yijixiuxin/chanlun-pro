from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


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
