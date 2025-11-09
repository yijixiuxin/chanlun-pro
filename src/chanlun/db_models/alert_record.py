from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


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