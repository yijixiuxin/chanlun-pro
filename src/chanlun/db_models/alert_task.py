from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


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
