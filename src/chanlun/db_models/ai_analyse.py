from sqlalchemy import (
    Column,
    Integer,
    String, DateTime, Text,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


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
