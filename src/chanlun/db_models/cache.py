from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base

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