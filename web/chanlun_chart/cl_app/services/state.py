"""
Shared application state for blueprints and services.

Currently used to track TradingView `/tv/history` request counters
to avoid excessive repeated requests within short intervals.
"""
from cachetools import TTLCache

# Counter for recent `/tv/history` requests keyed by `<symbol>_<resolution>`.
#
# 改用 TTLCache 替代普通 dict：
# - 普通 dict 永不淘汰，长时间运行后所有访问过的 symbol_resolution 组合都会驻留内存，
#   是典型的内存泄漏。
# - TTL 设为 600 秒：counter 本身用于"5 秒内连续请求"判定，超过 10 分钟未访问的条目
#   完全可以丢弃，下次请求会自然重建。
# - maxsize 设为 1000：一般用户活跃 symbol 远小于此，足够使用。
history_req_counter: TTLCache = TTLCache(maxsize=1000, ttl=600)