"""
同花顺特色数据（扶摇 API）客户端

文档：https://fuyao.aicubes.cn/llms-full.txt
仅接入 A 股「特色数据」模块，接口统一返回 ApiResponse 信封。

配置项（config.py）：
    FUYAO_API_URL = "https://fuyao.aicubes.cn"
    FUYAO_API_KEY = "sk-..."

用法：
    from chanlun.tools.ths_fuyao import get_ths_special_data

    client = get_ths_special_data()
    data = client.limit_up_pool(date="2026-10-30")
"""

import copy
import datetime
import threading
import time

import requests

from chanlun import config

DEFAULT_BASE_URL = "https://fuyao.aicubes.cn"
SPECIAL_DATA_PATH = "/api/a-share/special-data"
FINANCIALS_PATH = "/api/a-share/financials"
CALENDAR_PATH = "/api/a-share/calendar"
INDEX_PATH = "/api/a-share-index"
PRICES_PATH = "/api/a-share/prices"
_CN_TZ = datetime.timezone(datetime.timedelta(hours=8))


class ThsFuyaoError(Exception):
    """同花顺接口业务异常"""

    def __init__(self, code: int | str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def to_ths_code(code: str) -> str:
    """
    项目代码转同花顺代码

    SH.600519 -> 600519.SH
    SZ.000001 -> 000001.SZ
    BJ.430001 -> 430001.BJ
    """
    code = (code or "").strip().upper()
    if "." not in code:
        return code
    left, right = code.split(".", 1)
    if left in {"SH", "SZ", "BJ"} and right:
        return f"{right}.{left}"
    if right in {"SH", "SZ", "BJ"} and left:
        return f"{left}.{right}"
    return code


def to_local_code(code: str) -> str:
    """
    同花顺代码转项目代码

    600519.SH -> SH.600519
    000001.SZ -> SZ.000001
    """
    code = (code or "").strip().upper()
    if "." not in code:
        return code
    left, right = code.split(".", 1)
    if left in {"SH", "SZ", "BJ"} and right:
        return f"{left}.{right}"
    if right in {"SH", "SZ", "BJ"} and left:
        return f"{right}.{left}"
    return code


def date_to_ms(date_str: str) -> int:
    """交易日（YYYY-MM-DD）转 Asia/Shanghai 零点毫秒时间戳"""
    try:
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except (TypeError, ValueError) as e:
        raise ThsFuyaoError(1002, f"日期格式错误，应为 YYYY-MM-DD：{date_str}") from e
    date_obj = date_obj.replace(tzinfo=_CN_TZ)
    return int(date_obj.timestamp() * 1000)


class ThsSpecialData:
    """同花顺 A 股特色数据客户端"""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int = 15,
    ):
        self.api_key = (
            api_key
            if api_key is not None
            else getattr(config, "FUYAO_API_KEY", "").strip()
        )
        self.base_url = (
            base_url
            if base_url is not None
            else getattr(config, "FUYAO_API_URL", DEFAULT_BASE_URL).strip()
        ).rstrip("/")
        self.timeout = timeout
        self._cache: dict[tuple, tuple[float, dict]] = {}
        self._cache_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 基础请求
    # ------------------------------------------------------------------
    def _request(
        self,
        path: str,
        params: dict | None = None,
        cache_ttl: int = 0,
    ) -> dict:
        if not self.api_key:
            raise ThsFuyaoError(
                2001, "未配置同花顺 API Key，请在 config.py 中设置 FUYAO_API_KEY"
            )

        params = {k: v for k, v in (params or {}).items() if v is not None}
        cache_key = (path, tuple(sorted(params.items())))
        now = time.time()

        if cache_ttl > 0:
            with self._cache_lock:
                cached = self._cache.get(cache_key)
                if cached is not None and now - cached[0] < cache_ttl:
                    return copy.deepcopy(cached[1])

        try:
            response = requests.get(
                f"{self.base_url}{path}",
                headers={"X-api-key": self.api_key},
                params=params,
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            raise ThsFuyaoError(-1, f"同花顺数据请求失败：{e}") from e

        if response.status_code == 429:
            raise ThsFuyaoError(4001, "同花顺数据请求过于频繁，请稍后再试")
        if response.status_code != 200:
            raise ThsFuyaoError(
                response.status_code, f"同花顺数据请求失败：HTTP {response.status_code}"
            )

        try:
            payload = response.json()
        except ValueError as e:
            raise ThsFuyaoError(-1, "同花顺数据响应格式错误") from e

        code = payload.get("code", -1)
        try:
            success = int(code) == 0
        except (TypeError, ValueError):
            success = False
        if not success:
            raise ThsFuyaoError(code, payload.get("message") or "同花顺数据请求失败")

        data = payload.get("data")
        if not isinstance(data, dict):
            data = {}

        if cache_ttl > 0:
            with self._cache_lock:
                if len(self._cache) > 256:
                    self._cache.clear()
                self._cache[cache_key] = (now, copy.deepcopy(data))
        return data

    def latest_trade_date(self) -> str:
        """最近一个交易日（以连板天梯的日期窗口为准）"""
        try:
            ladder = self.limit_up_ladder()
            dates = (ladder.get("window") or {}).get("date_list") or []
            if dates:
                raw = str(dates[0]).strip()
                if len(raw) == 8 and raw.isdigit():
                    return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
                if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
                    return raw
        except ThsFuyaoError:
            pass
        return datetime.date.today().strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # 涨跌停与炸板
    # ------------------------------------------------------------------
    def limit_up_pool(
        self,
        date: str | None = None,
        page: int = 1,
        size: int = 50,
        sort_field: str = "last_price",
        sort_dir: str = "desc",
    ) -> dict:
        date = date or self.latest_trade_date()
        return self._request(
            f"{SPECIAL_DATA_PATH}/limit-up-pool",
            {
                "date_ms": date_to_ms(date),
                "page": page,
                "size": size,
                "sort_field": sort_field,
                "sort_dir": sort_dir,
            },
            cache_ttl=30,
        )

    def limit_down_pool(
        self,
        date: str | None = None,
        page: int = 1,
        size: int = 50,
        sort_field: str = "last_limit_time",
        sort_dir: str = "desc",
    ) -> dict:
        date = date or self.latest_trade_date()
        return self._request(
            f"{SPECIAL_DATA_PATH}/limit-down-pool",
            {
                "date_ms": date_to_ms(date),
                "page": page,
                "size": size,
                "sort_field": sort_field,
                "sort_dir": sort_dir,
            },
            cache_ttl=30,
        )

    def limit_break_pool(
        self,
        date: str | None = None,
        page: int = 1,
        size: int = 50,
        sort_field: str = "price_change_ratio_pct",
        sort_dir: str = "desc",
    ) -> dict:
        date = date or self.latest_trade_date()
        return self._request(
            f"{SPECIAL_DATA_PATH}/limit-break-pool",
            {
                "date_ms": date_to_ms(date),
                "page": page,
                "size": size,
                "sort_field": sort_field,
                "sort_dir": sort_dir,
            },
            cache_ttl=30,
        )

    def limit_up_ladder(self) -> dict:
        """近 30 个交易日连板天梯"""
        return self._request(f"{SPECIAL_DATA_PATH}/limit-up-ladder", cache_ttl=300)

    def limit_up_statistics(self, date: str | None = None) -> dict:
        """涨停强度与晋级统计"""
        return self._request(
            f"{SPECIAL_DATA_PATH}/limit-up-statistics",
            {"date": date},
            cache_ttl=60,
        )

    # ------------------------------------------------------------------
    # 同花顺热榜
    # ------------------------------------------------------------------
    def hot_stock_list(self, period: str = "day", limit: int = 30) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/hot-stock-list",
            {"period": period, "limit": limit},
            cache_ttl=60,
        )

    def skyrocket_list(self, period: str = "day", limit: int = 30) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/skyrocket-list",
            {"period": period, "limit": limit},
            cache_ttl=60,
        )

    def hot_stock_list_history(self, date: str | None = None) -> dict:
        date = date or self.latest_trade_date()
        return self._request(
            f"{SPECIAL_DATA_PATH}/hot-stock-list-history",
            {"date": date},
            cache_ttl=300,
        )

    def hot_stock_rank_trend(
        self,
        code: str,
        start_date: str,
        end_date: str,
    ) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/hot-stock-rank-trend",
            {
                "thscode": to_ths_code(code),
                "start_date": start_date,
                "end_date": end_date,
            },
            cache_ttl=300,
        )

    # ------------------------------------------------------------------
    # 个股异动解读
    # ------------------------------------------------------------------
    def anomaly_analysis_list(self, tag_codes: str | None = None) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/anomaly-analysis-list",
            {"tag_codes": tag_codes},
            cache_ttl=60,
        )

    def anomaly_analysis_stock(self, codes: str | list[str]) -> dict:
        if isinstance(codes, str):
            codes = [c for c in codes.split(",") if c.strip()]
        thscodes = ",".join(to_ths_code(c) for c in codes[:50])
        if not thscodes:
            raise ThsFuyaoError(1001, "缺少股票代码")
        return self._request(
            f"{SPECIAL_DATA_PATH}/anomaly-analysis-stock",
            {"thscodes": thscodes},
            cache_ttl=60,
        )

    # ------------------------------------------------------------------
    # 交易日历
    # ------------------------------------------------------------------
    def trading_days(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        return self._request(
            f"{CALENDAR_PATH}/trading-days",
            {"start_date": start_date, "end_date": end_date},
            cache_ttl=3600,
        )

    # ------------------------------------------------------------------
    # 行业指数与指数成分
    # ------------------------------------------------------------------
    def index_catalog(self, tag: str = "industry") -> dict:
        return self._request(
            f"{INDEX_PATH}/catalog/ths-index-list",
            {"tag": tag},
            cache_ttl=3600,
        )

    def index_constituents(self, thscode: str) -> dict:
        return self._request(
            f"{INDEX_PATH}/constituents/ths-stock-list",
            {"thscode": thscode},
            cache_ttl=600,
        )

    def index_historical(
        self,
        thscode: str,
        start_ms: int,
        end_ms: int,
        interval: str = "1d",
    ) -> dict:
        return self._request(
            f"{INDEX_PATH}/prices/historical",
            {
                "thscode": thscode,
                "interval": interval,
                "start": start_ms,
                "end": end_ms,
            },
            cache_ttl=3600,
        )

    def stock_snapshot(self, codes: str | list[str]) -> dict:
        if isinstance(codes, str):
            thscodes = codes
        else:
            thscodes = ",".join(codes)
        return self._request(
            f"{PRICES_PATH}/snapshot",
            {"thscodes": thscodes},
            cache_ttl=30,
        )

    # ------------------------------------------------------------------
    # 龙虎榜与营业部
    # ------------------------------------------------------------------
    def dragon_tiger_list(
        self,
        board_type: str = "all",
        date: str | None = None,
    ) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/dragon-tiger-list",
            {"board_type": board_type, "date": date},
            cache_ttl=120,
        )

    def dragon_tiger_stock_institution(
        self,
        code: str,
        date: str | None = None,
        range_days: int | None = None,
    ) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/dragon-tiger-stock-institution",
            {
                "thscode": to_ths_code(code),
                "date": date,
                "range_days": range_days,
            },
            cache_ttl=120,
        )

    def dragon_tiger_department_classic_record(self, department_id: str) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/dragon-tiger-department-classic-record",
            {"department_id": department_id},
            cache_ttl=3600,
        )

    def dragon_tiger_department_history(
        self,
        department_id: str,
        page: int = 1,
        size: int = 30,
        min_net_buy_amount: int | None = None,
        sort_field: str = "trade_date",
        sort_dir: str = "desc",
    ) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/dragon-tiger-department-history",
            {
                "department_id": department_id,
                "page": page,
                "size": size,
                "min_net_buy_amount": min_net_buy_amount,
                "sort_field": sort_field,
                "sort_dir": sort_dir,
            },
            cache_ttl=300,
        )

    # ------------------------------------------------------------------
    # 异动监控与集合竞价
    # ------------------------------------------------------------------
    def abnormal_monitor_list(
        self,
        state: str = "current",
        page: int = 1,
        size: int = 30,
    ) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/abnormal-monitor-list",
            {"state": state, "page": page, "size": size},
            cache_ttl=60,
        )

    def auction_stock_opportunities(
        self,
        date: str | None = None,
        warn_types: str | None = None,
        market: str | None = None,
        exclude_st: bool | None = None,
        codes: str | list[str] | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> dict:
        thscodes = None
        if codes:
            if isinstance(codes, str):
                codes = [c for c in codes.split(",") if c.strip()]
            thscodes = ",".join(to_ths_code(c) for c in codes[:50])
        return self._request(
            f"{SPECIAL_DATA_PATH}/auction-stock-opportunities",
            {
                "date": date,
                "warn_types": warn_types,
                "market": market,
                "exclude_st": exclude_st,
                "thscodes": thscodes,
                "sort_by": sort_by,
                "sort_order": sort_order,
                "page": page,
                "size": size,
            },
            cache_ttl=60,
        )

    def auction_opportunity_dates(self) -> dict:
        return self._request(
            f"{SPECIAL_DATA_PATH}/auction-opportunity-dates",
            cache_ttl=3600,
        )

    # ------------------------------------------------------------------
    # 财务数据
    # ------------------------------------------------------------------
    def search_ticker(
        self,
        query: str,
        asset_type: str | None = "a-share",
        exchange: str | None = None,
        limit: int = 10,
    ) -> dict:
        """按代码或名称检索标的，用于把股票消歧为唯一 A 股 thscode"""
        return self._request(
            "/api/meta/tickers/search",
            {
                "q": query,
                "asset_type": asset_type,
                "exchange": exchange,
                "limit": max(1, min(50, int(limit))),
            },
            cache_ttl=86400,
        )

    def income_statements(
        self,
        code: str,
        period: str = "quarterly",
        limit: int = 8,
    ) -> dict:
        return self._request(
            f"{FINANCIALS_PATH}/income-statements",
            {
                "thscode": to_ths_code(code),
                "period": period,
                "limit": max(1, min(20, int(limit))),
            },
            cache_ttl=3600,
        )

    def balance_sheets(
        self,
        code: str,
        period: str = "quarterly",
        limit: int = 8,
    ) -> dict:
        return self._request(
            f"{FINANCIALS_PATH}/balance-sheets",
            {
                "thscode": to_ths_code(code),
                "period": period,
                "limit": max(1, min(20, int(limit))),
            },
            cache_ttl=3600,
        )

    def cash_flow_statements(
        self,
        code: str,
        period: str = "quarterly",
        limit: int = 8,
    ) -> dict:
        return self._request(
            f"{FINANCIALS_PATH}/cash-flow-statements",
            {
                "thscode": to_ths_code(code),
                "period": period,
                "limit": max(1, min(20, int(limit))),
            },
            cache_ttl=3600,
        )

    def financial_indicators(self, code: str, report: str) -> dict:
        return self._request(
            f"{FINANCIALS_PATH}/indicators",
            {"thscode": to_ths_code(code), "report": report},
            cache_ttl=3600,
        )


_client: ThsSpecialData | None = None
_client_lock = threading.Lock()


def get_ths_special_data() -> ThsSpecialData:
    """获取全局同花顺特色数据客户端（复用内存缓存）"""
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = ThsSpecialData()
    return _client
