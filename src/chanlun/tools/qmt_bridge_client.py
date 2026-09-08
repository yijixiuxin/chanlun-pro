"""
大QMT桥接客户端（Redis Pub/Sub 模式，共享频道 + 账号频道）

通过 Redis 与运行在大QMT内置 Python 中的桥接服务（见 bigqmt_server/big_qmt_redis_bridge.py）
通信，替代原先 miniQMT 的 xtdata 服务与不稳定的 HTTP(socketserver) 桥接。

通信方式（请求/响应）：
    * 请求-行情：本客户端 PUBLISH 到共享频道 qmt_bridge:req，
                  请求 JSON 携带结果 key（qmt_bridge:resp:<请求ID>）；
    * 请求-交易：本客户端 PUBLISH 到按账号隔离的频道 qmt_bridge:req:<account_id>，
                  只有绑定该账号的大QMT桥接实例订阅并处理，避免多账号客户端串单；
    * 结果：大QMT处理完 SETEX 写入结果 key，本客户端轮询读取；
    * 存活：大QMT侧每 5 秒刷新共享心跳 key qmt_bridge:heartbeat（TTL 10 秒），
             任一实例存活即刷新，仅用于 health 诊断（至少一个实例可用）。

负载分担：行情可批量启动多个大QMT实例，全部订阅同一频道 qmt_bridge:req，
谁拿到谁处理（竞争消费）；请求超时后本客户端重试发布，空闲实例自动接管。

使用方式：
    from app.exchange.qmt_bridge_client import get_client
    client = get_client()
    stocks = client.all_stocks()
    client.balance("6000000248")  # 交易接口自动走账号频道
"""

import json
import logging
import threading
import time
import uuid

import redis

from chanlun import fun
from chanlun.config import (
    REDIS_HOST,
    REDIS_PORT,
)

logger = fun.get_logger("qmt_bridge.log")

# Redis key/频道命名（与大QMT侧 big_qmt_redis_bridge.py 保持一致）
REQ_CHANNEL = "qmt_bridge:req"  # 共享请求 Pub/Sub 频道（多实例竞争消费）
RESP_KEY_PREFIX = "qmt_bridge:resp:"  # 结果写入的 Redis key（按请求 ID）
HEARTBEAT_KEY = "qmt_bridge:heartbeat"  # 共享心跳 key（任一实例存活即刷新）
# 心跳超过该秒数视为无实例存活（大QMT侧 TTL 10 秒、每 5 秒刷新）
HEARTBEAT_FRESH_SECONDS = 12
# 结果 key 轮询间隔（秒）
RESULT_POLL_INTERVAL = 0.2


# 各接口默认超时（秒）
_TIMEOUTS = {
    "health": 5,
    "all_stocks": 60,
    "all_ticks": 60,
    "tick": 30,
    "instrument_detail": 15,
    "history_data": 120,
    "divid_factors": 15,
    "balance": 30,
    "positions": 30,
    "order": 30,
    "order_detail": 30,
    "order_cancel": 30,
}


class BigQmtBridgeClient:
    """大QMT桥接 Redis 客户端（共享频道，竞争消费）。"""

    def __init__(self, timeout=None, max_retry=None):
        self.timeout = timeout if timeout is not None else 30
        self.max_retry = max_retry if max_retry is not None else 2

        # 桥接通信用 Redis 连接（独立 db，避免与应用数据冲突）
        self._redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=1,
            password="",
            decode_responses=True,
            socket_timeout=self.timeout,
            socket_connect_timeout=5,
        )
        # 启动时确认 Redis 可用，避免误配置
        self._redis.ping()

    # ------------------------------------------------------------------ #
    # 存活检查（仅用于诊断；请求本身不依赖心跳）
    # ------------------------------------------------------------------ #
    def bridge_alive(self):
        """通过共享心跳 key 判断是否至少有一个大QMT实例存活。"""
        try:
            heartbeat_text = self._redis.get(HEARTBEAT_KEY)
            if heartbeat_text:
                return time.time() - float(heartbeat_text) <= HEARTBEAT_FRESH_SECONDS
        except Exception:  # noqa: BLE001, S110
            pass
        return False

    # ------------------------------------------------------------------ #
    # 请求/响应
    # ------------------------------------------------------------------ #
    @staticmethod
    def trade_channel(account_id):
        """交易请求的频道：按账号隔离，避免多账号 QMT 客户端串单。

        行情（/health、/all_stocks、/tick 等）走共享频道 REQ_CHANNEL；
        交易（/balance、/positions、/order...）走 REQ_CHANNEL:account_id，
        只有绑定该账号的大QMT桥接实例才会订阅并处理。
        """
        return f"{REQ_CHANNEL}:{account_id}" if account_id else REQ_CHANNEL

    def request(self, path, params=None, timeout=None, channel=None):
        """
        向大QMT桥接发起一次请求并等待结果。

        请求 PUBLISH 到指定频道（默认共享频道 qmt_bridge:req，交易接口应传
        trade_channel(account_id) 按账号隔离），存活的实例订阅后处理；
        请求 JSON 携带结果 key（qmt_bridge:resp:<请求ID>），处理完写入该 key，
        本项目轮询读取结果。超时/失败时重试发布（其它空闲实例自动接管）。
        """
        if channel is None:
            channel = self.trade_channel((params or {}).get("account_id", ""))
        if timeout is None:
            segments = [s for s in path.strip("/").split("/") if s]
            timeout = _TIMEOUTS.get(segments[-1] if segments else "", self.timeout)

        last_error = None
        for attempt in range(max(1, self.max_retry + 1)):
            request_id = uuid.uuid4().hex
            result_key = RESP_KEY_PREFIX + request_id
            try:
                # 清理可能残留的结果 key，再发布请求
                self._redis.delete(result_key)
                self._redis.publish(
                    channel,
                    json.dumps(
                        {
                            "id": request_id,
                            "path": path,
                            "params": params or {},
                            "result_key": result_key,
                        },
                        ensure_ascii=False,
                    ),
                )
                # logger.info(
                #     "发送请求 id=%s path=%s 频道=%s", request_id, path, channel
                # )
                _send_time = time.time()

                deadline = time.time() + timeout
                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        raise TimeoutError(
                            f"大QMT桥接请求 {path} 超时（{timeout}s）id={request_id}"
                        )
                    raw = self._redis.get(result_key)
                    if raw:
                        payload = json.loads(raw)
                        if payload.get("id") != request_id:
                            continue
                        if str(payload.get("status", "")).lower() != "success":
                            raise RuntimeError(
                                "大QMT桥接接口 {} 返回失败: {}".format(
                                    path, payload.get("message", "未知错误")
                                )
                            )
                        # logger.info(
                        #     "收到结果 id=%s path=%s 耗时 %.1fs",
                        #     request_id,
                        #     path,
                        #     time.time() - _send_time,
                        # )
                        return payload
                    time.sleep(RESULT_POLL_INTERVAL)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "大QMT桥接请求 %s 失败(第%s次尝试) id=%s: %s",
                    path,
                    attempt + 1,
                    request_id,
                    exc,
                )
            finally:
                try:
                    self._redis.delete(result_key)
                except Exception:  # noqa: BLE001, S110
                    pass

        raise RuntimeError(
            f"大QMT桥接服务不可用（已重试 {self.max_retry + 1} 次）: {last_error}"
        )

    # ------------------------------------------------------------------ #
    # 业务接口
    # ------------------------------------------------------------------ #
    def health(self):
        """查看桥接存活状态（用于诊断，不抛异常）。"""
        return {
            "status": "success",
            "mode": "redis",
            "channel": REQ_CHANNEL,
            "alive": self.bridge_alive(),
            "heartbeat": self._redis.get(HEARTBEAT_KEY) or "",
        }

    def all_stocks(self):
        """
        获取全部证券列表（QMT 原生代码格式，未过滤类型）。

        Returns:
            list[dict]: [{"code": "600519.SH", "name": "贵州茅台",
                          "price_tick": 0.01}, ...]
            类型筛选（stock/etf/index）由 exchange_qmt.all_stocks 完成。
        """
        payload = self.request("/all_stocks", timeout=_TIMEOUTS["all_stocks"])
        return payload.get("data") or []

    def all_ticks(self):
        """
        获取全市场 tick 快照（未过滤类型）。

        Returns:
            dict[str, dict]: qmt代码 -> tick信息
        """
        payload = self.request("/all_ticks", timeout=_TIMEOUTS["all_ticks"])
        return payload.get("ticks") or {}

    def ticks(self, codes):
        """
        获取指定证券的 tick 信息（可批量）。

        Args:
            codes: list[str]，qmt 格式代码，如 ["600519.SH", "000001.SZ"]

        Returns:
            dict[str, dict]: qmt代码 -> tick信息
        """
        if not codes:
            return {}
        payload = self.request(
            "/tick",
            params={"codes": ",".join(codes)},
            timeout=_TIMEOUTS["tick"],
        )
        return payload.get("ticks") or {}

    def instrument_detail(self, stock_code):
        """
        获取证券合约详情。

        Returns:
            dict: {"stock_code": ..., "stock_name": ..., "price_tick": ..., "detail": {...}}
        """
        return self.request(
            "/instrument_detail",
            params={"stock_code": stock_code},
            timeout=_TIMEOUTS["instrument_detail"],
        )

    def history_data(
        self,
        stock_code,
        period="1d",
        start_time="",
        end_time="",
        count=8000,
        dividend_type="none",
        fields="time,open,high,low,close,volume",
    ):
        """
        获取历史/实时行情。

        服务端每次请求都会先 download_history_data 下载最新数据（否则只能取到本地缓存），
        无需客户端指定下载开关。

        Args:
            stock_code: qmt 格式代码
            period: 周期，1m/5m/15m/30m/60m/1d
            start_time: 开始时间，格式 "YYYYMMDD" 或 "YYYYMMDDHHMMSS"
            end_time: 结束时间
            count: 数量限制（-1 表示不限制）
            dividend_type: 复权方式（默认 none 非复权，便于增量更新）
            fields: 逗号分隔的字段

        Returns:
            dict: {"data": [{"time": "YYYY-MM-DD HH:MM:SS", ...}, ...], ...}
        """
        return self.request(
            "/history_data",
            params={
                "stock_code": stock_code,
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "count": count,
                "dividend_type": dividend_type,
                "fields": fields,
            },
            timeout=_TIMEOUTS["history_data"],
        )

    def divid_factors(self, stock_code):
        """
        获取股票除权除息信息。

        Returns:
            list[dict]: 每条包含 time(毫秒)/interest/stockBonus/stockGift/
                        allotNum/allotPrice/gugai/dr 等字段
        """
        payload = self.request(
            "/divid_factors",
            params={"stock_code": stock_code},
            timeout=_TIMEOUTS["divid_factors"],
        )
        return payload.get("data") or []

    # ------------------------------------------------------------------ #
    # 交易接口（均需指定 account_id）
    # ------------------------------------------------------------------ #
    def balance(self, account_id=""):
        """
        查询账户资金。

        Args:
            account_id: 交易账号ID（为空时使用桥接侧 BIG_QMT_ACCOUNT_ID）

        Returns:
            dict: 账户资金字段（get_trade_detail_data(..., "account")）
                total_balance   总资产/账户市值（m_dBalance）
                available_cash  可用资金（m_dAvailable）
                market_value    证券市值（m_dInstrumentValue）
                total_assets    总资产（兼容字段，等于 total_balance）
                cash            现金（m_dCash）
                assure_asset    担保/信用资产（m_dAssureAsset，融资融券用）
                total_debit     总负债（m_dTotalDebit，融资融券用）
                position_profit 持仓盈亏（m_dPositionProfit）
        """
        return self.request(
            "/balance",
            params={"account_id": account_id},
            timeout=_TIMEOUTS["balance"],
        )

    def positions(self, account_id="", code=""):
        """
        查询账户持仓。

        Args:
            account_id: 交易账号ID
            code: 可选，qmt 格式代码（如 600519.SH），按代码过滤

        Returns:
            list[dict]: 持仓列表（get_trade_detail_data(..., "position")）
                基础信息：code/name/market/exchange_name/product_id/product_name
                方向数量：direction/volume/can_use_volume/frozen_volume/on_road_volume/
                         yesterday_volume/close_volume/close_amount
                价格盈亏：open_price/avg_open_price/last_price/settlement_price/
                         last_settlement_price/market_value/instrument_value/
                         float_profit/close_profit/position_profit/profit_rate/
                         position_cost/open_cost/margin
                其它：    stock_holder/trading_day/open_date/hedge_flag/is_today/
                         xt_tag/future_trade_type/expire_date/total_cost/single_cost
        """
        payload = self.request(
            "/positions",
            params={"account_id": account_id, "code": code},
            timeout=_TIMEOUTS["positions"],
        )
        return payload.get("data") or []

    def order(self, account_id, code, o_type, amount, price=0, pr_type=11, reason=""):
        """
        下单。

        Args:
            account_id: 交易账号ID
            code: qmt 格式代码（如 600519.SH）
            o_type: buy / sell
            amount: 股数（A股买入为100整数倍）
            price: 委托价格（pr_type=11 指定价时生效）
            pr_type: passorder 选价类型，仅允许 5(最新价)/11(指定价-模型价)/14(对手价)
            reason: 投资备注

        Returns:
            dict: {"order_id", "code", "o_type", "amount", ...}
        """
        # 客户端自动生成 userOrderId（对应委托 m_strRemark），用于匹配查询委托号
        user_order_id = f"chanlun_{uuid.uuid4().hex[:8]}"
        return self.request(
            "/order",
            params={
                "account_id": account_id,
                "code": code,
                "o_type": o_type,
                "amount": amount,
                "price": price,
                "pr_type": pr_type,
                "reason": reason,
                "user_order_id": user_order_id,
            },
            timeout=_TIMEOUTS["order"],
        )

    def order_detail(self, account_id, order_id=""):
        """
        查询订单详情。

        Args:
            account_id: 交易账号ID
            order_id: 委托号

        Returns:
            dict: {"order": {...}}，未找到时 order 为 None。
                order 为委托对象字段（get_trade_detail_data(..., "order")）：
                    order_id      委托号（优先 m_strOrderSysID）
                    code/name     证券代码/名称
                    market        证券市场（交易所代码）
                    order_type    委托类别/业务类型
                    direction     买卖方向（48=买/多，49=卖/空）
                    offset_flag   开平标志
                    price_type    下单选价类型（5=最新价，11=指定价/模型价，14=对手价）
                    order_volume  最初委托量（股）
                    price         委托价/限价
                    traded_price  成交均价
                    traded_volume 已成交量
                    cancel_volume 已撤数量
                    trade_amount  成交额
                    status        委托状态（50=已报，52=已成，53=已撤，54=废单）
                    submit_status 提交状态
                    status_msg    状态/错误/废单信息
                    order_date    委托日期
                    order_time    委托时间
                    strategy_name 策略名
                    order_remark  委托备注/用户自设委托ID（userOrderId）
        """
        return self.request(
            "/order_detail",
            params={"account_id": account_id, "order_id": order_id},
            timeout=_TIMEOUTS["order_detail"],
        )

    def order_cancel(self, account_id, order_id=""):
        """
        取消订单。

        Args:
            account_id: 交易账号ID
            order_id: 委托号

        Returns:
            dict: {"canceled": bool, "order_id": ...}
        """
        return self.request(
            "/order_cancel",
            params={"account_id": account_id, "order_id": order_id},
            timeout=_TIMEOUTS["order_cancel"],
        )


_client = None
_client_lock = threading.Lock()


def get_client():
    """获取全局大QMT桥接客户端单例。"""
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = BigQmtBridgeClient()
    return _client


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = get_client()
    s_time = time.time()
    print("health:", client.health())
    print("health 耗时：", time.time() - s_time)
    print(" == " * 20)
    s_time = time.time()
    stocks = client.all_stocks()
    print("all_stocks total:", len(stocks))
    print(stocks[:3])
    print("all_stocks 耗时：", time.time() - s_time)
    print(" == " * 20)
    s_time = time.time()
    print("ticks:", client.ticks(["600519.SH", "000001.SZ"]))
    print("ticks 耗时：", time.time() - s_time)
    print(" == " * 20)
    s_time = time.time()
    print("history:", client.history_data("600519.SH", period="1d", count=5))
    print("history 耗时：", time.time() - s_time)
    print(" == " * 20)
    s_time = time.time()
    print("divid:", client.divid_factors("600519.SH"))
    print("divid 耗时：", time.time() - s_time)
    print(" == " * 20)
