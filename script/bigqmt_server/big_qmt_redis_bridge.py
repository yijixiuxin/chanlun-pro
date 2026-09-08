# -*- coding: gbk -*-  # noqa: UP009
"""
大QMT行情桥接服务（Redis Pub/Sub 模式，共享频道版）

在大QMT内置 Python 中运行（依赖 redis 模块，可直接 import redis），
通过 Redis 与 cl_fastapi 应用通信，替代 miniQMT xtdata。

通信方式：
    * 请求-行情：项目 PUBLISH 到共享频道 qmt_bridge:req，本服务订阅该频道接收；
                  多个大QMT实例订阅同一频道，谁拿到谁处理（竞争消费，天然负载分担）；
    * 请求-交易：项目 PUBLISH 到按账号隔离的频道 qmt_bridge:req:<account_id>，
                  只有绑定该账号（BIG_QMT_ACCOUNT_ID）的大QMT实例订阅并处理，
                  避免多账号客户端串单；本服务在设置了 ACCOUNT_ID 时额外订阅该账号频道。
    * 结果：本服务处理完成后 SETEX 写入 result_key（TTL 300 秒），项目在 key 中取结果；
    * 心跳：本服务每 5 秒 SETEX qmt_bridge:heartbeat（TTL 10 秒），
             任一实例存活即刷新，供项目判断“至少一个实例可用”。

无需实例ID：行情实例共用频道与心跳 key；交易按 ACCOUNT_ID 分频道。
慢请求实例忙碌时，空闲实例会接管；重复处理无副作用（同一 result_key 后写覆盖）。
ContextInfo 通过 run_time 回调参数（listen_loop / heartbeat_loop）逐层向下传递，无全局变量。

代码格式：全部使用大QMT原生格式（600519.SH / 000001.SZ / 830779.BJ）。
格式转换（如 SZ.000001）在 cl_fastapi 项目 app/exchange/qmt_bridge_client.py 中完成。

接口（请求 JSON 中 path 字段）：
    /health            健康检查
    /all_stocks        全部证券列表（code/name/price_tick）
    /all_ticks         全市场 tick 快照
    /tick              指定证券 tick（codes=600519.SH,000001.SZ）
    /instrument_detail 证券合约详情
    /history_data      历史/实时行情（period/start_time/end_time/count/dividend_type/fields）
    /divid_factors     除权除息信息
    /balance           查询账户资金（params.account_id）
    /positions         查询账户持仓（params.account_id / params.code）
    /order             下单（params.account_id / code / o_type / amount / price / pr_type / user_order_id）
    /order_detail      订单详情（params.account_id / order_id）
    /order_cancel      取消订单（params.account_id / order_id）

账号：所有交易接口均要求 params.account_id，由环境变量 BIG_QMT_ACCOUNT_ID 提供默认值；
      下单调用 QMT 内置 passorder（opType=23/24, orderType=1101, strategyName=quickTrade 固定，
      userOrderId 由客户端生成，用于查询匹配委托号），依赖 set_account / get_trade_detail_data，
      故在 init 中调用 ContextInfo.set_account 完成账号绑定。

运行：复制本文件（GBK 版，由 to_gbk.py 生成）到大QMT内置 Python 执行。
详细日志：设置 BIG_QMT_DEBUG_LOG=1。
"""

import json
import os
import time
import traceback
from datetime import datetime

import redis

# ---------- 配置 ----------
REDIS_HOST = os.getenv("BIG_QMT_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("BIG_QMT_REDIS_PORT", "6379") or 6379)
REDIS_DB = int(os.getenv("BIG_QMT_REDIS_DB", "1") or 1)
REDIS_PASSWORD = os.getenv("BIG_QMT_REDIS_PASSWORD", "") or None
VERBOSE_LOG = os.getenv("BIG_QMT_DEBUG_LOG", "1") != "0"

# ---------- 交易账号配置 ----------
# 交易账号ID（所有交易接口均要求指定；请求 params 中可覆盖此默认值）
ACCOUNT_ID = os.getenv("BIG_QMT_ACCOUNT_ID", "")
ACCOUNT_TYPE = "stock"  # QMT 账号类型：股票
STRATEGY_NAME = "qmt"  # passorder 策略名（固定为 qmt），用于查询委托/成交
QUICK_TRADE = 2  # passorder 快速交易参数（2=不判断bar状态立即触发）
PR_TYPE_DEFAULT = 11  # passorder 默认选价类型（11=指定价/模型价），仅限 {5,11,14}
# 下单后等待 QMT 生成委托号的重试时间（秒）
ORDER_REF_WAIT = 5

REQ_CHANNEL = "qmt_bridge:req"  # 共享请求频道：多实例竞争消费
HEARTBEAT_KEY = "qmt_bridge:heartbeat"  # 共享心跳 key：任一实例存活即刷新
RESULT_TTL = 300  # 结果 key 过期时间（秒）
SOCKET_TIMEOUT = 30
HEARTBEAT_TTL = 10

# ---------- 行情接口 ----------

# 允许的市场与前缀
_STOCK_ALL_CODE = [
    ("BJ", "92"),
    ("BJ", "89"),
    ("SH", "60"),
    ("SH", "68"),
    ("SH", "58"),
    ("SH", "51"),
    ("SH", "52"),
    ("SH", "56"),
    ("SH", "00"),
    ("SH", "53"),
    ("SH", "55"),
    ("SZ", "00"),
    ("SZ", "15"),
    ("SZ", "39"),
    ("SZ", "30"),
    ("SZ", "98"),
]


# ---------- Redis 连接 ----------
def new_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=10,
    )


def sub_channels():
    """订阅频道列表：行情用共享频道；设置了 ACCOUNT_ID 时额外订阅账号频道（交易按账号隔离）。"""
    channels = [REQ_CHANNEL]
    if ACCOUNT_ID:
        channels.append(REQ_CHANNEL + ":" + ACCOUNT_ID)
    return channels


conn_cmd = new_redis()  # 结果写入/心跳共用连接
pubsub = new_redis().pubsub()
pubsub.subscribe(*sub_channels())

DEBUG_LOG_PATH = "~/chanlun_pro/logs/qmt-bridge-debug.ndjson"


# ---------- 日志 ----------
def log(message, debug=False):
    print(
        "[%s]%s %s"  # noqa: UP031
        % (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # noqa: DTZ005
            "[DEBUG]" if debug else "",
            message,
        ),
        flush=True,
    )


def log_error(prefix, exc):
    print(
        "[%s] %s: %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), prefix, exc),  # noqa: DTZ005, UP031
        flush=True,
    )
    traceback.print_exc()


def append_debug_log(event, payload):
    try:
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # noqa: DTZ005
                        "event": event,
                        "payload": payload,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:  # noqa: BLE001, S110
        pass


# ---------- 基础工具 ----------
def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def json_safe(v):
    """规整为 JSON 可序列化类型，处理 NaN/Inf/numpy 类型。"""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        f = float(v)
        return None if (f != f or f in (float("inf"), float("-inf"))) else f  # noqa: PLR0124
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


def clean_text(value):
    """名称文本规整：bytes 解码、伪乱码（GBK 字节被 latin1 解码）恢复。"""
    if value is None:
        return ""
    if isinstance(value, bytes):
        for enc in ("gbk", "utf-8"):
            try:
                text = value.decode(enc).strip()
                if text:
                    return text
            except Exception:  # noqa: BLE001, S110
                pass
        return ""
    text = str(value).strip()
    if text and any(ord(c) > 127 for c in text) and all(ord(c) <= 255 for c in text):
        try:
            recovered = text.encode("latin1").decode("gbk").strip()
            if recovered:
                return recovered
        except Exception:  # noqa: BLE001, S110
            pass
    return text


def level5(values):
    values = list(values or [])[:5]
    return [to_float(v) for v in values] + [0.0] * (5 - len(values))


def format_time(index_value):
    """get_market_data_ex 的 index 转 'YYYY-MM-DD HH:MM:SS'。"""
    if isinstance(index_value, (int, float)):
        ts = float(index_value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")  # noqa: DTZ006
        except (OSError, OverflowError, ValueError):
            return str(index_value)
    return str(index_value)


def get_detail(context, code):
    try:
        detail = context.get_instrumentdetail(code) or {}
        if isinstance(detail, dict):
            return detail
    except Exception:  # noqa: BLE001, S110
        pass
    return {}


def get_all_codes(context):
    ticks = context.get_full_tick(["SH", "SZ", "BJ"])
    stock_codes = []
    now_datetime = datetime.now()  # noqa: DTZ005
    for _c, _t in ticks.items():
        _m = _c.split(".")[1]
        _pc = _c.split(".")[0][0:2]
        if (_m, _pc) not in _STOCK_ALL_CODE:
            continue
        if (
            (now_datetime.hour == 9 and now_datetime.minute > 30)
            or (now_datetime.hour >= 10)
        ) and (_t["open"] == 0 or _t["lastPrice"] == 0 or _t["volume"] == 0):
            continue
        stock_codes.append(_c)
    return stock_codes


def build_health_payload(context):
    return {
        "status": "success",
        "runtime_ready": context is not None,
        "channel": REQ_CHANNEL,
        "redis": "%s:%s/%d" % (REDIS_HOST, REDIS_PORT, REDIS_DB),  # noqa: UP031
    }


def build_all_stocks_payload(context):
    start = time.time()
    codes = get_all_codes(context)
    log("all_stocks: 获取证券代码 %d 个" % len(codes))  # noqa: UP031
    data = []
    for i, code in enumerate(codes, 1):
        detail = get_detail(context, code)
        stock_name = clean_text(detail.get("InstrumentName"))
        # 如果最后一个字是 “债”，跳过
        if stock_name and stock_name[-1] == "债":
            continue
        data.append(
            {
                "code": code,
                "name": stock_name,
                "price_tick": to_float(detail.get("PriceTick")) or 0.01,
            }
        )
        if VERBOSE_LOG and i % 100 == 0:
            log("all_stocks: 已处理 %d/%d（%s）" % (i, len(codes), code), debug=True)  # noqa: UP031
    log("all_stocks: 完成 共 %d 只，耗时 %.1fs" % (len(data), time.time() - start))  # noqa: UP031
    return {"status": "success", "total": len(data), "data": data}


def tick_item(t):
    return {
        "lastPrice": to_float(t.get("lastPrice")),
        "open": to_float(t.get("open")),
        "high": to_float(t.get("high")),
        "low": to_float(t.get("low")),
        "lastClose": to_float(t.get("lastClose")),
        "volume": to_float(t.get("volume")),
        "amount": to_float(t.get("amount")),
        "time": json_safe(t.get("time")),
        "askPrice": level5(t.get("askPrice")),
        "bidPrice": level5(t.get("bidPrice")),
        "askVol": level5(t.get("askVol")),
        "bidVol": level5(t.get("bidVol")),
    }


def build_ticks_payload(context, params):
    codes = [c.strip() for c in str(params.get("codes", "")).split(",") if c.strip()]
    if not codes:
        return {"status": "error", "message": "证券代码无效"}
    tick_map = context.get_full_tick(codes) or {}
    ticks = {code: tick_item(t) for code, t in tick_map.items()}
    return {"status": "success", "total": len(ticks), "ticks": ticks}


def build_all_ticks_payload(context):
    tick_map = context.get_full_tick(["SH", "SZ", "BJ"]) or {}
    ticks = {}
    for code, t in tick_map.items():
        _m = code.split(".")[1]
        _pc = code.split(".")[0][0:2]
        if (_m, _pc) not in _STOCK_ALL_CODE:
            continue
        ticks[code] = tick_item(t)
    return {"status": "success", "total": len(ticks), "ticks": ticks}


def build_instrument_detail_payload(context, params):
    code = str(params.get("stock_code", ""))
    detail = get_detail(context, code)
    if not detail:
        return {"status": "error", "message": "未获取到 %s 的合约详情" % code}  # noqa: UP031
    return {
        "status": "success",
        "stock_code": code,
        "stock_name": clean_text(detail.get("InstrumentName")),
        "price_tick": to_float(detail.get("PriceTick")) or 0.01,
    }


def build_history_payload(context, params):
    code = str(params.get("stock_code", ""))
    period = str(params.get("period", "1d") or "1d")
    start = str(params.get("start_time", "") or "")
    end = str(params.get("end_time", "") or "")
    count = to_int(params.get("count", 60)) or 60
    dividend_type = str(params.get("dividend_type", "none") or "none")
    fields = [f for f in str(params.get("fields", "")).split(",") if f] or [
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    # 每次请求先 download_history_data 下载最新历史数据（否则 get_market_data_ex 只能取本地缓存）
    log("history_data: 触发下载 %s %s %s~%s" % (code, period, start, end))  # noqa: UP031
    try:
        download_history_data(code, period, start, end)  # pyright: ignore[reportUndefinedVariable] # noqa: F821
    except Exception as exc:  # noqa: BLE001
        log_error("download_history_data", exc)
    try:
        market = context.get_market_data_ex(
            fields=fields,
            stock_code=[code],
            period=period,
            start_time=start,
            end_time=end,
            count=count,
            dividend_type=dividend_type,
            fill_data=True,
            subscribe=False,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    stock = market.get(code)
    if stock is None or getattr(stock, "empty", True):
        return {"status": "success", "stock_code": code, "period": period, "data": []}
    rows = []
    for index, row in stock.iterrows():
        item = {"time": format_time(index)}
        for f in fields:
            if f != "time":
                item[f] = json_safe(row.get(f))
        rows.append(item)
    return {"status": "success", "stock_code": code, "period": period, "data": rows}


def build_divid_payload(context, params):
    code = str(params.get("stock_code", ""))
    try:
        result = context.get_divid_factors(code) or {}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    # 文档：dict{时间戳: [每股红利,每股送转,每股转赠,配股,配股价,是否股改,复权系数]}
    names = (
        "interest",
        "stockBonus",
        "stockGift",
        "allotNum",
        "allotPrice",
        "gugai",
        "dr",
    )
    rows = []
    for ts, values in result.items():
        item = {"time": json_safe(ts)}
        for i, name in enumerate(names):
            item[name] = json_safe(values[i] if i < len(values) else None)
        rows.append(item)
    return {"status": "success", "stock_code": code, "data": rows}


# ---------- 交易接口 ----------
def _get_account_id(params):
    """获取交易账号ID：优先请求参数，缺省使用 ACCOUNT_ID。"""
    account_id = str(params.get("account_id", "") or "").strip()
    return account_id or ACCOUNT_ID


# passorder 下单选价类型（股票）白名单：5=最新价，11=指定价/模型价，14=对手价
ALLOWED_PR_TYPE = (5, 11, 14)


def _order_pr_type(params):
    """解析 passorder 选价类型：仅允许 5(最新价)/11(指定价-模型价)/14(对手价)。

    prType=11(指定价) 时委托价 price 才生效；price<=0 表示随行就市，自动回退为最新价(5)。
    """
    pr_type = to_int(params.get("pr_type", PR_TYPE_DEFAULT)) or PR_TYPE_DEFAULT
    if pr_type not in ALLOWED_PR_TYPE:
        pr_type = PR_TYPE_DEFAULT
    if pr_type == 11 and to_float(params.get("price", 0)) <= 0:
        pr_type = 5  # 指定价但未给价，按最新价随行就市
    return pr_type


def _qmt_code(order):
    """由 QMT 委托/成交对象拼接 QMT 原生代码（600519.SH）。"""
    stock = getattr(order, "m_strInstrumentID", "") or ""
    exchange = getattr(order, "m_strExchangeID", "") or ""
    return "%s.%s" % (stock, exchange) if exchange else stock  # noqa: UP031


def _order_ref(order):
    """取委托号：优先 m_strOrderSysID，缺省回退 m_nOrderId。"""
    return str(
        getattr(order, "m_strOrderSysID", "") or getattr(order, "m_nOrderId", "") or ""
    )


def _order_to_dict(order):
    """QMT 委托对象 -> JSON 可序列化 dict。

    字段与 QMT_DOCS.txt 中委托结构体(orderdetail)及 get_trade_detail_data(...,
    'order') 对齐，每种取值均带安全默认值，缺失字段回退为 0/空串，不影响原有调用。
    """
    if order is None:
        return {}
    return {
        # ---- 基础信息 ----
        "order_id": _order_ref(order),  # 委托号（优先 m_strOrderSysID，回退 m_nOrderId）
        "code": _qmt_code(order),  # 证券代码/合约代码（QMT 原生格式，如 600519.SH）
        "name": getattr(order, "m_strInstrumentName", "") or "",  # 证券名称/合约名称
        "market": getattr(order, "m_strExchangeID", "") or "",  # 证券市场/交易所代码
        "exchange_name": getattr(order, "m_strExchangeName", "") or "",  # 市场名称
        "product_id": getattr(order, "m_strProductID", "") or "",  # 品种代码
        "product_name": getattr(order, "m_strProductName", "") or "",  # 品种名称
        # ---- 委托属性 ----
        "order_type": getattr(order, "m_nOrderType", getattr(order, "m_nBusinessType", 0)),  # 委托类别/业务类型
        "direction": getattr(order, "m_nDirection", 0),  # 买卖方向（48=买/多，49=卖/空）
        "offset_flag": getattr(order, "m_nOffsetFlag", 0),  # 开平标志（股票买卖即开平）
        "hedge_flag": getattr(order, "m_nHedgeFlag", 0),  # 投保标记
        "price_type": getattr(order, "m_nPriceType", getattr(order, "m_nOrderPriceType", 0)),  # 下单选价类型（5=最新价，11=指定价/模型价，14=对手价）
        "entrust_type": getattr(order, "m_nEntrustType", 0),  # 委托类别
        "strategy_name": getattr(order, "m_strStrategyName", "") or "",  # 策略名（passorder 的 strategyName，区分策略来源）
        "order_remark": getattr(order, "m_strRemark", "") or "",  # 委托备注/用户自设委托ID（对应 userOrderId）
        "order_source": getattr(order, "m_strOrderSource", "") or "",  # 委托来源
        # ---- 数量/价格 ----
        "order_volume": to_int(getattr(order, "m_nVolumeTotalOriginal", 0)),  # 最初委托量（股/手）
        "total_volume": to_int(getattr(order, "m_nVolumeTotal", 0)),  # 当前总委托量（股票不动用）
        "price": to_float(getattr(order, "m_dLimitPrice", getattr(order, "m_dOrderPrice", getattr(order, "m_dPrice", 0)))),  # 委托价/限价（prType=11 指定价时生效）
        "traded_price": to_float(getattr(order, "m_dTradedPrice", 0)),  # 成交均价
        "traded_volume": to_int(getattr(order, "m_nVolumeTraded", 0)),  # 已成交量
        "cancel_volume": to_int(getattr(order, "m_dCancelAmount", getattr(order, "m_nCancelAmount", 0))),  # 已撤数量
        "trade_amount": to_float(getattr(order, "m_dTradeAmount", 0)),  # 成交额（=均价*数量*合约乘数，股票乘数为1）
        # ---- 状态 ----
        "status": getattr(order, "m_nOrderStatus", 0),  # 委托状态（50=已报，52=已成，53=已撤，54=废单 等）
        "submit_status": getattr(order, "m_nOrderSubmitStatus", 0),  # 提交状态
        "status_msg": (getattr(order, "m_strStatusMsg", "") or getattr(order, "m_strErrorMsg", "") or getattr(order, "m_strCancelInfo", "") or ""),  # 状态/错误/废单信息
        "error_id": getattr(order, "m_nErrorID", 0),  # 错误号
        "cancel_info": getattr(order, "m_strCancelInfo", "") or "",  # 废单原因
        # ---- 时间 ----
        "order_date": getattr(order, "m_strInsertDate", "") or getattr(order, "m_strEntrustDate", "") or getattr(order, "m_strTradingDay", "") or "",  # 委托日期
        "order_time": getattr(order, "m_strInsertTime", "") or getattr(order, "m_strEntrustTime", "") or "",  # 委托时间
        # ---- 其它 ----
        "order_sysid": getattr(order, "m_strOrderSysID", "") or "",  # 交易所委托号（与成交列表一致，用于关联委托/成交）
        "opt_name": getattr(order, "m_strOptName", "") or "",  # 委托属性中文展示
        "frozen_margin": to_float(getattr(order, "m_dFrozenMargin", 0)),  # 冻结保证金
        "frozen_commission": to_float(getattr(order, "m_dFrozenCommission", 0)),  # 冻结手续费
        "xt_tag": getattr(order, "m_strXTTag", "") or "",  # 迅投标签
        "under_code": getattr(order, "m_strUnderCode", "") or "",  # 标的证券（用于期权/衍生品）
        "reference_rate": to_float(getattr(order, "m_dReferenceRate", 0)),  # 参考汇率（用于港股通）
    }


def _position_to_dict(pos):
    """QMT 持仓对象 -> JSON 可序列化 dict。

    字段与 QMT_DOCS.txt 中持仓结构体(positiondetail)及 get_trade_detail_data(...,
    'position') 对齐，每种取值均带安全默认值，缺失字段回退为 0/空串。
    """
    return {
        # ---- 基础信息 ----
        "code": _qmt_code(pos),  # 证券代码/合约代码（QMT 原生格式，如 600519.SH）
        "name": getattr(pos, "m_strInstrumentName", "") or "",  # 证券名称/合约名称
        "market": getattr(pos, "m_strExchangeID", "") or "",  # 证券市场/交易所代码
        "exchange_name": getattr(pos, "m_strExchangeName", "") or "",  # 市场名称
        "product_id": getattr(pos, "m_strProductID", "") or "",  # 品种代码
        "product_name": getattr(pos, "m_strProductName", "") or "",  # 品种名称
        # ---- 方向/数量 ----
        "direction": getattr(pos, "m_nDirection", 0),  # 买卖方向（48=买/多，49=卖/空）
        "volume": to_int(getattr(pos, "m_nVolume", 0)),  # 持仓量/当前股数
        "can_use_volume": to_int(getattr(pos, "m_nCanUseVolume", 0)),  # 可用数量（股票可卖数量）
        "frozen_volume": to_int(getattr(pos, "m_nFrozenVolume", 0)),  # 冻结数量
        "on_road_volume": to_int(getattr(pos, "m_nOnRoadVolume", 0)),  # 在途数量（未到账）
        "yesterday_volume": to_int(getattr(pos, "m_nYesterdayVolume", 0)),  # 昨日股份余额
        "close_volume": to_int(getattr(pos, "m_dCloseVolume", 0)),  # 平仓量（股票不动用）
        "close_amount": to_float(getattr(pos, "m_dCloseAmount", 0)),  # 平仓额（股票不动用）
        # ---- 价格/盈亏 ----
        "open_price": to_float(getattr(pos, "m_dOpenPrice", 0)),  # 开仓价/成本价
        "avg_open_price": to_float(getattr(pos, "m_dAvgOpenPrice", 0)),  # 开仓均价
        "last_price": to_float(getattr(pos, "m_dLastPrice", 0)),  # 最新价/当前价
        "settlement_price": to_float(getattr(pos, "m_dSettlementPrice", 0)),  # 结算价（股票=当前价）
        "last_settlement_price": to_float(getattr(pos, "m_dLastSettlementPrice", 0)),  # 最新结算价（股票不动用）
        "market_value": to_float(getattr(pos, "m_dMarketValue", 0)),  # 当前市值
        "instrument_value": to_float(getattr(pos, "m_dInstrumentValue", 0)),  # 合约价值
        "float_profit": to_float(getattr(pos, "m_dFloatProfit", 0)),  # 浮动盈亏
        "close_profit": to_float(getattr(pos, "m_dCloseProfit", 0)),  # 平仓盈亏（股票不动用）
        "position_profit": to_float(getattr(pos, "m_dPositionProfit", 0)),  # 持仓盈亏
        "profit_rate": to_float(getattr(pos, "m_dProfitRate", 0)),  # 持仓盈亏比例
        "position_cost": to_float(getattr(pos, "m_dPositionCost", 0)),  # 持仓成本
        "open_cost": to_float(getattr(pos, "m_dOpenCost", 0)),  # 开仓成本
        "margin": to_float(getattr(pos, "m_dMargin", 0)),  # 使用保证金（股票不动用）
        # ---- 其它 ----
        "stock_holder": getattr(pos, "m_strStockHolder", "") or "",  # 股东账号
        "trading_day": getattr(pos, "m_strTradingDay", "") or "",  # 交易日
        "open_date": getattr(pos, "m_strOpenDate", "") or "",  # 成交日期
        "hedge_flag": getattr(pos, "m_nHedgeFlag", 0),  # 投保标记
        "is_today": getattr(pos, "m_nIsToday", 0),  # 是否今仓
        "xt_tag": getattr(pos, "m_strXTTag", "") or "",  # 迅投标签
        "future_trade_type": getattr(pos, "m_nFutureTradeType", 0),  # 成交类型（期货）
        "expire_date": getattr(pos, "m_strExpireDate", "") or "",  # 到期日（逆回购用）
        "total_cost": to_float(getattr(pos, "m_dTotalCost", 0)),  # 自定义累计成本（股票信用用）
        "single_cost": to_float(getattr(pos, "m_dSingleCost", 0)),  # 自定义单股成本（股票信用用）
    }


def _collect_order_ids(account_id):
    """收集当前活跃委托号集合（用于下单后排重新委托）。"""
    try:
        orders = (
            get_trade_detail_data(account_id, ACCOUNT_TYPE, "order", STRATEGY_NAME)  # type: ignore  # noqa: F821
            or []
        )  # pyright: ignore[reportUndefinedVariable]
        return {ref for ref in (_order_ref(_o) for _o in orders) if ref}
    except Exception:  # noqa: BLE001
        return set()


def _find_new_order_ref(
    account_id, code, exclude_ids, user_order_id="", max_wait=ORDER_REF_WAIT
):
    """查询新增的委托号（委托可能瞬间成交从列表消失）。

    优先按 userOrderId 匹配（对应委托/成交的 m_strRemark 字段，最精确）；
    未指定 userOrderId 时回退为“排除已知委托号后取新增”。
    """
    import time as _time

    _time.sleep(0.5)
    for _i in range(int(max_wait / 0.5)):
        try:
            for order in reversed(
                get_trade_detail_data(account_id, ACCOUNT_TYPE, "order", STRATEGY_NAME)  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
                or []  # pyright: ignore[reportUndefinedVariable]
            ):
                ref = _order_ref(order)
                if not ref or ref in exclude_ids:
                    continue
                if user_order_id:
                    remark = getattr(order, "m_strRemark", "") or ""
                    if remark != user_order_id:
                        continue
                full_code = _qmt_code(order)
                if code and full_code and code not in full_code:
                    continue
                return True, ref
            for deal in reversed(
                get_trade_detail_data(account_id, ACCOUNT_TYPE, "deal", STRATEGY_NAME)  # noqa: F821 # type: ignore
                or []  # pyright: ignore[reportUndefinedVariable]
            ):
                ref = _order_ref(deal)
                if not ref or ref in exclude_ids:
                    continue
                if user_order_id:
                    remark = getattr(deal, "m_strRemark", "") or ""
                    if remark != user_order_id:
                        continue
                full_code = _qmt_code(deal)
                if code and full_code and code not in full_code:
                    continue
                return True, ref
        except Exception as exc:  # noqa: BLE001
            log_error("_find_new_order_ref", exc)
        _time.sleep(0.5)
    return False, ""


def build_balance_payload(context, params):
    account_id = _get_account_id(params)
    if not account_id:
        return {"status": "error", "message": "account_id 参数缺失"}
    try:
        account_list = get_trade_detail_data(account_id, ACCOUNT_TYPE, "account")  # pyright: ignore[reportUndefinedVariable] # noqa: F821
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    info = account_list[0] if account_list else None
    if not info:
        return {"status": "error", "message": "资金数据获取失败"}
    total_balance = to_float(getattr(info, "m_dBalance", 0))
    return {
        "status": "success",
        "account_id": account_id,
        # ---- 资金字段（get_trade_detail_data(..., 'account') 结构性字段）----
        "total_balance": total_balance,  # 总资产/账户市值（m_dBalance）
        "available_cash": to_float(getattr(info, "m_dAvailable", 0)),  # 可用资金（m_dAvailable）
        "market_value": to_float(getattr(info, "m_dInstrumentValue", getattr(info, "m_dMarketValue", 0))),  # 证券市值（m_dInstrumentValue，兜底 m_dMarketValue）
        "total_assets": total_balance,  # 总资产（与 m_dBalance 一致，兼容旧字段名）
        "cash": to_float(getattr(info, "m_dCash", 0)),  # 现金（m_dCash）
        "assure_asset": to_float(getattr(info, "m_dAssureAsset", 0)),  # 担保资产/信用资产（m_dAssureAsset，融资融券用）
        "total_debit": to_float(getattr(info, "m_dTotalDebit", 0)),  # 总负债（m_dTotalDebit，融资融券用）
        "position_profit": to_float(getattr(info, "m_dPositionProfit", 0)),  # 持仓盈亏（m_dPositionProfit）
    }


def build_positions_payload(context, params):
    account_id = _get_account_id(params)
    if not account_id:
        return {"status": "error", "message": "account_id 参数缺失"}
    code = str(params.get("code", "") or "")
    try:
        positions = get_trade_detail_data(  # noqa: F821 # type: ignore
            account_id, ACCOUNT_TYPE, "position"
        )  # pyright: ignore[reportUndefinedVariable]
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}
    data = []
    for pos in positions or []:
        item = _position_to_dict(pos)
        if code and item["code"] != code:
            continue
        data.append(item)
    return {
        "status": "success",
        "account_id": account_id,
        "total": len(data),
        "data": data,
    }


def build_order_payload(context, params):
    account_id = _get_account_id(params)
    code = str(params.get("code", "") or "")
    o_type = str(params.get("o_type", "") or "")
    amount = to_int(params.get("amount", 0))
    price = to_float(params.get("price", 0))
    pr_type = _order_pr_type(params)
    user_order_id = str(
        params.get("user_order_id", "") or "chanlun_%d" % int(time.time() * 1000)  # noqa: UP031
    ).strip()
    if not account_id:
        return {"status": "error", "message": "account_id 参数缺失"}
    if not code or not o_type or amount <= 0:
        return {
            "status": "error",
            "message": "参数错误：code/o_type/amount 必填且 amount>0",
        }
    op_type = {"buy": 23, "sell": 24}.get(o_type)
    if op_type is None:
        return {"status": "error", "message": "o_type 错误：仅支持 buy/sell"}
    before_ids = _collect_order_ids(account_id)
    try:
        order_ref = passorder(  # pyright: ignore[reportUndefinedVariable] # noqa: F821
            op_type,
            1101,  # orderType：单股、单账号、普通、股/手方式下单
            account_id,
            code,
            pr_type,
            price,
            amount,
            STRATEGY_NAME,
            QUICK_TRADE,
            user_order_id,
            context,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": "下单失败: %s" % exc}  # noqa: UP031
    ref_str = str(order_ref) if order_ref is not None else ""
    if not ref_str or ref_str in ("None", "0", "-1"):
        # passorder 可能异步下单未直接返回委托号，按 userOrderId 查询确认
        found, new_ref = _find_new_order_ref(
            account_id, code, before_ids, user_order_id
        )
        ref_str = new_ref if found else ""
    log(
        "下单 %s %s %s 数量 %d 委托号=%s"  # noqa: UP031
        % (o_type, code, account_id, amount, ref_str or "unknown")
    )
    return {
        "status": "success",
        "account_id": account_id,
        "code": code,
        "o_type": o_type,
        "amount": amount,
        "price": price,
        "pr_type": pr_type,
        "order_id": ref_str or "unknown",
    }


def build_order_detail_payload(context, params):
    account_id = _get_account_id(params)
    order_id = str(params.get("order_id", "") or "").strip()
    if not account_id:
        return {"status": "error", "message": "account_id 参数缺失"}
    if not order_id:
        return {"status": "error", "message": "order_id 参数缺失"}
    order = None
    try:
        order = get_value_by_order_id(order_id, account_id, ACCOUNT_TYPE, "order")  # pyright: ignore[reportUndefinedVariable] # noqa: F821
    except Exception:  # noqa: BLE001
        order = None
    if order is None:
        try:
            orders = (
                get_trade_detail_data(account_id, ACCOUNT_TYPE, "order", STRATEGY_NAME)  # noqa: F821 # type: ignore
                or []
            )  # pyright: ignore[reportUndefinedVariable]
            order = next((_o for _o in orders if _order_ref(_o) == order_id), None)
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
    if order is None:
        return {"status": "success", "order_id": order_id, "order": None}
    return {"status": "success", "order_id": order_id, "order": _order_to_dict(order)}


def build_order_cancel_payload(context, params):
    account_id = _get_account_id(params)
    order_id = str(params.get("order_id", "") or "").strip()
    if not account_id:
        return {"status": "error", "message": "account_id 参数缺失"}
    if not order_id:
        return {"status": "error", "message": "order_id 参数缺失"}
    try:
        cancellable = can_cancel_order(order_id, account_id, ACCOUNT_TYPE)  # pyright: ignore[reportUndefinedVariable] # noqa: F821
        if not cancellable:
            return {
                "status": "success",
                "account_id": account_id,
                "order_id": order_id,
                "canceled": False,
                "message": "该委托不可撤",
            }
        result = cancel(order_id, account_id, ACCOUNT_TYPE, context)  # pyright: ignore[reportUndefinedVariable] # noqa: F821
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": "撤单失败: %s" % exc}  # noqa: UP031
    return {
        "status": "success",
        "account_id": account_id,
        "order_id": order_id,
        "canceled": bool(result),
    }


# ---------- 请求处理 ----------
def dispatch(context, path, params):
    if path == "/health":
        return build_health_payload(context)
    if path == "/all_stocks":
        return build_all_stocks_payload(context)
    if path == "/all_ticks":
        return build_all_ticks_payload(context)
    if path == "/tick":
        return build_ticks_payload(context, params)
    if path == "/instrument_detail":
        return build_instrument_detail_payload(context, params)
    if path == "/history_data":
        return build_history_payload(context, params)
    if path == "/divid_factors":
        return build_divid_payload(context, params)
    if path == "/balance":
        return build_balance_payload(context, params)
    if path == "/positions":
        return build_positions_payload(context, params)
    if path == "/order":
        return build_order_payload(context, params)
    if path == "/order_detail":
        return build_order_detail_payload(context, params)
    if path == "/order_cancel":
        return build_order_cancel_payload(context, params)
    return {"status": "error", "message": "桥接接口不存在: " + path}


def handle_request(text, context):
    try:
        req = json.loads(text)
    except Exception:  # noqa: BLE001
        append_debug_log("invalid_request", str(text)[:200])
        return
    rid = str(req.get("id", ""))
    path = str(req.get("path", ""))
    result_key = str(req.get("result_key", "") or "")
    if not rid or not path or not result_key:
        log("请求缺少 id/path/result_key: %s" % str(text)[:120])  # noqa: UP031
        return
    start = time.time()
    log("收到请求 id=%s path=%s result_key=%s" % (rid, path, result_key))  # noqa: UP031
    try:
        result = dispatch(context, path, req.get("params") or {})
        result["id"] = rid
        log("请求处理完成 id=%s path=%s 耗时 %.1fs" % (rid, path, time.time() - start))  # noqa: UP031
    except Exception as exc:  # noqa: BLE001
        append_debug_log(
            "request_exception",
            {
                "id": rid,
                "path": path,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            },
        )
        log_error("请求处理异常 id=%s path=%s" % (rid, path), exc)  # noqa: UP031
        result = {"id": rid, "status": "error", "message": str(exc)}
    try:
        s_time = time.time()
        conn_cmd.setex(result_key, RESULT_TTL, json.dumps(result, ensure_ascii=False))
        log(
            "结果已写入 key=%s id=%s path=%s 耗时 %.2f"  # noqa: UP031
            % (result_key, rid, path, time.time() - s_time)
        )
    except Exception as exc:  # noqa: BLE001
        log_error("写入结果失败 id=%s" % rid, exc)  # noqa: UP031


def listen_loop(C):
    global pubsub
    try:
        msg = pubsub.get_message(timeout=1, ignore_subscribe_messages=True)
        if msg and msg.get("type") == "message":
            text = msg["data"]
            if isinstance(text, bytes):
                text = text.decode("utf-8")
            handle_request(text, C)
    except Exception as exc:  # noqa: BLE001
        log_error("监听异常（3秒后重连）", exc)
        pubsub = new_redis().pubsub()
        pubsub.subscribe(*sub_channels())


def heartbeat_loop(C):
    global conn_cmd
    try:
        conn_cmd.setex(HEARTBEAT_KEY, HEARTBEAT_TTL, str(time.time()))
    except Exception as exc:  # noqa: BLE001
        log_error("心跳更新异常（3秒后重连）", exc)
        try:
            if conn_cmd is not None:
                conn_cmd.close()
            conn_cmd = new_redis()
        except Exception:  # noqa: BLE001, S110
            pass


_STARTED = False


def start_bridge(C):
    global _STARTED
    if _STARTED:
        return
    _STARTED = True
    C.run_time("listen_loop", "100nMilliSecond", "2020-01-01 00:00:00")
    C.run_time("heartbeat_loop", "5nSecond", "2020-01-01 00:00:00")
    log(
        "big_qmt_app redis 桥接已启动: redis=%s:%s/%d 订阅频道=%s 账号=%s 心跳=%s"  # noqa: UP031
        % (
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
            ",".join(sub_channels()),
            ACCOUNT_ID or "-",
            HEARTBEAT_KEY,
        )
    )


# ---------- 大QMT策略生命周期 ----------
def init(ContextInfo):
    global ACCOUNT_ID  # noqa: PLW0602
    # 绑定交易账号（passorder/get_trade_detail_data 等交易函数依赖此设置）
    if ACCOUNT_ID:
        try:
            ContextInfo.set_account(ACCOUNT_ID)
            log("已绑定交易账号: %s" % ACCOUNT_ID)  # noqa: UP031
        except Exception as exc:  # noqa: BLE001
            log_error("set_account 绑定账号失败", exc)
    start_bridge(ContextInfo)


def handlebar(ContextInfo):
    pass  # QMT 策略必需函数；桥接逻辑由 run_time 驱动（listen_loop/heartbeat_loop）


def stop(ContextInfo):
    global conn_cmd
    if conn_cmd is not None:
        try:
            conn_cmd.close()
        except Exception:  # noqa: BLE001, S110
            pass
    conn_cmd = None
    log("桥接已停止")
