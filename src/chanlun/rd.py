import hashlib
import json
import random
import time
import uuid

import redis

from chanlun import config
from chanlun.cl_interface import *

r = None
rb = None


def Robj():
    global r
    if r is None:
        r = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
        )
    return r


def RobjByBytes():
    global rb
    if rb is None:
        rb = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False
        )
    return rb


def save_byte(key, val):
    """
    保存字节数据到 redis 中
    :param key:
    :param val:
    :return:
    """
    robj = redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False
    )
    robj.set(key, val)


def get_byte(key):
    """
    读取字节数据
    :param key:
    :return:
    """
    robj = redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False
    )
    return robj.get(key)


# 加锁的过程
def acquire_lock(lock_name, acquite_timeout=15, time_out=10):
    """
    redis 分布锁，加锁
    """
    identifier = str(uuid.uuid4())
    # 客户端获取锁的结束时间
    end = time.time() + acquite_timeout
    lock_names = "lock_name:" + lock_name
    # print(f"锁 {lock_names} end_time:{end}")
    while time.time() < end:
        # setnx(key,value) 只有key不存在情况下，将key的值设置为value 返回True,若key存在则不做任何动作,返回False
        if Robj().setnx(lock_names, identifier):
            # 设置键的过期时间，过期自动剔除，释放锁
            # print('获得锁:进程' + str(args))
            # print(f'分布式锁value:{identifier}')
            Robj().expire(lock_name, time_out)
            return identifier
        # 当锁未被设置过期时间时，重新设置其过期时间
        elif Robj().ttl(lock_name) == -1:
            Robj().expire(lock_name, time_out)
        time.sleep(0.001)


# 锁的释放
def release_lock(lock_name, identifire):
    lock_names = "lock_name:" + lock_name
    pipe = Robj().pipeline(True)
    while True:
        try:
            # 通过watch命令监视某个键，当该键未被其他客户端修改值时，事务成功执行。当事务运行过程中，发现该值被其他客户端更新了值，任务失败
            pipe.watch(lock_names)
            # print(pipe.get((lock_names)))
            if pipe.get(lock_names) == identifire:  # 检查客户端是否仍然持有该锁
                # multi命令用于开启一个事务，它总是返回ok
                # multi执行之后， 客户端可以继续向服务器发送任意多条命令， 这些命令不会立即被执行， 而是被放到一个队列中， 当 EXEC 命令被调用时， 所有队列中的命令才会被执行
                pipe.multi()
                # 删除键，释放锁
                pipe.delete(lock_names)
                # execute命令负责触发并执行事务中的所有命令
                pipe.execute()
                return True
            pipe.unwatch()
            pipe.delete(lock_names)
            break
        except redis.exceptions.WatchError:
            # # 释放锁期间，有其他客户端改变了键值对，锁释放失败，进行循环
            pass
    return False


def strategy_save(key, obj):
    """
    策略回测结果保存
    :param key:
    :param obj:
    :return:
    """
    robj = redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False
    )
    return robj.hset("strategy_back", key, obj)


def strategy_get(key):
    """
    策略回测结果读取
    :param key:
    :return:
    """
    robj = redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False
    )
    return robj.hget("strategy_back", key)


def strategy_keys():
    """
    获取保存的所有回测结果 key
    :return:
    """
    ks = Robj().hkeys("strategy_back")
    ks.sort(reverse=True)
    return ks


def save_ex(key: str, seconds: int, value: object):
    """
    设置有效期的 Key 并保存 value 值
    """
    return Robj().setex(key, seconds, json.dumps(value))


def get_ex(key: str):
    """
    读取 key，过期无值返回 None
    """
    value = Robj().get(key)
    if value is None:
        return None
    return json.loads(value)


def zx_save(market_type, name, stocks):
    """
    自选列表保存
    """
    if market_type == "a":
        market_type = "stock"
    Robj().hset(f"zixuan_{market_type}", name, json.dumps(stocks))
    return True


def zx_query(market_type, name):
    """
    查询自选列表
    """
    if market_type == "a":
        market_type = "stock"
    stocks = Robj().hget(f"zixuan_{market_type}", name)
    stocks = json.loads(stocks) if stocks else []
    return stocks


def jhs_query(market):
    """
    机会列表查询
    :return:
    """
    if market == "a":
        hkey = "stock_jh"
    elif market == "hk":
        hkey = "hk_jh"
    elif market == "us":
        hkey = "us_jh"
    elif market == "futures":
        hkey = "futures_jh"
    elif market == "currency":
        hkey = "currency_jh"
    else:
        raise Exception(f"jh save market error {market}")

    jhs = []
    h_keys = Robj().hkeys(hkey)
    for k in h_keys:
        v = Robj().hget(hkey, k)
        if v is not None:
            v = json.loads(v)
            # 时间转换
            v["datetime_str"] = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(v["datetime"])
            )
            jhs.append(v)
    # 按照 datetime 排序
    jhs.sort(key=lambda j: j["datetime"], reverse=True)
    return jhs


def jhs_save(market, code, name, jh: dict):
    """
    机会保存
    """
    if market == "a":
        hkey = "stock_jh"
    elif market == "hk":
        hkey = "hk_jh"
    elif market == "us":
        hkey = "us_jh"
    elif market == "futures":
        hkey = "futures_jh"
    elif market == "currency":
        hkey = "currency_jh"
    else:
        raise Exception(f"jh save market error {market}")

    bi: BI = jh.get("bi", None)
    xd: XD = jh.get("xd", None)

    if bi:
        is_done = "笔完成" if bi.is_done() else "笔未完成"
        is_td = " - TD" if jh["bi_td"] else "--"
    else:
        is_done = "线段完成" if xd.is_done() else "线段未完成"
        is_td = ""

    frequency = jh["frequency"]
    jh_type = jh["type"]

    key = f"stock_code:{code}_frequency:{frequency}_jhtype:{jh_type}"
    ex_val = Robj().hget(hkey, key)
    if ex_val is not None:
        ex_val = json.loads(ex_val)
        if (
            "is_done" in ex_val.keys()
            and ex_val["is_done"] == is_done
            and ex_val["is_td"] == is_td
        ):
            # 没有变化，直接返回
            return True

    val = {
        "market": market,
        "code": code,
        "name": name,
        "frequency": frequency,
        "jh_type": jh_type,
        "is_done": is_done,
        "is_td": is_td,
        "datetime": int(time.time()),
    }
    Robj().hset(hkey, key, json.dumps(val))

    # 检查超过 5 * 24 小时的机会
    if random.randint(0, 100) < 10:
        h_keys = Robj().hkeys(hkey)
        for k in h_keys:
            v = Robj().hget(hkey, k)
            if v is not None:
                v = json.loads(v)
                if int(time.time()) - int(v["datetime"]) > 5 * 24 * 60 * 60:
                    Robj().hdel(hkey, k)

    return False  # False 意思表示有更新


def currency_opt_record_save(symbol, info):
    """
    数字货币操盘记录
    :param symbol:
    :param info:
    :return:
    """
    global r
    record = {
        "symbol": symbol,
        "info": info,
        "datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    Robj().lpush("currency_opt_records", json.dumps(record))
    return True


def currency_opt_record_query(num=100):
    """
    数字货币操作记录查询
    :return:
    """
    global r
    res = Robj().lrange("currency_opt_records", 0, num)
    return [json.loads(_r) for _r in res]


def dl_hy_rank_query():
    """
    查询行业动量排行
    :return:
    """
    res = Robj().get("dl_ranks")
    if res is None:
        return {}
    res = json.loads(res)
    return res


def dl_hy_rank_save(ranks: Dict[str, dict]):
    """
    行业动量排行保存
    :param ranks:
    :return:
    """
    # 只保留倒数5天的数据
    days = sorted(ranks.keys())
    save_ranks = {day: ranks[day] for day in days[-5:]}
    Robj().set("dl_ranks", json.dumps(save_ranks))
    return True


def dl_gn_rank_query():
    """
    查询概念动量排行
    :return:
    """
    res = Robj().get("dl_gn_ranks")
    if res is None:
        return {}
    res = json.loads(res)
    return res


def dl_gn_rank_save(ranks: Dict[str, dict]):
    """
    概念动量排行保存
    :param ranks:
    :return:
    """
    days = sorted(ranks.keys())
    save_ranks = {}
    for day in days[-5:]:
        save_ranks[day] = ranks[day]
    Robj().set("dl_gn_ranks", json.dumps(ranks))
    return True


def add_code_marks(
    code: str,
    _time: int,
    color: str,
    label: str,
    tooltip: str,
    shape: str = "circle",
    show_freq: str = "",
) -> bool:
    """
    添加代码在 tv 时间轴显示的信息

    @param code : 代码标的
    @parma _time : int 时间戳
    @param color : 颜色 rgb，比如 'red'  '#FF0000'
    @param label : 时间刻度标记的标签，英文字母，最大 两位
    @param tooltip : 工具提示内容
    @param shape : "circle" | "earningUp" | "earningDown" | "earning" 形状
    @param show_freq: 需要在什么周期显示，默认 ‘’，所有周期，可以是 'd', '30m', '5m' 这样之下指定周期下展示
    """
    mark = {
        "code": code,
        "time": _time,
        "color": color,
        "label": label,
        "tooltip": tooltip,
        "shape": shape,
        "show_freq": show_freq,
    }
    key = hashlib.md5(
        f"{mark['time']}_{mark['label']}_{mark['shape']}".encode("utf-8")
    ).hexdigest()
    marks = Robj().hget("tv_code_marks", code)
    marks = {} if marks is None else json.loads(marks)
    marks[key] = mark
    Robj().hset("tv_code_marks", code, json.dumps(marks))
    return True


def get_code_marks(code) -> list:
    """
    获取指定代码，需要在 tv 时间轴上显示的标记信息
    """
    marks = Robj().hget("tv_code_marks", code)
    marks = {} if marks is None else json.loads(marks)
    return list(marks.values())


def del_all_marks_by_label(label: str):
    codes = Robj().hkeys("tv_code_marks")
    for _c in codes:
        marks = Robj().hget("tv_code_marks", _c)
        if marks is None:
            continue
        marks: dict = json.loads(marks)
        del_keys = []
        for _k, _v in marks.items():
            if _v["label"] == label:
                del_keys.append(_k)
        if len(del_keys) > 0:
            for _k in del_keys:
                del marks[_k]
            Robj().hset("tv_code_marks", _c, json.dumps(marks))
    return True


def order_save(market, code, order):
    """
    记录交易订单信息
    {
        "code": "SH.000001",
        "datetime": "2021-10-19 10:09:51",
        "type": "buy", (允许的值：buy 买入 sell 卖出  open_long 开多  close_long 平多 open_short 开空 close_short 平空)
        "price": 205.8,
        "amount": 300.0,
        "info": "涨涨涨"
    }
    :param market: 市场（a/hk/us/futures/currency）
    :param code: 标的代码
    :param order: 订单信息
    :return:
    """
    market_key_maps = {
        "a": "stock_orders",
        "hk": "hk_orders",
        "us": "us_orders",
        "futures": "futures_orders",
        "currency": "currency_orders",
    }
    orders = Robj().hget(market_key_maps[market], code)
    orders = {} if orders is None else json.loads(orders)
    key = order["datetime"]
    orders[key] = order
    Robj().hset(market_key_maps[market], code, json.dumps(orders))
    return True


def order_query(market, code):
    """
    股票订单查询
    :param market: 市场（a/hk/us/futures/currency）
    :param code: 标的代码
    :return: 订单列表信息
    """
    market_key_maps = {
        "a": "stock_orders",
        "hk": "hk_orders",
        "us": "us_orders",
        "futures": "futures_orders",
        "currency": "currency_orders",
    }
    orders = Robj().hget(market_key_maps[market], code)
    orders = {} if orders is None else json.loads(orders)
    orders = orders.values()
    return orders


def order_clean(market, code):
    """
    清除指定市场代码的订单信息
    :param market: 市场（a/hk/us/futures/currency）
    :param code: 标的代码
    """
    market_key_maps = {
        "a": "stock_orders",
        "hk": "hk_orders",
        "us": "us_orders",
        "futures": "futures_orders",
        "currency": "currency_orders",
    }
    Robj().hdel(market_key_maps[market], code)
    return True


def task_config_query(task_name, return_obj=True):
    """
    任务配置读取
    """
    task_config: str = Robj().get(f"task_{task_name}")
    task_config: dict = {} if task_config is None else json.loads(task_config)
    if return_obj is False:
        return task_config

    # 检查并设置默认值
    keys = ["is_run", "is_send_msg"]
    for key in keys:
        if (
            key in task_config
            and task_config[key] == ""
            or key not in task_config.keys()
        ):
            task_config[key] = False
        else:
            task_config[key] = bool(int(task_config[key]))
    # 默认的整形参数
    default_int_keys = {
        "interval_minutes": 5,
    }
    for _k, _v in default_int_keys.items():
        task_config[_k] = int(task_config[_k]) if _k in task_config else _v
    if "zixuan" not in task_config.keys():
        task_config["zixuan"] = None

    arr_keys = [
        "frequencys",
        "check_beichi",
        "check_mmd",
        "check_beichi_xd",
        "check_mmd_xd",
    ]
    for ak in arr_keys:
        task_config[ak] = task_config[ak].split(",") if ak in task_config else []
    return task_config


def task_config_save(task_name, task_config: dict):
    """
    任务配置的保存
    """
    task_config = json.dumps(task_config, ensure_ascii=False)
    return Robj().set(f"task_{task_name}", task_config)


if __name__ == "__main__":
    # marks = get_code_marks('SZ.000002')
    # print(marks)

    res = del_all_marks_by_label("V3")
