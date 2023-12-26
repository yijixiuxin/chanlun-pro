import time
import traceback
from typing import Union

from chanlun.exchange import get_exchange, Market
from chanlun import fun, rd, zixuan, monitor
from chanlun.cl_utils import query_cl_chart_config


def query_task_config(task_name) -> Union[None, dict]:
    """
    查询任务配置
    """
    now_time = int(time.time())
    config = rd.task_config_query(task_name, True)
    if config["is_run"] is False:
        return None

    if now_time % (config["interval_minutes"] * 60) != 0:
        return None

    if len(config["frequencys"]) == 0:
        return None

    return config


def task_a_1():
    """
    股票行情监控任务
    """
    config = query_task_config("a_1")
    if config is None:
        return True

    ex = get_exchange(Market.A)
    if ex.now_trading() is False:
        return True

    print(f"{fun.now_dt()} Run a_1 Task: Config {config}")

    zx = zixuan.ZiXuan("a")
    stocks = zx.zx_stocks(config["zixuan"])
    for stock in stocks:
        try:
            code = stock["code"]
            name = stock["name"]
            cl_config = query_cl_chart_config("a", code)
            print(f"{fun.now_dt()} : Task a_1 Run : {code} - {name}")
            monitor.monitoring_code(
                "沪深监控" "a",
                code,
                name,
                config["frequencys"],
                {
                    "beichi": config["check_beichi"],
                    "mmd": config["check_mmd"],
                    "beichi_xd": config["check_beichi_xd"],
                    "mmd_xd": config["check_mmd_xd"],
                },
                is_send_msg=config["is_send_msg"],
                cl_config=cl_config,
            )
        except Exception as e:
            print(f"a_1 任务 {stock} 执行异常：", e)
            print(traceback.format_exc())

    return True


def task_a_2():
    """
    股票行情监控任务
    """
    config = query_task_config("a_2")
    if config is None:
        return True

    ex = get_exchange(Market.A)
    if ex.now_trading() is False:
        return True

    print(f"{fun.now_dt()} Run a_2 Task: Config {config}")

    zx = zixuan.ZiXuan("a")
    stocks = zx.zx_stocks(config["zixuan"])
    for stock in stocks:
        try:
            code = stock["code"]
            name = stock["name"]
            cl_config = query_cl_chart_config("a", code)
            print(f"{fun.now_dt()} : Task a_2 Run : {code} - {name}")
            monitor.monitoring_code(
                "沪深监控",
                "a",
                code,
                name,
                config["frequencys"],
                {
                    "beichi": config["check_beichi"],
                    "mmd": config["check_mmd"],
                    "beichi_xd": config["check_beichi_xd"],
                    "mmd_xd": config["check_mmd_xd"],
                },
                is_send_msg=config["is_send_msg"],
                cl_config=cl_config,
            )
        except Exception as e:
            print(f"a_2 任务 {stock} 执行异常：", e)
    return True


def task_hk():
    """
    股票行情监控任务
    """
    config = query_task_config("hk")
    if config is None:
        return True

    ex = get_exchange(Market.HK)
    if ex.now_trading() is False:
        return True

    print(f"{fun.now_dt()} Run hk Task: Config {config}")

    zx = zixuan.ZiXuan("hk")
    stocks = zx.zx_stocks(config["zixuan"])
    for stock in stocks:
        try:
            code = stock["code"]
            name = stock["name"]
            cl_config = query_cl_chart_config("hk", code)
            print(f"{fun.now_dt()} : Task hk Run : {code} - {name}")
            monitor.monitoring_code(
                "港股监控",
                "hk",
                code,
                name,
                config["frequencys"],
                {
                    "beichi": config["check_beichi"],
                    "mmd": config["check_mmd"],
                    "beichi_xd": config["check_beichi_xd"],
                    "mmd_xd": config["check_mmd_xd"],
                },
                is_send_msg=config["is_send_msg"],
                cl_config=cl_config,
            )
        except Exception as e:
            print(f"hk 任务 {stock} 执行异常：", e)

    return True


def task_us():
    """
    美股行情监控任务
    """
    config = query_task_config("us")
    if config is None:
        return True

    ex = get_exchange(Market.US)
    if ex.now_trading() is False:
        return True

    print(f"{fun.now_dt()} Run us Task: Config {config}")

    zx = zixuan.ZiXuan("us")
    stocks = zx.zx_stocks(config["zixuan"])
    for stock in stocks:
        try:
            code = stock["code"]
            name = stock["name"]
            cl_config = query_cl_chart_config("us", code)
            print(f"{fun.now_dt()} : Task us Run : {code} - {name}")
            monitor.monitoring_code(
                "美股监控",
                "us",
                code,
                name,
                config["frequencys"],
                {
                    "beichi": config["check_beichi"],
                    "mmd": config["check_mmd"],
                    "beichi_xd": config["check_beichi_xd"],
                    "mmd_xd": config["check_mmd_xd"],
                },
                is_send_msg=config["is_send_msg"],
                cl_config=cl_config,
            )
        except Exception as e:
            print(f"us 任务 {stock} 执行异常：", e)

    return True


def task_futures():
    """
    期货行情监控任务
    """
    config = query_task_config("futures")
    if config is None:
        return True

    ex = get_exchange(Market.FUTURES)
    if ex.now_trading() is False:
        return True

    print(f"{fun.now_dt()} Run futures Task: Config {config}")

    zx = zixuan.ZiXuan("futures")
    stocks = zx.zx_stocks(config["zixuan"])
    for stock in stocks:
        try:
            code = stock["code"]
            name = stock["name"]
            cl_config = query_cl_chart_config("futures", code)
            print(f"{fun.now_dt()} : Task futures Run : {code} - {name}")
            monitor.monitoring_code(
                "期货监控",
                "futures",
                code,
                name,
                config["frequencys"],
                {
                    "beichi": config["check_beichi"],
                    "mmd": config["check_mmd"],
                    "beichi_xd": config["check_beichi_xd"],
                    "mmd_xd": config["check_mmd_xd"],
                },
                is_send_msg=config["is_send_msg"],
                cl_config=cl_config,
            )
        except Exception as e:
            print(f"futures 任务 {stock} 执行异常：", e)

    return True


def task_currency():
    """
    数字货币行情监控任务
    """
    config = query_task_config("currency")
    if config is None:
        return True

    print(f"{fun.now_dt()} Run currency Task: Config {config}")

    zx = zixuan.ZiXuan("currency")
    stocks = zx.zx_stocks(config["zixuan"])
    for stock in stocks:
        try:
            code = stock["code"]
            name = stock["name"]
            cl_config = query_cl_chart_config("currency", code)
            print(f"{fun.now_dt()} : Task currency Run : {code} - {name}")
            monitor.monitoring_code(
                "数字货币监控",
                "currency",
                code,
                name,
                config["frequencys"],
                {
                    "beichi": config["check_beichi"],
                    "mmd": config["check_mmd"],
                    "beichi_xd": config["check_beichi_xd"],
                    "mmd_xd": config["check_mmd_xd"],
                },
                is_send_msg=config["is_send_msg"],
                cl_config=cl_config,
            )
        except Exception as e:
            print(f"currency 任务 {stock} 执行异常：", e)

    return True
