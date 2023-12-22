import json

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
    pass
