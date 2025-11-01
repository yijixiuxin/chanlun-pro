"""
检查当前环境是否OK
"""

import os
import sys
import telnetlib

import pymysql
import redis
import pathlib
import re


def check_env():
    # 检查 Python 版本
    version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    print(f"当前Python版本：{version}")
    allow_version = ["3.8", "3.9", "3.10", "3.11"]
    if version not in allow_version:
        print(f"当前Python不在支持的列表中：{allow_version}")
        return

    # 检查 环境变量是否设置正确
    try:
        from chanlun.core import cl_interface
    except:
        print("无法导入 chanlun 模块，环境变量未设置或设置错误")
        print(f"当前的环境变量如下：{sys.path}")
        print(f"需要当 PYTHONPATH 环境变量设置为 {os.getcwd()}\src 目录")
        return

    # 检查 环境变量是否设置正确
    try:
        from chanlun import config
    except:
        print(
            "无法导入 config , 请在 src/chanlun 目录， 复制 config.py.demo 文件粘贴为 config.py"
        )
        return

    # 检查代理是否设置
    if config.PROXY_HOST != "":
        try:
            telnetlib.Telnet(config.PROXY_HOST, config.PROXY_PORT)
        except:
            print("当前设置的 VPN 代理不可用，如不使用数字货币行情，可忽略")

    # 检查 Redis
    try:
        if config.REDIS_HOST != "":
            R = redis.Redis(
                host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True
            )
            R.get("check")
    except:
        print("Redis 连接失败，请检查是否有安装并启动 Redis 服务端，并且配置正确")
        print("Redis 不是必须的，不使用可以忽略")
    # 检查 MySQL
    try:
        if config.DB_TYPE == "mysql":
            pymysql.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PWD,
                database=config.DB_DATABASE,
            )
    except:
        print(
            "MySQL 连接失败，请检查是否安装并运行 MySQL，并且检查配置的 ip、端口、用户名、密码、数据库 是否正确"
        )

    # 检查授权信息
    pyarmor_path = pathlib.Path().cwd() / "src" / "pyarmor_runtime_005445"
    if (pyarmor_path / "pyarmor.rkey").is_file() is False:
        print(" - " * 20)
        print("授权文件不存在")
        print("请联系作者，获取试用授权文件")
        print(f"并将授权文件文件存放在 ：{pyarmor_path} 路径之下")
        print(" - " * 20)
    else:
        with open(pyarmor_path / "pyarmor.rkey", "rb") as fp:
            key_str = fp.read()
            mac_re = r"([0-9a-f]{2}(?::[0-9a-f]{2}){5})"
            key_str = str(key_str)
            match = re.search(mac_re, key_str)
            if match:
                print("授权信息：", match.group(0))

    print("环境OK")


if __name__ == "__main__":
    check_env()
