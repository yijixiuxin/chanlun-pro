"""
检查当前环境是否OK
"""
import os
import sys
import telnetlib

import pymysql
import redis


def check_env():
    # 检查 Python 版本
    version = f'{sys.version_info[0]}.{sys.version_info[1]}'
    print(f'当前Python版本：{version}')
    allow_version = ['3.7', '3.8', '3.9', '3.10']
    if version not in allow_version:
        print(f'当前Python不在支持的列表中：{allow_version}')
        return
    if version != '3.10':
        print(f'建议将 Python 版本升级到 3.10，以获得更好的体验')

    # 检查 环境变量是否设置正确
    try:
        from chanlun import config
    except:
        print('无法导入 chanlun 模块，环境变量未设置或设置错误')
        print(f'当前的环境变量如下：{sys.path}')
        print(f'需要当 PYTHONPATH 环境变量设置为 {os.getcwd()}\src 目录')
        return

    # 检查代理是否设置
    if config.PROXY_HOST != '':
        try:
            telnetlib.Telnet(config.PROXY_HOST, config.PROXY_PORT)
        except:
            print('当前设置的 VPN 代理不可用，如不使用数字货币行情，可忽略')

    # 检查 Redis
    try:
        R = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
        R.get('check')
    except:
        print('Redis 连接失败，请检查是否有安装并启动 Redis 服务端，并且配置正确')
        return
    # 检查 MySQL
    try:
        pymysql.connect(
            host=config.DB_HOST, port=config.DB_PORT,
            user=config.DB_USER, password=config.DB_PWD,
            database=config.DB_DATABASE
        )
    except:
        print('MySQL 连接失败，请检查是否安装并运行 MySQL，并且检查配置的 ip、端口、用户名、密码、数据库 是否正确')

    print('环境OK')


if __name__ == '__main__':
    check_env()
