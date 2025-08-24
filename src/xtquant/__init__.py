#coding: utf-8

__version__ = "xtquant"


def check_for_update(package_name):
    import requests
    from pkg_resources import get_distribution
    # 获取当前安装的版本
    current_version = get_distribution(package_name).version
    # 查询PyPI的API获取最新版本信息
    response = requests.get(f"https://pypi.org/pypi/{package_name}/json", timeout = 10)
    if response.status_code == 200:
        latest_version = response.json()['info']['version']
        if current_version != latest_version:
            print(f"xtquant{latest_version}已经发布,前往 http://dict.thinktrader.net/nativeApi/download_xtquant.html 查看更新说明\n")
        else:
            print("xtquant文档地址：http://dict.thinktrader.net/nativeApi/start_now.html")
    else:
        pass


try:
    check_for_update("xtquant")
except:
    pass

