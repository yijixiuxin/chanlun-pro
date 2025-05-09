# coding: utf-8
# see https://github.com/rainx/pytdx/issues/38 IP寻优的简单办法
# by yutianst

import datetime

from pytdx.exhq import TdxExHq_API
from pytdx.hq import TdxHq_API

stock_ip = [
    # 旧有的数据源
    # {"ip": "106.120.74.86", "port": 7711, "name": "北京行情主站1"},
    # {"ip": "113.105.73.88", "port": 7709, "name": "深圳行情主站"},
    # {"ip": "113.105.73.88", "port": 7711, "name": "深圳行情主站"},
    # {"ip": "114.80.80.222", "port": 7711, "name": "上海行情主站"},
    # {"ip": "117.184.140.156", "port": 7711, "name": "移动行情主站"},
    # {"ip": "119.147.171.206", "port": 443, "name": "广州行情主站"},
    # {"ip": "119.147.171.206", "port": 80, "name": "广州行情主站"},
    # {"ip": "218.108.50.178", "port": 7711, "name": "杭州行情主站"},
    # {"ip": "221.194.181.176", "port": 7711, "name": "北京行情主站2"},
    # {"ip": "106.120.74.86", "port": 7709},
    # {"ip": "112.95.140.74", "port": 7709},
    # {"ip": "112.95.140.92", "port": 7709},
    # {"ip": "112.95.140.93", "port": 7709},
    # {"ip": "113.05.73.88", "port": 7709},
    # {"ip": "114.67.61.70", "port": 7709},
    # {"ip": "114.80.149.19", "port": 7709},
    # {"ip": "114.80.149.22", "port": 7709},
    # {"ip": "114.80.149.84", "port": 7709},
    # {"ip": "114.80.80.222", "port": 7709},
    # {"ip": "115.238.56.198", "port": 7709},
    # {"ip": "115.238.90.165", "port": 7709},
    # {"ip": "117.184.140.156", "port": 7709},
    # {"ip": "119.147.164.60", "port": 7709},
    # {"ip": "119.147.171.206", "port": 7709},
    # {"ip": "119.29.51.30", "port": 7709},
    # {"ip": "121.14.104.70", "port": 7709},
    # {"ip": "121.14.104.72", "port": 7709},
    # {"ip": "121.14.110.194", "port": 7709},
    # {"ip": "121.14.2.7", "port": 7709},
    # {"ip": "123.125.108.23", "port": 7709},
    # {"ip": "123.125.108.24", "port": 7709},
    # {"ip": "124.160.88.183", "port": 7709},
    # {"ip": "180.153.18.17", "port": 7709},
    {"ip": "180.153.18.170", "port": 7709},
    # {"ip": "180.153.18.171", "port": 7709},
    # {"ip": "180.153.39.51", "port": 7709},
    # {"ip": "218.108.47.69", "port": 7709},
    # {"ip": "218.108.50.178", "port": 7709},
    # {"ip": "218.108.98.244", "port": 7709},
    {"ip": "218.75.126.9", "port": 7709},
    # {"ip": "218.9.148.108", "port": 7709},
    # {"ip": "221.194.181.176", "port": 7709},
    # {"ip": "59.173.18.69", "port": 7709},
    {"ip": "60.12.136.250", "port": 7709},
    {"ip": "60.191.117.167", "port": 7709},
    # {"ip": "60.28.29.69", "port": 7709},
    # {"ip": "61.135.142.73", "port": 7709},
    # {"ip": "61.135.142.88", "port": 7709},
    # {"ip": "61.152.107.168", "port": 7721},
    # {"ip": "61.152.249.56", "port": 7709},
    # {"ip": "61.153.144.179", "port": 7709},
    # {"ip": "61.153.209.138", "port": 7709},
    # {"ip": "61.153.209.139", "port": 7709},
    # {"ip": "hq.cjis.cn", "port": 7709},
    # {"ip": "hq1.daton.com.cn", "port": 7709},
    # {"ip": "jstdx.gtjas.com", "port": 7709},
    {"ip": "shtdx.gtjas.com", "port": 7709},
    {"ip": "sztdx.gtjas.com", "port": 7709},
    # {"ip": "113.105.142.162", "port": 7721},
    # {"ip": "23.129.245.199", "port": 7721},
    # 新的数据源
    {"ip": "110.41.147.114", "port": 7709, "name": "通达信深圳双线主站1"},
    {"ip": "110.41.2.72", "port": 7709, "name": "通达信深圳双线主站2"},
    {"ip": "110.41.4.4", "port": 7709, "name": "通达信深圳双线主站3"},
    {"ip": "175.178.112.197", "port": 7709, "name": "通达信深圳双线主站4"},
    {"ip": "175.178.128.227", "port": 7709, "name": "通达信深圳双线主站5"},
    {"ip": "110.41.154.219", "port": 7709, "name": "通达信深圳双线主站6"},
    {"ip": "124.70.176.52", "port": 7709, "name": "通达信上海双线主站1"},
    {"ip": "122.51.120.217", "port": 7709, "name": "通达信上海双线主站2"},
    {"ip": "123.60.186.45", "port": 7709, "name": "通达信上海双线主站3"},
    {"ip": "123.60.164.122", "port": 7709, "name": "通达信上海双线主站4"},
    {"ip": "111.229.247.189", "port": 7709, "name": "通达信上海双线主站5"},
    {"ip": "124.70.199.56", "port": 7709, "name": "通达信上海双线主站6"},
    {"ip": "121.36.54.217", "port": 7709, "name": "通达信北京双线主站1"},
    {"ip": "121.36.81.195", "port": 7709, "name": "通达信北京双线主站2"},
    {"ip": "123.249.15.60", "port": 7709, "name": "通达信北京双线主站3"},
    {"ip": "124.71.85.110", "port": 7709, "name": "通达信广州双线主站1"},
    {"ip": "139.9.51.18", "port": 7709, "name": "通达信广州双线主站2"},
    {"ip": "139.159.239.163", "port": 7709, "name": "通达信广州双线主站3"},
    {"ip": "122.51.232.182", "port": 7709, "name": "通达信上海双线主站7"},
    {"ip": "118.25.98.114", "port": 7709, "name": "通达信上海双线主站8"},
    {"ip": "121.36.225.169", "port": 7709, "name": "通达信上海双线主站9"},
    {"ip": "123.60.70.228", "port": 7709, "name": "通达信上海双线主站10"},
    {"ip": "123.60.73.44", "port": 7709, "name": "通达信上海双线主站11"},
    {"ip": "124.70.133.119", "port": 7709, "name": "通达信上海双线主站12"},
    {"ip": "124.71.187.72", "port": 7709, "name": "通达信上海双线主站13"},
    {"ip": "124.71.187.122", "port": 7709, "name": "通达信上海双线主站14"},
    # {"ip": "119.97.185.59", "port": 7709, "name": "通达信武汉电信主站1"},
    {"ip": "129.204.230.128", "port": 7709, "name": "通达信深圳双线主站7"},
    {"ip": "124.70.75.113", "port": 7709, "name": "通达信北京双线主站4"},
    {"ip": "124.71.9.153", "port": 7709, "name": "通达信广州双线主站4"},
    {"ip": "123.60.84.66", "port": 7709, "name": "通达信上海双线主站15"},
    {"ip": "111.230.186.52", "port": 7709, "name": "通达信深圳双线主站8"},
    {"ip": "120.46.186.223", "port": 7709, "name": "通达信北京双线主站5"},
    {"ip": "124.70.22.210", "port": 7709, "name": "通达信北京双线主站6"},
    {"ip": "139.9.133.247", "port": 7709, "name": "通达信北京双线主站7"},
    {"ip": "116.205.163.254", "port": 7709, "name": "通达信广州双线主站5"},
    {"ip": "116.205.171.132", "port": 7709, "name": "通达信广州双线主站6"},
    {"ip": "116.205.183.150", "port": 7709, "name": "通达信广州双线主站7"},
]

future_ip = [
    # 旧有的数据源
    # {"ip": "106.14.95.149", "port": 7727, "name": "扩展市场上海双线"},
    {"ip": "112.74.214.43", "port": 7727, "name": "扩展市场深圳双线1"},
    # {"ip": "119.147.86.171", "port": 7727, "name": "扩展市场深圳主站"},
    # {"ip": "119.97.185.5", "port": 7727, "name": "扩展市场武汉主站1"},
    # {"ip": "120.24.0.77", "port": 7727, "name": "扩展市场深圳双线2"},
    # {"ip": "124.74.236.94", "port": 7721},
    # {"ip": "202.103.36.71", "port": 443, "name": "扩展市场武汉主站2"},
    # {"ip": "47.92.127.181", "port": 7727, "name": "扩展市场北京主站"},
    # {"ip": "59.175.238.38", "port": 7727, "name": "扩展市场武汉主站3"},
    # {"ip": "61.152.107.141", "port": 7727, "name": "扩展市场上海主站1"},
    # {"ip": "61.152.107.171", "port": 7727, "name": "扩展市场上海主站2"},
    # {"ip": "47.107.75.159", "port": 7727, "name": "扩展市场深圳双线3"},
    # 新的数据源
    {"ip": "120.25.218.6", "port": 7727, "name": "扩展市场深圳双线2"},
    {"ip": "43.139.173.246", "port": 7727, "name": "扩展市场深圳双线3"},
    {"ip": "159.75.90.107", "port": 7727, "name": "扩展市场深圳双线4"},
    {"ip": "106.52.170.195", "port": 7727, "name": "扩展市场深圳双线5"},
    {"ip": "139.9.191.175", "port": 7727, "name": "扩展市场广州双线3"},
    {"ip": "175.24.47.69", "port": 7727, "name": "扩展市场上海双线7"},
    {"ip": "150.158.9.199", "port": 7727, "name": "扩展市场上海双线1"},
    {"ip": "150.158.20.127", "port": 7727, "name": "扩展市场上海双线2"},
    {"ip": "49.235.119.116", "port": 7727, "name": "扩展市场上海双线3"},
    {"ip": "49.234.13.160", "port": 7727, "name": "扩展市场上海双线4"},
    {"ip": "116.205.143.214", "port": 7727, "name": "扩展市场广州双线1"},
    {"ip": "124.71.223.19", "port": 7727, "name": "扩展市场广州双线2"},
    {"ip": "113.45.175.47", "port": 7727, "name": "扩展市场广州双线4"},
    {"ip": "123.60.173.210", "port": 7727, "name": "扩展市场上海双线5"},
    {"ip": "118.89.69.202", "port": 7727, "name": "扩展市场上海双线6"},
]


def ping(ip, port=7709, type_="stock"):
    api = TdxHq_API()
    apix = TdxExHq_API()
    __time1 = datetime.datetime.now()
    try:
        if type_ in ["stock"]:
            with api.connect(ip, port, time_out=0.7):
                res = api.get_security_list(0, 1)
                if res is not None:
                    if len(res) > 800:
                        print("GOOD RESPONSE {}".format(ip))
                        return datetime.datetime.now() - __time1
                    else:
                        print("BAD RESPONSE {}".format(ip))
                        return datetime.timedelta(9, 9, 0)

                else:
                    print("BAD RESPONSE {}".format(ip))
                    return datetime.timedelta(9, 9, 0)
        elif type_ in ["future"]:
            with apix.connect(ip, port, time_out=0.7):
                res = apix.get_instrument_count()
                if res is not None:
                    if res > 20000:
                        print("GOOD RESPONSE {}".format(ip))
                        return datetime.datetime.now() - __time1
                    else:
                        print("️Bad FUTUREIP REPSONSE {}".format(ip))
                        return datetime.timedelta(9, 9, 0)
                else:
                    print("️Bad FUTUREIP REPSONSE {}".format(ip))
                    return datetime.timedelta(9, 9, 0)
    except Exception as e:
        if isinstance(e, TypeError):
            pass
        else:
            print("BAD RESPONSE {}".format(ip))
        return datetime.timedelta(9, 9, 0)


def select_best_ip(_type="stock"):
    """目前这里给的是单线程的选优, 如果需要多进程的选优/ 最优ip缓存 可以参考
    https://github.com/QUANTAXIS/QUANTAXIS/blob/master/QUANTAXIS/QAFetch/QATdx.py#L106


    Keyword Arguments:
        _type {str} -- [description] (default: {'stock'})

    Returns:
        [type] -- [description]
    """

    ip_list = stock_ip if _type == "stock" else future_ip

    data = [ping(x["ip"], x["port"], _type) for x in ip_list]
    results = []
    for i in range(len(data)):
        # 删除ping不通的数据
        if data[i] < datetime.timedelta(0, 9, 0):
            results.append((data[i], ip_list[i]))

    # 按照ping值从小大大排序
    results = [x[1] for x in sorted(results, key=lambda x: x[0])]

    return results[0]


if __name__ == "__main__":
    print(len(stock_ip))
    ip = select_best_ip("stock")
    print(ip)

    print(len(future_ip))
    ip = select_best_ip("future")
    print(ip)
