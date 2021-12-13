import base64
import datetime
import hashlib
import hmac
import time
import urllib.parse
import pyttsx3
import requests

from . import config
from . import cl
from . import exchange_futu
from . import exchange_binance
from . import rd


def send_dd_msg(market, msg):
    """
    发送钉钉消息
    :param market:
    :param msg:
    :return:
    """
    dd_info = None
    if market == 'a':
        dd_info = config.DINGDING_KEY_A
    elif market == 'hk':
        dd_info = config.DINGDING_KEY_HK
    elif market == 'currency':
        dd_info = config.DINGDING_KEY_CURRENCY
    else:
        raise Exception('没有配置的钉钉信息')

    url = 'https://oapi.dingtalk.com/robot/send?access_token=%s&timestamp=%s&sign=%s'

    def sign():
        timestamp = str(round(time.time() * 1000))
        secret = dd_info['secret']
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        s = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, s

    t, s = sign()
    url = url % (dd_info['token'], t, s)
    requests.post(url, json={
        'msgtype': 'text',
        'text': {"content": msg},
    })
    return True


def monitoring_code(market: str, code: str, name: str, frequencys: list,
                    check_types: dict = None, is_send_msg: bool = False):
    """
    监控指定股票是否出现指定的信号
    :param market: 市场
    :param code: 代码
    :param name: 名称
    :param frequencys: 检查周期
    :param check_types: 监控项目
    :param is_send_msg: 是否发送消息
    :return:
    """
    if check_types is None:
        check_types = {'beichi': True, 'buy': True, 'sell': True, 'ding': [], 'di': []}

    if market == 'currency':
        exchange = exchange_binance.ExchangeBinance()
    else:
        exchange = exchange_futu.ExchangeFutu()

    klines = {}
    for f in frequencys:
        klines[f] = exchange.klines(code, f)
    cl_datas = cl.batch_cls(code, klines)

    jh_msgs = []
    for cd in cl_datas:
        bis = cd.bis
        frequency = cd.frequency

        check_msgs = {'code': code, 'name': name, 'period': frequency,
                      'date': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                      'bi_done': True, 'beichi': [], 'mmd': []}
        if len(bis) == 0:
            continue

        # 检查背驰和买卖点
        end_bi = bis[-1]
        check_msgs['bi_done'] = '笔完成' if end_bi.done else '笔未完成'

        # 顶 底 分型停顿提醒
        ding_td = end_bi.type == 'up' and end_bi.td
        di_td = end_bi.type == 'down' and end_bi.td

        if check_types['beichi'] and end_bi.qs_beichi:
            jh_msgs.append({
                'type': end_bi.type + ' 趋势背驰',
                'frequency': frequency,
                'bi': end_bi
            })
        if check_types['beichi'] and end_bi.pz_beichi:
            jh_msgs.append({
                'type': end_bi.type + ' 盘整背驰',
                'frequency': frequency,
                'bi': end_bi
            })

        mmd_maps = {
            '1buy': '一买点', '2buy': '二买点', 'l2buy': '类二买点', '3buy': '三买点', 'l3buy': '类三买点',
            '1sell': '一卖点', '2sell': '二卖点', 'l2sell': '类二卖点', '3sell': '三卖点', 'l3sell': '类三卖点',
        }
        for mmd in end_bi.mmds:
            if check_types['buy'] and 'buy' in mmd:
                jh_msgs.append({
                    'type': mmd_maps[mmd],
                    'frequency': frequency,
                    'bi': end_bi
                })
            if check_types['sell'] and 'sell' in mmd:
                jh_msgs.append({
                    'type': mmd_maps[mmd],
                    'frequency': frequency,
                    'bi': end_bi
                })

        say_msg = ''
        if frequency in check_types['ding'] and ding_td:
            jh_msgs.append({
                'type': '顶分型停顿',
                'frequency': frequency,
                'bi': end_bi
            })
        if frequency in check_types['di'] and di_td:
            jh_msgs.append({
                'type': '底分型停顿',
                'frequency': frequency,
                'bi': end_bi
            })

    send_msgs = ""
    for jh in jh_msgs:
        bi_done = '笔完成' if jh['bi'].done else '笔未完成'
        if market in ['a', 'hk']:
            is_exists = rd.stock_jh_save(code, name, jh['frequency'], jh['type'], jh['bi'])
        else:
            is_exists = rd.currency_jh_save(name, jh['frequency'], jh['type'], jh['bi'])

        if is_exists is False and market in ['a', 'hk'] and '停顿' in jh['type']:
            say_msg = '%s %s  %s' % (name, jh['frequency'], jh['type'])
            # 语音播报
            engine = pyttsx3.init()
            engine.say(say_msg)
            engine.runAndWait()

        if is_exists is False and is_send_msg:
            send_msgs += '【%s - %s】触发 %s (%s - TD:%s) \n' % (name, jh['frequency'], jh['type'], bi_done, jh['bi'].td)

    # print('Send_msgs: ', send_msgs)

    if len(send_msgs) > 0:
        send_dd_msg(market, send_msgs)

    return jh_msgs


def convert_stock_order_by_frequency(orders, frequency):
    """
    订单专用转换器
    :param orders:
    :param frequency:
    :return:
    """
    new_orders = []
    for o in orders:
        if isinstance(o['datetime'], str):
            dt = datetime.datetime.strptime(o['datetime'], '%Y-%m-%d %H:%M:%S')
        else:
            dt = o['datetime']
        dt_time = int(time.mktime(dt.timetuple()))
        seconds = 0
        if frequency == 'd':
            seconds = 24 * 60 * 60
        elif frequency == '120m':
            seconds = 2 * 60 * 60
        elif frequency == '60m':
            seconds = 60 * 60
        elif frequency == '30m':
            seconds = 30 * 60
        elif frequency == '15m':
            seconds = 15 * 60
        elif frequency == '5m':
            seconds = 5 * 60
        elif frequency == '1m':
            seconds = 1 * 60
        if seconds == 0:
            return new_orders
        dt_time -= dt_time % seconds

        if frequency in ['d']:
            dt_time -= 8 * 60 * 60
        if 'm' in frequency:
            dt_time += seconds

        if frequency == '60m':
            if (dt.hour == 9) or (dt.hour == 10 and dt.minute <= 30):
                dt_time = datetime.datetime.timestamp(
                    datetime.datetime.strptime(dt.strftime('%Y-%m-%d 10:30:00'), '%Y-%m-%d %H:%M:%S'))
            elif (dt.hour == 10 and dt.minute >= 30) or (dt.hour == 11):
                dt_time = datetime.datetime.timestamp(
                    datetime.datetime.strptime(dt.strftime('%Y-%m-%d 11:30:00'), '%Y-%m-%d %H:%M:%S'))
        if frequency == '120m':
            if dt.hour == 9 or dt.hour == 10 or dt.hour == 11:
                dt_time = datetime.datetime.timestamp(
                    datetime.datetime.strptime(dt.strftime('%Y-%m-%d 11:30:00'), '%Y-%m-%d %H:%M:%S'))
            elif dt.hour >= 13:
                dt_time = datetime.datetime.timestamp(
                    datetime.datetime.strptime(dt.strftime('%Y-%m-%d 15:00:00'), '%Y-%m-%d %H:%M:%S'))
        dt_time = datetime.datetime.fromtimestamp(dt_time)
        dt_time = dt_time.strftime('%Y-%m-%d %H:%M:%S')
        new_orders.append({
            'datetime': dt_time,
            'type': o['type'],
            'price': o['price'],
            'amount': o['amount'],
            'info': '' if 'info' not in o else o['info'],
        })
    return new_orders


def convert_currency_order_by_frequency(orders, frequency):
    """
    数字货币专用订单转换器
    :param orders:
    :param frequency:
    :return:
    """
    new_orders = []
    for o in orders:
        if isinstance(o['datetime'], str):
            dt = datetime.datetime.strptime(o['datetime'], '%Y-%m-%d %H:%M:%S')
        else:
            dt = o['datetime']
        dt_time = int(time.mktime(dt.timetuple()))
        seconds = 0
        if frequency == 'd':
            seconds = 24 * 60 * 60
        elif frequency == '4h':
            seconds = 4 * 60 * 60
        elif frequency == '60m':
            seconds = 60 * 60
        elif frequency == '30m':
            seconds = 30 * 60
        elif frequency == '15m':
            seconds = 15 * 60
        elif frequency == '5m':
            seconds = 5 * 60
        elif frequency == '1m':
            seconds = 1 * 60
        dt_time -= dt_time % seconds
        dt_time = datetime.datetime.fromtimestamp(dt_time)
        dt_time = dt_time.strftime('%Y-%m-%d %H:%M:%S')
        new_orders.append({
            'datetime': dt_time,
            'type': o['type'],
            'price': o['price'],
            'amount': o['amount'],
            'info': '' if 'info' not in o else o['info'],
        })
    return new_orders


def time_to_str(_t, _format='%Y-%m-%d %H:%M:%S'):
    """
    时间戳转字符串
    :param _t:
    :param _format:
    :return:
    """
    timeArray = time.localtime(int(_t))
    return time.strftime(_format, timeArray)


def time_to_int(_t, _format='%Y-%m-%d %H:%M:%S'):
    """
    字符串转时间戳
    :param _t:
    :param _format:
    :return:
    """
    return int(time.mktime(time.strptime(_t, _format)))


def str_to_time(_s, _format='%Y-%m-%d %H:%M:%S'):
    """
    字符串转datetime类型
    :param _s:
    :param _format:
    :return:
    """
    return datetime.datetime.strptime(_s, '%Y-%m-%d %H:%M:%S')
