import datetime
import json
import random
import time

import redis

from . import config
from . import cl

r = None


def Robj():
    global r
    if r is None:
        r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
    return r


def save_byte(key, val):
    """
    保存字节数据到 redis 中
    :param key:
    :param val:
    :return:
    """
    robj = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False)
    robj.set(key, val)


def get_byte(key):
    """
    读取字节数据
    :param key:
    :return:
    """
    robj = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False)
    return robj.get(key)


def strategy_save(key, obj):
    """
    策略回测结果保存
    :param key:
    :param obj:
    :return:
    """
    robj = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False)
    return robj.hset('strategy_back', key, obj)


def strategy_get(key):
    """
    策略回测结果读取
    :param key:
    :return:
    """
    robj = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=False)
    return robj.hget('strategy_back', key)


def strategy_keys():
    """
    获取保存的所有回测结果 key
    :return:
    """
    return Robj().hkeys('strategy_back')


def stock_jh_query():
    """
    股票的机会查询
    :return:
    """
    jhs = []
    h_keys = Robj().hkeys('stock_jh')
    for k in h_keys:
        v = Robj().hget('stock_jh', k)
        if v:
            v = json.loads(v)
            # 时间转换
            v['datetime_str'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(v['datetime']))
            jhs.append(v)
    # 按照 datetime 排序
    jhs.sort(key=lambda j: j['datetime'], reverse=True)
    return jhs


def stock_jh_save(code, name, frequency, jh_type, bi: cl.BI):
    """
    股票的机会保存
    :param code:
    :param name:
    :param frequency:
    :param jh_type:
    :param bi:
    :return: 返回是否之前存在过
    """
    global r
    bi_done = '笔完成' if bi.done else '未完成'
    bi_td = 'TD' if bi.td else '--'

    key = 'stock_code:%s_frequency:%s_jhtype:%s' % (code, frequency, jh_type)
    is_exists = Robj().hexists('stock_jh', key)
    if is_exists:
        ex_val = Robj().hget('stock_jh', key)
        ex_val = json.loads(ex_val)
        if ex_val['bi_done'] == bi_done and ex_val['bi_td'] == bi_td:
            # 没有变化，直接返回
            return is_exists

    val = {
        'code': code,
        'name': name,
        'frequency': frequency,
        'jh_type': jh_type,
        'bi_done': bi_done,
        'bi_td': bi_td,
        'datetime': int(time.time())
    }
    Robj().hset('stock_jh', key, json.dumps(val))

    # 检查超过 24 小时的机会
    if random.randint(0, 100) < 10:
        h_keys = Robj().hkeys('stock_jh')
        for k in h_keys:
            v = Robj().hget('stock_jh', k)
            if v:
                v = json.loads(v)
                if int(time.time()) - int(v['datetime']) > 24 * 60 * 60:
                    Robj().hdel('stock_jh', k)

    return False  # False 意思表示有更新


def currency_jh_query():
    """
    数字货币的机会查询
    :return:
    """
    global r
    jhs = []
    h_keys = Robj().hkeys('currency_jh')
    for k in h_keys:
        v = Robj().hget('currency_jh', k)
        if v:
            v = json.loads(v)
            # 时间转换
            v['datetime_str'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(v['datetime']))
            jhs.append(v)
    # 按照 datetime 排序
    jhs.sort(key=lambda j: j['datetime'], reverse=True)
    return jhs


def currency_jh_save(symbol, frequency, jh_type, bi: cl.BI):
    """
    数字货币的机会保存
    :param symbol:
    :param frequency:
    :param jh_type:
    :param bi:
    :return: 返回是否之前存在过
    """
    global r
    bi_done = '笔完成' if bi.done else '未完成'
    bi_td = 'TD' if bi.td else '--'

    key = 'currency_symbol:%s_frequency:%s_jhtype:%s' % (symbol, frequency, jh_type)
    is_exists = Robj().hexists('currency_jh', key)
    if is_exists:
        ex_val = Robj().hget('currency_jh', key)
        ex_val = json.loads(ex_val)
        if ex_val['bi_done'] == bi_done and ex_val['bi_td'] == bi_td:
            # 没有变化，直接返回
            return is_exists

    val = {
        'symbol': symbol,
        'frequency': frequency,
        'jh_type': jh_type,
        'bi_done': bi_done,
        'bi_td': bi_td,
        'datetime': int(time.time())
    }
    Robj().hset('currency_jh', key, json.dumps(val))

    # 检查超过 24 小时的机会
    if random.randint(0, 100) < 10:
        h_keys = Robj().hkeys('currency_jh')
        for k in h_keys:
            v = Robj().hget('currency_jh', k)
            if v:
                v = json.loads(v)
                if int(time.time()) - int(v['datetime']) > 24 * 60 * 60:
                    Robj().hdel('currency_jh', k)

    return False  # False 意思表示有更新


def currency_position_check_setting_add(symbol, set_settings):
    """
    数字货币持仓监控配置保存
    :param symbol:
    :param set_settings:
    :return:
    """
    '''
        配置信息
        frequency 周期
        fangxiang 方向
        beichi 是否背驰
        td 是否停顿
        bi_done 笔是否完成
        mmds 买卖点
    '''
    key = symbol
    settings = Robj().hget('currency_positions', key)
    if settings is None:
        settings = {}
    else:
        settings = json.loads(settings)
    set_settings['num'] = len(settings)
    set_settings['symbol'] = symbol
    settings[set_settings['num']] = set_settings
    Robj().hset('currency_positions', key, json.dumps(settings))
    return True


def currency_position_check_setting_query(symbol):
    """
    数字货币持仓监控配置读取
    :param symbol:
    :return:
    """
    key = symbol
    settings = Robj().hget('currency_positions', key)
    if settings is None:
        settings = {}
    else:
        settings = json.loads(settings)
    return settings.values()


def currency_position_check_setting_del(symbol, num):
    """
    数字货币持仓监控配置删除
    :param symbol:
    :param num:
    :return:
    """
    key = symbol
    settings = Robj().hget('currency_positions', key)
    if settings is None:
        settings = {}
    else:
        settings = json.loads(settings)
    if str(num) in settings:
        del (settings[str(num)])
        Robj().hset('currency_positions', key, json.dumps(settings))
    return True


def currency_position_check_setting_clear(symbol):
    """
    数字货币持仓检查设置清除
    :param symbol:
    :return:
    """
    Robj().hdel('currency_positions', symbol)
    return True


def currency_open_setting_save(symbol, set_settings):
    """
    数字货币建仓配置保存
    :param symbol:
    :param set_settings:
    :return:
    """
    '''
        配置信息
        symbol 标的
        open_usdt 开仓USDT
        trade_type 交易方向
        leverage 杠杠
        
        frequency 周期
        fangxiang 方向
        beichi 是否背驰
        td 是否停顿
        bi_done 笔是否完成
        mmds 买卖点
    '''
    key = symbol
    settings = Robj().hget('currency_open', key)
    if settings is None:
        settings = {}
    else:
        settings = json.loads(settings)
    set_settings['num'] = len(settings)
    set_settings['symbol'] = symbol
    settings[set_settings['num']] = set_settings
    Robj().hset('currency_open', key, json.dumps(settings))
    return True


def currency_open_setting_del(symbol, num):
    """
    数字货币开仓配置删除
    :param symbol:
    :param num:
    :return:
    """
    key = symbol
    settings = Robj().hget('currency_open', key)
    if settings is None:
        settings = {}
    else:
        settings = json.loads(settings)
    if str(num) in settings:
        del (settings[str(num)])
        Robj().hset('currency_open', key, json.dumps(settings))
    return True


def currency_open_setting_query(symbol=None):
    """
    数字货币建仓监控配置读取
    :param symbol:
    :return:
    """
    if symbol is None:
        keys = Robj().hkeys('currency_open')
        settings = []
        for k in keys:
            v = Robj().hget('currency_open', k)
            v = json.loads(v)
            settings += list(v.values())
        return settings
    else:
        key = symbol
        settings = Robj().hget('currency_open', key)
        if settings is None:
            settings = {}
        else:
            settings = json.loads(settings)
        return list(settings.values())


def currency_open_setting_clear(symbol):
    """
    数字货币建仓检查设置清除
    :param symbol:
    :return:
    """
    Robj().hdel('currency_open', symbol)
    return True


def currency_pos_loss_price_save(symbol, price):
    """
    数字货币持仓止损设置
    :param symbol:
    :param price:
    :return:
    """
    Robj().hset('currency_pos_loss', symbol, price)
    return True


def currency_pos_loss_price_query(symbol):
    """
    数字货币持仓止损查询
    :param symbol:
    :return:
    """
    price = Robj().hget('currency_pos_loss', symbol)
    if price is not None:
        price = float(price)
    else:
        price = 0
    return price


def currency_pos_profit_rate_save(symbol, rate):
    """
    数字货币持仓止盈设置
    :param symbol:
    :param rate:
    :return:
    """
    Robj().hset('currency_pos_profit_rate', symbol, rate)
    return True


def currency_pos_profit_rate_query(symbol):
    """
    数字货币持仓止盈查询
    :param symbol:
    :return:
    """
    rate = Robj().hget('currency_pos_profit_rate', symbol)
    if rate is not None:
        rate = float(rate)
    else:
        rate = 0
    return rate


def currency_order_save(symbol, order):
    """
    记录币种订单信息
    :param symbol:
    :param order:
    :return:
    """
    orders = Robj().hget('currency_orders', symbol)
    if orders is None:
        orders = []
    else:
        orders = json.loads(orders)
    orders.append(order)
    Robj().hset('currency_orders', symbol, json.dumps(orders))
    return True


def currency_order_query(symbol):
    """
    数字货币订单查询
    :param symbol:
    :return:
    """
    orders = Robj().hget('currency_orders', symbol)
    if orders is None:
        orders = []
    else:
        orders = json.loads(orders)
    return orders


def currency_opt_record_save(symbol, info):
    """
    数字货币操盘记录
    :param symbol:
    :param info:
    :return:
    """
    global r
    record = {
        'symbol': symbol,
        'info': info,
        'datetime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    Robj().lpush('currency_opt_records', json.dumps(record))
    return True


def currency_opt_record_query(num=100):
    """
    数字货币操作记录查询
    :return:
    """
    global r
    res = Robj().lrange('currency_opt_records', 0, num)
    records = []
    for _r in res:
        records.append(json.loads(_r))
    return records


def dl_hy_rank_query():
    """
    查询行业动量排行
    :return:
    """
    res = Robj().get('dl_ranks')
    if res is None:
        return {}
    res = json.loads(res)
    return res


def dl_hy_rank_save(ranks):
    """
    行业动量排行保存
    :param ranks:
    :return:
    """
    Robj().set('dl_ranks', json.dumps(ranks))
    return True


def dl_gn_rank_query():
    """
    查询概念动量排行
    :return:
    """
    res = Robj().get('dl_gn_ranks')
    if res is None:
        return {}
    res = json.loads(res)
    return res


def dl_gn_rank_save(ranks):
    """
    概念动量排行保存
    :param ranks:
    :return:
    """
    Robj().set('dl_gn_ranks', json.dumps(ranks))
    return True


def stock_order_save(code, order):
    """
    记录股票交易订单信息
    :param code:
    :param order:
    :return:
    """
    orders = Robj().hget('stock_orders', code)
    if orders is None:
        orders = {}
    else:
        orders = json.loads(orders)
    key = order['datetime']
    orders[key] = order
    Robj().hset('stock_orders', code, json.dumps(orders))
    return True


def stock_order_query(code):
    """
    股票订单查询
    :param code:
    :return:
    """
    orders = Robj().hget('stock_orders', code)
    if orders is None:
        orders = {}
    else:
        orders = json.loads(orders)
    orders = orders.values()
    return orders
