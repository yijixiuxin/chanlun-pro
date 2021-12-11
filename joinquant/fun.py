from jqdata import *
import cl
import datetime


# 获取市值前几的股票列表
def stock_market_cap(num):
    df = get_fundamentals(query(valuation.code, valuation.market_cap).filter(
        valuation.market_cap > num
    ).order_by(
        # 按市值降序排列
        valuation.market_cap.desc()
    ), date=None)
    stocks = []
    for d in df.iterrows():
        s = get_security_info(d[1]['code'])
        stocks.append({'code': d[1]['code'], 'name': s.display_name})
    return stocks


# 获取指定周期的缠论数据
def get_cl_datas(code, frequencys):
    klines = {}
    for f in frequencys:
        _k = get_bars(code, 1000, unit=f, fields=['date', 'open', 'high', 'low', 'close', 'volume'], include_now=True,
                      end_dt=None, df=True)
        _k['code'] = code
        _k = _k[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
        klines[f] = _k
    cl_datas = cl.batch_cls(code, klines)
    return cl_datas


def find_buy_point(cd, buy_types):
    if len(cd.bis) == 0:
        return None
    bi = cd.bis[-1]
    if len(set(buy_types) & set(bi.mmds)) > 0:
        return ' 买点 : %s (%s)' % ('/'.join(bi.mmds), '笔完成' if bi.done else '未完成')
    return None


# 格式化代码与富途的统一
def reformat_code(code):
    symbol = code[0:6]
    exchange = code[-5:]
    if exchange == '.XSHG':
        return 'SH.' + symbol
    else:
        return 'SZ.' + symbol


def stock_names(codes):
    names = []
    for code in codes:
        stock = get_security_info(code)
        names.append(stock.display_name)
    return names


# 获取股票行业信息
def stock_hys(code):
    # 股票的行业信息
    stock_industrys = get_industry(security=[code])
    hy = stock_industrys[code]
    if 'jq_l2' not in hy:
        return '--'
    return hy['jq_l2']['industry_name']


# 获取股票概念信息
def stock_gns(code):
    # 股票的概念信息
    stock_concepts = get_concept(code, date=datetime.date.today())
    gn = ''
    for co in stock_concepts[code]['jq_concept']:
        gn += co['concept_name'] + ' / '
    return gn[0:-3]


def stock_jj_bx_share_ok(code):
    # 查询前十大流通股中，基金占比，需要大于 2%
    lt_top10 = finance.run_query(query(finance.STK_SHAREHOLDER_FLOATING_TOP10).filter(
        finance.STK_SHAREHOLDER_FLOATING_TOP10.code == code).order_by(
        finance.STK_SHAREHOLDER_FLOATING_TOP10.end_date.desc()).limit(10))
    jj_share_ratio = 0
    for lt in lt_top10.iterrows():
        shareholder_class = lt[1]['shareholder_class']
        share_ratio = lt[1]['share_ratio']
        if shareholder_class == '证券投资基金':
            jj_share_ratio += share_ratio
    if jj_share_ratio >= 2:
        return True

    # 查询北向持股大于 3000万
    if 'XSHG' in code:
        link_id = 310001
    else:
        link_id = 310002
    bx = finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.link_id == link_id,
                                                                  finance.STK_HK_HOLD_INFO.code == code).order_by(
        finance.STK_HK_HOLD_INFO.day.desc()).limit(1))
    price = get_ticks(code, end_dt=datetime.datetime.now(), fields=['current'], count=1)
    price = price[0][0]
    share_balance = 0
    if len(bx) == 1:
        share_balance = bx.iloc[0]['share_number'] * price
    if share_balance >= 30000000:
        return True
    return False