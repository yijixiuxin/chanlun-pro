import datetime
from vnpy.event.engine import EventEngine
from vnpy_ctabacktester.engine import MainEngine
from vnpy_datamanager.engine import ManagerEngine, Exchange, Interval

mainE = MainEngine()
eventE = EventEngine()

me = ManagerEngine(mainE, eventE)

# 股票数据下载
# for code in ['000001', '000858', '002603', '000002', '002594']:
#     _l = me.download_bar_data(
#         code, Exchange.SZSE, Interval.MINUTE.value, start=datetime.datetime(year=2019, month=1, day=1)
#     )
#     print(code, _l)

# # 期货行情下载
for code in ['ag2206', 'al2205', 'au2206', 'bu2206', 'cu2206', 'fu2209', 'hc2210', 'ni2205', 'pb2205', 'rb2210',
             'ru2209', 'sn2205', 'sp2209', 'ss2206', 'wr2210', 'zn2206']:
    try:
        _l = me.download_bar_data(
            code, Exchange.SHFE, Interval.MINUTE.value, start=datetime.datetime(year=2019, month=1, day=1)
        )
        print(code, _l)
    except Exception as e:
        print('Error : ', e)

for code in ['SA209', 'FG209', 'TA209', 'AP210', 'OI209', 'MA209', 'CF209', 'SR209', 'UR209', 'SF209',
             'RM209', 'SM209', 'PF206', 'PK210']:
    try:
        _l = me.download_bar_data(
            code, Exchange.CZCE, Interval.MINUTE.value, start=datetime.datetime(year=2019, month=1, day=1)
        )
        print(code, _l)
    except Exception as e:
        print('Error : ', e)

for code in ['IC2205', 'IF2205', 'IH2205', 'T2206', 'TF2206', 'TS2206']:
    try:
        _l = me.download_bar_data(
            code, Exchange.CFFEX, Interval.MINUTE.value, start=datetime.datetime(year=2019, month=1, day=1)
        )
        print(code, _l)
    except Exception as e:
        print('Error : ', e)

for code in ['lu2207', 'nr2206', 'sc2206']:
    try:
        _l = me.download_bar_data(
            code, Exchange.INE, Interval.MINUTE.value, start=datetime.datetime(year=2019, month=1, day=1)
        )
        print(code, _l)
    except Exception as e:
        print('Error : ', e)

for code in ['p2209', 'y2209', 'i2209', 'm2209', 'v2209', 'pp2209', 'eg2209', 'pg2206', 'l2209', 'c2209', 'j2209']:
    try:
        _l = me.download_bar_data(
            code, Exchange.DCE, Interval.MINUTE.value, start=datetime.datetime(year=2019, month=1, day=1)
        )
        print(code, _l)
    except Exception as e:
        print('Error : ', e)

for code in ['SA209', 'FG209', 'TA209', 'AP210', 'OI209', 'MA209', 'CF209', 'SR209', 'UR209', 'SF209']:
    try:
        _l = me.download_bar_data(
            code, Exchange.CZCE, Interval.MINUTE.value, start=datetime.datetime(year=2019, month=1, day=1)
        )
        print(code, _l)
    except Exception as e:
        print('Error : ', e)

exit(0)
