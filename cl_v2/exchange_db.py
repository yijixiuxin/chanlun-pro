import pymysql
import pandas as pd
import datetime
from dbutils.pooled_db import PooledDB

from . import config
from . import exchange


class ExchangeDB(exchange.Exchange):
    """
    数据库行情
    """

    pool_db = PooledDB(pymysql,
                       mincached=5, maxcached=10, maxshared=10, maxconnections=20, blocking=True,
                       host=config.DB_HOST,
                       port=config.DB_PORT,
                       user=config.DB_USER,
                       passwd=config.DB_PWD,
                       db=config.DB_DATABASE, charset='utf8')

    def __init__(self, market):
        """
        :param market: 市场 a A股市场，hk 香港市场 currency 数字货币市场
        """
        self.market = market
        self.exchange = None

    def __table(self, code):
        """
        根据 code  获取对应的数据表名称
        :param code:
        :return:
        """
        if self.market == 'hk' or self.market == 'a':
            return 'stock_klines_' + code.replace('.', '_').lower()
        elif self.market == 'currency':
            return 'futures_klines_' + code.replace('/', '_').lower()

    def create_tables(self, codes):
        """
        批量创建表
        :return:
        """
        db = self.pool_db.connection()
        cursor = db.cursor()
        for code in codes:
            table = self.__table(code)
            try:
                create_sql = """
                    create table %s (
                        dt DATETIME not null,
                        f VARCHAR(5) not null,
                        h decimal(20,8) not null,
                        l decimal(20, 8) not null,
                        o decimal(20, 8) not null,
                        c decimal(20, 8) not null,
                        v decimal(20, 8) not null,
                        UNIQUE INDEX dt_f (dt, f)
                    )
                """ % table
                cursor.execute(create_sql)
            except:
                pass
        cursor.close()
        db.close()

    def query_last_datetime(self, code, frequency) -> str:
        """
        查询交易对儿最后更新时间
        :param frequency:
        :param code:
        :return:
        """
        db = self.pool_db.connection()
        cursor = db.cursor()
        table = self.__table(code)
        cursor.execute("select dt from %s where f = '%s' order by dt desc limit 1" % (table, frequency))
        dt = cursor.fetchone()
        cursor.close()
        db.close()
        if dt is not None:
            return dt[0].strftime('%Y-%m-%d %H:%M:%S')
        return None

    def insert_klines(self, code, frequency, klines):
        """
        批量添加交易对儿Kline数据
        :param code:
        :param frequency
        :param klines:
        :return:
        """
        db = self.pool_db.connection()
        cursor = db.cursor()
        table = self.__table(code)
        for kline in klines.iterrows():
            k = kline[1]
            sql = "replace into %s(dt, f, h, l, o, c, v) values ('%s', '%s', %f, %f, %f, %f, %f)" % (
                table, k['date'].strftime('%Y-%m-%d %H:%M:%S'), frequency, k['high'], k['low'], k['open'], k['close'],
                k['volume'])
            cursor.execute(sql)
        db.commit()
        return

    def klines(self, code: str, frequency: str,
               start_date: str = None, end_date: str = None,
               args: dict = {}) -> [pd.DataFrame, None]:

        if 'limit' not in args:
            args['limit'] = 1000
        table = self.__table(code)
        sql = "select dt, f, h, l, o, c, v from %s where f='%s'" % (table, frequency)
        if start_date is not None:
            sql += " and dt >= '%s'" % start_date
        if end_date is not None:
            sql += " and dt <= '%s'" % end_date
        sql += ' order by dt desc'
        if args['limit'] is not None:
            sql += ' limit %s' % args['limit']

        db = self.pool_db.connection()
        cursor = db.cursor()
        cursor.execute(sql)
        klines = cursor.fetchall()
        cursor.close()
        db.close()
        kline_pd = pd.DataFrame(klines, columns=['date', 'f', 'high', 'low', 'open', 'close', 'volume'])
        kline_pd = kline_pd.iloc[::-1]
        kline_pd['code'] = code
        kline_pd['date'] = kline_pd['date'].map(lambda d: d.to_pydatetime())
        return kline_pd[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
