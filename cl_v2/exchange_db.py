import MySQLdb
import pandas as pd

from . import config
from . import exchange


class ExchangeDB(exchange.Exchange):
    """
    数据库行情
    """

    def __init__(self, market):
        """
        :param market: 市场 a A股市场，hk 香港市场 currency 数字货币市场
        """
        self.market = market
        self.exchange = None

        self.db = MySQLdb.connect(config.DB_HOST, config.DB_USER, config.DB_PWD, config.DB_DATABASE, charset='utf8')
        self.cursor = self.db.cursor()

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
                self.cursor.execute(create_sql)
            except:
                pass

    def query_last_datetime(self, code, frequency) -> str:
        """
        查询交易对儿最后更新时间
        :param frequency:
        :param code:
        :return:
        """
        table = self.__table(code)
        self.cursor.execute("select dt from %s where f = '%s' order by dt desc limit 1" % (table, frequency))
        dt = self.cursor.fetchone()
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
        table = self.__table(code)
        for kline in klines.iterrows():
            k = kline[1]
            sql = "replace into %s(dt, f, h, l, o, c, v) values ('%s', '%s', %f, %f, %f, %f, %f)" % (
            table, k['date'], frequency, k['high'], k['low'], k['open'], k['close'], k['volume'])
            self.cursor.execute(sql)
            self.db.commit()
        return

    def klines(self, code: str, frequency: str,
               start_date: str = None, end_date: str = None,
               args: dict = {}) -> [pd.DataFrame, None]:
        table = self.__table(code)
        sql = "select dt, f, h, l, o, c, v from %s where f='%s'" % (table, frequency)
        if start_date is not None:
            sql += " and dt >= '%s'" % start_date
        if end_date is not None:
            sql += " and dt <= '%s'" % end_date
        sql += ' order by dt desc'
        if args['limit'] is not None:
            sql += ' limit %s' % args['limit']
        self.cursor.execute(sql)
        klines = self.cursor.fetchall()
        kline_pd = pd.DataFrame(klines, columns=['date', 'f', 'high', 'low', 'open', 'close', 'volume'])
        kline_pd = kline_pd.iloc[::-1]
        kline_pd['code'] = code
        kline_pd['date'] = kline_pd['date'].map(lambda d: d.strftime('%Y-%m-%d %H:%M:%S'))
        return kline_pd[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
