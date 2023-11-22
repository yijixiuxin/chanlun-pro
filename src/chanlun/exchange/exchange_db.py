import contextlib
import datetime
from typing import List, Dict

import pandas as pd
import pymysql
import pytz
from dbutils.pooled_db import PooledDB

from chanlun import config
from chanlun.exchange.exchange import (
    Exchange,
    convert_futures_kline_frequency,
    convert_stock_kline_frequency,
    convert_us_kline_frequency,
    convert_currency_kline_frequency,
    Tick,
)

if config.DB_HOST != "":
    g_pool_db = PooledDB(
        pymysql,
        mincached=1,
        maxcached=3,
        maxshared=3,
        maxconnections=3,
        blocking=True,
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        passwd=config.DB_PWD,
        db=config.DB_DATABASE,
        charset="utf8",
    )
else:
    g_pool_db = None


class ExchangeDB(Exchange):
    """
    数据库行情
    """

    def __init__(self, market):
        """
        :param market: 市场 a A股市场 hk 香港市场 us 美股市场 currency 数字货币市场  futures 期货市场
        """
        self.market = market
        self.exchange = None
        self.online_ex = None

        # 设置时区
        self.tz = pytz.timezone("Asia/Shanghai")
        if self.market == "us":
            self.tz = pytz.timezone("US/Eastern")

    def default_code(self):
        if self.market == "a":
            return "SH.000001"
        elif self.market == "hk":
            return "HK.00700"
        elif self.market == "futures":
            return "KQ.m@SHFE.rb"
        elif self.market == "us":
            return "AAPL"
        elif self.market == "currency":
            return "BTC/USDT"
        return ""

    def support_frequencys(self):
        if self.market == "a":
            return {
                "y": "Y",
                "m": "M",
                "w": "W",
                "d": "D",
                "120m": "120m",
                "60m": "60m",
                "30m": "30m",
                "15m": "15m",
                "10m": "10m",
                "5m": "5m",
            }
        elif self.market == "hk":
            return {
                "y": "Y",
                "q": "Q",
                "m": "M",
                "w": "W",
                "d": "D",
                "60m": "60m",
                "30m": "30m",
                "15m": "15m",
                "5m": "5m",
            }
        elif self.market == "futures":
            return {
                "w": "W",
                "d": "D",
                "120m": "2H",
                "60m": "1H",
                "30m": "30m",
                "15m": "15m",
                "10m": "10m",
                "5m": "5m",
                "1m": "1m",
            }
        elif self.market == "us":
            return {
                "w": "Week",
                "d": "Day",
                "60m": "60m",
                "30m": "30m",
                "10m": "10m",
                "15m": "15m",
                "5m": "5m",
            }
        elif self.market == "currency":
            return {
                "w": "Week",
                "d": "Day",
                "4h": "4H",
                "60m": "1H",
                "30m": "30m",
                "15m": "15m",
                "10m": "5m",
                "5m": "5m",
                "3m": "3m",
                "2m": "2m",
                "1m": "1m",
            }
        return ["d", "30m", "5m"]

    def table(self, code):
        """
        根据 code  获取对应的数据表名称
        :param code:
        :return:
        """
        if self.market == "hk":
            return "stock_klines_" + code.replace(".", "_").lower()
        elif self.market == "a":
            return "a_stock_klines_" + code.replace(".", "_").lower()[:7]
        elif self.market == "us":
            return "us_klines_" + code.lower()[0]
        elif self.market == "currency":
            return "currency_klines_" + code.replace("/", "_").lower()
        elif self.market == "futures":
            return "futures_klines_" + code.replace(".", "_").replace("@", "_").lower()

    def create_tables(self, codes):
        """
        批量创建表
        :return:
        """
        global g_pool_db

        # 沪深 A 股的建表语句，增加 code，避免创建太多表
        code_sql = """
        create table %s (
                        code VARCHAR(12) not null,
                        dt DATETIME not null,
                        f VARCHAR(5) not null,
                        h decimal(20,8) not null,
                        l decimal(20, 8) not null,
                        o decimal(20, 8) not null,
                        c decimal(20, 8) not null,
                        v decimal(28, 8) not null,
                        UNIQUE INDEX dt_f (code, dt, f)
                    )
        """

        # 香港与数字货币，每个 code 创建一个表
        no_code_sql = """
        create table %s (
                        dt DATETIME not null,
                        f VARCHAR(5) not null,
                        h decimal(20,8) not null,
                        l decimal(20, 8) not null,
                        o decimal(20, 8) not null,
                        c decimal(20, 8) not null,
                        v decimal(28, 8) not null,
                        UNIQUE INDEX dt_f (dt, f)
                    )
        """

        db = g_pool_db.connection()
        cursor = db.cursor()
        create_tables = []
        for code in codes:
            create_tables.append(self.table(code))
        create_tables = list(set(create_tables))
        for table in create_tables:
            with contextlib.suppress(Exception):
                # 删除并重新创建
                # cursor.execute(f'drop table {table}')
                create_sql = (
                    code_sql % table
                    if self.market in ["a", "us", "hk"]
                    else no_code_sql % table
                )
                cursor.execute(create_sql)
        cursor.close()
        db.close()

    def query_last_datetime(self, code, frequency) -> [None, str]:
        """
        查询交易对儿最后更新时间
        :param frequency:
        :param code:
        :return:
        """
        global g_pool_db

        db = g_pool_db.connection()
        cursor = db.cursor()
        table = self.table(code)
        if self.market in ["a", "hk", "us"]:
            cursor.execute(
                "select dt from %s where code = '%s' and f = '%s' order by dt desc limit 1"
                % (table, code, frequency)
            )
        else:
            cursor.execute(
                "select dt from %s where f = '%s' order by dt desc limit 1"
                % (table, frequency)
            )

        dt = cursor.fetchone()
        cursor.close()
        db.close()
        if dt is not None:
            if self.market == "a":
                return dt[0].strftime("%Y-%m-%d")
            else:
                return dt[0].strftime("%Y-%m-%d %H:%M:%S")
        return None

    def insert_klines(self, code, frequency, klines):
        """
        批量添加交易对儿Kline数据
        :param code:
        :param frequency
        :param klines:
        :return:
        """
        global g_pool_db

        db = g_pool_db.connection()
        cursor = db.cursor()
        table = self.table(code)
        if self.market in ["a", "hk", "us"]:
            sql = f"replace into `{table}`(`code`, `dt`, `f`, `h`, `l`, `o`, `c`, `v`) values (%s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            sql = f"replace into `{table}`(`dt`, `f`, `h`, `l`, `o`, `c`, `v`) values (%s, %s, %s, %s, %s, %s, %s)"
        data_all = []
        for kline in klines.iterrows():
            k = kline[1]
            if self.market in ["a", "hk", "us"]:
                data_all.append(
                    (
                        code,
                        k["date"].strftime("%Y-%m-%d %H:%M:%S"),
                        frequency,
                        k["high"],
                        k["low"],
                        k["open"],
                        k["close"],
                        k["volume"],
                    )
                )
            else:
                data_all.append(
                    (
                        k["date"].strftime("%Y-%m-%d %H:%M:%S"),
                        frequency,
                        k["high"],
                        k["low"],
                        k["open"],
                        k["close"],
                        k["volume"],
                    )
                )
        cursor.executemany(sql, data_all)
        db.commit()
        return

    def del_klines(self, code, frequency, _datetime):
        """
        删除一条记录
        """
        global g_pool_db

        db = g_pool_db.connection()
        cursor = db.cursor()
        table = self.table(code)
        if self.market in ["a", "hk", "us"]:
            sql = "delete from %s where code='%s' and f = '%s' and dt='%s'" % (
                table,
                code,
                frequency,
                _datetime.strftime("%Y-%m-%d %H:%M:%S"),
            )
        else:
            sql = "delete from %s where f = '%s' and dt='%s'" % (
                table,
                frequency,
                _datetime.strftime("%Y-%m-%d %H:%M:%S"),
            )
        cursor.execute(sql)
        db.commit()
        return

    def del_klines_by_code(self, code):
        global g_pool_db
        db = g_pool_db.connection()
        cursor = db.cursor()
        table = self.table(code)
        if self.market in ["a", "hk", "us"]:
            sql = "delete from %s where code='%s'" % (table, code)
        else:
            sql = "drop table `%s`" % table
        cursor.execute(sql)
        db.commit()
        return

    @staticmethod
    def execute_sql(sql: str):
        global g_pool_db
        db = g_pool_db.connection()
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        return

    def klines(
        self,
        code: str,
        frequency: str,
        start_date: str = None,
        end_date: str = None,
        args=None,
    ) -> [pd.DataFrame, None]:
        if args is None:
            args = {}
        global g_pool_db

        if start_date is not None and end_date is not None and "limit" not in args:
            args["limit"] = None
        if "limit" not in args:
            args["limit"] = 5000

        table = self.table(code)
        if self.market in ["a", "hk", "us"]:
            sql = "select dt, f, h, l, o, c, v from %s where code='%s' and f='%s'" % (
                table,
                code,
                frequency,
            )
        else:
            sql = "select dt, f, h, l, o, c, v from %s where f='%s'" % (
                table,
                frequency,
            )
        if start_date is not None:
            sql += " and dt >= '%s'" % start_date
        if end_date is not None:
            sql += " and dt <= '%s'" % end_date
        if "order" in args and args["order"] == "asc":
            sql += " order by dt asc"
        else:
            sql += " order by dt desc"

        if args["limit"] is not None:
            sql += f" limit {args['limit']}"

        db = g_pool_db.connection()
        cursor = db.cursor()
        cursor.execute(sql)
        klines = cursor.fetchall()
        cursor.close()
        db.close()
        kline_pd = pd.DataFrame(
            klines, columns=["date", "f", "high", "low", "open", "close", "volume"]
        )
        kline_pd = kline_pd.iloc[::-1]
        kline_pd["code"] = code
        kline_pd["date"] = pd.to_datetime(kline_pd["date"]).dt.tz_localize(
            self.tz
        )  # .map(lambda d: d.to_pydatetime())
        kline_pd["date"] = kline_pd["date"].apply(self.__convert_date)
        kline_pd["open"] = kline_pd["open"].astype("float")
        kline_pd["close"] = kline_pd["close"].astype("float")
        kline_pd["high"] = kline_pd["high"].astype("float")
        kline_pd["low"] = kline_pd["low"].astype("float")
        kline_pd["volume"] = kline_pd["volume"].astype("float")

        kline_pd = kline_pd[["code", "date", "open", "close", "high", "low", "volume"]]
        kline_pd = kline_pd.reset_index(drop=True)

        return kline_pd

    def __convert_date(self, dt: datetime.datetime):
        """
        统一各个市场的时间格式
        TODO 需要根据自己数据源的数据格式进行调整
        TODO 将日及以上周期（大多数这类的时间都是 0点0分），修改为交易日结束或开始时间（根据日期是前对其还是后对其来决定是开盘时间还是收盘时间）
        """
        if self.market == "a":
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=15, minute=0)
        if self.market == "hk":
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=16, minute=0)
        if self.market == "futures":
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=9, minute=0)
        if self.market == "us":
            if dt.hour == 0 and dt.minute == 0:
                return dt.replace(hour=9, minute=30)
        return dt

    def convert_kline_frequency(self, klines: pd.DataFrame, to_f: str) -> pd.DataFrame:
        """
        转换K线周期
        """
        if self.market == "currency":
            return convert_currency_kline_frequency(klines, to_f)
        elif self.market == "futures":
            return convert_futures_kline_frequency(klines, to_f)
        elif self.market == "us":
            return convert_us_kline_frequency(klines, to_f)
        else:
            return convert_stock_kline_frequency(klines, to_f)

    def all_stocks(self):
        return []

    def now_trading(self):
        pass

    def ticks(self, codes: List[str]) -> Dict[str, Tick]:
        ticks = {}
        for _code in codes:
            klines = self.klines(_code, "d", args={"limit": 1})
            if len(klines) == 0:
                continue
            ticks[_code] = Tick(
                _code,
                klines.iloc[-1]["close"],
                klines.iloc[-1]["close"],
                klines.iloc[-1]["close"],
                klines.iloc[-1]["high"],
                klines.iloc[-1]["low"],
                klines.iloc[-1]["open"],
                klines.iloc[-1]["volume"],
                0,
            )
        return ticks

    def stock_info(self, code: str) -> [Dict, None]:
        return {
            "code": code,
            "name": code,
        }

    def stock_owner_plate(self, code: str):
        pass

    def plate_stocks(self, code: str):
        pass

    def balance(self):
        pass

    def positions(self, code: str = ""):
        pass

    def order(self, code: str, o_type: str, amount: float, args=None):
        pass


if __name__ == "__main__":
    ex = ExchangeDB("us")
    # ticks = ex.ticks(['SHSE.000001'])
    # print(ticks)

    klines = ex.klines(
        "AAPL",
        "d",
        start_date="1990-01-01 00:00:00",
        args={"limit": 10, "order": "asc"},
    )
    print(klines.tail(20))
