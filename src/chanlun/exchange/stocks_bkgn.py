import json
import random
import time, datetime
from typing import Tuple

import akshare as ak
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from tqdm.auto import tqdm
import pandas as pd
from chanlun.config import get_data_path

"""
股票板块概念
"""


class StocksBKGN(object):
    def __init__(self):
        self.file_path = get_data_path() / "json"
        if self.file_path.is_dir() == False:
            self.file_path.mkdir(parents=True)

        self.file_name = self.file_path / "stocks_bkgn.json"

        self.cache_file_bk = None

    def reload_ths_bkgn(self):
        """
        下载更新保存新的板块概念信息
        通过 同花顺 接口获取板块概念

        """
        error_msgs = []
        stock_industrys = {}
        ak_industry = ak.stock_board_industry_name_ths()
        for _, b in tqdm(ak_industry.iterrows()):
            b_name = b["name"]
            b_code = b["code"]
            try_nums = 0
            while True:
                try:
                    time.sleep(random.randint(4, 5))
                    # 获取板块的成分股
                    b_stocks = ak.stock_board_cons_ths(b_code)
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_industrys.keys():
                            stock_industrys[s_code] = []
                        stock_industrys[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 行业板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        stock_concepts = {}
        ak_concept = ak.stock_board_concept_name_ths()
        for _, b in tqdm(ak_concept.iterrows()):
            b_name = b["概念名称"]
            b_code = b["代码"]
            try_nums = 0
            while True:
                try:
                    time.sleep(random.randint(4, 5))
                    # 获取概念的成分股
                    b_stocks = ak.stock_board_cons_ths(b_code)
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_concepts.keys():
                            stock_concepts[s_code] = []
                        stock_concepts[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 概念板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump({"hy": stock_industrys, "gn": stock_concepts}, fp)

        print("错误信息：", error_msgs)
        return True

    def reload_dfcf_bkgn(self):
        """
        下载更新保存新的板块概念信息
        通过 东方财富 接口获取板块概念
        """
        error_msgs = []
        stock_industrys = {}
        ak_industry = ak.stock_board_industry_name_em()
        for _, b in tqdm(ak_industry.iterrows()):
            b_name = b["板块名称"]
            b_code = b["板块代码"]
            try_nums = 0
            while True:
                try:
                    # time.sleep(random.randint(1, 3))
                    # 获取板块的成分股
                    b_stocks = ak.stock_board_industry_cons_em(b_name)
                    print(f"{b_name} {b_code} 行业成分股数量：{len(b_stocks)}")
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_industrys.keys():
                            stock_industrys[s_code] = []
                        stock_industrys[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 行业板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        stock_concepts = {}
        ak_concept = ak.stock_board_concept_name_em()
        for _, b in tqdm(ak_concept.iterrows()):
            b_name = b["板块名称"]
            b_code = b["板块代码"]
            try_nums = 0
            while True:
                try:
                    # time.sleep(random.randint(1, 3))
                    # 获取概念的成分股
                    b_stocks = ak.stock_board_concept_cons_em(b_name)
                    print(f"{b_name} {b_code} 概念成分股数量：{len(b_stocks)}")
                    for _, s in b_stocks.iterrows():
                        s_code = s["代码"]
                        if s_code not in stock_concepts.keys():
                            stock_concepts[s_code] = []
                        stock_concepts[s_code].append(b_name)
                except Exception as e:
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} {b_code} 概念板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        print(msg)
                        break
                finally:
                    break

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump({"hy": stock_industrys, "gn": stock_concepts}, fp)

        print("错误信息：", error_msgs)
        return True

    def reload_tdx_bkgn(self):
        """
        下载更新保存新的板块概念信息
        通过 通达信 接口获取板块概念

        """
        stock_industrys = {}  # 保存行业的股票信息
        stock_concepts = {}  # 保存概念的股票信息

        # tdx_host = best_ip.select_best_ip('stock')
        tdx_host = {"ip": "221.194.181.176", "port": 7709}
        api = TdxHq_API(raise_exception=True, auto_retry=True)
        with api.connect(tdx_host["ip"], tdx_host["port"]):
            # 获取行业
            hy_infos = api.get_and_parse_block_info(TDXParams.BLOCK_DEFAULT)
            for _hy in hy_infos:
                _code = _hy["code"]
                if _code not in stock_industrys.keys():
                    stock_industrys[_code] = []
                stock_industrys[_code].append(_hy["blockname"])
                stock_industrys[_code] = list(set(stock_industrys[_code]))

            # 获取概念
            gn_infos = api.get_and_parse_block_info(TDXParams.BLOCK_GN)
            for _gn in gn_infos:
                _code = _gn["code"]
                if _code not in stock_concepts.keys():
                    stock_concepts[_code] = []
                stock_concepts[_code].append(_gn["blockname"])
                stock_concepts[_code] = list(set(stock_concepts[_code]))

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump({"hy": stock_industrys, "gn": stock_concepts}, fp)

        return True

    def file_bkgns(self) -> Tuple[dict, dict]:
        if self.cache_file_bk is None:
            if self.file_name.is_file():
                with open(self.file_name, "r", encoding="utf-8") as fp:
                    bkgns = json.load(fp)
                self.cache_file_bk = bkgns
            else:
                self.cache_file_bk = {
                    "hy": {},
                    "gn": {},
                }
        return self.cache_file_bk["hy"], self.cache_file_bk["gn"]

    def get_code_bkgn(self, code: str):
        """
        获取代码板块概念
        """
        code = (
            code.replace("SZ.", "")
            .replace("SH.", "")
            .replace("SZSE.", "")
            .replace("SHSE.", "")
        )
        hys, gns = self.file_bkgns()
        code_hys = []
        code_gns = []
        if code in hys.keys():
            code_hys = hys[code]
        if code in gns.keys():
            code_gns = gns[code]
        return {"HY": code_hys, "GN": code_gns}

    def get_codes_by_hy(self, hy_name):
        """
        根据行业名称，获取其中的股票代码列表
        """
        hys, gns = self.file_bkgns()
        codes = []
        for _code, _hys in hys.items():
            if hy_name in _hys:
                codes.append(_code)

        return codes

    def get_codes_by_gn(self, gn_name):
        """
        根据概念名称，获取其中的股票代码列表
        """
        hys, gns = self.file_bkgns()
        codes = []
        for _code, _gns in gns.items():
            if gn_name in _gns:
                codes.append(_code)

        return codes

    def get_index_klines(self, bk_name, source="dfcf"):
        """
        获取板块的K线数据
        source: dfcf 东方财富 ths 同花顺
        """
        end_dt = datetime.datetime.now() + datetime.timedelta(days=1)
        end_dt = end_dt.strftime("%Y%m%d")
        code = f"SH.0000_HY_{bk_name}"
        if source == "dfcf":
            klines = ak.stock_board_industry_hist_em(
                symbol=bk_name,
                start_date="20000101",
                end_date=end_dt,
                period="日k",
                adjust="qfq",
            )
            klines = klines.rename(
                columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最低": "low",
                    "最高": "high",
                    "成交量": "volume",
                }
            )
        else:
            klines = ak.stock_board_industry_index_ths(
                symbol=bk_name, start_date="20000101", end_date=end_dt
            )
            klines = klines.rename(
                columns={
                    "日期": "date",
                    "开盘价": "open",
                    "收盘价": "close",
                    "最低价": "low",
                    "最高价": "high",
                    "成交量": "volume",
                }
            )
        klines["code"] = code
        klines["date"] = pd.to_datetime(klines["date"])
        klines["date"] = klines["date"].apply(lambda _d: _d.replace(hour=15, minute=0))
        klines = klines.dropna()
        # 将 close/low/high/open/volume 转换为float
        klines[["open", "close", "low", "high", "volume"]] = klines[
            ["open", "close", "low", "high", "volume"]
        ].astype(float)
        return klines[["code", "date", "open", "close", "low", "high", "volume"]]


if __name__ == "__main__":
    """
    更新行业概念信息并保存
    """
    bkgn = StocksBKGN()
    # 重新更新并保存行业与板块信息
    # bkgn.reload_dfcf_bkgn()

    # 所有行业概念
    hys, gns = bkgn.file_bkgns()
    all_hy_names = []
    all_gn_names = []
    for _c, _v in hys.items():
        all_hy_names += _v
        all_hy_names = list(set(all_hy_names))
    for _c, _v in gns.items():
        all_gn_names += _v
        all_gn_names = list(set(all_gn_names))
    print(len(all_hy_names))
    print(len(all_gn_names))

    # 同步所有行业指数到数据库
    from chanlun.exchange.exchange_db import ExchangeDB

    ex = ExchangeDB("a")
    for _hy in all_hy_names:
        klines = bkgn.get_index_klines(_hy, "dfcf")
        ex.insert_klines(klines.iloc[0]["code"], "d", klines)
        print(f"Insert {_hy} success len : {len(klines)}")

    # klines = bkgn.get_index_klines("文化传媒", "dfcf")
    # print(klines)

    # print("行业")
    # print(all_hy_names)
    # print("概念")
    # print(all_gn_names)

    # 获取代码的板块概念信息
    code_bkgn = bkgn.get_code_bkgn("600895")
    print(code_bkgn)

    # 根据行业获取其中的代码
    # codes = bkgn.get_codes_by_hy('珠宝首饰')
    # print(codes)

    # 根据概念获取其中的代码
    # codes = bkgn.get_codes_by_gn('电子竞技')
    # print(codes)
