import json
import pathlib
import time
import webbrowser
from typing import Dict, List, Union

import akshare as ak
from tqdm.auto import tqdm

from chanlun import config, fun
from chanlun.config import get_data_path

"""
股票板块概念
"""


class StocksBKGN(object):
    def __init__(self):

        if config.TDX_PATH == "":
            self.tdx_path = None
        else:
            self.tdx_path = pathlib.Path(config.TDX_PATH)

        self.file_path = get_data_path() / "json"
        if self.file_path.is_dir() is False:
            self.file_path.mkdir(parents=True)

        self.file_name = self.file_path / "new_stocks_bkgn.json"

        self.cache_file_bk = None

        self.logger = fun.get_logger("stocks_bkgn.log")

    def reload_bkgn(self):
        if self.tdx_path:
            self.logger.info("开始加载通达信板块数据")
            return self.reload_tdx_bkgn()
        else:
            self.logger.info("开始加载东方财富板块数据")
            return self.reload_dfcf_bkgn()

    def reload_dfcf_bkgn(self):
        """
        下载更新保存新的板块概念信息
        通过 东方财富 接口获取板块概念
        """

        error_msgs = []
        bkgn_hys = []
        bkgn_gns = []
        stock_hy_codes = {}
        stock_gn_codes = {}
        ak_hys = ak.stock_board_industry_name_em()
        for _, b in tqdm(ak_hys.iterrows()):
            b_name = b["板块名称"]
            bkgn_hys.append(b_name)
            stock_hy_codes[b_name] = []
            try_nums = 0
            while True:
                try:
                    time.sleep(1)
                    # 获取板块的成分股
                    b_stocks = ak.stock_board_industry_cons_em(b_name)
                    self.logger.info(f"{b_name} 行业成分股数量：{len(b_stocks)}")
                    for _, s in b_stocks.iterrows():
                        stock_hy_codes[b_name].append(s["代码"])
                    break
                except Exception as e:
                    print("请打开浏览器，在东方财富网站，手动验证后，继续抓取")
                    webbrowser.open("https://www.eastmoney.com/")
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} 行业板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        self.logger.error(msg)
                        break

        ak_gns = ak.stock_board_concept_name_em()
        for _, b in tqdm(ak_gns.iterrows()):
            b_name = b["板块名称"]
            bkgn_gns.append(b_name)
            stock_gn_codes[b_name] = []
            try_nums = 0
            while True:
                try:
                    time.sleep(1)
                    # 获取概念的成分股
                    b_stocks = ak.stock_board_concept_cons_em(b_name)
                    self.logger.info(f"{b_name} 概念成分股数量：{len(b_stocks)}")
                    for _, s in b_stocks.iterrows():
                        stock_gn_codes[b_name].append(s["代码"])
                    break
                except Exception as e:
                    print("请打开浏览器，在东方财富网站，手动验证后，继续抓取")
                    webbrowser.open("https://www.eastmoney.com/")
                    time.sleep(60)
                    try_nums += 1
                    if try_nums >= 10:
                        msg = f"{b_name} 概念板块获取成分股异常：{e}"
                        error_msgs.append(msg)
                        self.logger.error(msg)
                        break

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump(
                {
                    "hys": bkgn_hys,
                    "gns": bkgn_gns,
                    "hy_codes": stock_hy_codes,
                    "gn_codes": stock_gn_codes,
                },
                fp,
            )

        if len(error_msgs) > 0:
            self.logger.error(f"错误信息：{error_msgs}")
        return True

    def reload_tdx_bkgn(self):
        """
        通过读取通达信的文件读取行业与概念
        """

        # 记录读取到的行业概念信息
        bkgn_hys = []
        bkgn_gns = []
        stock_hy_codes = {}
        stock_gn_codes = {}

        # 通达信行业相关文件
        tdx_hy_file = self.tdx_path / "incon.dat"
        tdx_stock_file = self.tdx_path / "T0002" / "hq_cache" / "tdxhy.cfg"

        tdx_hy_info = {}  # 只获取二级行业，并关联二级行业下的子行业
        with open(tdx_hy_file, "r", encoding="gbk") as fp:
            for l_txt in fp.readlines():
                l_txt = l_txt.strip()
                if l_txt.startswith("X") and "|" in l_txt:
                    hy_code = l_txt.split("|")[0]
                    hy_name = l_txt.split("|")[1]
                    if len(hy_code) == 5:  # 属于二级行业
                        tdx_hy_info[hy_code] = {
                            "name": hy_name,
                            "code": hy_code,
                            "sub_codes": [],
                        }
                        stock_hy_codes[hy_name] = []
                    if len(hy_code) > 5:  # 属于三级行业
                        for _hy_c, _hy_i in tdx_hy_info.items():
                            if hy_code.startswith(_hy_c):  # 获取所属行业
                                _hy_i["sub_codes"].append(hy_code)

        # 读取股票的与行业的关系
        with open(tdx_stock_file, "r") as fp:
            for l_txt in fp.readlines():
                l_txt = l_txt.strip()
                if "|" in l_txt:
                    _info = l_txt.split("|")
                    _code = _info[1]
                    _hy_code = _info[-1]
                    _hy_name = None
                    for _hy_c, _hy_i in tdx_hy_info.items():
                        if _hy_code == _hy_c:
                            _hy_name = _hy_i["name"]
                        if _hy_code in _hy_i["sub_codes"]:
                            _hy_name = _hy_i["name"]
                        if _hy_name is not None:
                            break
                    if _hy_name:
                        stock_hy_codes[_hy_name].append(_code)

        stock_hy_codes = {
            _hy_name: _codes
            for _hy_name, _codes in stock_hy_codes.items()
            if len(_codes) > 0
        }
        bkgn_hys = list(stock_hy_codes.keys())

        # 概念的文件
        tdx_gn_file = self.tdx_path / "T0002" / "hq_cache" / "infoharbor_block.dat"
        with open(tdx_gn_file, "r", encoding="gbk") as fp:
            # 读取文件，整理获取概念与代码的关系
            current_gn = None
            codes = []
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#"):
                    # 保存上一个概念
                    if current_gn is not None:
                        stock_gn_codes[current_gn] = codes
                    # 解析新概念名称
                    parts = line.split(",")
                    if len(parts) > 0:
                        gn_name = parts[0].replace("#GN_", "")
                        current_gn = gn_name
                        codes = []
                else:
                    # 解析股票代码
                    for item in line.split(","):
                        item = item.strip()
                        if not item:
                            continue
                        # 只保留#后面的6位代码
                        if "#" in item:
                            code = item.split("#")[1]
                            codes.append(code)
            # 保存最后一个概念
            if current_gn is not None:
                stock_gn_codes[current_gn] = codes

        stock_gn_codes = {
            _gn_name: _codes
            for _gn_name, _codes in stock_gn_codes.items()
            if len(_codes) > 0 and "#" not in _gn_name
        }
        bkgn_gns = list(stock_gn_codes.keys())

        with open(self.file_name, "w", encoding="utf-8") as fp:
            json.dump(
                {
                    "hys": bkgn_hys,
                    "gns": bkgn_gns,
                    "hy_codes": stock_hy_codes,
                    "gn_codes": stock_gn_codes,
                },
                fp,
            )

        return True

    def file_bkgns(self) -> Dict[str, Union[Dict[str, list], List[str]]]:
        if self.cache_file_bk is None:
            if self.file_name.is_file():
                with open(self.file_name, "r", encoding="utf-8") as fp:
                    bkgns = json.load(fp)
                self.cache_file_bk = bkgns
            else:
                self.cache_file_bk = {
                    "hys": [],
                    "gns": [],
                    "hy_codes": {},
                    "gn_codes": {},
                }
        return self.cache_file_bk

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
        bkgn_infos = self.file_bkgns()
        code_hys = []
        code_gns = []
        for hy, codes in bkgn_infos["hy_codes"].items():
            if code in codes:
                code_hys.append(hy)
        for gn, codes in bkgn_infos["gn_codes"].items():
            if code in codes:
                code_gns.append(gn)
        return {"HY": code_hys, "GN": code_gns}

    def get_codes_by_hy(self, hy_name) -> List[str]:
        """
        根据行业名称，获取其中的股票代码列表
        """
        bkgn_infos = self.file_bkgns()
        if hy_name in bkgn_infos["hy_codes"].keys():
            return bkgn_infos["hy_codes"][hy_name]
        return []

    def get_codes_by_gn(self, gn_name):
        """
        根据概念名称，获取其中的股票代码列表
        """
        bkgn_infos = self.file_bkgns()
        if gn_name in bkgn_infos["gn_codes"].keys():
            return bkgn_infos["gn_codes"][gn_name]

        return []

    @staticmethod
    def ths_to_tdx_codes(_codes):
        """
        同花顺 6位 数字的代码，转换成通达信的代码
        """
        _res_codes = []
        for _c in _codes:
            if _c[0:3] == "688":  # 科创板
                _res_codes.append(f"SH.{_c}")
            elif _c[0] == "8" or _c[0] == "4" or _c[0] == "9":  # 京交所
                _res_codes.append(f"BJ.{_c}")
            elif _c[0] == "6":
                _res_codes.append(f"SH.{_c}")
            else:
                _res_codes.append(f"SZ.{_c}")
        return _res_codes


if __name__ == "__main__":
    """
    更新行业概念信息并保存
    """
    bkgn = StocksBKGN()
    # 重新更新并保存行业与板块信息

    bkgn.reload_bkgn()

    # 所有行业概念
    bkgn_infos = bkgn.file_bkgns()
    print("行业数量：", len(bkgn_infos["hys"]))
    print("概念数量：", len(bkgn_infos["gns"]))

    # # 同步所有行业指数到数据库
    # from chanlun.exchange.exchange_db import ExchangeDB

    # ex = ExchangeDB("a")
    # for _hy in all_hy_names:
    #     klines = bkgn.get_index_klines(_hy, "dfcf")
    #     ex.insert_klines(klines.iloc[0]["code"], "d", klines)
    #     print(f"Insert {_hy} success len : {len(klines)}")

    # klines = bkgn.get_index_klines("文化传媒", "dfcf")
    # print(klines)

    # print("行业")
    # print(all_hy_names)
    # print("概念")
    # print(all_gn_names)

    # 获取代码的板块概念信息
    # code_bkgn = bkgn.get_code_bkgn("SH.600143")
    # print(code_bkgn)

    # 根据行业获取其中的代码
    # codes = bkgn.get_codes_by_hy("塑料制品")
    # print(codes)

    # 根据概念获取其中的代码
    # codes = bkgn.get_codes_by_gn('电子竞技')
    # print(codes)
    # print(codes)
