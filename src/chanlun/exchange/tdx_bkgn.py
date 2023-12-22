"""
通达信板块概念
"""
import pathlib
import struct
from typing import Dict, List

import pandas as pd
from pytdx.hq import TdxHq_API
from pytdx.util import best_ip

from chanlun import config
from chanlun.db import db


class TdxBKGN:
    """
    通达信本地文件读取 行业板块、概念板块信息
    """

    def __init__(self):
        if config.TDX_PATH == "":
            self.tdx_path = None
        else:
            self.tdx_path = pathlib.Path(config.TDX_PATH)

        # 选择最优的服务器，并保存到 redis 中
        connect_info = db.cache_get("tdx_connect_ip")
        if connect_info is None:
            connect_info = best_ip.select_best_ip("stock")
            connect_info = {"ip": connect_info["ip"], "port": connect_info["port"]}
            db.cache_set("tdx_connect_ip", connect_info)

        # 获取所有板块指数代码
        self.all_stocks = []
        for market in range(2):
            client = TdxHq_API(raise_exception=True, auto_retry=True)
            with client.connect(connect_info["ip"], connect_info["port"]):
                count = client.get_security_count(market)
                data = pd.concat(
                    [
                        client.to_df(client.get_security_list(market, i * 1000))
                        for i in range(int(count / 1000) + 1)
                    ],
                    axis=0,
                    sort=False,
                )
                for _d in data.iterrows():
                    code = _d[1]["code"]
                    name = _d[1]["name"]
                    if code[0:2] != "88":
                        continue
                    code = f"SH.{str(code)}"
                    self.all_stocks.append({"code": code, "name": name})

        self.cache_hy = []
        self.cache_gn = []

    @staticmethod
    def to_tdx_code(_c: str):
        if _c[0] == "6":
            return "SH." + _c
        if _c[0] in ["0", "3"]:
            return "SZ." + _c
        return None

    def get_all_bkgn(self):
        """
        获取所有的行业概念信息
        """
        if self.tdx_path is None:
            return {"HY": [], "GN": []}

        hys = self.get_all_hy()
        gns = self.get_all_gn()

        return {
            "HY": hys,
            "GN": gns,
        }

    def get_all_hy(self):
        """
        获取所有行业信息
        {'name': '煤炭开采', 'code': 'SH.880302', 'in_codes': ['SZ.000552',]}
        """
        if self.tdx_path is None:
            return []

        if len(self.cache_hy) > 0:
            return self.cache_hy

        tdx_hy_file = self.tdx_path / "incon.dat"
        tdx_stock_file = self.tdx_path / "T0002" / "hq_cache" / "tdxhy.cfg"

        hy_info: Dict[str, List] = {}
        with open(tdx_hy_file, "r", encoding="gbk") as fp:
            hy_type_key = ""
            for l_txt in fp.readlines():
                l_txt = l_txt.strip()
                if l_txt.startswith("#"):
                    hy_type_key = l_txt.split("#")[1]
                    if hy_type_key not in hy_info.keys():
                        hy_info[hy_type_key] = []
                else:
                    if "|" in l_txt:
                        hy_info[hy_type_key].append(
                            {"_code": l_txt.split("|")[0], "name": l_txt.split("|")[1]}
                        )

        # 获取所有通达信行业信息
        tdx_hy = []
        for _info in hy_info["TDXNHY"]:
            # 从所有股票中，找出行业对应的板块指数代码
            hy_stocks = [
                _s
                for _s in self.all_stocks
                if _s["name"] == _info["name"] and _s["code"][0:5] == "SH.88"
            ]
            if len(hy_stocks) == 1:
                _info["code"] = hy_stocks[0]["code"] if len(hy_stocks) == 1 else "--"
                _info["in_codes"] = []
                tdx_hy.append(_info)

        # 读取股票的与行业的关系
        with open(tdx_stock_file, "r") as fp:
            for l_txt in fp.readlines():
                l_txt = l_txt.strip()
                if "|" in l_txt:
                    _info = l_txt.split("|")
                    _tdx_code = self.to_tdx_code(_info[1])
                    if _tdx_code is None:
                        continue
                    for _hy in tdx_hy:
                        if _info[2] == _hy["_code"]:
                            _hy["in_codes"].append(_tdx_code)

        tdx_new_hy = []

        for _hy in tdx_hy:
            if len(_hy["in_codes"]) == 0:
                continue
            tdx_new_hy.append(_hy)

        self.cache_hy = tdx_new_hy

        return tdx_new_hy

    def get_all_gn(self):
        """
        获取所有概念信息
        {'gn_name': '通达信88', 'gn_code': 'SH.880515', 'in_codes': ['SZ.000002',]}
        """
        if self.tdx_path is None:
            return []

        if len(self.cache_gn) > 0:
            return self.cache_gn

        gn_name_map = {
            "锂电池": "锂电池概",
            "TOPCon": "TOPCon电",
            "钙钛矿": "钙钛矿电",
            "核污防治": "核污染防",
            "三代半导": "第三代半",
            "有机硅": "有机硅概",
            "热管理": "汽车热管",
            "元宇宙": "元宇宙概",
            "幽门菌": "幽门螺杆",
            "新冠药": "新冠药概",
            "装配建筑": "装配式建",
            "临界发电": "超临界发",
            "毫米雷达": "毫米波雷",
            "时空数据": "时空大数",
            "可控核变": "可控核聚",
            "英伟达": "英伟达概",
        }

        tdx_gn_file = self.tdx_path / "T0002" / "hq_cache" / "block_gn.dat"
        data = tdx_gn_file.read_bytes()
        gn_info = []
        pos = 384
        (num,) = struct.unpack("<H", data[pos : pos + 2])
        pos += 2
        for i in range(num):
            gn_name_raw = data[pos : pos + 9]
            pos += 9
            gn_name = gn_name_raw.decode("gbk", "ignore").rstrip("\x00")
            stock_count, gn_type = struct.unpack("<HH", data[pos : pos + 4])
            pos += 4
            block_stock_begin = pos
            # 查询概念的行情代码
            gn_code = [
                _s
                for _s in self.all_stocks
                if _s["name"]
                == (gn_name if gn_name not in gn_name_map else gn_name_map[gn_name])
                and _s["code"][0:5] == "SH.88"
            ]
            # if len(gn_code) == 0:
            #     print(gn_name, '没有找到行情代码')
            # if len(gn_code) != 1:
            #     print(gn_name, '有多个行情代码')
            gn_res = {
                "name": gn_name,
                "code": gn_code[0]["code"] if len(gn_code) == 1 else "--",
                "in_codes": [],
            }
            for code_index in range(stock_count):
                one_code = data[pos : pos + 7].decode("utf-8", "ignore").rstrip("\x00")
                pos += 7
                tdx_code = self.to_tdx_code(one_code)
                if tdx_code is not None:
                    gn_res["in_codes"].append(tdx_code)
            pos = block_stock_begin + 2800
            gn_info.append(gn_res)

        self.cache_gn = gn_info

        return gn_info

    def get_code_bkgn(self, code):
        """
        获取指定代码的行业与概念信息
        """
        if self.tdx_path is None:
            return {"HY": [], "GN": []}

        hys = []
        gns = []
        for _hy in self.get_all_hy():
            if code in _hy["in_codes"]:
                hys.append({"code": _hy["code"], "name": _hy["name"]})
        for _gn in self.get_all_gn():
            if code in _gn["in_codes"]:
                gns.append({"code": _gn["code"], "name": _gn["name"]})

        return {"HY": hys, "GN": gns}

    def get_bk_codes(self, bk: str):
        """
        获取板块中包含的股票列表，bk 支持 板块名称或板块代码
        """
        hys = self.get_all_hy()
        gns = self.get_all_gn()

        codes = []
        for _hy in hys:
            if _hy["name"] == bk or _hy["code"] == bk:
                codes += _hy["in_codes"]
        for _gn in gns:
            if _gn["name"] == bk or _gn["code"] == bk:
                codes += _gn["in_codes"]

        return codes


if __name__ == "__main__":
    thg = TdxBKGN()
    # hys = thg.get_all_hy()
    # for _hy in hys:
    #     print(_hy)
    # gns = thg.get_all_gn()
    # for _gn in gns:
    #     print(_gn)
    #
    # hygns = thg.get_all_bkgn()
    # print(hygns)
    #
    code_bkgn = thg.get_code_bkgn("SH.600779")
    print(code_bkgn)

    # codes = thg.get_bk_codes('白酒')
    # print(codes)
