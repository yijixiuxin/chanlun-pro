from typing import List, Dict

from chanlun.base import Market
from chanlun.db import db

from chanlun.exchange import get_exchange


class ZiXuan(object):
    """
    自选池功能
    """

    def __init__(self, market_type):
        """
        初始化
        """
        self.market_type = market_type
        self.zixuan_list = self.get_zx_groups()

        self.zx_names = [_zx["name"] for _zx in self.zixuan_list]

    def get_zx_groups(self):
        # 获取自选分组
        zx_groups = db.zx_get_groups(self.market_type)
        if len(zx_groups) == 0:
            db.zx_add_group(self.market_type, "我的关注")
            zx_groups = db.zx_get_groups(self.market_type)
        return [{"name": _g.zx_group} for _g in zx_groups]

    def add_zx_group(self, zx_group_name):
        if zx_group_name in ["我的关注"]:
            return False
        if zx_group_name in [_z["name"] for _z in self.zixuan_list]:
            return False
        db.zx_add_group(self.market_type, zx_group_name)
        self.zixuan_list = self.get_zx_groups()
        self.zx_names = [_zx["name"] for _zx in self.zixuan_list]
        return True

    def del_zx_group(self, zx_group_name):
        if zx_group_name in ["我的关注"]:
            return False
        self.clear_zx_stocks(zx_group_name)
        db.zx_del_group(self.market_type, zx_group_name)
        self.zixuan_list = self.get_zx_groups()
        self.zx_names = [_zx["name"] for _zx in self.zixuan_list]
        return True

    def query_all_zs_stocks(self):
        """
        查询自选分组下所有的代码信息
        """
        return [
            {"zx_name": zx_name, "stocks": self.zx_stocks(zx_name)}
            for zx_name in self.zx_names
        ]

    def zx_stocks(self, zx_group) -> List[Dict[str, str]]:
        """
        根据自选名称，获取其中的 代码列表
        """
        if zx_group not in self.zx_names:
            return []
        stocks = db.zx_get_group_stocks(self.market_type, zx_group)
        return [
            {
                "code": _stock.stock_code,
                "name": _stock.stock_name,
                "color": _stock.stock_color,
                "memo": _stock.stock_memo,
                "add_datetime": _stock.add_datetime,
            }
            for _stock in stocks
        ]

    def add_stock(
        self, zx_group: str, code: str, name: str, location="bottom", color="", memo=""
    ):
        """
        添加自选

        #ff5722  红色
        #ffb800  橙色
        #16baaa  绿色
        #1e9fff  蓝色
        #a233c6  紫色

        """
        if zx_group not in self.zx_names:
            return False
        # 如果名称为空，则自动进行获取
        if name is None or name == "" or name == "undefined":
            try:
                ex = get_exchange(Market(self.market_type))
                stock_info = ex.stock_info(code)
                name = stock_info["name"]
            except Exception:
                pass
        db.zx_add_group_stock(
            self.market_type, zx_group, code, name, memo, color, location
        )
        return True

    def del_stock(self, zx_group, code):
        """
        删除自选中的代码
        """
        db.zx_del_group_stock(self.market_type, zx_group, code)
        return True

    def color_stock(self, zx_group, code, color):
        """
        给指定的代码加上颜色
        """
        db.zx_update_stock_color(self.market_type, zx_group, code, color)
        return True

    def rename_stock(self, zx_group, code, rename):
        """
        修改指定代码的自选名称
        """
        db.zx_update_stock_name(self.market_type, zx_group, code, rename)
        return True

    def sort_top_stock(self, zx_group, code):
        """
        将股票排在最上面
        """
        db.zx_stock_sort_top(self.market_type, zx_group, code)
        return True

    def sort_bottom_stock(self, zx_group, code):
        """
        将股票排在最下面
        """
        db.zx_stock_sort_bottom(self.market_type, zx_group, code)
        return True

    def clear_zx_stocks(self, zx_group):
        """
        清空自选组内的股票
        """
        db.zx_clear_by_group(self.market_type, zx_group)
        return True

    def query_code_zx_names(self, code):
        """
        查询代码所在的自选分组
        """
        exists_group = db.zx_query_group_by_code(self.market_type, code)
        res_zx_group = [
            {
                "zx_name": _g["name"],
                "code": code,
                "exists": 1 if _g["name"] in exists_group else 0,
            }
            for _g in self.zixuan_list
        ]
        return res_zx_group


if __name__ == "__main__":
    zx = ZiXuan("currency")
    zx.add_stock("我的关注", "LTC/USDT", None)
