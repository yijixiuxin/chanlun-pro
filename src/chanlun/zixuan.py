from typing import List, Dict

from chanlun import config
from chanlun import rd
from chanlun.exchange import get_exchange, Market


class ZiXuan(object):
    """
    自选池功能
    """

    def __init__(self, market_type):
        """
        初始化
        """
        self.market_type = market_type
        if market_type == 'a':
            self.zixuan_list = config.STOCK_ZX
        elif market_type == 'futures':
            self.zixuan_list = config.FUTURES_ZX
        elif market_type == 'currency':
            self.zixuan_list = config.CURRENCY_ZX
        elif market_type == 'hk':
            self.zixuan_list = config.HK_ZX
        elif market_type == 'us':
            self.zixuan_list = config.US_ZX
        else:
            raise Exception('暂不支持的市场自选列表')

        self.zx_names = [_zx['name'] for _zx in self.zixuan_list]

    def query_all_zs_stocks(self):
        """
        查询自选分组下所有的代码信息
        """
        return [{'zx_name': zx_name, 'stocks': self.zx_stocks(zx_name)} for zx_name in self.zx_names]

    def zx_stocks(self, zx_group) -> List[Dict[str, str]]:
        """
        根据自选名称，获取其中的 代码列表
        """
        if zx_group not in self.zx_names:
            return []
        return rd.zx_query(self.market_type, zx_group)

    def add_stock(self, zx_group: str, code: str, name: str, location='top', color=''):
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
        if name is None or name == '' or name == 'undefined':
            try:
                ex = get_exchange(Market(self.market_type))
                stock_info = ex.stock_info(code)
                name = stock_info['name']
            except Exception:
                pass
        # 先删除原来的，如果有的话
        self.del_stock(zx_group, code)
        stocks = self.zx_stocks(zx_group)
        if location == 'top':
            stocks.insert(0, {'code': code, 'name': name, 'color': color})
        else:
            stocks.append({'code': code, 'name': name, 'color': color})
        rd.zx_save(self.market_type, zx_group, stocks)
        return True

    def del_stock(self, zx_group, code):
        """
        删除自选中的代码
        """
        if zx_group not in self.zx_names:
            return False
        stocks = self.zx_stocks(zx_group)
        del_index = next((i for i in range(len(stocks)) if stocks[i]['code'] == code), None)

        if del_index is not None:
            del (stocks[del_index])
            rd.zx_save(self.market_type, zx_group, stocks)
        return True

    def color_stock(self, zx_group, code, color):
        """
        给指定的代码加上颜色
        """
        stocks = self.zx_stocks(zx_group)
        for s in stocks:
            if s['code'] == code:
                s['color'] = color
        rd.zx_save(self.market_type, zx_group, stocks)
        return True

    def rename_stock(self, zx_group, code, rename):
        """
        修改指定代码的自选名称
        """
        stocks = self.zx_stocks(zx_group)
        for s in stocks:
            if s['code'] == code:
                s['name'] = rename
        rd.zx_save(self.market_type, zx_group, stocks)
        return True

    def sort_top_stock(self, zx_group, code):
        """
        将股票排在最上面
        """
        stocks = self.zx_stocks(zx_group)
        for s in stocks:
            if s['code'] == code:
                self.add_stock(zx_group, s['code'], s['name'], 'top', s['color'])
                break
        return True

    def sort_bottom_stock(self, zx_group, code):
        """
        将股票排在最下面
        """
        stocks = self.zx_stocks(zx_group)
        for s in stocks:
            if s['code'] == code:
                self.add_stock(zx_group, s['code'], s['name'], 'bottom', s['color'])
                break
        return True

    def clear_zx_stocks(self, zx_group):
        """
        清空自选组内的股票
        """
        stocks = self.zx_stocks(zx_group)
        for s in stocks:
            self.del_stock(zx_group, s['code'])
        return True

    def query_code_zx_names(self, code):
        """
        查询代码所在的自选分组
        """
        res = []
        for zx_name in self.zx_names:
            stocks = self.zx_stocks(zx_name)
            if code in [_s['code'] for _s in stocks]:
                res.append({'zx_name': zx_name, 'code': code, 'exists': 1})
            else:
                res.append({'zx_name': zx_name, 'code': code, 'exists': 0})

        return res


if __name__ == '__main__':
    zx = ZiXuan('a')
    zx.add_stock('强势', 'SH.880540', '创投概念')
