from typing import List, Dict

from cl_v2 import cl
from cl_v2 import trader


class Strategy:
    """
    交易策略基类
    """

    def look(self, cl_datas: List[cl.CL]) -> List:
        """
        观察行情数据，给出操作建议
        :param cl_datas:
        :return:
        """
        return []

    def stare(self, mmd: str, pos:trader.POSITION, cl_datas: List[cl.CL]) -> [Dict, None]:
        """
        盯当前持仓，给出当下建议
        :param mmd:
        :param pos:
        :param cl_datas:
        :return:
        """
        return []
