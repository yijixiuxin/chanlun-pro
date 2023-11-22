"""
策略参数优化
"""
from itertools import product
from typing import List


class OptimizationSetting:
    """
    策略参数优化设置
    """

    def __init__(self):
        # 缠论配置的参数优化配置
        self.cl_config_params = {}

    def add_cl_parameter(self, name: str, values: list):
        """
        添加缠论的参数配置
        """
        self.cl_config_params[name] = values

    def generate_cl_settings(self) -> List[dict]:
        """"""
        keys = self.cl_config_params.keys()
        values = self.cl_config_params.values()
        products = list(product(*values))

        settings = []
        for p in products:
            setting = dict(zip(keys, p))
            settings.append(setting)

        return settings
