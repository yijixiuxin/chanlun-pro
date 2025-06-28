"""
根据笔段的相似度进行选股
"""

import datetime
import json
import math
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from multiprocessing import get_context
from typing import Any, Dict, List, Tuple, Union

import numpy as np
from dtaidistance import dtw_ndim
from tqdm.auto import tqdm

from chanlun import cl, fun
from chanlun.base import Market
from chanlun.cl_interface import ICL
from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from chanlun.exchange import get_exchange


@dataclass
class XGFeatures:
    """
    选股特征
    """

    bi_features: List[Tuple[float, int]]
    xd_features: List[Tuple[float, int]]
    k_features: List[Tuple[float, float]]
    bi_mmds: List[str]
    bi_bcs: List[str]


class XuanguBySame:

    def __init__(
        self,
        market: str,
    ):
        self.market = market

        # 笔和段的数量
        self.k_num = 200
        self.bi_num = 9
        self.xd_num = 3

        # K线，笔和段权重
        self.k_weight = 0.3
        self.bi_weight = 0.6
        self.xd_weight = 0.1

        self.logger = fun.get_logger("xuangu_by_same.log")

    def run(
        self,
        target_code: str,
        frequency: str,
        target_end_datetime: datetime.datetime,
        find_codes: List[str],
        cl_config: dict,
        run_type: str = "process",
    ) -> List[Dict[str, Any]]:
        # 获取目标代码的k线，并计算缠论数据
        target_klines = get_exchange(Market(self.market)).klines(target_code, frequency)
        if target_end_datetime:
            target_klines = target_klines[target_klines["date"] <= target_end_datetime]
        target_cd: ICL = cl.CL(target_code, frequency, cl_config).process_klines(
            target_klines
        )
        target_features = self.extract_cd_features(target_cd)
        if target_features is None:
            self.logger.warning(
                f"目标代码{target_code}提取没有提取到特征，检查 bi / xd / k 数量是否设置正确"
            )
            return []

        same_similar_codes = []
        run_args = [
            {
                "code": _c,
                "frequency": frequency,
                "cl_config": cl_config,
                "target_features": target_features,
            }
            for _c in find_codes
            if _c != target_code
        ]
        if run_type == "single":
            # 单进程
            for _r in tqdm(run_args, desc="相似度计算"):
                _xg_r = self.run_by_code(_r)
                if _xg_r is not None:
                    same_similar_codes.append(_xg_r)
        else:
            # 多进程
            with ProcessPoolExecutor(5, mp_context=get_context("spawn")) as executor:
                bar = tqdm(total=len(find_codes), desc="进度")
                for _r in executor.map(self.run_by_code, run_args):
                    if _r is not None:
                        same_similar_codes.append(_r)
                    bar.update(1)

        return same_similar_codes

    def run_by_code(self, args: dict):
        code = args["code"]
        frequency = args["frequency"]
        cl_config = args["cl_config"]
        target_features: XGFeatures = args["target_features"]

        try:
            klines = get_exchange(Market(self.market)).klines(code, frequency)
            cd = web_batch_get_cl_datas(
                self.market, code, {frequency: klines}, cl_config
            )[0]
            features = self.extract_cd_features(cd)
            if features is None:
                return None

            # 如果目标最后一笔有买卖点或背驰，优先比对要匹配的股票，最后一笔是否也有其中任意一个买卖点或背驰
            if len(target_features.bi_mmds) > 0 or len(target_features.bi_bcs) > 0:
                mmd_bc_is_ok = False
                if len(set(target_features.bi_mmds) & set(features.bi_mmds)) > 0:
                    mmd_bc_is_ok = True
                if len(set(target_features.bi_bcs) & set(features.bi_bcs)) > 0:
                    mmd_bc_is_ok = True
                if mmd_bc_is_ok is False:
                    return None  # 背驰或买卖点都没有
            similarity = self.combined_similarity(
                target_features,
                features,
            )
            return {
                "code": code,
                "similarity": similarity,
            }
        except Exception as e:
            self.logger.error(f"{code} 处理异常：{e}")
            return None

    def extract_cd_features(self, cd: ICL) -> Union[XGFeatures, None]:
        if (
            len(cd.get_src_klines()) < self.k_num
            or len(cd.get_bis()) < self.bi_num
            or len(cd.get_xds()) < self.xd_num
        ):
            return None

        # 笔的特征
        bi_features = []
        for i in range(1, self.bi_num + 1):
            bi = cd.get_bis()[-i]
            bi_features.append(
                (
                    bi.end.val,
                    bi.end.k.k_index,
                )
            )
        bi_features.reverse()
        # 线段的特征
        xd_features = []
        for i in range(1, self.xd_num + 1):
            xd = cd.get_xds()[-i]
            xd_features.append(
                (
                    xd.end.val,
                    xd.end.k.k_index,
                )
            )
        xd_features.reverse()

        # k线的特征
        k_features = [(_k.c, _k.a) for _k in cd.get_src_klines()[-self.k_num :]]

        # 笔的买卖点/背驰特征
        bi = cd.get_bis()[-1]
        bi_mmds = list(set(bi.line_mmds("|")))
        bi_bcs = list(set(bi.line_bcs("|")))

        xg_features = XGFeatures(
            k_features=k_features,
            bi_features=bi_features,
            xd_features=xd_features,
            bi_mmds=bi_mmds,
            bi_bcs=bi_bcs,
        )
        return xg_features

    def normalize_segment(self, segment: List[Tuple[float, int]]) -> np.array:
        """
        标准化处理线段数据
        :param segment: 线段数据列表，每个元素是(价格, k线数量)的元组
        :return: 标准化后的numpy数组
        """
        # 转换为numpy数组
        data = np.array(segment)

        # 分离价格和时间
        prices = data[:, 0]
        times = data[:, 1]

        # 标准化价格和时间 (0-1范围)
        norm_prices = (prices - np.min(prices)) / (np.max(prices) - np.min(prices))
        norm_times = (times - np.min(times)) / (np.max(times) - np.min(times))

        # 组合成二维特征
        normalized = np.column_stack((norm_prices, norm_times))

        return normalized

    def calculate_similarity(
        self, segment1: List[Tuple[float, int]], segment2: List[Tuple[float, int]]
    ) -> float:
        """
        计算两个线段的相似度得分 (0-1之间，1表示完全相同)
        :param segment1: 第一个线段数据
        :param segment2: 第二个线段数据
        :return: 相似度得分
        """
        # 标准化处理
        norm_seg1 = self.normalize_segment(segment1)
        norm_seg2 = self.normalize_segment(segment2)

        # 计算DTW距离
        distance = dtw_ndim.distance(norm_seg1, norm_seg2)

        # 将距离转换为相似度 (0-1)
        max_distance = np.sqrt(2) * max(len(segment1), len(segment2))  # 最大可能距离
        similarity = 1 - (distance / max_distance)

        return max(0, min(1, similarity))  # 确保在0-1范围内

    def calculate_angles(self, segment: List[Tuple[float, int]]) -> List[float]:
        """
        计算线段中每相邻两点间的角度
        :param segment: 线段数据列表，每个元素是(价格, k线数量)的元组
        :return: 角度列表(弧度制)
        """
        angles = []
        for i in range(1, len(segment)):
            x1, y1 = segment[i - 1][1], segment[i - 1][0]  # 前一点的x(k线数量),y(价格)
            x2, y2 = segment[i][1], segment[i][0]  # 当前点的x,y

            dx = x2 - x1
            dy = y2 - y1

            if dx == 0:  # 避免除以0
                angle = math.pi / 2  # 垂直
            else:
                angle = math.atan(dy / dx)

            angles.append(angle)

        return angles

    def angle_similarity(self, angles1: List[float], angles2: List[float]) -> float:
        """
        计算两个角度序列的相似度
        :param angles1: 第一个角度序列
        :param angles2: 第二个角度序列
        :return: 相似度得分(0-1)
        """
        min_len = min(len(angles1), len(angles2))
        if min_len == 0:
            return 0.0

        # 计算角度差的余弦相似度
        dot_product = 0
        norm1 = 0
        norm2 = 0

        for i in range(min_len):
            dot_product += math.cos(angles1[i] - angles2[i])
            norm1 += math.cos(angles1[i]) ** 2
            norm2 += math.cos(angles2[i]) ** 2

        # 避免除以0
        if norm1 == 0 or norm2 == 0:
            return 0.0

        norm1 = math.sqrt(norm1)
        norm2 = math.sqrt(norm2)

        similarity = dot_product / (norm1 * norm2)
        return max(0, min(1, similarity))  # 确保在0-1之间

    def calculate_kline_similarity(
        self,
        target: List[Tuple[float, float]],
        compare: List[Tuple[float, float]],
    ):
        """
        计算两个K线序列的相似度
        :param target: 目标K线序列
        :param compare: 比较K线序列
        :param method: 计算方法(fast/normal/precise)
        :return: 相似度分数(0-1)
        """
        norm_target = self.normalize_segment(target)
        norm_compare = self.normalize_segment(compare)

        # 精确算法 - 多维DTW
        distance = dtw_ndim.distance(norm_target, norm_compare)
        if min(len(norm_target), len(norm_compare)) < 30:
            # 短序列使用理论最大距离
            max_dist = np.sqrt(norm_target.shape[1]) * max(
                len(norm_target), len(norm_compare)
            )
            return 1 - (distance / max_dist)
        else:
            # 长序列使用经验值（动态计算合适的除数）
            base_divisor = 10 * (norm_target.shape[1] / 2)  # 考虑维度数
            dynamic_divisor = base_divisor * (
                np.mean([len(norm_target), len(norm_compare)]) / 100
            )
            return max(0, 1 - distance / dynamic_divisor)

    def combined_similarity(
        self,
        target_features: XGFeatures,
        compare_features: XGFeatures,
    ) -> float:
        """
        计算组合相似度
        :return: 综合相似度(0-1)
        """
        # 计算形状相似度
        bi_shape_sim = self.calculate_similarity(
            target_features.bi_features, compare_features.bi_features
        )
        xd_shape_sim = self.calculate_similarity(
            target_features.xd_features, compare_features.xd_features
        )

        # 计算角度相似度
        # target_bi_angles = self.calculate_angles(target_features.bi_features)
        # compare_bi_angles = self.calculate_angles(compare_features.bi_features)
        # bi_angle_sim = self.angle_similarity(target_bi_angles, compare_bi_angles)

        # target_duan_angles = self.calculate_angles(target_features.xd_features)
        # compare_duan_angles = self.calculate_angles(compare_features.xd_features)
        # duan_angle_sim = self.angle_similarity(target_duan_angles, compare_duan_angles)

        # # 组合形状和角度相似度(各50%)
        # bi_combined = 0.5 * bi_shape_sim + 0.5 * bi_angle_sim
        # duan_combined = 0.5 * duan_shape_sim + 0.5 * duan_angle_sim

        bi_combined = bi_shape_sim
        xd_combined = xd_shape_sim

        # K线的相似度
        k_combined = self.calculate_kline_similarity(
            target_features.k_features, compare_features.k_features
        )

        # 最终加权平均
        total_similarity = (
            self.k_weight * k_combined
            + self.bi_weight * bi_combined
            + self.xd_weight * xd_combined
        )

        return max(0, min(1, total_similarity))


if __name__ == "__main__":

    xgs = XuanguBySame("a")
    cl_config = query_cl_chart_config("a", "SH.000001")

    all_codes = get_exchange(Market(xgs.market)).all_stocks()
    all_codes = [
        c["code"]
        for c in all_codes
        if c["code"].startswith("SH.60")
        or c["code"].startswith("SZ.00")
        or c["code"].startswith("SZ.30")
    ]

    print(f"获取股票代码：{len(all_codes)}")

    res_similarity = xgs.run(
        target_code="SZ.300149",
        frequency="d",
        target_end_datetime=None,
        find_codes=all_codes,
        cl_config=cl_config,
        run_type="process",
    )
    # print(res_similarity)
    # 按照 相似度排序
    res_similarity = sorted(res_similarity, key=lambda x: x["similarity"], reverse=True)
    # 输出前 10
    for i in res_similarity[:10]:
        print(i)
    with open(r"D:\xuangu_similar.json", "w", encoding="utf-8") as f:
        json.dump(res_similarity, f, ensure_ascii=False)
