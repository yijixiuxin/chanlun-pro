import datetime
from typing import Union

from chanlun import fun
from chanlun.cl_interface import BI, ICL, XD
from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from chanlun.db import TableByAIAnalyse, db
from chanlun.exchange import Market, get_exchange
from chanlun.tools.ai_client import request_ai_model
from chanlun.tools.ai_predict import format_recent_kline_ma_table


class AIAnalyse:
    def __init__(self, market: str):
        self.market = market
        self.ex = get_exchange(market=Market(self.market))

        self.map_dircetion_type = {"up": "向上", "down": "向下"}
        self.map_zs_type = {"up": "上涨中枢", "down": "下跌中枢", "zd": "震荡中枢"}
        self.map_zss_direction = {
            "up": "上涨趋势",
            "down": "下跌趋势",
            None: "中枢扩展",
        }
        self.map_config_zs_type = {
            "zs_type_bz": "标准中枢",
            "zs_type_dn": "段内中枢",
            "zs_type_fx": "方向中枢",
            "zs_type_fl": "分类中枢",
        }
        self.map_mmd_type = {
            "1buy": "一买",
            "1sell": "一卖",
            "2buy": "二买",
            "2sell": "二卖",
            "3buy": "三买",
            "3sell": "三卖",
            "l2buy": "类二买",
            "l2sell": "类二卖",
            "l3buy": "类三买",
            "l3sell": "类三卖",
        }
        self.map_bc_type = {
            "bi": "笔背驰",
            "xd": "线段背驰",
            "pz": "盘整背驰",
            "qs": "趋势背驰",
        }

    def analyse(self, code: str, frequency: str) -> dict:
        """
        获取缠论数据，生成 prompt，调用接口进行分析，并返回数据
        """

        cl_config = query_cl_chart_config(self.market, code)
        stock = self.ex.stock_info(code)
        klines = self.ex.klines(code, frequency)
        cds = web_batch_get_cl_datas(self.market, code, {frequency: klines}, cl_config)
        cd = cds[0]
        try:
            prompt = self.prompt(cd=cd)
        except Exception as e:
            return {"ok": False, "msg": f"获取缠论当前 Prompt 异常：{e}"}

        analyse_res = self.req_llm_ai_model(prompt)

        if analyse_res["ok"]:
            # 记录数据库
            with db.Session() as session:
                record = TableByAIAnalyse(
                    market=self.market,
                    stock_code=code,
                    stock_name=stock["name"],
                    frequency=frequency,
                    model=analyse_res["model"],
                    msg=analyse_res["msg"],
                    prompt=prompt,
                    dt=datetime.datetime.now(),
                )
                session.add(record)
                session.commit()

        return {"ok": True, "msg": analyse_res["msg"]}

    def analyse_records(self, page: int = 1, limit: int = 20):
        """
        返回市场中历史的分析记录（支持分页）

        Args:
            page: 页码，从1开始
            limit: 每页记录数

        Returns:
            tuple: (记录列表, 总记录数)
        """
        with db.Session() as session:
            # 查询总记录数
            total = (
                session.query(TableByAIAnalyse)
                .filter(TableByAIAnalyse.market == self.market)
                .count()
            )

            # 分页查询记录
            offset = (page - 1) * limit
            records = (
                session.query(TableByAIAnalyse)
                .filter(TableByAIAnalyse.market == self.market)
                .order_by(TableByAIAnalyse.dt.desc())
                .offset(offset)
                .limit(limit)
                .all()
            )

        record_dicts = []
        for _r in records:
            _dr = _r.__dict__
            # 删除 _dr 中的 _sa_instance_state
            if "_sa_instance_state" in _dr.keys():
                del _dr["_sa_instance_state"]
            # 时间转换
            _dr["dt"] = fun.datetime_to_str(_dr["dt"])
            record_dicts.append(_dr)
        return record_dicts, total

    def get_line_mmds(self, line: Union[BI, XD]):
        mmds = list(set(line.line_mmds("|")))
        mmds = [self.map_mmd_type[_m] for _m in mmds]
        return "|".join(mmds)

    def get_line_bcs(self, line: Union[BI, XD]):
        bcs = list(set(line.line_bcs("|")))
        bcs = [self.map_bc_type[_b] for _b in bcs]
        return "|".join(bcs)

    def prompt(self, cd: ICL) -> str:
        stock_info = self.ex.stock_info(cd.get_code())
        if stock_info is None:
            raise Exception(f"股票信息获取失败 {cd.get_code()}")
        stock_name = stock_info["name"]

        # 设置数值的精度变量
        precision = (
            len(str(stock_info["precision"])) - 1
            if "precision" in stock_info.keys()
            else 2
        )

        k = cd.get_src_klines()[-1]
        # Markdown 格式的提示词
        prompt = "```markdown\n# 缠论技术分析\n\n"
        prompt += "请根据以下缠论数据，分析后续可能走势，并按照概率排序输出。\n\n"
        prompt += "**输出格式：Markdown**\n\n"
        prompt += f"## 当前品种\n- **代码/名称**：`{cd.get_code()} - {stock_name}`\n- **数据周期**：`{cd.get_frequency()}`\n- **当前时间**：`{fun.datetime_to_str(k.date)}`\n- **最新价格**：`{round(k.c, precision)}`\n\n"

        # 最近 K 线与均线结构化数据
        prompt += "## 最近20根K线与均线表格\n\n"
        prompt += "以下表格按时间升序排列，字段包含时间、开高低收、成交量、5日均线、10日均线，可用于判断短期量价与均线结构。\n\n"
        prompt += f"{format_recent_kline_ma_table(cd.get_src_klines(), precision)}\n\n"

        # 笔数据
        bis_count = 9 if len(cd.get_bis()) >= 9 else len(cd.get_bis())
        prompt += f"## 最新的 {bis_count} 条缠论笔数据\n\n"
        prompt += "| 起始时间 | 结束时间 | 方向 | 起始值 | 完成状态 | 买点 | 背驰 |\n"
        prompt += "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|\n"
        for bi in cd.get_bis()[-9:]:
            prompt += f"| {fun.datetime_to_str(bi.start.k.date)} | {fun.datetime_to_str(bi.end.k.date)} | {self.map_dircetion_type[bi.type]} | {round(bi.start.val, precision)} - {round(bi.end.val, precision)} | {bi.is_done()} | {self.get_line_mmds(bi)} | {self.get_line_bcs(bi)} |\n"
        prompt += "\n"

        # 线段数据
        xds_count = 3 if len(cd.get_xds()) >= 3 else len(cd.get_xds())
        prompt += f"## 最新的 {xds_count} 条缠论线段数据\n\n"
        prompt += "| 起始时间 | 结束时间 | 方向 | 起始值 | 完成状态 | 买点 | 背驰 |\n"
        prompt += "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|\n"
        for xd in cd.get_xds()[-3:]:
            prompt += f"| {fun.datetime_to_str(xd.start.k.date)} | {fun.datetime_to_str(xd.end.k.date)} | {self.map_dircetion_type[xd.type]} | {round(xd.start.val, precision)} - {round(xd.end.val, precision)} | {xd.is_done()} | {self.get_line_mmds(xd)} | {self.get_line_bcs(xd)} |\n"
        prompt += "\n"

        # 中枢数据
        for zs_type in cd.get_config()["zs_bi_type"]:
            zss = cd.get_bi_zss(zs_type)
            if len(zss) >= 1:
                prompt += f"### 中枢信息：{self.map_config_zs_type[zs_type]}\n\n"
                if len(zss) >= 2:
                    zs_direction = cd.zss_is_qs(zss[-2], zss[-1])
                    prompt += f"- 最新两个中枢的位置关系：**{self.map_zss_direction[zs_direction]}**\n\n"
                else:
                    prompt += "- 目前只有单个中枢\n\n"
                prompt += "| 起始时间 | 结束时间 | 方向 | 最高值 | 最低值 | 高点 | 低点 | 级别 |\n"
                prompt += "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|\n"
                for zs in zss[-2:]:
                    prompt += f"| {fun.datetime_to_str(zs.start.k.date)} | {fun.datetime_to_str(zs.end.k.date)} | {self.map_zs_type[zs.type]} | {round(zs.gg, precision)} | {round(zs.dd, precision)} | {round(zs.zg, precision)} | {round(zs.zd, precision)} | {zs.level} |\n"
                prompt += "\n"

        prompt += "> **数据说明**：中枢级别的意思，1表示是本级别，根据中枢内的线段数量计算，小于等于9表示本级别，大于1表示中枢内的线段大于9，中枢级别升级 (计算公式: `round(max([1, zs.line_num / 9]), 2)`)\n\n"
        prompt += "---\n"
        prompt += "请根据以上提供的笔/线段/中枢数据，进行分析。"
        return prompt

    def req_llm_ai_model(self, prompt: str) -> dict:
        """
        根据配置，调用 OpenAI 兼容的大模型服务
        """
        return request_ai_model(prompt)


if __name__ == "__main__":
    ai = AIAnalyse("a")
    print(ai.analyse("SH.000001", "d"))
    # print(ai.analyse_records())
