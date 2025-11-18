import time
from typing import Union
import MyTT
import numpy as np
import openai
import requests
import talib

from chanlun.cl_interface import BI, ICL, XD
from chanlun.exchange import get_exchange, Market
from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from chanlun import config, fun
import json, datetime
from chanlun.db import db, TableByAIAnalyse


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
        根据配置，调用不同的大模型服务
        """
        if config.OPENROUTER_AI_KEYS != "" and config.OPENROUTER_AI_MODEL != "":
            return self.req_openrouter_ai_model(prompt)
        if config.AI_TOKEN != "" and config.AI_MODEL != "":
            return self.req_siliconflow_ai_model(prompt)

        return {
            "ok": False,
            "msg": "未正确配置大模型的 API key 和模型名称",
            "model": "",
        }

    def req_siliconflow_ai_model(self, prompt: str) -> dict:

        # TODO 测试
        # msg = "根据缠论技术分析，结合当前行情数据，以下是操作建议：\n\n### 1. **当前笔分析**\n   - **最新笔**：方向向上，起始值3140.98，结束值3274.39，尚未完成（笔完成状态为False）。\n   - **上一笔**：方向向下，起始值3418.95，结束值3140.98，已完成（笔完成状态为True）。\n   - **当前笔尚未完成**，且处于上升趋势中。如果价格继续上涨并突破3274.39，可能会形成新的上升笔。如果价格回落并跌破3140.98，则当前笔可能结束并形成新的下降笔。\n\n### 2. **当前线段分析**\n   - **最新线段**：方向向下，起始值3674.41，结束值3140.98，尚未完成（线段完成状态为False）。\n   - **当前线段处于下降趋势中**，但最新笔的上升可能预示着线段的调整或反转。如果价格继续上涨并突破3674.41，则可能结束当前的下降线段并开始新的上升线段。\n\n### 3. **中枢分析**\n   - **标准中枢**：震荡中枢，最高值3674.41，最低值3140.98，中枢高点3509.82，中枢低点3227.35。\n   - **段内中枢**：震荡中枢，最高值3509.82，最低值3140.98，中枢高点3494.87，中枢低点3227.35。\n   - **当前价格3250.6位于标准中枢的中枢低点3227.35和中枢高点3509.82之间**。如果价格继续上涨并突破3509.82，可能会进入强势区域；如果价格回落并跌破3227.35，则可能进入弱势区域。\n\n### 4. **均线分析**\n   - **5日均线**：最新值为3238.39、3235.69、3234.52、3236.68、3237.93。\n   - **10日均线**：最新值为3220.37、3218.72、3220.6、3229.01、3237.99。\n   - **20日均线**：最新值为3272.92、3263"
        # return {"ok": True, "msg": msg}

        url = "https://api.siliconflow.cn/v1/chat/completions"

        payload = {
            "model": config.AI_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "stream": False,
            "max_tokens": 4096,
            "stop": ["null"],
            "temperature": 0.7,
            "top_p": 0.7,
            "top_k": 50,
            "frequency_penalty": 0.5,
            "n": 1,
            "response_format": {"type": "text"},
        }
        headers = {
            "Authorization": f"Bearer {config.AI_TOKEN}",
            "Content-Type": "application/json",
        }

        response = requests.request("POST", url, json=payload, headers=headers)
        try:
            ai_res = json.loads(response.text)
        except Exception as e:
            print("解析JSON 报错，返回的数据：", response.text)
            return {"ok": False, "msg": f"JSON 解析异常：{e}", "model": config.AI_MODEL}

        if response.status_code != 200:
            return {
                "ok": False,
                "msg": f"AI 接口调用失败：{ai_res['message']}",
                "model": config.AI_MODEL,
            }

        msg = ai_res["choices"][0]["message"]["content"]
        return {"ok": True, "msg": msg, "model": config.AI_MODEL}

    def req_openrouter_ai_model(self, prompt: str) -> dict:
        """
        调用大语言模型，返回回答内容。
        :param key: OpenRouter API Key
        :param model: 模型名称（如 openai/gpt-4o）
        :param prompt: 问题内容（如 markdown 格式）
        :return: 回答内容
        """

        try:
            client = openai.OpenAI(
                api_key=config.OPENROUTER_AI_KEYS,
                base_url="https://openrouter.ai/api/v1",
            )
            response = client.chat.completions.create(
                model=config.OPENROUTER_AI_MODEL,
                messages=[{"role": "user", "content": prompt}],
            )
            if (
                response.choices[0].message.content == ""
                and response.choices[0].message.refusal is not None
            ):
                return {
                    "ok": False,
                    "msg": f"**[OpenAI API 错误]**: {response.choices[0].message.refusal}",
                    "model": config.OPENROUTER_AI_MODEL,
                }

            return {
                "ok": True,
                "msg": response.choices[0].message.content,
                "model": config.OPENROUTER_AI_MODEL,
            }
        except openai.OpenAIError as oe:
            return {
                "ok": False,
                "msg": f"**[OpenAI API 错误]**: {str(oe)}",
                "model": config.OPENROUTER_AI_MODEL,
            }
        except Exception as e:
            return {
                "ok": False,
                "msg": f"**[系统异常]**: {str(e)}",
                "model": config.OPENROUTER_AI_MODEL,
            }


if __name__ == "__main__":
    ai = AIAnalyse("a")
    print(ai.analyse("SH.000001", "d"))
    # print(ai.analyse_records())
