import time
import MyTT
import numpy as np
import requests
import talib

from chanlun.cl_interface import ICL
from chanlun.exchange import get_exchange, Market
from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from chanlun import config, fun
import json, datetime
from chanlun.db import db, TableByAIAnalyse


class AIAnalyse:

    def __init__(self, market: str):
        self.market = market
        self.ex = get_exchange(market=Market(self.market))

        # AI 配置
        self.token = config.AI_TOKEN
        self.model = config.AI_MODEL

        self.map_dircetion_type = {"up": "向上", "down": "向下"}
        self.map_zs_type = {"up": "上涨中枢", "down": "下跌中枢", "zd": "震荡中枢"}
        self.map_config_zs_type = {
            "zs_type_bz": "标准中枢",
            "zs_type_dn": "段内中枢",
            "zs_type_fx": "方向中枢",
            "zs_type_fl": "分类中枢",
        }

    def analyse(self, code: str, frequency: str) -> dict:
        """
        获取缠论数据，生成 prompt，调用接口进行分析，并返回数据
        """

        if not self.token:
            return {"ok": False, "msg": "请先配置 AI_TOKEN"}
        if not self.model:
            return {"ok": False, "msg": "请先配置 AI_MODEL"}

        cl_config = query_cl_chart_config(self.market, code)
        stock = self.ex.stock_info(code)
        klines = self.ex.klines(code, frequency)
        cds = web_batch_get_cl_datas(self.market, code, {frequency: klines}, cl_config)
        cd = cds[0]
        try:
            prompt = self.prompt(cd=cd)
        except Exception as e:
            return {"ok": False, "msg": f"获取缠论当前 Prompt 异常：{e}"}

        analyse_res = self.req_ai_model(prompt)

        if analyse_res["ok"]:
            # 记录数据库
            with db.Session() as session:
                record = TableByAIAnalyse(
                    market=self.market,
                    stock_code=code,
                    stock_name=stock["name"],
                    frequency=frequency,
                    model=self.model,
                    msg=analyse_res["msg"],
                    prompt=prompt,
                    dt=datetime.datetime.now(),
                )
                session.add(record)
                session.commit()

        return analyse_res

    def analyse_records(self, limit: int = 20):
        """
        返回市场中历史的分析记录
        """
        with db.Session() as session:
            records = (
                session.query(TableByAIAnalyse)
                .filter(TableByAIAnalyse.market == self.market)
                .order_by(TableByAIAnalyse.dt.desc())
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
        return record_dicts

    def prompt(self, cd: ICL) -> str:

        # 设置数值的精度变量
        precision = 2

        k = cd.get_src_klines()[-1]
        prompt = "使用缠论技术分析以下行情\n"
        prompt += f"当前品种 {cd.get_code()} 在 {cd.get_frequency()} 周期，当前时间 {fun.datetime_to_str(k.date)} 最新价格 {round(k.c, precision)}：\n"

        if len(cd.get_bis()) >= 1:
            bi = cd.get_bis()[-1]
            prompt += f"最新笔时间：[{fun.datetime_to_str(bi.start.k.date)} / {fun.datetime_to_str(bi.end.k.date)}] 笔方向:{self.map_dircetion_type[bi.type]}，笔起始值 {round(bi.start.val, precision)}，笔结束值 {round(bi.end.val, precision)}，笔完成状态：{bi.is_done()}，笔买点：{bi.line_mmds('|')}，笔背驰：{bi.line_bcs('|')}\n"
        if len(cd.get_bis()) >= 2:
            bi = cd.get_bis()[-2]
            prompt += f"上一笔时间：[{fun.datetime_to_str(bi.start.k.date)} / {fun.datetime_to_str(bi.end.k.date)}] 笔方向:{self.map_dircetion_type[bi.type]}，笔起始值 {round(bi.start.val, precision)}，笔结束值 {round(bi.end.val, precision)}，笔完成状态：{bi.is_done()}，笔买点：{bi.line_mmds('|')}，笔背驰：{bi.line_bcs('|')}\n"
        if len(cd.get_xds()) >= 1:
            xd = cd.get_xds()[-1]
            prompt += f"最新线段时间：[{fun.datetime_to_str(xd.start.k.date)} / {fun.datetime_to_str(xd.end.k.date)}] 线段方向:{self.map_dircetion_type[xd.type]}，线段起始值 {round(xd.start.val, precision)}，线段结束值 {round(xd.end.val, precision)}，线段完成状态：{xd.is_done()}，线段买点：{xd.line_mmds('|')}，线段背驰：{xd.line_bcs('|')}\n"

        for zs_type in cd.get_config()["zs_bi_type"]:
            if len(cd.get_bi_zss(zs_type)) >= 1:
                zs = cd.get_bi_zss(zs_type)[-1]
                prompt += f"最新 {self.map_config_zs_type[zs_type]} 笔中枢时间：[{fun.datetime_to_str(zs.start.k.date)} / {fun.datetime_to_str(zs.end.k.date)}] 中枢方向：{self.map_zs_type[zs.type]}，中枢最高值 {round(zs.gg, precision)}，中枢最低值 {round(zs.dd, precision)}，中枢高点 {round(zs.zg, precision)}，中枢低点 {round(zs.zd, precision)}，中枢级别：{zs.level}\n"

        # 计算均线，5/10/20
        closes = np.array([_k.c for _k in cd.get_klines()])
        highs = np.array([_k.h for _k in cd.get_klines()])
        lows = np.array([_k.l for _k in cd.get_klines()])

        for m in [5, 10, 20]:
            idx_ma = talib.MA(closes, timeperiod=m)
            prompt += f"最新{m}均线值：{[round(_i, precision) for _i in idx_ma[-5:]]}\n"

        # 计算 DMI 指标
        PDI, MDI, ADX, ADXR = MyTT.DMI(closes, highs, lows, M1=14, M2=6)

        prompt += f"最新 DMI 指标：\n"
        prompt += f"\t PDI: {[round(_i, precision) for _i in PDI[-5:]]}\n"
        prompt += f"\t MDI: {[round(_i, precision) for _i in MDI[-5:]]}\n"
        prompt += f"\t ADX: {[round(_i, precision) for _i in ADX[-5:]]}\n"
        prompt += f"\t ADXR: {[round(_i, precision) for _i in ADXR[-5:]]}\n"

        prompt += "请给出操作建议"
        return prompt

    def req_ai_model(self, prompt: str) -> dict:

        # TODO 测试
        # msg = "根据缠论技术分析，结合当前行情数据，以下是操作建议：\n\n### 1. **当前笔分析**\n   - **最新笔**：方向向上，起始值3140.98，结束值3274.39，尚未完成（笔完成状态为False）。\n   - **上一笔**：方向向下，起始值3418.95，结束值3140.98，已完成（笔完成状态为True）。\n   - **当前笔尚未完成**，且处于上升趋势中。如果价格继续上涨并突破3274.39，可能会形成新的上升笔。如果价格回落并跌破3140.98，则当前笔可能结束并形成新的下降笔。\n\n### 2. **当前线段分析**\n   - **最新线段**：方向向下，起始值3674.41，结束值3140.98，尚未完成（线段完成状态为False）。\n   - **当前线段处于下降趋势中**，但最新笔的上升可能预示着线段的调整或反转。如果价格继续上涨并突破3674.41，则可能结束当前的下降线段并开始新的上升线段。\n\n### 3. **中枢分析**\n   - **标准中枢**：震荡中枢，最高值3674.41，最低值3140.98，中枢高点3509.82，中枢低点3227.35。\n   - **段内中枢**：震荡中枢，最高值3509.82，最低值3140.98，中枢高点3494.87，中枢低点3227.35。\n   - **当前价格3250.6位于标准中枢的中枢低点3227.35和中枢高点3509.82之间**。如果价格继续上涨并突破3509.82，可能会进入强势区域；如果价格回落并跌破3227.35，则可能进入弱势区域。\n\n### 4. **均线分析**\n   - **5日均线**：最新值为3238.39、3235.69、3234.52、3236.68、3237.93。\n   - **10日均线**：最新值为3220.37、3218.72、3220.6、3229.01、3237.99。\n   - **20日均线**：最新值为3272.92、3263"
        # return {"ok": True, "msg": msg}

        url = "https://api.siliconflow.cn/v1/chat/completions"

        payload = {
            "model": self.model,
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
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        response = requests.request("POST", url, json=payload, headers=headers)
        try:
            ai_res = json.loads(response.text)
        except Exception as e:
            print("解析JSON 报错，返回的数据：", response.text)
            return {"ok": False, "msg": f"JSON 解析异常：{e}"}

        if response.status_code != 200:
            return {"ok": False, "msg": f"AI 接口调用失败：{ai_res['message']}"}

        msg = ai_res["choices"][0]["message"]["content"]
        return {"ok": True, "msg": msg}


if __name__ == "__main__":
    ai = AIAnalyse("a")
    print(ai.analyse("SH.000001", "d"))
    print(ai.analyse_records())
