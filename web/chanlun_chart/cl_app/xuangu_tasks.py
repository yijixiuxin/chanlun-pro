import datetime
from typing import Dict, List

from apscheduler.schedulers.background import BackgroundScheduler
from chanlun import zixuan
from chanlun import fun
from chanlun import utils
from chanlun.exchange import Market, get_exchange
from chanlun.xuangu import xuangu
from chanlun.trader.online_market_datas import OnlineMarketDatas
from tqdm.auto import tqdm
from chanlun.cl_utils import query_cl_chart_config, web_batch_get_cl_datas
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context

log = fun.get_logger()

# 选股运行配置
xuangu_task_configs: Dict[str, Dict[str, object]] = {
    "xg_single_bi_1mmd": {
        "name": "笔的一类买卖点",
        "task_fun": xuangu.xg_single_bi_1mmd,
        "task_memo": "笔的一类买卖点，并且所在的中枢笔数量小于9",
        "frequency_num": 1,
        "frequency_memo": "单周期",
    },
    "xg_single_bi_2mmd": {
        "name": "笔的二类买卖点",
        "task_fun": xuangu.xg_single_bi_2mmd,
        "task_memo": "笔的二类买卖点，并且所在的中枢笔数量小于9",
        "frequency_num": 1,
        "frequency_memo": "单周期",
    },
    "xg_single_bi_3mmd": {
        "name": "笔的三类买卖点",
        "task_fun": xuangu.xg_single_bi_3mmd,
        "task_memo": "笔的三类买卖点，并且所在的中枢笔数量小于9",
        "frequency_num": 1,
        "frequency_memo": "单周期",
    },
    "xg_single_find_3buy_by_1buy": {
        "name": "一类买卖点后的三类买卖点",
        "task_fun": xuangu.xg_single_find_3buy_by_1buy,
        "task_memo": "找三类买卖点，前提是前面中枢内有一类买卖点（不同的中枢配置，筛选的条件会有差异）",
        "frequency_num": 1,
        "frequency_memo": "单周期",
    },
    "xg_single_find_3buy_by_zhuanzhe": {
        "name": "趋势下跌后的三类买卖点",
        "task_fun": xuangu.xg_single_find_3buy_by_zhuanzhe,
        "task_memo": "找三类买卖点，之前段内要有是一个上涨/下跌趋势，后续趋势结束，出现转折中枢的三买（缠论的笔中枢配置要是段内中枢）",
        "frequency_num": 1,
        "frequency_memo": "单周期",
    },
    "xg_single_xd_and_bi_mmd": {
        "name": "线段和笔都有出现买点",
        "task_fun": xuangu.xg_single_xd_and_bi_mmd,
        "task_memo": "线段和笔都有出现买点",
        "frequency_num": 1,
        "frequency_memo": "单周期",
    },
    "xg_multiple_xd_bi_mmd": {
        "name": "高级别线段买点或背驰，并且次级别笔买点或背驰",
        "task_fun": xuangu.xg_multiple_xd_bi_mmd,
        "task_memo": "高级别线段买点或背驰，并且次级别笔买点或背驰",
        "frequency_num": 2,
        "frequency_memo": "两个周期",
    },
    "xg_multiple_low_level_12mmd": {
        "name": "高级别出现背驰或者买卖点，并且低级别中出现一二类买点",
        "task_fun": xuangu.xg_multiple_low_level_12mmd,
        "task_memo": "高级别出现背驰或者买卖点，并且在低级别中，其中有任意一个低级别有出现过1/2类买点",
        "frequency_num": 3,
        "frequency_memo": "三个周期",
    },
    "xg_single_ma_250": {
        "name": "均线250选股",
        "task_fun": xuangu.xg_single_ma_250,
        "task_memo": "最新价格在均线 250 之上 或者 之下",
        "frequency_num": 1,
        "frequency_memo": "一个周期",
    },
}


def process_xuangu_by_code(args):
    try:
        code, market, frequencys, task_name, opt_types = args
        ex = get_exchange(Market(market))
        cl_config = query_cl_chart_config(market, "----")
        task_fun = xuangu_task_configs[task_name]["task_fun"]
        mk_datas = OnlineMarketDatas(market, frequencys, ex, cl_config, use_cache=False)
        xg_res = task_fun(code, mk_datas, opt_types)
        return xg_res
    except Exception as e:
        tqdm.write(f"{market} {code} {frequencys} 执行选股任务 {task_name} 失败：{e}")
        return None


def process_xuangu_task(
    market: str,
    task_name: str,
    freqs: List[str],
    opt_types: List[str],
    src_zx_group: str,
    target_zx_group: str,
):
    """
    执行选股的任务
    """
    log.info(f"{market} 开始执行选股任务 {task_name}")
    try:
        zx = zixuan.ZiXuan(market)
        ex = get_exchange(Market(market))
        run_codes = []
        if src_zx_group == "all":
            # 获取交易所下的股票代码
            run_codes = [_s["code"] for _s in ex.all_stocks()]
            if market == "a":
                run_codes = [
                    _c
                    for _c in run_codes
                    if _c[0:5] in ["SZ.00", "SZ.30", "SH.60", "SH.68"]
                ]
            if market == "futures":
                run_codes = [_c for _c in run_codes if _c[-2:] == "L8"]
        else:
            run_codes = zx.zx_stocks(src_zx_group)
            run_codes = [_s["code"] for _s in run_codes]

        tqdm.write(f"{market} {task_name} 选股任务开始，选股代码数量 {len(run_codes)}")
        zx.clear_zx_stocks(target_zx_group)

        # 多进程版本
        # with ProcessPoolExecutor(
        #     max_workers=5,
        #     mp_context=get_context("spawn"),
        # ) as executor:
        #     bar = tqdm(run_codes, desc="选股进度")
        #     for _xg_res in executor.map(
        #         process_xuangu_by_code,
        #         [(_c, market, freqs, task_name, opt_types) for _c in run_codes],
        #         chunksize=5,
        #     ):
        #         bar.update(1)
        #         if _xg_res is not None:
        #             tqdm.write(
        #                 f"{market} {task_name} 选择 {_xg_res['code']} : {_xg_res['msg']}"
        #             )
        #             zx.add_stock(target_zx_group, _xg_res["code"], None)

        # 单进程版本
        for _code in tqdm(run_codes, desc="选股进度"):
            _xg_res = process_xuangu_by_code(
                (_code, market, freqs, task_name, opt_types)
            )
            if _xg_res is not None:
                tqdm.write(
                    f"{market} {task_name} 选择 {_xg_res['code']} : {_xg_res['msg']}"
                )
                zx.add_stock(target_zx_group, _xg_res["code"], None)

        xg_stocks = zx.zx_stocks(target_zx_group)
        utils.send_fs_msg(
            market,
            f"选股任务：{xuangu_task_configs[task_name]['name']}",
            f"{xuangu_task_configs[task_name]['name']} 选股完成，选出 {len(xg_stocks)} 只代码",
        )

    except Exception as e:
        tqdm.write(f"{market} {task_name} 异常 ：{e}")
    return True


class XuanguTasks(object):
    def __init__(self, scheduler: BackgroundScheduler):
        self.scheduler = scheduler

    def xuangu_task_config_list(self) -> dict:
        return xuangu_task_configs

    def run_xuangu(
        self,
        market: str,
        xuangu_task_name: str,
        freqs: List[str],
        opt_type: List[str],
        src_zx_group: str,
        target_zx_group: str,
    ):
        """
        执行选个股
        """
        if xuangu_task_name not in xuangu_task_configs.keys():
            return False

        task_id = f"{market}_{xuangu_task_name}"
        if (
            task_id in self.scheduler.my_task_list.keys()
            and self.scheduler.my_task_list[task_id]["state"] != "已完成"
        ):
            return False

        task_name = f"{market}:{xuangu_task_configs[xuangu_task_name]['name']} {freqs} -> 【{target_zx_group}】"

        self.scheduler.add_job(
            func=process_xuangu_task,
            args=(
                market,
                xuangu_task_name,
                freqs,
                opt_type,
                src_zx_group,
                target_zx_group,
            ),
            trigger="date",
            next_run_time=datetime.datetime.now(),
            id=task_id,
            name=task_name,
        )
        return True


if __name__ == "__main__":
    xt = XuanguTasks(None)

    # print(xt.xuangu_list())

    print(process_xuangu_task("a", "xg_single_xd_and_bi_mmd", ["d"], "测试"))
