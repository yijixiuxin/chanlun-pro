import json
import traceback
import datetime
import time
from typing import Dict, List

from apscheduler.schedulers.background import BackgroundScheduler
from tqdm.auto import tqdm

from chanlun import fun, monitor
from chanlun.cl_utils import query_cl_chart_config
from chanlun.db import TableByAlertTask, db
from chanlun.exchange import Market, get_exchange
from chanlun.zixuan import ZiXuan


class AlertTasks(object):
    def __init__(self, scheduler: BackgroundScheduler):
        """
        异步执行后台定时任务
        """
        self.scheduler: BackgroundScheduler = scheduler
        self.task_ids = []
        self.log = fun.get_logger("alert_tasks.log")

    def run(self):
        for _id in self.task_ids:
            self.scheduler.remove_job(_id)
        self.task_ids = []

        task_list = self.task_list()
        for _t in task_list:
            # print(al)
            if _t.is_run == 1:
                # 根据interval_minutes设置定时任务
                if _t.interval_minutes < 60:
                    # 60分钟以下，按分钟运行
                    _job = self.scheduler.add_job(
                        func=self.alert_run,
                        trigger="cron",
                        args=(_t.id,),
                        id=str(_t.id),
                        name=f"监控-{_t.task_name}",
                        minute=f"*/{_t.interval_minutes}",
                        second="0",
                        coalesce=True,
                        misfire_grace_time=60,
                        max_instances=2,
                    )
                else:
                    # 60分钟及以上，按小时运行
                    hours = _t.interval_minutes // 60
                    _job = self.scheduler.add_job(
                        func=self.alert_run,
                        trigger="cron",
                        args=(_t.id,),
                        id=str(_t.id),
                        name=f"监控-{_t.task_name}",
                        hour=f"*/{hours}",
                        minute="0",
                        second="0",
                        coalesce=True,
                        misfire_grace_time=60,
                        max_instances=2,
                    )

                self.task_ids.append(_job.id)
        return True

    def alert_run(self, alert_id):
        run_start_ts = time.time()
        alert_config = self.alert_get(alert_id)
        ex = get_exchange(Market(alert_config.market))
        if ex.now_trading() is False:
            self.log.info(
                f"跳过 {alert_config.task_name}({alert_config.market})，非交易时间 now={fun.datetime_to_str(datetime.datetime.now())}"
            )
            return True

        zx = ZiXuan(alert_config.market)
        # 获取自选股票
        stocks = zx.zx_stocks(alert_config.zx_group)
        self.log.info(
            f"执行 {alert_config.task_name} 警报提醒，获取 {alert_config.zx_group} 自选组中 {len(stocks)} 数量股票"
        )
        
        # 使用 ThreadPoolExecutor 并发执行监控
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        def _to_bool(v) -> bool:
            if v is None:
                return False
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v != 0
            s = str(v).strip().lower()
            return s in {"1", "true", "yes", "y", "on"}
        
        def process_stock(s):
            try:
                s: Dict[str, str] = s
                cl_config = query_cl_chart_config(alert_config.market, s["code"])
                return monitor.monitoring_code(
                    alert_config.task_name,
                    alert_config.market,
                    s["code"],
                    s["name"],
                    [alert_config.frequency],
                    check_cl_types={
                        "bi_types": alert_config.check_bi_type.split(","),
                        "bi_beichi": alert_config.check_bi_beichi.split(","),
                        "bi_mmd": alert_config.check_bi_mmd.split(","),
                        "xd_types": alert_config.check_xd_type.split(","),
                        "xd_beichi": alert_config.check_xd_beichi.split(","),
                        "xd_mmd": alert_config.check_xd_mmd.split(","),
                    },
                    check_idx_types={
                        "idx_ma": (
                            json.loads(alert_config.check_idx_ma_info)
                            if alert_config.check_idx_ma_info
                            else {"enable": 0}
                        ),
                        "idx_macd": (
                            json.loads(alert_config.check_idx_macd_info)
                            if alert_config.check_idx_macd_info
                            else {"enable": 0}
                        ),
                        "idx_zhixing": (
                            json.loads(alert_config.check_idx_zhixing_info)
                            if hasattr(alert_config, "check_idx_zhixing_info")
                            and alert_config.check_idx_zhixing_info
                            else {"enable": 0}
                        ),
                    },
                    is_send_msg=_to_bool(alert_config.is_send_msg),
                    is_send_img=_to_bool(alert_config.is_send_img)
                    if hasattr(alert_config, "is_send_img")
                    else False,
                    cl_config=cl_config,
                )
            except Exception as e:
                self.log.error(f'run {s["code"]} alert exception {e} {traceback.format_exc()}')
                return []

        # 限制最大线程数为 10，避免过多线程导致资源耗尽
        err_num = 0
        trigger_num = 0
        lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_stock, s) for s in stocks]
            # 使用 tqdm 显示进度
            for f in tqdm(as_completed(futures), total=len(stocks)):
                try:
                    res = f.result()
                    if res:
                        with lock:
                            trigger_num += len(res)
                except Exception:
                    with lock:
                        err_num += 1

        self.log.info(
            f"完成 {alert_config.task_name}({alert_config.market}) {alert_config.frequency} "
            f"stocks={len(stocks)} triggers={trigger_num} errors={err_num} cost={round(time.time()-run_start_ts,2)}s"
        )

        return True

    @staticmethod
    def task_list(market: str = None) -> List[TableByAlertTask]:
        """
        获取警报列表
        """
        alert_list = db.task_query(market=market)
        return alert_list

    @staticmethod
    def alert_get(_id) -> TableByAlertTask:
        alert_config = db.task_query(id=_id)
        if alert_config is None or len(alert_config) == 0:
            return None
        alert_config = alert_config[0]
        return alert_config

    def alert_save(self, alert_config: Dict):
        """
        添加一个警报
        """
        if alert_config["id"] == "":
            del alert_config["id"]
            db.task_save(**alert_config)
        else:
            alert_config["id"] = int(alert_config["id"])
            db.task_update(**alert_config)

        # 重新运行新的监控
        self.run()
        return True

    def alert_del(self, alert_id):
        """
        删除一个警报
        """
        db.task_delete(alert_id)
        self.run()
        return True


if __name__ == "__main__":
    at = AlertTasks(None)

    ls = at.task_list("a")

    print(ls)
