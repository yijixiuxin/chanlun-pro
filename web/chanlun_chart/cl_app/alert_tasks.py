import json
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
        self.log = fun.get_logger()

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
                        args={_t.id},
                        id=str(_t.id),
                        name=f"监控-{_t.task_name}",
                        minute=f"*/{_t.interval_minutes}",
                        second="0",
                    )
                else:
                    # 60分钟及以上，按小时运行
                    hours = _t.interval_minutes // 60
                    _job = self.scheduler.add_job(
                        func=self.alert_run,
                        trigger="cron",
                        args={_t.id},
                        id=str(_t.id),
                        name=f"监控-{_t.task_name}",
                        hour=f"*/{hours}",
                        minute="0",
                        second="0",
                    )

                self.task_ids.append(_job.id)
        return True

    def alert_run(self, alert_id):
        alert_config = self.alert_get(alert_id)
        ex = get_exchange(Market(alert_config.market))
        if ex.now_trading() is False:
            return True

        zx = ZiXuan(alert_config.market)
        # 获取自选股票
        stocks = zx.zx_stocks(alert_config.zx_group)
        self.log.info(
            f"执行 {alert_config.task_name} 警报提醒，获取 {alert_config.zx_group} 自选组中 {len(stocks)} 数量股票"
        )
        for s in tqdm(stocks):
            try:
                s: Dict[str, str] = s
                cl_config = query_cl_chart_config(alert_config.market, s["code"])
                monitor.monitoring_code(
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
                    },
                    is_send_msg=bool(alert_config.is_send_msg),
                    cl_config=cl_config,
                )
            except Exception as e:
                self.log.error(f'run {s["code"]} alert exception {e}')

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
