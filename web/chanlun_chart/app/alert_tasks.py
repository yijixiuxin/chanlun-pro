import json
import uuid
from typing import Dict

from apscheduler.schedulers.background import BackgroundScheduler
from tqdm.auto import tqdm

from chanlun import fun, rd, monitor
from chanlun.cl_utils import query_cl_chart_config
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

        alerts = self.alert_list()
        for al in alerts:
            # print(al)
            if al['enable'] == '1':
                _job = self.scheduler.add_job(
                    func=self.alert_run, trigger='cron', args={al['id']},
                    id=al['id'], name=al['alert_name'],
                    hour='9-12,13-14', minute=f'*/{int(al["interval_minutes"])}', second='0',
                )
                self.task_ids.append(_job.id)
        return True

    def alert_run(self, alert_id):
        alert_config = self.alert_get(alert_id)
        ex = get_exchange(Market(alert_config['market']))
        if ex.now_trading() is False:
            return True

        zx = ZiXuan(alert_config['market'])
        # 获取自选股票
        stocks = zx.zx_stocks(alert_config['zixuan_group'])
        self.log.info(
            f'执行 {alert_config["alert_name"]} 警报提醒，获取 {alert_config["zixuan_group"]} 自选组中 {len(stocks)} 数量股票'
        )
        self.log.info(f'{alert_config}')
        for s in tqdm(stocks):
            try:
                s: Dict[str, str] = s
                cl_config = query_cl_chart_config(alert_config['market'], s['code'])
                monitor.monitoring_code(
                    alert_config['market'], s['code'], s['name'], [alert_config['frequency']], {
                        'beichi': alert_config['check_bi_bc'].split(','),
                        'mmd': alert_config['check_bi_mmd'].split(','),
                        'beichi_xd': alert_config['check_xd_bc'].split(','),
                        'mmd_xd': alert_config['check_xd_mmd'].split(','),
                    }, is_send_msg=bool(alert_config['is_send_msg']), cl_config=cl_config)
            except Exception as e:
                self.log.error(f'run {s["code"]} alert exception {e}')

        return True

    @staticmethod
    def alert_list():
        """
        获取警报列表
        """
        alert_list = []

        alert_ids = rd.Robj().hkeys('alert')
        if alert_ids is None:
            return alert_list

        for _id in alert_ids:
            alert_config = rd.Robj().hget('alert', _id)
            if alert_config is not None:
                alert_config = json.loads(alert_config)
                alert_list.append(alert_config)
        return alert_list

    @staticmethod
    def alert_get(_id):
        alert_config = rd.Robj().hget('alert', _id)
        if alert_config is None:
            return None
        alert_config = json.loads(alert_config)
        return alert_config

    def alert_save(self, alert_config: Dict):
        """
        添加一个警报
        """
        if alert_config['id'] == '':
            alert_config['id'] = str(uuid.uuid1())
        alert_json = json.dumps(alert_config)
        rd.Robj().hset('alert', alert_config['id'], alert_json)

        # 重新允许新的监控
        self.run()
        return alert_config['id']

    def alert_del(self, alert_id):
        """
        删除一个警报
        """
        rd.Robj().hdel('alert', alert_id)
        self.run()
        return True


if __name__ == '__main__':
    at = AlertTasks(None)

    ls = at.alert_list()

    print(ls)
