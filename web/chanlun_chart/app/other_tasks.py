import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from chanlun.exchange.stocks_bkgn import StocksBKGN


class OtherTasks:
    def __init__(self, scheduler: BackgroundScheduler):
        self.scheduler = scheduler

        self.stock_bkgn = StocksBKGN()

        self.run_task()

    def run_task(self):
        # 检查当前是否有行业板块文件，没有现在就进行同步
        if self.stock_bkgn.file_name.is_file() is False:
            self.scheduler.add_job(
                self.stock_bkgn.reload_ths_bkgn,
                trigger="date",
                next_run_time=datetime.datetime.now(),
                id="now_update_fri_stock_bkgn",
                name="文件不存在，更新行业概念信息",
            )
        # 每周 5 下午16点更新行业概念信息
        self.scheduler.add_job(
            self.stock_bkgn.reload_ths_bkgn,
            trigger="cron",
            day_of_week="fri",
            hour=16,
            minute=00,
            id="update_fri_stock_bkgn",
            name="每周五16点更新行业概念信息",
        )
