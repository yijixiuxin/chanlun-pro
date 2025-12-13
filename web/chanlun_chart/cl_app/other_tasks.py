import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from chanlun.exchange.stocks_bkgn import StocksBKGN


class OtherTasks:
    def __init__(self, scheduler: BackgroundScheduler):
        self.scheduler = scheduler

        self.stock_bkgn = StocksBKGN()

        self.run_task()

    def run_task(self):
        # 东方财富抓取的不好用了，建议直接用通达信本地的方式

        # 关于沪深股票行业与板块的获取方式，项目中提供了两种方式：
        # 1. 通过 Akshare 抓取东方财务网页内容，获取行业、板块信息，有可能会被封禁，需要手动打开页面进行验证才可继续使用
        # 2. 通过设置 config.py 配置的 TDX_PATH 本地通达信安装路径，读取通达信文件获取行业与概念；（推荐使用）

        # 检查当前是否有行业板块文件，没有现在就进行同步
        # if self.stock_bkgn.file_name.is_file() is False:
        #     self.scheduler.add_job(
        #         self.stock_bkgn.reload_bkgn,
        #         trigger="date",
        #         next_run_time=datetime.datetime.now(),
        #         id="now_update_fri_stock_bkgn",
        #         name="文件不存在，更新行业概念信息",
        #     )
        # 每周 5 下午16点更新行业概念信息
        # self.scheduler.add_job(
        #     self.stock_bkgn.reload_bkgn,
        #     trigger="cron",
        #     day_of_week="fri",
        #     hour=16,
        #     minute=00,
        #     id="update_fri_stock_bkgn",
        #     name="每周五16点更新行业概念信息",
        # )
        pass
