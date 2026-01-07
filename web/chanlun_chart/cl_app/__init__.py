import datetime
import pytz
from apscheduler.events import (
    EVENT_ALL,
    EVENT_EXECUTOR_ADDED,
    EVENT_EXECUTOR_REMOVED,
    EVENT_JOB_ADDED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_MISSED,
    EVENT_JOB_MODIFIED,
    EVENT_JOB_REMOVED,
    EVENT_JOB_SUBMITTED,
    EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED,
)
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.schedulers.tornado import TornadoScheduler
from flask import Flask, redirect, render_template, request
from flask_login import LoginManager, UserMixin, login_required, login_user
from chanlun import config, fun
from .alert_tasks import AlertTasks
from .other_tasks import OtherTasks
from .xuangu_tasks import XuanguTasks
__all__ = ["create_app"]


def create_app(test_config=None):
    # 任务对象
    scheduler = TornadoScheduler(timezone=pytz.timezone("Asia/Shanghai"))
    scheduler.add_executor(TornadoExecutor())
    scheduler.my_task_list = {}

    def run_tasks_listener(event):
        state_map = {
            EVENT_EXECUTOR_ADDED: "已添加",
            EVENT_EXECUTOR_REMOVED: "删除调度",
            EVENT_JOBSTORE_ADDED: "已添加",
            EVENT_JOBSTORE_REMOVED: "删除存储",
            EVENT_JOB_ADDED: "已添加",
            EVENT_JOB_REMOVED: "删除作业",
            EVENT_JOB_MODIFIED: "修改作业",
            EVENT_JOB_SUBMITTED: "运行中",
            EVENT_JOB_MAX_INSTANCES: "等待运行",
            EVENT_JOB_EXECUTED: "已完成",
            EVENT_JOB_ERROR: "执行异常",
            EVENT_JOB_MISSED: "未执行",
        }
        if event.code not in state_map.keys():
            return
        if hasattr(event, "job_id"):
            job_id = event.job_id
            if job_id not in scheduler.my_task_list.keys():
                scheduler.my_task_list[job_id] = {
                    "id": job_id,
                    "name": "--",
                    "update_dt": fun.datetime_to_str(datetime.datetime.now()),
                    "next_run_dt": "--",
                    "state": "未知",
                }
            scheduler.my_task_list[job_id]["update_dt"] = fun.datetime_to_str(
                datetime.datetime.now()
            )
            job = scheduler.get_job(event.job_id)
            if job is not None:
                scheduler.my_task_list[job_id]["name"] = job.name
                scheduler.my_task_list[job_id]["next_run_dt"] = fun.datetime_to_str(
                    job.next_run_time
                )
            scheduler.my_task_list[job_id]["state"] = state_map[event.code]
            # print('任务更新', task_list[job_id])
        return

    scheduler.add_listener(run_tasks_listener, EVENT_ALL)
    scheduler.start()

    # 统一从 services.constants 引用常量，降低耦合
    from .services.constants import (
        frequency_maps,
        resolution_maps,
        market_frequencys,
        market_default_codes,
        market_session,
        market_timezone,
        market_types,
    )

    _alert_tasks = AlertTasks(scheduler)
    _alert_tasks.run()

    _xuangu_tasks = XuanguTasks(scheduler)

    # _other_tasks = OtherTasks(scheduler)

    __log = fun.get_logger()

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.logger.addFilter(
        lambda record: "/static/" not in record.getMessage().lower()
    )  # 过滤静态资源请求日志

    # 添加登录验证
    app.secret_key = "cl_pro_secret_key"
    login_manager = LoginManager()  # 实例化登录管理对象
    login_manager.init_app(app)  # 初始化应用
    login_manager.login_view = "login_opt"  # 设置用户登录视图函数 endpoint

    class LoginUser(UserMixin):
        def __init__(self) -> None:
            super().__init__()
            self.id = "cl_pro"

    @login_manager.user_loader
    def load_user(user_id):
        return LoginUser()

    @app.route("/login", methods=["GET", "POST"])
    def login_opt():
        # 未设置登录密码，默认直接进行登录
        if config.LOGIN_PWD == "":
            login_user(
                LoginUser(), remember=True, duration=datetime.timedelta(days=365)
            )
            return redirect("/")

        emsg = ""
        if request.method == "POST":
            password = request.form.get("password")
            if password == config.LOGIN_PWD:
                login_user(
                    LoginUser(), remember=True, duration=datetime.timedelta(days=365)
                )
                return redirect("/")
            else:
                emsg = "密码错误"

        return render_template("login.html", emsg=emsg)

    @app.route("/")
    @login_required
    def index_show():
        """
        首页
        """

        return render_template(
            "index.html",
            market_default_codes=market_default_codes,
            market_frequencys=market_frequencys,
        )

    # 注册蓝图：TradingView 相关接口
    from .blueprints.tv import tv_bp
    app.register_blueprint(tv_bp)

    # 注册蓝图：自选、提醒、选股、设置、AI、板块概念、杂项、配置项
    from .blueprints.zixuan import zixuan_bp
    from .blueprints.alert import alert_bp
    from .blueprints.xuangu import xuangu_bp
    from .blueprints.setting import setting_bp
    from .blueprints.ai import ai_bp
    from .blueprints.bkgn import bkgn_bp
    from .blueprints.other import other_bp
    from .blueprints.options import options_bp

    app.register_blueprint(zixuan_bp)
    app.register_blueprint(alert_bp)
    app.register_blueprint(xuangu_bp)
    app.register_blueprint(setting_bp)
    app.register_blueprint(ai_bp)
    app.register_blueprint(bkgn_bp)
    app.register_blueprint(other_bp)
    app.register_blueprint(options_bp)

    # 共享对象存入 app.extensions，供蓝图访问
    app.extensions = getattr(app, "extensions", {})
    app.extensions["scheduler"] = scheduler
    app.extensions["alert_tasks"] = _alert_tasks
    app.extensions["xuangu_tasks"] = _xuangu_tasks
    # app.extensions["other_tasks"] = _other_tasks
    return app
