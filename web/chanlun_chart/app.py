"""
WSGI entrypoint for the TradingView web application.

Adds project `src` and web server path to `sys.path`, supports a WPF
launcher mode that wraps stdio into GBK to avoid encoding issues, and
bootstraps the Flask/Tornado server.
"""

import os
import pathlib
import sys

from chanlun.tools.log_util import LogUtil

# 将项目中的 src 目录，添加到 sys.path 中
src_path = pathlib.Path(__file__).parent.parent / ".." / "src"
sys.path.append(str(src_path))
web_server_path = pathlib.Path(__file__).parent
sys.path.append(str(web_server_path))

def _wrap_stdio_gbk() -> None:
    """
    Wrap stdin/stdout/stderr to GBK encoding for WPF launcher mode.
    Ensures print flushes and converts unicode to GBK safely.
    """

    class _Filter:
        def __init__(self, target):
            self.target = target

        def write(self, s):
            self.target.buffer.write(s.encode("gbk"))
            self.target.flush()

        def flush(self):
            self.target.flush()

        def close(self):
            self.target.close()

    sys.stdin = _Filter(sys.stdin)
    sys.stdout = _Filter(sys.stdout)
    sys.stderr = _Filter(sys.stderr)


import webbrowser
from concurrent.futures import ThreadPoolExecutor
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.wsgi import WSGIContainer
from chanlun import config
from cl_app import create_app
from cl_app.blueprints.tv import start_symbol_preload_thread

def main() -> None:
    """Start the Tornado HTTP server hosting the Flask app."""
    is_wpf_launcher = "wpf_launcher" in sys.argv
    # WPF 启动，每次 print 都 flush，并且将字符编码转为 GBK（避免乱码）
    if is_wpf_launcher:
        _wrap_stdio_gbk()

    try:
        app = create_app()

        # ★ B5 修复：HTTP 线程池容量可配置，默认从 10 提升到 32。
        # 之前的 10 worker 在多 tab + 多周期同时切换时极易堵塞：
        # 一个图表初始化就要并行 N 个请求（symbols/config/history/marks/...），
        # 多余请求在 Tornado 队列里排队，叠加 history 单仓信号量限流后体感"卡死"。
        # tv.py 里大部分耗时在 IO（QMT/CQ 拉数据），不是 CPU 密集，
        # 即使把 worker 数放大到 32~64 也不会显著抢占 GIL。
        # 用环境变量 CHANLUN_HTTP_WORKERS 覆盖，便于按部署环境调优。
        try:
            http_workers = int(os.environ.get('CHANLUN_HTTP_WORKERS', '32'))
            if http_workers < 1:
                http_workers = 32
        except (TypeError, ValueError):
            http_workers = 32
        LogUtil.info(f"HTTP 线程池容量: {http_workers}")
        s = HTTPServer(WSGIContainer(app, executor=ThreadPoolExecutor(http_workers)))
        s.bind(9900, config.WEB_HOST)

        # 先启动 symbol 预加载后台线程：daemon 线程，不阻塞主流程，
        # 让其与 HTTP 服务启动并行，争取在用户首次发起请求前完成首轮缓存填充。
        start_symbol_preload_thread()

        LogUtil.info("启动成功")
        # ⚠️ 严禁改为 s.start(0) 或 s.start(N)（多进程模式）！
        # 当前架构所有缓存（tv.py 的 chart_data_cache / stock_cache / chart_calc_locks /
        # _history_req_locks，以及 file_db、QMT/CQ 的 singleton 实例字段）都是
        # **进程内内存**，多进程会让缓存命中率瞬间归零、per-key 锁失效（不同进程不共享锁）。
        # 如需扩容，请用反向代理 + 多端口部署，或先把缓存改造到 Redis。
        s.start(1)

        if len(sys.argv) >= 2 and sys.argv[1] == "nobrowser":
            pass
        else:
            webbrowser.open("http://127.0.0.1:9900")
        IOLoop.instance().start()

    except Exception:
        # 完整堆栈仅写入日志，控制台只提示简短信息，避免暴露内部路径与变量。
        LogUtil.exception("启动 Web 服务时发生异常")
        if is_wpf_launcher is False:
            input("启动失败，详情见日志文件，按回车键退出")


if __name__ == "__main__":
    main()