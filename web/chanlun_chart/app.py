"""
WSGI entrypoint for the TradingView web application.

Adds project `src` and web server path to `sys.path`, supports a WPF
launcher mode that wraps stdio into GBK to avoid encoding issues, and
bootstraps the Flask/Tornado server.
"""

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


import traceback
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.wsgi import WSGIContainer
from chanlun import config
from cl_app import create_app

def main() -> None:
    """Start the Tornado HTTP server hosting the Flask app."""
    is_wpf_launcher = "wpf_launcher" in sys.argv
    # WPF 启动，每次 print 都 flush，并且将字符编码转为 GBK（避免乱码）
    if is_wpf_launcher:
        _wrap_stdio_gbk()

    try:
        app = create_app()

        s = HTTPServer(WSGIContainer(app, executor=ThreadPoolExecutor(10)))
        s.bind(9900, config.WEB_HOST)

        LogUtil.info("启动成功")
        s.start(1)

        if len(sys.argv) >= 2 and sys.argv[1] == "nobrowser":
            pass
        else:
            webbrowser.open("http://127.0.0.1:9900")
        IOLoop.instance().start()

    except Exception:
        traceback.print_exc()
        if is_wpf_launcher is False:
            input("出现异常，按回车键退出")


if __name__ == "__main__":
    main()
