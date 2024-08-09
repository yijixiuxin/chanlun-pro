import chanlun.encodefix  # Fix Windows print 乱码问题
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import pathlib
import sys
import traceback
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from chanlun import config

cmd_path = pathlib.Path.cwd()
sys.path.append(str(cmd_path))

try:
    from app import create_app
except Exception as e:
    print(e)
    traceback.print_exc()

    input("出现异常，按回车键退出")

if __name__ == "__main__":
    try:
        app = create_app()

        s = HTTPServer(WSGIContainer(app, executor=ThreadPoolExecutor(10)))
        s.bind(9900, config.WEB_HOST)

        print("启动成功")
        s.start(1)

        if len(sys.argv) >= 2 and sys.argv[1] == "nobrowser":
            pass
        else:
            webbrowser.open("http://127.0.0.1:9900")
        IOLoop.instance().start()

    except Exception as e:
        print(e)
        traceback.print_exc()

        input("出现异常，按回车键退出")
