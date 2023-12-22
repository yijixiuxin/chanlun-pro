from gevent import monkey

monkey.patch_all()

import chanlun.encodefix  # Fix Windows print 乱码问题
import pathlib
import sys
import traceback
import webbrowser
from gevent.pywsgi import WSGIServer


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
        # DEBUG
        # app.run('127.0.0.1', 9900, True)

        print("启动成功")
        http_server = WSGIServer(("0.0.0.0", 9900), app)

        if len(sys.argv) >= 2 and sys.argv[1] == "nobrowser":
            pass
        else:
            webbrowser.open("http://127.0.0.1:9900")

        http_server.serve_forever()

    except Exception as e:
        print(e)
        traceback.print_exc()

        input("出现异常，按回车键退出")
