# -*- coding: utf-8 -*-  # noqa: UP009
"""
将 big_qmt_redis_bridge_utf8.py（UTF-8 主版本）转换为
big_qmt_redis_bridge.py（GBK 部署版本），供大QMT内置 Python 使用。

大QMT内置 Python 的编辑器/导入链路按 GBK 处理脚本内容，
GBK 部署文件必须声明 # -*- coding: gbk -*-（与参考示例大QMTserver.py 一致），
否则启动时会出现 UnicodeDecodeError 导致策略自动关闭。

用法（在外部 Python 3 环境执行）：
    python to_gbk.py
    python to_gbk.py big_qmt_redis_bridge_utf8.py big_qmt_redis_bridge.py

默认：big_qmt_redis_bridge_utf8.py -> big_qmt_redis_bridge.py
"""

import os
import sys


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    source = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.path.join(here, "big_qmt_redis_bridge_utf8.py")
    )
    target = (
        sys.argv[2]
        if len(sys.argv) > 2
        else os.path.join(here, "big_qmt_redis_bridge.py")
    )

    with open(source, "rb") as f:
        raw = f.read()

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("gbk")

    # 编码声明改为 gbk
    text = text.replace("# -*- coding: utf-8 -*-", "# -*- coding: gbk -*-", 1)

    with open(target, "wb") as f:
        f.write(text.encode("gbk"))

    print(f"已生成 GBK 编码文件：{target}")


if __name__ == "__main__":
    main()
