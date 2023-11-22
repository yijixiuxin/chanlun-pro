"""
缠论数据计算入口（按运行平台分发）

本模块根据运行平台加载对应的 cl_fast，并沿用原来的用法：

    from chanlun import cl
    cd = cl.CL("SH.000001", "d", cl_config).process_klines(klines)

    from chanlun.cl import CL
"""

import platform

# platform.machine() 与 pyarmor 平台名的差异
_MACHINE_ALIASES = {
    "x64": "x86_64",
    "amd64": "x86_64",
    "intel": "x86_64",
    "arm64": "aarch64",
}


def _platform_name() -> str:
    """当前平台名，如 windows_x86_64 / darwin_aarch64"""
    machine = platform.machine().lower()
    machine = _MACHINE_ALIASES.get(machine, machine)
    return f"{platform.system().lower()}_{machine}"


_platform = _platform_name()

if _platform == "windows_x86_64":
    from chanlun.core.windows_x86_64 import cl_fast as cl
elif _platform == "linux_x86_64":
    from chanlun.core.linux_x86_64 import cl_fast as cl
elif _platform == "darwin_x86_64":
    from chanlun.core.darwin_x86_64 import cl_fast as cl
elif _platform == "darwin_aarch64":
    from chanlun.core.darwin_aarch64 import cl_fast as cl
else:
    raise ImportError(
        f"chanlun 不支持当前平台：{_platform}，"
        f"仅支持 windows_x86_64 / linux_x86_64 / darwin_x86_64 / darwin_aarch64"
    )

CL = cl.CL
