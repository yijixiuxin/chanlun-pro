#coding:utf-8

from .functions import *


def run_file(user_script, param = {}):
    import os, sys, time, types
    from .contextinfo import ContextInfo
    from .stgframe import StrategyLoader

    pypath = param.get('pythonpath')
    if pypath:
        lib_search = [os.path.abspath(p) for p in pypath.split(';')]
        sys.path = lib_search + [p for p in sys.path if p not in lib_search]

    user_args = param.get('user_args')
    if user_args and type(user_args) == dict:
        for k, v in user_args.items():
            globals()[k] = v

    user_module = compile(open(user_script, 'rb').read(), user_script, 'exec', optimize = 2)
    #print({'user_module': user_module})

    try:
        pywentrance = param.get('pywentrance', '')
        user_variable = compile(open(os.path.join(pywentrance, "..", "user_config.py"), "rb").read(),
                                "user_config.py", 'exec', optimize=2)
        exec(user_variable, globals())
    except Exception as e:
        pass

    exec(user_module, globals())

    _C = ContextInfo()
    _C._param = param
    _C.user_script = user_script

    def try_set_func(C, func_name):
        func = globals().get(func_name)
        if func:
            C.__setattr__(func_name, types.MethodType(func, C))
        return

    try_set_func(_C, 'init')
    try_set_func(_C, 'after_init')
    try_set_func(_C, 'handlebar')
    try_set_func(_C, 'on_backtest_finished')
    try_set_func(_C, 'stop')

    try_set_func(_C, 'account_callback')
    try_set_func(_C, 'order_callback')
    try_set_func(_C, 'deal_callback')
    try_set_func(_C, 'position_callback')
    try_set_func(_C, 'orderError_callback')

    loader = StrategyLoader()

    loader.C = _C

    loader.init()
    loader.start()
    loader.run()
    loader.stop()
    loader.shutdown()

    mode = _C.trade_mode
    if mode == 'backtest':
        from .stgframe import BackTestResult
        return BackTestResult(_C.request_id)

    if mode in ['simulation', 'trading']:
        while True:
            time.sleep(2)
        from .stgframe import Result
        return Result(_C.request_id)

    return None

