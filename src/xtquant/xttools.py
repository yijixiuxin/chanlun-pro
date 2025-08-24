#coding:utf-8

def init_pyside2_path():
    try:
        import os, PySide2
        dirname = os.path.dirname(PySide2.__file__)
        plugin_path = os.path.join(dirname, 'plugins', 'platforms')
        os.environ['QT_QPA_PLATFORM_PLUGIN_PATH'] = plugin_path
        return True, None
    except Exception as e:
        return False, e
