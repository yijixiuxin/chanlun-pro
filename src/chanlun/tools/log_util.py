import logging
import os
from logging.handlers import RotatingFileHandler


def _resolve_level(env_name: str, default: int) -> int:
    """从环境变量读取日志级别，支持 'DEBUG' / 'INFO' / 'WARNING' / 'ERROR' / 'CRITICAL'。

    取不到或值非法时回退到 default。
    """
    raw = os.environ.get(env_name)
    if not raw:
        return default
    level = logging.getLevelName(raw.strip().upper())
    if isinstance(level, int):
        return level
    return default



class LogUtil:
    """
    一个简单易用的日志工具类。
    - 日志会同时输出到控制台和文件。
    - 日志文件会自动分割（当大小达到5MB时）。
    - 通过调用静态方法 LogUtil.info("...") 来使用。

    级别可通过环境变量动态控制（无需改代码）：
    - LOG_CONSOLE_LEVEL：控制台级别，默认 INFO（关键节点日志可见；高频日志已被降级为 DEBUG，不会刷屏）
    - LOG_FILE_LEVEL：   文件级别，  默认 DEBUG（保留全量日志便于排查）
    示例：
        export LOG_CONSOLE_LEVEL=DEBUG   # 排查问题时全开（连高频 DEBUG 都展示）
        export LOG_CONSOLE_LEVEL=WARNING # 极简模式，只看告警和错误
    """
    _logger = None

    # 日志格式中 %(filename)s:%(lineno)d 显示的是 logging 调用点的位置。
    # 业务代码通过 LogUtil.info(...) -> logger.info(...) 调用，默认会打印成
    # log_util.py:<本文件中 logger.xxx 的行>，看不到真实调用方。
    # 调用栈：业务代码 -> LogUtil.info -> logger.info  ←  stacklevel=3 指回业务代码
    _STACKLEVEL = 3

    @staticmethod
    def get_logger():
        """
        获取并配置 logger 实例。
        这是一个内部方法，确保 logger 只被初始化一次（单例模式）。
        """
        if LogUtil._logger is not None:
            return LogUtil._logger

        # 1. 创建 logger 实例
        # 我们使用一个固定的名字，这样在项目的任何地方获取到的都是同一个 logger 实例
        logger = logging.getLogger("Logger")
        logger.setLevel(logging.DEBUG)  # 设置 logger 的最低级别为 DEBUG

        # 2. 防止日志重复输出
        # 如果 logger.handlers 已经有内容，说明已经配置过了，直接返回
        if logger.handlers:
            LogUtil._logger = logger
            return logger

        # 3. 创建日志格式
        # 定义日志输出的格式，包括时间、日志级别、文件名、行号和日志消息
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )

        # 4. 创建并配置控制台 Handler
        # 控制台默认 INFO，能看到关键运行节点（启动、预热、刷新完成等）。
        # 由于高频日志（K线缓存命中、tv_history 请求、线段构造等）已统一改为 DEBUG，
        # 控制台不会再被刷屏；要看全量调试日志：export LOG_CONSOLE_LEVEL=DEBUG
        console_level = _resolve_level("LOG_CONSOLE_LEVEL", logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(formatter)

        # 5. 创建并配置日志文件 Handler
        # 确保日志文件夹存在
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_path = os.path.join(log_dir, 'app.log')

        # 使用 RotatingFileHandler 可以让日志文件在达到一定大小时自动创建新文件。
        # 文件默认 DEBUG，保留全量信息便于排查（占用磁盘有限：单文件 5MB，最多 5 份备份）。
        file_level = _resolve_level("LOG_FILE_LEVEL", logging.DEBUG)
        file_handler = RotatingFileHandler(
            filename=log_path,
            maxBytes=5 * 1024 * 1024,  # 单个日志文件最大为 5MB
            backupCount=5,  # 最多保留5个备份日志文件
            encoding='utf-8'
        )
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)

        # 6. 将 Handlers 添加到 logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        LogUtil._logger = logger
        return logger

    # --- 提供给外部调用的静态方法 ---
    # 通过 stacklevel 让 %(filename)s:%(lineno)d 指向真实调用方，而不是本文件。
    # 注意：stacklevel 是 Python 3.8+ 才支持的 logging 关键字。

    @staticmethod
    def debug(message, *args, **kwargs):
        """记录一条 debug 级别的日志"""
        kwargs.setdefault("stacklevel", LogUtil._STACKLEVEL)
        LogUtil.get_logger().debug(message, *args, **kwargs)

    @staticmethod
    def info(message, *args, **kwargs):
        """记录一条 info 级别的日志"""
        kwargs.setdefault("stacklevel", LogUtil._STACKLEVEL)
        LogUtil.get_logger().info(message, *args, **kwargs)

    @staticmethod
    def warning(message, *args, **kwargs):
        """记录一条 warning 级别的日志"""
        kwargs.setdefault("stacklevel", LogUtil._STACKLEVEL)
        LogUtil.get_logger().warning(message, *args, **kwargs)

    @staticmethod
    def error(message, *args, **kwargs):
        """记录一条 error 级别的日志"""
        kwargs.setdefault("stacklevel", LogUtil._STACKLEVEL)
        LogUtil.get_logger().error(message, *args, **kwargs)

    @staticmethod
    def critical(message, *args, **kwargs):
        """记录一条 critical 级别的日志"""
        kwargs.setdefault("stacklevel", LogUtil._STACKLEVEL)
        LogUtil.get_logger().critical(message, *args, **kwargs)
