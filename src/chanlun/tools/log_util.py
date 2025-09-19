import logging
import os
from logging.handlers import RotatingFileHandler


class LogUtil:
    """
    一个简单易用的日志工具类。
    - 日志会同时输出到控制台和文件。
    - 日志文件会自动分割（当大小达到5MB时）。
    - 通过调用静态方法 LogUtil.info("...") 来使用。
    """
    _logger = None

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
        # 这个 Handler 负责将日志输出到控制台
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # 控制台只显示 INFO 及以上级别的日志
        console_handler.setFormatter(formatter)

        # 5. 创建并配置日志文件 Handler
        # 确保日志文件夹存在
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_path = os.path.join(log_dir, 'app.log')

        # 使用 RotatingFileHandler 可以让日志文件在达到一定大小时自动创建新文件
        file_handler = RotatingFileHandler(
            filename=log_path,
            maxBytes=5 * 1024 * 1024,  # 单个日志文件最大为 5MB
            backupCount=5,  # 最多保留5个备份日志文件
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)  # 文件中记录所有 DEBUG 及以上级别的日志
        file_handler.setFormatter(formatter)

        # 6. 将 Handlers 添加到 logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        LogUtil._logger = logger
        return logger

    # --- 提供给外部调用的静态方法 ---

    @staticmethod
    def debug(message, *args, **kwargs):
        """记录一条 debug 级别的日志"""
        LogUtil.get_logger().debug(message, *args, **kwargs)

    @staticmethod
    def info(message, *args, **kwargs):
        """记录一条 info 级别的日志"""
        LogUtil.get_logger().info(message, *args, **kwargs)

    @staticmethod
    def warning(message, *args, **kwargs):
        """记录一条 warning 级别的日志"""
        LogUtil.get_logger().warning(message, *args, **kwargs)

    @staticmethod
    def error(message, *args, **kwargs):
        """记录一条 error 级别的日志"""
        LogUtil.get_logger().error(message, *args, **kwargs)

    @staticmethod
    def critical(message, *args, **kwargs):
        """记录一条 critical 级别的日志"""
        LogUtil.get_logger().critical(message, *args, **kwargs)