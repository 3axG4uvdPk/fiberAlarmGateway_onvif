# -*- coding: utf-8 -*-
"""
日志配置模块。
提供按日期分目录、按小时分文件的日志Handler，以及旧日志清理功能。
"""
import os
import sys
import logging
import threading
from datetime import datetime, timedelta

from core.config import resolve_runtime_path


def ensure_dir(path):
    """确保目录存在。"""
    os.makedirs(path, exist_ok=True)


class DailyDirectoryTimedFileHandler(logging.Handler):
    """按日期建目录、按小时建文件的日志处理器。"""

    def __init__(self, base_dir, filename_prefix, encoding='utf-8'):
        super().__init__()
        self.base_dir = base_dir
        self.filename_prefix = filename_prefix
        self.encoding = encoding
        self.current_log_path = ""
        self.stream = None
        self.terminator = '\n'

    def _get_log_path(self):
        now = datetime.now()
        date_dir = os.path.join(self.base_dir, now.strftime('%Y-%m-%d'))
        ensure_dir(date_dir)
        log_filename = f"{self.filename_prefix}_{now.strftime('%Y-%m-%d_%H')}.log"
        return os.path.join(date_dir, log_filename)

    def _open_stream(self):
        if self.stream:
            self.stream.close()
        self.stream = open(self.current_log_path, 'a', encoding=self.encoding)

    def emit(self, record):
        try:
            new_log_path = self._get_log_path()
            if new_log_path != self.current_log_path or self.stream is None:
                self.current_log_path = new_log_path
                self._open_stream()
            msg = self.format(record)
            self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

    def flush(self):
        self.acquire()
        try:
            if self.stream:
                self.stream.flush()
        finally:
            self.release()

    def close(self):
        self.acquire()
        try:
            if self.stream:
                self.stream.close()
                self.stream = None
            super().close()
        finally:
            self.release()


def _cleanup_old_logs(base_dir, days_to_keep=90):
    """清理超过指定天数的旧日志目录。"""
    if not os.path.isdir(base_dir):
        return
    cutoff = datetime.now() - timedelta(days=days_to_keep)
    for dir_name in os.listdir(base_dir):
        dir_path = os.path.join(base_dir, dir_name)
        if os.path.isdir(dir_path):
            try:
                dir_date = datetime.strptime(dir_name, '%Y-%m-%d')
                if dir_date < cutoff:
                    import shutil
                    shutil.rmtree(dir_path)
                    logging.getLogger("alarm_app").info(f"已删除旧日志目录: {dir_path}")
            except Exception:
                pass


def setup_logging():
    """
    初始化日志系统：
    - 根 logger 为 "alarm_app"
    - 同时输出控制台和文件
    - 启动后异步清理旧日志
    """
    logger = logging.getLogger("alarm_app")
    # logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台 handler
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 文件 handler：日志目录固定在运行根目录（打包后为可执行文件所在目录）下的 logs/
    log_base_dir = resolve_runtime_path('logs')
    file_handler = DailyDirectoryTimedFileHandler(
        base_dir=log_base_dir,
        filename_prefix='alarm_app',
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 异步清理旧日志
    cleanup_thread = threading.Thread(
        target=_cleanup_old_logs,
        args=(log_base_dir, 90),
        daemon=True
    )
    cleanup_thread.start()

    logger.info("=" * 50)
    logger.info("应用程序启动，日志系统初始化完成。")
    logger.info("=" * 50)

    return logger
