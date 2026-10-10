# -*- coding: utf-8 -*-
"""
MinIO 对象存储管理器。
提供连接管理、自动重连、图片上传和文件上传功能。
重连后自动通知注册的回调函数，用于重试失败的上传任务。
"""
import io
import os
import threading
import time
import logging
from minio import Minio

logger = logging.getLogger("alarm_app.minio")


class MinioManager:
    def __init__(self, config):
        self.client = None
        self._http_client = None  # 保存 PoolManager 引用，用于连接泄漏防护
        self.bucket_name = ""
        self.base_url = ""
        self.is_available = False
        self.config = config.get('minio')
        self.path_prefix = config.get('minio', {}).get('path_prefix', '').strip('/')
        self.lock = threading.Lock()

        # 重连回调列表：MinIO 恢复连接后通知所有注册的回调
        self._reconnect_callbacks = []
        self._callbacks_lock = threading.Lock()

        if not self.config:
            logger.error("配置文件中缺少 'minio' 配置节！MinIO功能将禁用。")
            return

        self.try_connect()
        self.start_reconnect_thread()

    def build_object_name(self, date_folder, base_filename):
        """根据配置和参数构建完整的 MinIO 对象名。"""
        if self.path_prefix:
            return f"{self.path_prefix}/{date_folder}/{base_filename}"
        else:
            return f"{date_folder}/{base_filename}"

    def register_reconnect_callback(self, callback):
        """注册重连回调函数。回调将在 MinIO 恢复连接时被调用，用于重试失败的上传。"""
        with self._callbacks_lock:
            self._reconnect_callbacks.append(callback)

    def _fire_reconnect_callbacks(self):
        """通知所有注册的回调，MinIO 连接已恢复。"""
        with self._callbacks_lock:
            callbacks = list(self._reconnect_callbacks)
        for cb in callbacks:
            try:
                cb()
            except Exception as exc:
                logger.error(f"执行重连回调时出错: {exc}")

    def _mark_unavailable(self):
        """将 MinIO 标记为不可用，并立即关闭所有连接以防止 socket 泄漏。

        上传失败时必须调用此方法而非仅设置 is_available = False，
        否则 PoolManager 中的 socket 连接（指向 9000 端口）将不会被关闭，
        导致连接持续泄漏直至耗尽文件描述符。
        """
        with self.lock:
            self.is_available = False
            self.client = None
            if self._http_client is not None:
                try:
                    self._http_client.clear()
                except Exception:
                    pass
                self._http_client = None

    def try_connect(self, timeout_seconds=300):
        """尝试连接到 MinIO 并验证 bucket。连接成功时触发所有注册的重连回调。
        
        Args:
            timeout_seconds: 读取超时时间（秒），默认300秒。大文件上传需要更长超时。
        
        注意：回调在锁外触发，避免回调内再次调用 try_connect 导致死锁。
        """
        should_fire_callbacks = False
        with self.lock:
            if self.is_available:
                return True

            # 关闭旧的连接，防止 socket 连接泄漏
            self.client = None
            if self._http_client is not None:
                try:
                    self._http_client.clear()
                except Exception:
                    pass
                self._http_client = None

            logger.info("正在尝试连接到 MinIO...")
            try:
                client = Minio(
                    endpoint=self.config['endpoint'],
                    access_key=self.config['access_key'],
                    secret_key=self.config['secret_key'],
                    secure=self.config.get('secure', False),
                )

                bucket_name = self.config['bucket_name']
                found = client.bucket_exists(bucket_name)
                if not found:
                    raise ValueError(f"MinIO bucket '{bucket_name}' does not exist.")

                self.client = client
                self._http_client = None  # 默认客户端，由 SDK 自行管理连接池
                self.bucket_name = bucket_name
                self.base_url = f"http://{self.config['endpoint']}/{self.bucket_name}"
                self.is_available = True
                should_fire_callbacks = True
                logger.info(f"成功连接到 MinIO，并确认存储桶 '{self.bucket_name}' 存在。")

            except Exception as exc:
                logger.error(f"MinIO 连接尝试失败: {exc}")
                self.is_available = False
                self.client = None
                return False

        # 在锁外触发回调，避免回调内再次调用 try_connect 导致死锁
        if should_fire_callbacks:
            logger.info("MinIO 连接已恢复，将重试所有待处理的上传任务...")
            self._fire_reconnect_callbacks()

        return True

    def start_reconnect_thread(self):
        """启动后台守护线程，定期尝试重连 MinIO。"""
        reconnect_thread = threading.Thread(target=self._reconnect_loop, daemon=True)
        reconnect_thread.start()
        logger.info("MinIO 后台自动重连线程已启动。")

    def _reconnect_loop(self):
        """重连循环，每 10 秒尝试一次。"""
        reconnect_interval = 10
        while True:
            time.sleep(reconnect_interval)
            if self.is_available:
                continue
            logger.info("后台线程检测到 MinIO 不可用，正在尝试重连...")
            self.try_connect()

    def upload_image_data(self, object_name, image_data):
        """将内存中的图片二进制数据上传到 MinIO。"""
        if not self.is_available:
            logger.warning("上传时发现 MinIO 未连接，尝试立即重连...")
            if not self.try_connect():
                return False

        try:
            image_stream = io.BytesIO(image_data)
            image_size = len(image_data)
            result = self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=image_stream,
                length=image_size,
                content_type='image/jpeg'
            )
            logger.debug(f"成功上传到 MinIO: {result.object_name}")
            return True
        except Exception as exc:
            logger.error(f"上传到 MinIO 时发生错误: {exc}。将MinIO状态标记为不可用。")
            self._mark_unavailable()
            return False

    def upload_file_chunked(self, file_path, object_name, content_type='video/mp4', max_retries=5):
        """使用 fput_object 将文件上传到 MinIO。

        Args:
            file_path: 本地文件路径
            object_name: MinIO 对象名称
            content_type: 内容类型
            max_retries: 最大重试次数，默认5次

        Returns:
            True 上传成功，False 上传失败
        """
        if not self.is_available:
            logger.warning("上传时发现 MinIO 未连接，尝试立即重连...")
            if not self.try_connect():
                return False

        # 校验文件存在并获取大小
        try:
            file_size = os.path.getsize(file_path)
        except OSError as exc:
            logger.error(f"无法读取待上传文件: {file_path}, {exc}")
            return False

        if file_size == 0:
            logger.error(f"待上传文件为空，跳过: {file_path}")
            return False

        backoff = 2  # 初始退避秒数

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    logger.info(
                        f"第 {attempt} 次重试上传（{file_path}），"
                        f"退避 {backoff} 秒..."
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 10)
                    self._mark_unavailable()
                    if not self.try_connect():
                        logger.error("重试时无法重新连接 MinIO")
                        continue

                result = self.client.fput_object(
                    bucket_name=self.bucket_name,
                    object_name=object_name,
                    file_path=file_path,
                    content_type=content_type,
                )
                logger.info(
                    f"成功上传到 MinIO: {result.object_name} "
                    f"(大小 {file_size} 字节)"
                )
                return True

            except Exception as exc:
                error_msg = str(exc)

                if attempt < max_retries:
                    logger.warning(
                        f"上传到 MinIO 失败（第 {attempt + 1}/{max_retries + 1} 次尝试）: "
                        f"{error_msg}，将在 {backoff} 秒后重试..."
                    )
                else:
                    logger.error(
                        f"上传到 MinIO 时发生错误（已重试 {max_retries} 次）: {error_msg}。"
                        f"将MinIO状态标记为不可用。"
                    )
                    self._mark_unavailable()
                    return False

        return False

    def get_object_url(self, object_name):
        """获取 MinIO 对象的完整 URL。MinIO 不可用时仍返回构造的 URL 但记录警告。"""
        if not object_name:
            return ""
        if not self.is_available:
            logger.warning(
                f"MinIO 当前不可用，为对象 '{object_name}' 生成的 URL 指向的文件可能不存在。"
            )
        base = self.base_url
        if not base:
            base = f"http://{self.config['endpoint']}/{self.config['bucket_name']}"
        return f"{base}/{object_name}"

