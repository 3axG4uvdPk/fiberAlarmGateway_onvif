# -*- coding: utf-8 -*-
"""
异步上传线程。
负责 MinIO 图片上传和 RabbitMQ 结果消息转发。
支持通过配置 forward_enabled 开关控制是否转发消息。
MinIO 不可用时自动暂存失败任务，待连接恢复后自动重试。
"""
import os
import json
import subprocess
import tempfile
import threading
import collections
import logging
import queue
import time
import pika
from datetime import datetime

logger = logging.getLogger("alarm_app.upload")

from core.ffmpeg_utils import resolve_ffmpeg_path, no_window_kwargs

class UploadWorker(threading.Thread):
    """异步上传线程：将 MQ 转发和 MinIO 上传解耦。"""

    # 待重试任务队列最大长度，防止内存过度占用
    MAX_PENDING_TASKS = 500
    # 内部图片上传/MQ 转发队列的最大长度，防止 MinIO 上传或 MQ 转发变慢时
    # 任务无界积压导致内存只涨不降（每个任务都携带图片字节/转发消息）。
    INTERNAL_QUEUE_MAX = 500
    END_ALARM_WAIT_RETRY_SECONDS = 0.5
    # 结束报警最多等待开始报警确认发送的时间（秒），超时后按尽力而为直接转发。
    # 防止开始报警始终未发送成功时，结束报警无限等待导致任务无法释放（内存泄漏）。
    MAX_END_WAIT_FOR_START_SECONDS = 30
    # 转发失败后原地顺序重试（不丢弃），MQ 发送线程对失败消息原地 sleep 后重试，
    # 确保多条失败消息按原有顺序逐一发送成功。
    MQ_FORWARD_RETRY_INTERVAL_SECONDS = 10.0
    # 告警发送状态条目的最长存活时间（秒），防止开始报警已发送但结束报警未到达时
    # 该状态条目无限期占用内存（内存泄漏）。
    ALARM_SEND_STATE_TTL_SECONDS = 3000

    def __init__(self, minio_mgr, upload_queue, config):
        super().__init__(daemon=True, name="UploadWorker")
        self.minio_mgr = minio_mgr
        self.queue = upload_queue
        self._upload_queue = queue.Queue()
        self._forward_queue = queue.Queue()
        self.config = config
        self.rabbit_out_cfg = config['rabbitmq_out']
        self.forward_enabled = self.rabbit_out_cfg.get('forward_enabled', True)
        self.alarm_send_delay_seconds = self._parse_send_delay_seconds(
            config.get('alarm_send_delay_seconds', 0)
        )
        # JPEG 压缩质量（1-100，越大越清晰体积越大；<=0 或缺失表示不压缩上传原图）
        self.jpg_quality = self._parse_jpg_quality(config.get('jpg_quality', -1))
        # 图片重压缩所用 ffmpeg：优先使用随项目分发的免安装包（ffmpeg-win64/bin）
        self.ffmpeg_path = resolve_ffmpeg_path()
        if self.jpg_quality > 0 and not self.ffmpeg_path:
            logger.warning(
                "未找到 ffmpeg，jpg_quality 图片压缩功能不可用，将按原图上传。"
            )
        self._is_running = True
        self._upload_thread = threading.Thread(
            target=self._upload_loop,
            daemon=True,
            name="MinioUploadWorker"
        )
        self._forward_thread = threading.Thread(
            target=self._forward_loop,
            daemon=True,
            name="MqForwardWorker"
        )
        self._dispatch_timers = set()
        self._dispatch_timers_lock = threading.Lock()
        self._forward_retry_timers = set()
        self._forward_retry_timers_lock = threading.Lock()
        self._alarm_send_state = {}
        self._alarm_send_state_lock = threading.Lock()
        # 关闭事件：用于可中断的等待（如 MQ 发送失败原地重试时能及时响应停止）。
        self._stop_event = threading.Event()

        # 上传失败任务暂存队列，等待 MinIO 恢复后自动补传
        self._pending_tasks = collections.deque()
        self._pending_lock = threading.Lock()

        # 注册 MinIO 重连回调
        self.minio_mgr.register_reconnect_callback(self._retry_pending_tasks)

        if self.forward_enabled:
            logger.info("处理结果转发开关：已开启")
        else:
            logger.info("处理结果转发开关：已关闭，将跳过 MQ 消息转发")
        logger.info(f"告警消息延迟发送配置: {self.alarm_send_delay_seconds:g} 秒")

    def stop(self):
        self._is_running = False
        # 置位停止事件，唤醒可能在 MQ 发送失败原地重试等待中的发送线程
        self._stop_event.set()
        with self._dispatch_timers_lock:
            for timer in list(self._dispatch_timers):
                timer.cancel()
            self._dispatch_timers.clear()
        with self._forward_retry_timers_lock:
            for timer in list(self._forward_retry_timers):
                timer.cancel()
            self._forward_retry_timers.clear()
        self.queue.put(None)
        self._upload_queue.put(None)
        self._forward_queue.put(None)

    def publish_message(self, msg_body):
        """短连接方式发送单条 RabbitMQ 消息。"""
        conn = None
        try:
            logger.debug(f"[MQ发送] 目标交换机: {self.rabbit_out_cfg['exchange_out']}, "
                        f"routing_key: {self.rabbit_out_cfg.get('routing_key', '')}")
            logger.debug(f"[MQ发送] 消息内容: {msg_body}")
            params = pika.URLParameters(self.rabbit_out_cfg['url'])
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.exchange_declare(
                exchange=self.rabbit_out_cfg['exchange_out'],
                exchange_type=self.rabbit_out_cfg['exchange_type'],
                durable=self.rabbit_out_cfg['durable']
            )
            ch.basic_publish(
                exchange=self.rabbit_out_cfg['exchange_out'],
                routing_key=self.rabbit_out_cfg.get('routing_key', ''),
                body=msg_body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            return True
        except Exception as e:
            logger.error(f"异步发送 MQ 消息失败: {e}")
            return False
        finally:
            if conn and conn.is_open:
                try:
                    conn.close()
                except Exception as close_exc:
                    logger.debug(f"安全关闭 RabbitMQ 连接时异常 (可忽略): {close_exc}")

    def _upload_single_task(self, task):
        """执行单个任务的上传操作，返回是否上传成功。"""
        upload_success = False
        try:
            if task.get('local_path') and task.get('object_name'):
                image_data = task.get('image_data', b'')
                if not image_data and os.path.exists(task['local_path']):
                    with open(task['local_path'], 'rb') as f:
                        image_data = f.read()
                if image_data:
                    # 上传 MinIO 前按配置的 JPEG 质量重压缩，以控制图片大小
                    image_data = self._recompress_jpeg(image_data)
                    task['image_data'] = image_data
                    upload_success = self.minio_mgr.upload_image_data(
                        task['object_name'], image_data
                    )
                else:
                    logger.error(f"本地图片为空，无法上传: {task['local_path']}")
        except Exception as upload_exc:
            logger.error(f"读取图片或上传 MinIO 时发生严重异常: {upload_exc}")
        return upload_success

    def _enqueue_pending(self, task):
        """将上传失败的任务加入等待重试队列。"""
        task['_is_retry_upload'] = True
        with self._pending_lock:
            if len(self._pending_tasks) >= self.MAX_PENDING_TASKS:
                # 待重试队列已满，丢弃最旧任务
                dropped = self._pending_tasks.popleft()
                logger.error(
                    f"待重试队列已满 ({self.MAX_PENDING_TASKS})，丢弃最旧任务: "
                    f"{dropped.get('object_name', 'unknown')}"
                )
            self._pending_tasks.append(task)
            logger.info(
                f"上传失败任务已加入等待队列，当前待重试: {len(self._pending_tasks)} 个任务。"
            )

    def _bounded_put(self, target_queue, task, queue_name):
        """有界入队：队列达到上限时丢弃最旧任务，避免任务无界积压导致内存只涨不降。

        停止哨兵 (None) 始终透传，保证线程能正常退出。
        """
        if task is None:
            target_queue.put(task)
            return
        if target_queue.qsize() >= self.INTERNAL_QUEUE_MAX:
            try:
                dropped = target_queue.get_nowait()
            except queue.Empty:
                dropped = None
            if dropped is not None:
                logger.error(
                    f"{queue_name}队列积压达到上限 ({self.INTERNAL_QUEUE_MAX})，"
                    f"丢弃最旧的待处理任务: "
                    f"{dropped.get('mq_message', {}).get('alarmid', 'unknown')}"
                )
        target_queue.put(task)

    def _retry_pending_tasks(self):
        """重连回调：将待补传任务重新投递给独立上传线程。"""
        with self._pending_lock:
            if not self._pending_tasks:
                return
            pending = list(self._pending_tasks)
            self._pending_tasks.clear()

        if not self.minio_mgr.is_available:
            logger.warning(
                f"重连回调被触发但 MinIO 仍不可用，{len(pending)} 个任务将保留在队列中。"
            )
            with self._pending_lock:
                self._pending_tasks.extendleft(reversed(pending))
            return

        logger.info(f"开始补传 {len(pending)} 个待处理上传任务（成功后再发送 MQ）...")
        for task in pending:
            self._bounded_put(self._upload_queue, task, "图片上传")

    @staticmethod
    def _normalize_alarm_flags(mq_message):
        """根据 alarmstate 统一矫正 isAlarm，避免上下游状态不一致。"""
        alarm_state = str(mq_message.get('alarmstate', ''))
        if alarm_state == "1":
            mq_message['isAlarm'] = "1"
        elif alarm_state == "3":
            mq_message['isAlarm'] = "0"

    @staticmethod
    def _is_start_alarm_message(task):
        return str(task.get('mq_message', {}).get('alarmstate', '')) == "1"

    @staticmethod
    def _is_end_alarm_message(task):
        return str(task.get('mq_message', {}).get('alarmstate', '')) == "3"

    def _mark_start_alarm_sent(self, alarm_id):
        if not alarm_id:
            return
        with self._alarm_send_state_lock:
            state = self._alarm_send_state.setdefault(alarm_id, {})
            state['start_sent'] = True
            state['ts'] = time.time()

    def _has_start_alarm_been_sent(self, alarm_id):
        if not alarm_id:
            return False
        with self._alarm_send_state_lock:
            return bool(self._alarm_send_state.get(alarm_id, {}).get('start_sent'))

    def _clear_alarm_send_state(self, alarm_id):
        if not alarm_id:
            return
        with self._alarm_send_state_lock:
            self._alarm_send_state.pop(alarm_id, None)

    def _sweep_alarm_send_state(self, now):
        """清理超过 TTL 的告警发送状态条目，防止内存泄漏。"""
        with self._alarm_send_state_lock:
            stale_ids = [
                alarm_id for alarm_id, state in self._alarm_send_state.items()
                if (state.get('ts') or 0) < now - self.ALARM_SEND_STATE_TTL_SECONDS
            ]
            for alarm_id in stale_ids:
                self._alarm_send_state.pop(alarm_id, None)
        for alarm_id in stale_ids:
            logger.warning(
                f"告警 {alarm_id} 的开始报警已发送但超过 "
                f"{self.ALARM_SEND_STATE_TTL_SECONDS}s 未收到结束报警确认，"
                f"已清理其发送状态（防止内存泄漏）。"
            )

    def _schedule_forward_retry(self, task, delay_seconds=None):
        """延迟后将任务重新放回 MQ 发送队列。"""
        delay_seconds = max(float(delay_seconds or self.END_ALARM_WAIT_RETRY_SECONDS), 0.0)
        timer = None

        def _requeue():
            try:
                if self._is_running:
                    self._bounded_put(self._forward_queue, task, "MQ转发")
            finally:
                with self._forward_retry_timers_lock:
                    self._forward_retry_timers.discard(timer)

        timer = threading.Timer(delay_seconds, _requeue)
        timer.daemon = True
        with self._forward_retry_timers_lock:
            self._forward_retry_timers.add(timer)
        timer.start()

    def _forward_message(self, task):
        """发送 MQ 消息（统一入口）。"""
        self._normalize_alarm_flags(task['mq_message'])
        task['mq_message']['sendtime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg_body = json.dumps(task['mq_message'])
        # 是否有图直接以消息中的图片 URL 判断，不依赖异步上传是否已完成的 _upload_success
        has_image = bool(task['mq_message'].get('alarmimage') or task['mq_message'].get('bgimage'))
        if self.forward_enabled:
            if self.publish_message(msg_body):
                logger.debug(
                    f"成功异步转发报警回执 (是否有图: {has_image}): "
                    f"{task['mq_message'].get('alarmid')}"
                )
                return True
            else:
                logger.error(
                    f"致命错误：异步转发报警回执 MQ 失败: "
                    f"{task['mq_message'].get('alarmid')}"
                )
                return False
        else:
            logger.info(
                f"转发已关闭，跳过 MQ 发送: "
                f"{task['mq_message'].get('alarmid')}"
            )
            return True

    def _handle_forward_task(self, task):
        """处理单条待发送 MQ 任务，并保证结束告警晚于开始告警成功发送。"""
        alarm_id = task.get('mq_message', {}).get('alarmid')

        if self._is_end_alarm_message(task) and not self._has_start_alarm_been_sent(alarm_id):
            if not task.get('_waiting_for_start_alarm_logged'):
                logger.debug(
                    f"结束告警等待开始告警发送成功确认后再发送: {alarm_id}"
                )
                task['_waiting_for_start_alarm_logged'] = True
            if not task.get('_defer_start_time'):
                task['_defer_start_time'] = time.time()
            if time.time() - task['_defer_start_time'] > self.MAX_END_WAIT_FOR_START_SECONDS:
                logger.warning(
                    f"结束报警 {alarm_id} 等待开始报警发送确认超过 "
                    f"{self.MAX_END_WAIT_FOR_START_SECONDS}s，按尽力而为直接转发。"
                )
            else:
                self._schedule_forward_retry(task)
                return "deferred"

        sent = self._forward_message(task)
        retry_count = 0
        # 发送失败时原地无限重试，直到成功或收到停止信号。
        # 由于 MQ 发送线程逐条处理且重试期间不取新的任务，多条失败消息会按
        # forward_queue 的原有顺序逐一发送，避免因并发的定时器重排导致乱序。
        while not sent and not self._stop_event.is_set():
            retry_count += 1
            logger.warning(
                f"MQ 转发失败（第 {retry_count} 次），"
                f"{self.MQ_FORWARD_RETRY_INTERVAL_SECONDS} 秒后重试: "
                f"{task['mq_message'].get('alarmid')}"
            )
            self._stop_event.wait(self.MQ_FORWARD_RETRY_INTERVAL_SECONDS)
            if self._stop_event.is_set():
                break
            sent = self._forward_message(task)

        if not self._is_running:
            return "stopped"

        if self._is_start_alarm_message(task):
            self._mark_start_alarm_sent(alarm_id)
        elif self._is_end_alarm_message(task):
            self._clear_alarm_send_state(alarm_id)
        return "sent"

    def _compute_alarm_send_delay(self, task):
        """计算本次告警消息的延迟发送时长（秒）。

        延迟时长以【收到 MQ 告警消息】的时刻为起点计算（alarm_send_delay_seconds）。
        若处理（预生成开始/结束告警图片等）耗时已超过配置延迟，则立即推送不再追加延迟。
        """
        delay_seconds = self._parse_send_delay_seconds(
            task.get('send_delay_seconds', self.alarm_send_delay_seconds)
        )
        if delay_seconds <= 0:
            return 0.0
        received_at = task.get('_received_at')
        if isinstance(received_at, (int, float)) and received_at > 0:
            remaining = delay_seconds - (time.time() - received_at)
            return max(0.0, remaining)
        return delay_seconds

    def _dispatch_upload(self, task):
        """立即分发图片上传（不受延迟调度影响），保证开始/结束告警图片尽快上传 MinIO。"""
        has_upload_payload = bool(task.get('local_path') and task.get('object_name'))
        task.setdefault('_is_retry_upload', False)
        if has_upload_payload:
            if not self.minio_mgr.is_available:
                logger.warning(
                    f"图片上传延后（不影响告警消息推送）: {task.get('object_name', 'unknown')}，"
                    f"等待 MinIO 恢复后自动补传。"
                )
                self._enqueue_pending(task)
            else:
                self._bounded_put(self._upload_queue, task, "图片上传")
        else:
            task['_upload_success'] = False

    def _schedule_forward(self, task, delay_seconds):
        """按配置延迟将告警消息送入 MQ 发送队列；未配置延迟或已到点时立即发送。"""
        if delay_seconds <= 0 or not self._is_running:
            self._bounded_put(self._forward_queue, task, "MQ转发")
            return

        timer = None

        def _forward_when_due():
            try:
                if self._is_running:
                    self._bounded_put(self._forward_queue, task, "MQ转发")
            finally:
                with self._dispatch_timers_lock:
                    self._dispatch_timers.discard(timer)

        timer = threading.Timer(delay_seconds, _forward_when_due)
        timer.daemon = True
        with self._dispatch_timers_lock:
            self._dispatch_timers.add(timer)
        logger.debug(
            f"告警消息已进入延迟发送调度，{delay_seconds:g} 秒后推送 MQ: "
            f"{task.get('mq_message', {}).get('alarmid', 'unknown')}"
        )
        timer.start()

    def _dispatch_task(self, task):
        """立即处理任务：图片上传与告警消息推送解耦（已到点或未配置延迟时使用）。

        告警消息与图片上传解耦：无论是否带图、是否上传成功，都立即推送 MQ；
        图片上传交给独立上传线程并行处理，MinIO 不可用或很慢时，告警消息不会被拖住。
        """
        self._dispatch_upload(task)
        self._bounded_put(self._forward_queue, task, "MQ转发")

    def _schedule_task(self, task):
        """分发任务：图片上传与告警消息推送解耦。

        - 图片上传立即执行（不受 alarm_send_delay_seconds 延迟调度影响），
          收到告警即预生成并上传开始/结束告警图片；
        - 告警消息按配置的 alarm_send_delay_seconds 延迟推送，
          延迟时长从收到 MQ 告警消息的时刻开始计算（结束告警消息同样经过该调度）。

        处理线程已把告警消息的构建提前到抓图之前（URL 预生成），并将图片上传
        与消息推送拆成两个独立任务通过 _dispatch_mode 标记区分：
        - forward_only: 仅调度告警消息推送（按延迟），不上传图片；
        - upload_only:  仅上传图片（立即），不推送消息；
        - 未标记: 完整路径（图片上传 + 消息推送），与历史行为一致。
        """
        dispatch_mode = task.get('_dispatch_mode')
        if dispatch_mode == 'forward_only':
            self._schedule_forward(task, self._compute_alarm_send_delay(task))
            return
        if dispatch_mode == 'upload_only':
            self._dispatch_upload(task)
            return
        delay_seconds = self._compute_alarm_send_delay(task)
        if delay_seconds <= 0 or not self._is_running:
            # 未配置延迟或已到点：图片与消息立即处理（与历史行为一致）
            self._dispatch_task(task)
            return
        # 1. 立即分发图片上传（MinIO），不等延迟
        self._dispatch_upload(task)
        # 2. 仅告警消息按配置延迟调度推送
        self._schedule_forward(task, delay_seconds)

    @staticmethod
    def _parse_jpg_quality(raw_value):
        """解析 JPEG 质量配置（0-100）。<=0 或无效表示不压缩按原图上传。"""
        try:
            quality = float(raw_value or 0)
        except (TypeError, ValueError):
            logger.warning(f"无效的 jpg_quality 配置: {raw_value}，已回退为不压缩（-1）。")
            return -1
        if quality <= 0:
            return -1
        return max(1, min(100, int(round(quality))))

    def _recompress_jpeg(self, image_data):
        """按配置的 JPEG 质量压缩图片，用于控制上传图片大小。不可压缩时返回原图数据。"""
        if self.jpg_quality <= 0 or not self.ffmpeg_path or not image_data:
            return image_data
        # 将 0-100 的质量映射为 ffmpeg mjpeg 的 qscale（2-31，值越小质量越高）
        qscale = max(2, int(round(2 + (100 - self.jpg_quality) * 29 / 99)))
        tmp_in = None
        tmp_out = None
        try:
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as in_f:
                tmp_in = in_f.name
                in_f.write(image_data)
            tmp_out = tmp_in + '.out.jpg'
            result = subprocess.run(
                [self.ffmpeg_path, '-y', '-loglevel', 'error',
                 '-i', tmp_in, '-q:v', str(qscale), '-frames:v', '1',
                 '-update', '1', tmp_out],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=30,
                check=False,
                **no_window_kwargs(),
            )
            if result.returncode != 0 or not os.path.exists(tmp_out) or os.path.getsize(tmp_out) == 0:
                raise RuntimeError(result.stderr.strip() or 'JPEG 重压缩失败')
            with open(tmp_out, 'rb') as out_f:
                compressed = out_f.read()
            logger.debug(
                f"图片已按 jpg_quality={self.jpg_quality} (qscale={qscale}) 重压缩，"
                f"大小 {len(image_data)} -> {len(compressed)} 字节"
            )
            return compressed or image_data
        except Exception as exc:
            logger.warning(f"JPEG 重压缩图片失败，将按原图上传: {exc}")
            return image_data
        finally:
            for path in (tmp_in, tmp_out):
                if path:
                    try:
                        if os.path.exists(path):
                            os.remove(path)
                    except OSError:
                        pass

    @staticmethod
    def _parse_send_delay_seconds(raw_value):
        try:
            return max(float(raw_value or 0), 0.0)
        except (TypeError, ValueError):
            logger.warning(f"无效的 alarm_send_delay_seconds 配置: {raw_value}，已回退为 0 秒。")
            return 0.0

    def _process_upload_task(self, task):
        """独立上传线程消费上传任务，仅负责 MinIO 上传。

        告警消息已在 _dispatch_task 中先行推送，这里不再转发 MQ，
        上传失败仅会影响 MinIO 上的图片是否就绪，不影响已推送的告警消息。
        """
        if not self.minio_mgr.is_available:
            logger.warning(
                f"图片上传延后（不影响告警消息推送）: {task.get('object_name', 'unknown')}，"
                f"MinIO 当前不可用，任务已重新加入待补传队列。"
            )
            self._enqueue_pending(task)
            return

        upload_success = self._upload_single_task(task)
        if upload_success:
            object_name = task.get('object_name', 'unknown')
            if task.get('_is_retry_upload'):
                logger.debug(f"补传成功: {object_name}")
            else:
                logger.debug(f"上传成功: {object_name}")
            task['_upload_success'] = True
            return

        logger.warning(
            f"图片上传失败: {task.get('object_name', 'unknown')}，"
            f"任务已暂存待 MinIO 恢复后自动重试（告警消息已先行推送）。"
        )
        self._enqueue_pending(task)

    def _upload_loop(self):
        """独立 MinIO 上传线程。"""
        logger.info("MinIO 上传线程已启动。")
        while self._is_running:
            task = self._upload_queue.get()
            if task is None:
                break

            try:
                self._process_upload_task(task)
            except Exception as exc:
                logger.error(f"处理 MinIO 上传任务时出错: {exc}", exc_info=True)

        logger.info("MinIO 上传线程已停止。")

    def _forward_loop(self):
        """独立 MQ 发送线程。"""
        logger.info("MQ 发送线程已启动。")
        last_sweep_time = time.time()
        while self._is_running:
            try:
                task = self._forward_queue.get(timeout=60)
            except queue.Empty:
                task = None
            now = time.time()
            if now - last_sweep_time >= 60:
                last_sweep_time = now
                try:
                    self._sweep_alarm_send_state(now)
                except Exception as sweep_exc:
                    logger.error(f"清理告警发送状态时发生错误: {sweep_exc}", exc_info=True)
            if task is None:
                if not self._is_running:
                    break
                continue

            try:
                self._handle_forward_task(task)
            except Exception as exc:
                logger.error(f"处理 MQ 发送任务时出错: {exc}", exc_info=True)

        logger.info("MQ 发送线程已停止。")

    def run(self):
        logger.info("异步上传分发线程已启动。")
        self._upload_thread.start()
        self._forward_thread.start()
        while self._is_running:
            task = self.queue.get()
            if task is None:
                break

            try:
                self._schedule_task(task)

            except Exception as e:
                logger.error(f"处理异步任务大框架时出错: {e}", exc_info=True)

        self._upload_queue.put(None)
        self._forward_queue.put(None)
        if self._upload_thread.is_alive():
            self._upload_thread.join(timeout=5)
        if self._forward_thread.is_alive():
            self._forward_thread.join(timeout=5)

        # 线程退出时，报告未上传的待处理任务统计
        with self._pending_lock:
            remaining = len(self._pending_tasks)
        if remaining > 0:
            logger.warning(
                f"异步上传分发线程已停止，仍有 {remaining} 个未上传的待处理任务。"
            )
        else:
            logger.info("异步上传分发线程已停止。")
