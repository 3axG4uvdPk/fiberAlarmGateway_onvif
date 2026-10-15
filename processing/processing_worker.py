# -*- coding: utf-8 -*-
"""
报警任务处理线程。
负责从处理队列中取出报警消息，为每条报警启动独立处理线程并发执行
摄像机抓图、触发视频下载任务，并将处理结果推入异步上传队列。
同一报警（相同 alarm_id）的任务串行保序；命中同一相机的抓图序列按序执行。
"""
import os
import time
import shutil
import subprocess
import threading
import logging
import queue
import collections
import itertools
import json
import pika
from datetime import datetime

logger = logging.getLogger("alarm_app.processing")

from core.config import resolve_runtime_path
from core.ffmpeg_utils import (
    resolve_ffmpeg_path,
    resolve_ffprobe_path,
    no_window_kwargs,
    ffmpeg_missing_hint,
)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


class ProcessingWorker(threading.Thread):
    """报警任务处理线程：每收到一条报警立即分发独立线程处理（多线程并发）。

    - 不同报警并发处理，互不阻塞；
    - 同一报警（相同 alarm_id）通过串行槽保序，结束报警不会超越开始报警；
    - 命中同一相机的抓图序列（转预置位→稳定等待→抓图）通过相机级序列锁
      按到达顺序执行，不同相机完全并行。
    """

    # 活跃告警上下文最长存活时间（秒），超时后强制清理，防止内存泄漏。
    ACTIVE_CONTEXT_TTL_SECONDS = 5000
    # 已结束告警 ID 记录的最大条数，超出后淘汰最旧的记录。
    COMPLETED_ALARMS_MAX = 5000
    # 单次抓图的最大尝试次数。
    MAX_CAPTURE_ATTEMPTS = 50
    # 定焦抓图线程等待完成的最长时间（秒）。
    FIXED_SNAPSHOT_JOIN_TIMEOUT = 60
    # 等待vff_path图片超时放弃时间（秒），默认30秒。
    VFF_WAIT_TIMEOUT_SECONDS = 30
    # 同时处理的报警任务数上限，防止告警洪峰时线程无界膨胀。
    MAX_CONCURRENT_ALARM_TASKS = 80

    def __init__(self, cam_mgr, minio_mgr, processing_queue, video_queue, upload_queue):
        super().__init__(daemon=True, name="ProcessingWorker")
        self.cam_mgr = cam_mgr
        self.minio_mgr = minio_mgr
        self.queue = processing_queue
        self.video_queue = video_queue
        self.upload_queue = upload_queue
        self.config = self.cam_mgr.config
        self.snapshots_dir = resolve_runtime_path(
            self.config.get('snapshots_dir', 'snapshots')
        )
        self.videos_dir = resolve_runtime_path(
            self.config.get('videos_dir', 'videos')
        )
        self.boxid = self.config.get('boxid', 'UNKNOWN_BOX')
        # ffmpeg/ffprobe 优先使用随项目分发的免安装包（ffmpeg-win64/bin），
        # 不再依赖系统 PATH；缺失时图片拼接与尺寸探测不可用（仅记录告警）。
        self.ffmpeg_path = resolve_ffmpeg_path()
        self.ffprobe_path = resolve_ffprobe_path()
        if not self.ffmpeg_path:
            logger.warning(ffmpeg_missing_hint('ffmpeg'))
        if not self.ffprobe_path:
            logger.warning(ffmpeg_missing_hint('ffprobe'))
        self.vff_wait_timeout_seconds = self._parse_vff_wait_timeout(
            self.config.get('vff_wait_timeout_seconds', self.VFF_WAIT_TIMEOUT_SECONDS)
        )
        self._is_running = True

        # 转发配置
        self.rabbit_out_cfg = self.config.get('rabbitmq_out', {})
        self.forward_enabled = self.rabbit_out_cfg.get('forward_enabled', True)

        # 活跃告警上下文：记录开始报警的状态，结束报警到达时据此拼接/复用数据
        self.active_alarms = {}
        self._active_alarms_lock = threading.RLock()

        # 已结束（或已超时清理）的告警 ID，用于过滤结束报警之后才到达的迟到开始消息
        self._completed_alarms = collections.OrderedDict()

        # 等待 vff 图片的待发送开始报警（alarm_id -> {'tasks': [...], 'timer': Timer}）
        self._pending_vff_alarms = {}
        # 同时作为 _arrived_vff_paths 的锁，保证提前缓存与登记等待的原子性
        self._pending_vff_lock = threading.RLock()

        # 接收线程提前投递的 vff 图片路径缓存（alarm_id -> (vff_path, arrived_at)），
        # 在开始报警完成抓图登记等待时原子消费，避免 vff 先于登记到达被丢弃。
        self._arrived_vff_paths = {}

        # 是否转发未配置点位的报警
        self.forward_unconfigured = self.config.get('forward_unconfigured_alarms', False)

        # 并发处理：每条报警一个处理线程；同一报警(alarm_id)经串行槽保序；
        # 全局并发量由信号量限制，防止告警洪峰时线程无界膨胀。
        self._alarm_serial_slots = {}
        self._alarm_serial_slots_guard = threading.Lock()
        self._alarm_task_semaphore = threading.Semaphore(self.MAX_CONCURRENT_ALARM_TASKS)
        # 并发处理下生成唯一下游 ID 用的自增序列（itertools.count 的 __next__ 线程安全）
        self._unique_seq = itertools.count(1)

        # 已经就"定焦相机配置不完整"告警过的相机 IP，避免每条报警重复刷屏
        self._fixed_camera_warned = set()

        if self.boxid == "UNKNOWN_BOX":
            logger.warning("配置文件中未找到 'boxid'，将使用默认值 'UNKNOWN_BOX'。")
        self._log_fixed_camera_config()

    def _log_fixed_camera_config(self):
        """启动时逐台打印定焦相机的启用情况。"""
        cameras = self.config.get('cameras')
        if not isinstance(cameras, dict) or not cameras:
            logger.warning("配置中未定义任何云台相机 (cameras)。")
            return
        for cam_name, cam_cfg in cameras.items():
            if not isinstance(cam_cfg, dict):
                continue
            fixed_cfg, enabled = self._resolve_fixed_camera(cam_cfg)
            if enabled:
                logger.info(
                    f"相机 {cam_name} ({cam_cfg.get('ip')}) 的定焦摄像机已启用: {fixed_cfg['ip']}"
                )

    def _resolve_fixed_camera(self, cam_cfg):
        """解析云台相机自带的定焦相机配置，返回 ``(fixed_cfg, enabled)``。

        定焦相机配置位于 ``cameras.<name>.fixed_camera``：
        - 缺失或不是字典：不启用定焦抓图与拼接；
        - ``enabled=false``：显式禁用；
        - ``enabled=true`` 但字段不完整（缺 ip/username/pwd/manufacturer）：
          记录警告并禁用，避免影响云台相机本身的抓图流程。
        """
        fixed_cfg = (cam_cfg or {}).get('fixed_camera')
        if not isinstance(fixed_cfg, dict):
            return None, False
        if not fixed_cfg.get('enabled', True):
            logger.debug(f"相机 {cam_cfg.get('ip')} 的定焦相机已禁用，跳过定焦抓图。")
            return None, False
        if not self.cam_mgr.is_camera_configured(fixed_cfg):
            warn_key = cam_cfg.get('ip')
            warned = getattr(self, '_fixed_camera_warned', None)
            if warned is None or warn_key not in warned:
                if warned is not None:
                    warned.add(warn_key)
                logger.warning(
                    f"相机 {cam_cfg.get('ip')} 的 fixed_camera 配置字段不完整，"
                    f"已禁用该相机的定焦抓图与拼接功能。"
                )
            return None, False
        return fixed_cfg, True

    @staticmethod
    def _parse_vff_wait_timeout(raw_value):
        try:
            return max(float(raw_value or 0), 0.0)
        except (TypeError, ValueError):
            logger.warning(f"无效的 vff_wait_timeout_seconds 配置: {raw_value}，已回退为 30 秒。")
            return 30.0

    def stop(self):
        self._is_running = False
        self.queue.put(None)

    def has_active_alarm_context(self, alarm_id):
        """供接收线程快速判断某个开始报警是否已进入活跃处理态。"""
        if not alarm_id:
            return False
        with self._active_alarms_lock:
            return alarm_id in self.active_alarms

    def _get_active_alarm_context(self, alarm_id):
        with self._active_alarms_lock:
            return self.active_alarms.get(alarm_id)

    def _set_active_alarm_context(self, alarm_id, context):
        with self._active_alarms_lock:
            self.active_alarms[alarm_id] = context

    def _pop_active_alarm_context(self, alarm_id):
        with self._active_alarms_lock:
            return self.active_alarms.pop(alarm_id, None)

    def _remember_completed_alarm(self, alarm_id):
        """记录已完成（或已超时清理）的告警 ID，用于过滤结束报警之后才到达的迟到开始消息。"""
        if not alarm_id:
            return
        with self._active_alarms_lock:
            self._completed_alarms[alarm_id] = time.time()
            while len(self._completed_alarms) > self.COMPLETED_ALARMS_MAX:
                self._completed_alarms.popitem(last=False)

    def _is_completed_alarm(self, alarm_id):
        if not alarm_id:
            return False
        with self._active_alarms_lock:
            return alarm_id in self._completed_alarms

    def _receive_vff(self, alarm_id, vff_path):
        """投递 vff 图片：若存在等待中的开始报警则立即拼接发送。

        若尚无等待中的开始报警（开始报警仍在抓图尚未登记、或本条消息本身同时携带
        start_time+vff_path 且是第一次到达），则先将 vff 路径缓存进 _arrived_vff_paths，
        供开始报警登记等待时原子消费拼接；绝不能把已到达的 vff 直接丢弃，
        否则携带 start_time+vff_path 的单条告警消息（无后续独立 vff 消息）会等满
        vff_wait_timeout 后以未拼接图片发送，导致开始图片永远缺 vff 拼图。
        返回是否命中并处理了等待中的开始报警。
        """
        if not alarm_id or not vff_path:
            return False
        pending = self._claim_pending_vff(alarm_id)
        if pending is not None:
            timer = pending.get('timer')
            if timer is not None:
                timer.cancel()
            logger.debug(f"处理线程命中已等待的开始报警 {alarm_id}，立即拼接 vff: {vff_path}")
            self._append_vff_to_tasks(pending['tasks'], vff_path)
            self._dispatch_tasks(alarm_id, pending['tasks'])
            return True
        with self._pending_vff_lock:
            if alarm_id not in self._arrived_vff_paths:
                self._arrived_vff_paths[alarm_id] = (vff_path, time.time())
        logger.debug(f"收到 vff_path 但尚无等待中的开始报警，已缓存待登记时拼接: {alarm_id} -> {vff_path}")
        return False

    def deliver_vff_early(self, alarm_id, vff_path):
        """接收线程在入队前直接调用：若该报警已登记等待 vff（开始报警已处理完、尚未超时），
        则直接在接收线程拼接，避免依赖处理线程从队列取到 vff 消息而错过等待超时；
        否则将 vff 路径缓存，供开始报警登记时原子消费。"""
        if not alarm_id or not vff_path:
            return
        with self._pending_vff_lock:
            # 原子取回等待条目，防止与超时/其他线程并发重复发送。
            pending = self._pending_vff_alarms.pop(alarm_id, None)
            if pending is not None:
                timer = pending.get('timer')
                if timer is not None:
                    timer.cancel()
                logger.debug(
                    f"接收线程命中已等待的开始报警 {alarm_id}，直接拼接 vff: {vff_path}"
                )
                self._append_vff_to_tasks(pending['tasks'], vff_path)
                self._dispatch_tasks(alarm_id, pending['tasks'])
                return
            self._arrived_vff_paths[alarm_id] = (vff_path, time.time())
        logger.debug(f"接收线程提前缓存 vff 路径: {alarm_id} -> {vff_path}")

    def _register_pending_vff(self, alarm_id, tasks):
        """登记等待 vff 图片的开始报警任务，超时后按无 vff 直接发送。"""
        cached = None
        with self._pending_vff_lock:
            cached = self._arrived_vff_paths.pop(alarm_id, None)
            if cached is None:
                timer = threading.Timer(
                    self.vff_wait_timeout_seconds,
                    self._on_vff_timeout,
                    args=(alarm_id,),
                )
                timer.daemon = True
                self._pending_vff_alarms[alarm_id] = {'tasks': tasks, 'timer': timer}
        if cached is not None:
            early_vff_path = cached[0]
            logger.debug(f"报警 {alarm_id} 登记时消费已提前到达的 vff，立即拼接。")
            self._append_vff_to_tasks(tasks, early_vff_path)
            self._dispatch_tasks(alarm_id, tasks)
            return
        logger.debug(
            f"报警 {alarm_id} 等待 vff 图片，超时 {self.vff_wait_timeout_seconds:g} 秒后放弃等待。"
        )
        timer.start()

    def _claim_pending_vff(self, alarm_id):
        """原子地取出待处理的 vff 等待条目，避免超时与到账并发重复发送。"""
        with self._pending_vff_lock:
            return self._pending_vff_alarms.pop(alarm_id, None)

    def _on_vff_timeout(self, alarm_id):
        """vff 等待超时，直接发送未拼接 vff 的开始图片。"""
        pending = self._claim_pending_vff(alarm_id)
        if pending is None:
            return
        logger.debug(f"报警 {alarm_id} 等待 vff 超时，直接发送未拼接图片。")
        self._dispatch_tasks(alarm_id, pending['tasks'])

    def _append_vff_to_tasks(self, tasks, vff_path):
        """将 vff 图片拼接到每条开始报警任务的图片下方。"""
        for task in tasks:
            local_path = task.get('local_path')
            if not local_path or not os.path.exists(local_path):
                logger.warning(f"开始图片不存在，无法拼接 vff: {local_path}")
                continue
            try:
                before_h = self._probe_image_height(local_path)
                self._append_vff_below(local_path, vff_path, local_path)
                task['image_data'] = self._read_image_file(local_path, 'vff拼接图片')
                after_w, after_h = self._probe_image_dim(local_path)
                logger.debug(
                    f"已将 vff 拼接至开始图片: {local_path} "
                    f"(vff源: {vff_path}, 拼接前高: {before_h}, 拼接后: {after_w}x{after_h})"
                )
            except Exception as e:
                logger.error(f"拼接 vff 到开始图片失败，将保留未拼接图片: {e}", exc_info=True)

    def _dispatch_tasks(self, alarm_id, tasks):
        """将最终任务推入异步上传队列。"""
        logger.debug(f"报警 {alarm_id} 将 {len(tasks)} 条任务推入异步上传队列...")
        for task_bundle in tasks:
            self.upload_queue.put(task_bundle)

    def _sweep_stale_contexts(self, now):
        """定期清理超过 TTL 仍未收到结束报警的告警上下文，防止内存泄漏。"""
        stale_ids = []
        with self._active_alarms_lock:
            for alarm_id, context in list(self.active_alarms.items()):
                created_at = context.get('created_at') or 0
                if now - created_at > self.ACTIVE_CONTEXT_TTL_SECONDS:
                    stale_ids.append(alarm_id)
        expired_vff = 0
        vff_ttl = max(10.0 * self.vff_wait_timeout_seconds, 300.0)
        with self._pending_vff_lock:
            for key, (_path, arrived_at) in list(self._arrived_vff_paths.items()):
                if now - arrived_at > vff_ttl:
                    self._arrived_vff_paths.pop(key, None)
                    expired_vff += 1
        if expired_vff:
            logger.debug(f"清理 {expired_vff} 条超时未消费的 vff 提前投递缓存。")

        for alarm_id in stale_ids:
            self._pop_active_alarm_context(alarm_id)
            self._remember_completed_alarm(alarm_id)
            logger.warning(
                f"告警 {alarm_id} 在 {self.ACTIVE_CONTEXT_TTL_SECONDS} 秒内未收到结束报警，"
                f"已强制清理其上下文（防止内存泄漏）。"
            )

    def publish_message_short_conn(self, msg_body):
        """短连接方式发送单条消息到输出交换机。"""
        conn = None
        try:
            try:
                logger.info(
                    f"[MQ发送] 目标交换机: {self.rabbit_out_cfg['exchange_out']}, "
                    f"routing_key: {self.rabbit_out_cfg.get('routing_key', '')}"
                )
                logger.info(f"[MQ发送] 消息内容: {msg_body}")
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
            except Exception as e:
                logger.error(f"使用短连接发送信息时出错: {e}", exc_info=True)
                return False
            finally:
                if conn and conn.is_open:
                    try:
                        conn.close()
                    except Exception as close_exc:
                        logger.debug(f"安全关闭 RabbitMQ 短连接时异常 (可忽略): {close_exc}")
            return True
        finally:
            if conn and conn.is_open:
                try:
                    conn.close()
                except Exception as close_exc:
                    logger.debug(f"安全关闭 RabbitMQ 短连接时异常 (可忽略): {close_exc}")

    def run(self):
        logger.info("任务处理线程已启动。")
        last_sweep_time = time.time()

        while self._is_running:
            try:
                alarm = self.queue.get(timeout=60)
            except queue.Empty:
                alarm = None
            now = time.time()
            if now - last_sweep_time >= 60:
                last_sweep_time = now
                try:
                    self._sweep_stale_contexts(now)
                except Exception as sweep_exc:
                    logger.error(f"清理过期告警上下文时发生错误: {sweep_exc}", exc_info=True)
            if alarm is None:
                if not self._is_running:
                    break
                continue
            # 每收到一条报警立即分发独立处理线程，多条报警并发处理（多线程），
            # 不再被前一条报警的抓图/等待耗时阻塞顺延。
            self._dispatch_alarm_handling(alarm)

        logger.info("任务处理线程已停止。")

    def _dispatch_alarm_handling(self, alarm):
        """为单条报警启动独立处理线程（守护线程）。"""
        alarm_id = alarm.get('alarm_id') or 'unknown'
        try:
            thread = threading.Thread(
                target=self._run_alarm_handling,
                args=(alarm,),
                daemon=True,
                name=f"AlarmTask-{alarm_id}",
            )
            thread.start()
        except Exception as e:
            logger.critical(f"启动报警 {alarm_id} 的处理线程失败: {e}", exc_info=True)

    def _run_alarm_handling(self, alarm):
        """单条报警的处理入口：同一报警(alarm_id)串行保序 + 全局并发限流 + 异常隔离。"""
        alarm_id = alarm.get('alarm_id')
        if not alarm_id:
            self._handle_alarm_task_guarded(alarm)
            return
        serial_slot = self._acquire_alarm_serial_slot(alarm_id)
        try:
            with serial_slot['lock']:
                with self._alarm_task_semaphore:
                    self._handle_alarm_task_guarded(alarm)
        finally:
            self._release_alarm_serial_slot(alarm_id, serial_slot)

    def _handle_alarm_task_guarded(self, alarm):
        try:
            self.handle_alarm_task(alarm)
        except Exception as e:
            logger.critical(f"处理报警任务时发生未知错误，处理线程将继续运行: {e}", exc_info=True)

    def _acquire_alarm_serial_slot(self, alarm_id):
        """获取同一报警的串行槽（含等待计数），用于结束报警不被并发处理超越开始报警。"""
        with self._alarm_serial_slots_guard:
            slot = self._alarm_serial_slots.get(alarm_id)
            if slot is None:
                slot = {'lock': threading.Lock(), 'waiters': 0}
                self._alarm_serial_slots[alarm_id] = slot
            slot['waiters'] += 1
            return slot

    def _release_alarm_serial_slot(self, alarm_id, slot):
        """释放串行槽；无其它等待者时从字典移除，避免长期运行内存增长。"""
        with self._alarm_serial_slots_guard:
            slot['waiters'] -= 1
            if slot['waiters'] <= 0 and self._alarm_serial_slots.get(alarm_id) is slot:
                self._alarm_serial_slots.pop(alarm_id, None)

    def _next_unique_seq(self):
        """返回 4 位十进制自增序列（并发下保证 ID 唯一，超过 9999 后自然扩位）。"""
        return f'{next(self._unique_seq):04d}'

    def handle_alarm_task(self, alarm):
        """处理单个报警任务。"""
        alarm_id = alarm.get('alarm_id')
        is_end_alarm = bool(alarm.get('end_time'))
        has_start_time = bool(alarm.get('start_time'))
        vff_path = alarm.get('vff_path')
        if vff_path:
            logger.info(f"收到携带 vff_path 的完整告警 JSON: {json.dumps(alarm, ensure_ascii=False)}")

        # vff 图片到账：找到等待中的开始报警并拼接（仅开始态才拼接 vff，结束态不拼接）。
        if vff_path and alarm_id:
            self._receive_vff(alarm_id, vff_path)

        if is_end_alarm:
            active_context = self._get_active_alarm_context(alarm_id)
            if not active_context:
                self._remember_completed_alarm(alarm_id)
                logger.debug(f"处理线程：忽略未知结束报警任务:{alarm_id}")
                return
            try:
                self._handle_end_alarm(alarm, active_context)
            finally:
                self._pop_active_alarm_context(alarm_id)
                self._remember_completed_alarm(alarm_id)
            return

        # 纯 vff 消息（无开始/结束时间）：仅投递 vff，不触发开始/结束抓图。
        if vff_path and not has_start_time:
            return

        # 以下按开始报警处理（携带 vff 的开始消息也会正常抓图）
        if self._get_active_alarm_context(alarm_id):
            logger.debug(f"处理线程：忽略重复的报警任务:{alarm_id}")
            return
        if self._is_completed_alarm(alarm_id):
            logger.debug(f"处理线程：忽略结束报警之后才到达的迟到开始消息:{alarm_id}")
            return

        # 新的开始报警：生成下游告警 ID 并建立活跃上下文。
        # 并发处理下多个报警线程可能落在同一毫秒，追加全局自增序列保证下游 ID 唯一，
        # 避免下游把同毫秒生成的多个报警误判为同一告警。
        new_alarm_id_for_downstream = f'hhkj_jgld_event_{int(time.time() * 1000)}{self._next_unique_seq()}'
        active_context = {
            'downstream_alarm_id': new_alarm_id_for_downstream,
            'video_url': '',
            'camera_payloads': {},
            'created_at': time.time(),
        }
        self._set_active_alarm_context(alarm_id, active_context)

        tasks = self.cam_mgr._find_point_cfgs(alarm.get('point'))
        if not tasks:
            logger.warning(f"处理线程：报警 {alarm_id} 点位 {alarm.get('point')} 未配置，跳过抓图。")
            if self.forward_unconfigured:
                logger.info(f"根据配置，为未配置点位的报警 {alarm_id} 生成一条无图片信息。")
                now = datetime.now()
                ts = f"{now.strftime('%Y%m%d%H%M%S')}{now.microsecond // 1000:03d}"
                message_id = f'hhkj_jgld_alarm_{ts}{self._next_unique_seq()}'
                msg = {
                    'messageid': message_id,
                    'alarmid': new_alarm_id_for_downstream,
                    'alarmlevel': '1',
                    'alarmtime': alarm.get('start_time'),
                    'objectposition': [],
                    'alarmstate': '1',
                    'boxid': self.boxid,
                    'deviceid': 'N/A',
                    'channelid': 'T0001',
                    'bgimage': '',
                    'alarmVideo': '',
                    'alarminfo': [{'box': [], 'category': '0', 'confidence': '-1'}],
                    'isAlarm': '1',
                    'alarmimage': '',
                    'remark': 'Point not configured, no snapshot taken.',
                }
                upload_task = {
                    'local_path': '',
                    'object_name': '',
                    'mq_message': msg,
                    '_received_at': alarm.get('_received_at'),
                }
                self.upload_queue.put(upload_task)
            self._pop_active_alarm_context(alarm_id)
            return

        logger.debug(f"开始处理报警: {alarm_id}")
        upload_tasks = []
        all_tasks_successful = True

        for cam_name, cam_cfg, preset in tasks:
            image_url = ''
            final_video_url = ''
            local_file_path = ''
            minio_object_name = ''
            image_data = b''
            prepared = None
            # 抓图前先预生成图片/视频 URL、创建视频下载任务并构建告警消息，
            # 消息立即进入延迟发送调度（从收到告警时刻起算 alarm_send_delay_seconds），
            # 与抓图、视频下载并行处理，保证报警在配置延迟点准时发出。
            try:
                prepared = self._prepare_camera_task(
                    alarm, cam_name, cam_cfg, 'start', new_alarm_id_for_downstream, ''
                )
                self.upload_queue.put(prepared['forward_task'])
                logger.debug(f"报警 {alarm_id} 相机 {cam_name} 的告警消息已提前进入延迟发送调度。")
                image_url = prepared['image_full_url']
                final_video_url = prepared['video_full_url']
                local_file_path = prepared['local_file_path']
                minio_object_name = prepared['minio_object_name']
            except Exception as e:
                logger.error(f"报警 {alarm_id} 相机 {cam_name} 预生成告警消息任务失败: {e}", exc_info=True)
                all_tasks_successful = False
                # 兜底：构建无图消息仅调度推送，保证报警必达
                self.upload_queue.put(self._build_fallback_forward_task(
                    alarm, cam_cfg, 'start', new_alarm_id_for_downstream
                ))
            if final_video_url:
                active_context['video_url'] = final_video_url
            active_context['camera_payloads'][cam_name] = {
                'alarmimage': image_url,
                'alarmVideo': final_video_url,
                'local_path': local_file_path,
                'object_name': minio_object_name,
            }
            # 抓图（与消息调度、视频下载并行），完成后回填图片数据仅上传 MinIO
            try:
                task_result = self._process_single_camera_task(
                    alarm, cam_name, cam_cfg, preset, 'start', new_alarm_id_for_downstream, '',
                    prebuilt=prepared,
                )
                image_data = task_result['image_data']
            except Exception as e:
                logger.error(f"处理报警 {alarm_id} 的相机 {cam_name} 任务时发生错误: {e}", exc_info=True)
                all_tasks_successful = False
            if prepared is not None:
                prepared['upload_task']['image_data'] = image_data
                upload_tasks.append(prepared['upload_task'])
            logger.debug(f"已为相机 {cam_name} 生成待上传任务 (抓图是否就绪: {bool(image_data)})")

        if upload_tasks:
            if self.vff_wait_timeout_seconds > 0:
                self._register_pending_vff(alarm_id, upload_tasks)
            else:
                logger.debug(f"报警 {alarm_id} 所有抓图动作已完成，将 {len(upload_tasks)} 条任务推入异步上传队列...")
                self._dispatch_tasks(alarm_id, upload_tasks)
        if all_tasks_successful:
            logger.debug(f"报警 {alarm_id} 所有本地任务成功处理完成。")

    def _handle_end_alarm(self, alarm, active_context):
        """结束告警重新执行 PTZ/抓图，并复用开始告警阶段的视频 URL。"""
        alarm_id = alarm.get('alarm_id')
        tasks = self.cam_mgr._find_point_cfgs(alarm.get('point'))

        if not tasks:
            logger.warning(f"处理线程：结束报警 {alarm_id} 点位 {alarm.get('point')} 未配置，跳过发送。")
            return

        for cam_name, cam_cfg, _preset in tasks:
            camera_payload = active_context.get('camera_payloads', {}).get(cam_name, {})
            image_url = ''
            local_file_path = ''
            minio_object_name = ''
            image_data = b''
            video_url = camera_payload.get('alarmVideo', active_context.get('video_url', ''))
            prepared = None
            # 抓图前先预生成结束态图片 URL 并构建结束告警消息，
            # 消息立即进入延迟发送调度（从收到结束告警时刻起算），
            # 与结束态抓图并行处理，保证结束报警在配置延迟点准时发出。
            try:
                prepared = self._prepare_camera_task(
                    alarm, cam_name, cam_cfg, 'end',
                    active_context['downstream_alarm_id'], video_url
                )
                self.upload_queue.put(prepared['forward_task'])
                logger.debug(f"结束报警 {alarm_id} 相机 {cam_name} 的告警消息已提前进入延迟发送调度。")
                image_url = prepared['image_full_url']
                local_file_path = prepared['local_file_path']
                minio_object_name = prepared['minio_object_name']
                video_url = prepared['video_full_url']
            except Exception as e:
                logger.error(f"处理结束报警 {alarm_id} 的相机 {cam_name} 任务时发生错误: {e}", exc_info=True)
                # 兜底：构建无图消息仅调度推送，保证结束报警必达
                self.upload_queue.put(self._build_fallback_forward_task(
                    alarm, cam_cfg, 'end', active_context['downstream_alarm_id']
                ))
            # 结束态抓图（与消息调度并行），完成后回填图片数据仅上传 MinIO
            try:
                task_result = self._process_single_camera_task(
                    alarm, cam_name, cam_cfg, _preset, 'end',
                    active_context['downstream_alarm_id'], video_url,
                    prebuilt=prepared,
                )
                image_data = task_result['image_data']
            except Exception as e:
                logger.error(f"处理结束报警 {alarm_id} 的相机 {cam_name} 抓图时发生错误: {e}", exc_info=True)
            if prepared is not None:
                prepared['upload_task']['image_data'] = image_data
                self.upload_queue.put(prepared['upload_task'])
            logger.debug(f"已为相机 {cam_name} 生成结束告警待上传任务 (结束态重新抓图是否就绪: {bool(image_data)})")

        logger.debug(f"结束报警 {alarm_id} 已生成 {len(tasks)} 条带结束态图片的延迟转发任务。")

    def _prepare_camera_task(self, alarm, cam_name, cam_cfg, alarm_state, downstream_alarm_id, existing_video_url=''):
        """抓图前预生成图片/视频 URL、创建视频下载任务并构建告警消息任务。

        图片/视频 URL 完全由本地信息（告警ID/告警时间/通道号）决定，不依赖
        抓图结果，可在抓图开始前提前生成。告警消息构建后由调用方立即交上传
        线程按 alarm_send_delay_seconds（从收到告警时刻起算）延迟调度推送，
        与抓图、视频下载并行处理，保证报警在配置的延迟点准时发出。

        返回 dict，包含：
        - image_full_url / video_full_url / generated_video_url
        - local_file_path / minio_object_name / local_snapshot_dir
        - mq_message: 告警消息体（forward_task 与 upload_task 共享同一引用）
        - forward_task: 仅调度消息推送（_dispatch_mode='forward_only'）
        - upload_task: 仅上传图片的任务骨架（_dispatch_mode='upload_only'），
          image_data 由抓图完成后回填
        """
        image_full_url = ''
        minio_object_name = ''
        generated_video_url = None

        alarm_dt = self._get_alarm_datetime(alarm, alarm_state)
        today_str = alarm_dt.strftime('%Y-%m-%d')
        base_filename = self._build_alarm_image_filename(alarm_dt, alarm_state, alarm.get('alarm_id'))
        local_snapshot_dir = os.path.join(self.snapshots_dir, today_str)
        ensure_dir(local_snapshot_dir)
        local_file_path = os.path.abspath(os.path.join(local_snapshot_dir, base_filename))

        # 预生成图片 URL：抓图完成后图片数据落位该路径，并上传至同名 MinIO 对象
        try:
            minio_object_name = self.minio_mgr.build_object_name(today_str, base_filename)
            image_full_url = self.minio_mgr.get_object_url(minio_object_name)
        except Exception as e:
            logger.error(f"预生成图片URL时出错: {e}")

        # 预生成视频 URL 并提前创建视频下载任务（与抓图并行，下载更早启动）
        video_full_url = ''
        if alarm_state == 'start':
            # NVR 已移除：所有相机均从相机自身下载录像，统一创建下载任务
            try:
                start_time_str = alarm.get('start_time')
                if '.' in start_time_str:
                    start_time_str_clean = start_time_str.split('.')[0]
                else:
                    start_time_str_clean = start_time_str
                alarm_start_dt = datetime.strptime(start_time_str_clean, '%Y-%m-%d %H:%M:%S')
                today_str_v = alarm_start_dt.strftime('%Y-%m-%d')
                ts_video = alarm_start_dt.strftime('%Y%m%d_%H%M%S')
                video_channel = cam_cfg.get('channel', 1)
                video_base_filename = (
                    f"{alarm['alarm_id']}_{alarm.get('point', 'N_A')}_"
                    f"{video_channel}_{ts_video}_video.mp4"
                )
                video_object_name = self.minio_mgr.build_object_name(today_str_v, video_base_filename)
                video_full_url = self.minio_mgr.get_object_url(video_object_name)
                generated_video_url = video_full_url
                video_task = {
                    'alarm_id': alarm['alarm_id'],
                    'alarm_time': start_time_str_clean,
                    'minio_object_name': video_object_name,
                    'base_filename': video_base_filename,
                    'camera_config': cam_cfg,
                    # 收到 MQ 告警消息的时刻：下载延迟（download_delay_seconds）
                    # 以此为起点计算，而不是以 alarm_time（上游上报的振动事件时间）为起点。
                    '_received_at': alarm.get('_received_at'),
                }
                self.video_queue.put(video_task)
                logger.debug(f"已为报警 {alarm['alarm_id']} 创建视频下载任务，预期URL: {video_full_url}。")
            except Exception as e:
                logger.error(f"创建视频下载任务时失败: {e}", exc_info=True)
        elif alarm_state == 'end':
            video_full_url = existing_video_url

        now = datetime.now()
        ts = f"{now.strftime('%Y%m%d%H%M%S')}{now.microsecond // 1000:03d}"
        message_id = f'hhkj_jgld_alarm_{ts}'
        msg = {
            'messageid': message_id,
            'alarmid': downstream_alarm_id,
            'alarmlevel': '1',
            'alarmtime': alarm.get('start_time' if alarm_state == 'start' else 'end_time'),
            'objectposition': [],
            'alarmstate': '1' if alarm_state == 'start' else '3',
            'boxid': self.boxid,
            'deviceid': cam_cfg.get('cameraid', 'UNKNOWN_CAM'),
            'channelid': 'T0001',
            'bgimage': '',
            'alarmVideo': video_full_url,
            'alarminfo': [{'box': [], 'category': '0', 'confidence': '-1'}],
            'isAlarm': '1' if alarm_state == 'start' else '0',
            'alarmimage': image_full_url,
        }
        forward_task = {
            'mq_message': msg,
            '_received_at': alarm.get('_received_at'),
            '_dispatch_mode': 'forward_only',
        }
        upload_task = {
            'local_path': local_file_path,
            'object_name': minio_object_name,
            'image_data': b'',
            'mq_message': msg,
            '_received_at': alarm.get('_received_at'),
            '_dispatch_mode': 'upload_only',
        }
        return {
            'image_full_url': image_full_url,
            'video_full_url': video_full_url,
            'generated_video_url': generated_video_url,
            'local_file_path': local_file_path,
            'minio_object_name': minio_object_name,
            'local_snapshot_dir': local_snapshot_dir,
            'mq_message': msg,
            'forward_task': forward_task,
            'upload_task': upload_task,
        }

    def _build_fallback_forward_task(self, alarm, cam_cfg, alarm_state, downstream_alarm_id):
        """预生成告警消息任务失败时的兜底：构建无图消息仅调度推送，保证报警必达。"""
        now = datetime.now()
        ts = f"{now.strftime('%Y%m%d%H%M%S')}{now.microsecond // 1000:03d}"
        msg = {
            'messageid': f'hhkj_jgld_alarm_{ts}',
            'alarmid': downstream_alarm_id,
            'alarmlevel': '1',
            'alarmtime': alarm.get('start_time' if alarm_state == 'start' else 'end_time'),
            'objectposition': [],
            'alarmstate': '1' if alarm_state == 'start' else '3',
            'boxid': self.boxid,
            'deviceid': cam_cfg.get('cameraid', 'UNKNOWN_CAM'),
            'channelid': 'T0001',
            'bgimage': '',
            'alarmVideo': '',
            'alarminfo': [{'box': [], 'category': '0', 'confidence': '-1'}],
            'isAlarm': '1' if alarm_state == 'start' else '0',
            'alarmimage': '',
        }
        return {
            'mq_message': msg,
            '_received_at': alarm.get('_received_at'),
            '_dispatch_mode': 'forward_only',
        }

    def _process_single_camera_task(self, alarm, cam_name, cam_cfg, preset, alarm_state, new_alarm_id, existing_video_url='', prebuilt=None):
        """处理单个摄像头的抓图任务（URL 预生成与消息构建见 _prepare_camera_task）。

        prebuilt 为 _prepare_camera_task 的预生成结果：流程层已提前生成 URL 并
        调度消息推送时传入，避免重复创建视频下载任务；为 None（兼容直接调用）
        时内部自动调用 _prepare_camera_task。
        """
        _ = new_alarm_id
        if prebuilt is None:
            prebuilt = self._prepare_camera_task(
                alarm, cam_name, cam_cfg, alarm_state, new_alarm_id, existing_video_url
            )
        image_full_url = prebuilt['image_full_url']
        local_file_path = prebuilt['local_file_path']
        minio_object_name = prebuilt['minio_object_name']
        local_snapshot_dir = prebuilt['local_snapshot_dir']
        logger.debug(f"开始为报警 {alarm['alarm_id']} 处理相机 {cam_name} 的抓图任务。")

        image_data = b''
        primary_capture_path = local_file_path
        fixed_capture_path = ''

        # 定焦相机取自本云台相机的配置（cameras.<name>.fixed_camera），可启用/禁用
        fixed_cfg, fixed_enabled = self._resolve_fixed_camera(cam_cfg)
        if fixed_enabled:
            primary_capture_path = os.path.abspath(
                os.path.join(local_snapshot_dir, f".{alarm['alarm_id']}_{cam_name}_{alarm_state}_primary.jpg")
            )
            fixed_capture_path = os.path.abspath(
                os.path.join(local_snapshot_dir, f".{alarm['alarm_id']}_{cam_name}_{alarm_state}_fixed.jpg")
            )

        retry_count = 0
        fixed_result = {'success': False, 'error': ''}
        fixed_thread = None

        if fixed_enabled:
            fixed_thread = threading.Thread(
                target=self._capture_optional_fixed_snapshot,
                args=(fixed_cfg, fixed_capture_path, fixed_result),
                daemon=True,
                name=f"FixedSnapshot-{alarm['alarm_id']}"
            )
            fixed_thread.start()

        # 抓图/转预置位任务完全并行：多条报警同时到达时，命中同一相机的任务
        # 也同时执行“转预置位→稳定等待→抓图”，相互之间不做任何串行排队。
        while retry_count < self.MAX_CAPTURE_ATTEMPTS:
            try:
                try:
                    connection_info = self.cam_mgr.get_client(cam_cfg)
                    channel = cam_cfg.get('channel', 1)
                    self.cam_mgr.move_to_preset(connection_info, cam_cfg, channel, preset)
                    # 云台转到预置点后必须"等云台转到位、画面稳定"再抓图：
                    # 等待时长取配置项 ptz_capture_wait_seconds（见 config.json）。
                    # preset == -1 表示本点位不移动云台，无需等待。
                    ptz_wait_seconds = getattr(self.cam_mgr, 'ptz_capture_wait_seconds', 0) or 0
                    if ptz_wait_seconds > 0 and str(preset) != '-1':
                        logger.info(
                            f"相机 {cam_name} 云台已转到预置点 {preset}，等待 "
                            f"{ptz_wait_seconds:g} 秒（ptz_capture_wait_seconds）后再抓图。"
                        )
                        time.sleep(ptz_wait_seconds)
                    # 统一预抓图钩子（ONVIF 为短连接，内部为空实现，保留以兼容历史流程）
                    self.cam_mgr.relogin_before_capture(connection_info, cam_cfg)
                    self.cam_mgr.capture(connection_info, cam_cfg, channel, primary_capture_path)
                    primary_image_data = self._read_image_file(primary_capture_path, f'相机 {cam_name} 主抓图')
                    if fixed_thread:
                        fixed_thread.join(timeout=self.FIXED_SNAPSHOT_JOIN_TIMEOUT)
                        if fixed_thread.is_alive():
                            logger.warning(f'相机 {cam_name} 的定焦抓图线程超过 '
                                f'{self.FIXED_SNAPSHOT_JOIN_TIMEOUT}s 未完成，放弃定焦拼图。')
                    fixed_success = fixed_thread is not None and fixed_result.get('success')
                    if not fixed_success and fixed_thread:
                        logger.warning(
                            f"报警 {alarm['alarm_id']} 的定焦图抓取未成功，"
                            f"放弃定焦拼图，仅使用云台主抓图: {fixed_result.get('error', 'unknown')}"
                        )
                    if fixed_success:
                        self._merge_snapshots(primary_capture_path, fixed_capture_path, local_file_path)
                        image_data = self._read_image_file(local_file_path, '拼接图片')
                    else:
                        if primary_capture_path != local_file_path:
                            shutil.copyfile(primary_capture_path, local_file_path)
                        image_data = primary_image_data
                    if retry_count > 0:
                        logger.info(
                            f"相机 {cam_name} 抓图成功（历经 {retry_count} 次重试后成功，"
                            f"总尝试 {retry_count + 1} 次）"
                        )
                    break
                except Exception as cam_e:
                    if fixed_thread and fixed_thread.is_alive():
                        fixed_thread.join(timeout=5)
                    retry_count += 1
                    if retry_count >= self.MAX_CAPTURE_ATTEMPTS:
                        logger.error(
                            f"相机 {cam_name} 连续 {self.MAX_CAPTURE_ATTEMPTS} 次抓图失败，"
                            f"放弃本次抓图: {type(cam_e).__name__}: {cam_e}"
                        )
                        raise
                    logger.error(
                        f"相机 {cam_name} 第 {retry_count} 次抓图失败: "
                        f"{type(cam_e).__name__}: {cam_e}",
                        exc_info=retry_count <= 2
                    )
                    self.cam_mgr.close_and_remove_client(cam_cfg)
                    logger.warning(f"相机 {cam_name} 将立即进行第 {retry_count + 1} 次尝试。")
                self._safe_remove_file(
                    primary_capture_path if primary_capture_path != local_file_path else ''
                )
            finally:
                self._safe_remove_file(
                    primary_capture_path if primary_capture_path != local_file_path else ''
                )
        self._safe_remove_file(fixed_capture_path)

        return {
            'image_full_url': image_full_url,
            'generated_video_url': prebuilt.get('generated_video_url'),
            'video_full_url': prebuilt['video_full_url'],
            'local_file_path': local_file_path,
            'minio_object_name': minio_object_name,
            'image_data': image_data,
        }

    def _capture_optional_fixed_snapshot(self, fixed_cfg, save_path, result_holder):
        """抓取定焦相机图片，失败后立即重试，直至成功。

        定焦相机与云台无关：**不调用 PTZ 预置位**、不做转到位等待，直接 ONVIF 抓图。
        """
        retry_count = 0

        while self._is_running and retry_count < self.MAX_CAPTURE_ATTEMPTS:
            try:
                connection_info = self.cam_mgr.get_client(fixed_cfg)
                channel = fixed_cfg.get('channel', 1)
                # 定焦相机无 PTZ 预置位操作，直接通过 ONVIF 抓图
                self.cam_mgr.relogin_before_capture(connection_info, fixed_cfg)
                self.cam_mgr.capture(connection_info, fixed_cfg, channel, save_path)
                self._read_image_file(save_path, '定焦相机抓图')
                result_holder['success'] = True
                result_holder['error'] = ''
                if retry_count > 0:
                    logger.info(
                        f'定焦相机抓图成功（历经 {retry_count} 次重试后成功，'
                        f'总尝试 {retry_count + 1} 次）'
                    )
                return
            except Exception as exc:
                retry_count += 1
                result_holder['success'] = False
                result_holder['error'] = str(exc)
                if retry_count >= self.MAX_CAPTURE_ATTEMPTS:
                    logger.error(
                        f'定焦相机连续 {self.MAX_CAPTURE_ATTEMPTS} 次抓图失败，'
                        f'放弃定焦抓图: {exc}'
                    )
                    return
                self._safe_remove_file(save_path)
                logger.error(
                    f'定焦相机第 {retry_count} 次抓图失败: '
                    f'{type(exc).__name__}: {exc}',
                    exc_info=retry_count <= 2
                )
                self.cam_mgr.close_and_remove_client(fixed_cfg)
                logger.warning(f'定焦相机将立即进行第 {retry_count + 1} 次抓图尝试。')
        return

    def _merge_snapshots(self, primary_path, fixed_path, output_path):
        """将定焦图放左侧、云台主抓图放右侧，左右拼接成一张图。"""
        if not self.ffmpeg_path:
            raise RuntimeError(ffmpeg_missing_hint('ffmpeg'))
        command = [
            self.ffmpeg_path,
            '-y',
            '-loglevel', 'error',
            '-i', primary_path,
            '-i', fixed_path,
            '-filter_complex', '[1:v][0:v]scale2ref=-1:ih[fixed][main];[fixed][main]hstack=inputs=2[out]',
            '-map', '[out]',
            '-frames:v', '1',
            '-update', '1',
            output_path,
        ]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=30,
            check=False,
            **no_window_kwargs(),
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or 'ffmpeg 拼接失败')
        merged_w, merged_h = self._probe_image_dim(output_path)
        primary_w, primary_h = self._probe_image_dim(primary_path)
        fixed_w, fixed_h = self._probe_image_dim(fixed_path)
        logger.debug(
            f"定焦+云台左右拼接完成: 输出 {output_path} = {merged_w}x{merged_h} "
            f"(定焦 {fixed_w}x{fixed_h} + 云台 {primary_w}x{primary_h})"
        )
        self._read_image_file(output_path, '拼接结果')

    def _probe_image_width(self, image_path):
        """读取图片宽度（像素），失败返回 0。"""
        if not self.ffprobe_path:
            return 0
        result = subprocess.run(
            [self.ffprobe_path, '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'stream=width', '-of', 'default=noprint_wrappers=1:nokey=1', image_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=10,
            check=False,
            **no_window_kwargs(),
        )
        try:
            return int(result.stdout.strip())
        except (ValueError, TypeError):
            return 0

    def _probe_image_height(self, image_path):
        """读取图片高度（像素），失败返回 0。"""
        if not self.ffprobe_path:
            return 0
        result = subprocess.run(
            [self.ffprobe_path, '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'stream=height', '-of', 'default=noprint_wrappers=1:nokey=1', image_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=10,
            check=False,
            **no_window_kwargs(),
        )
        try:
            return int(result.stdout.strip())
        except (ValueError, TypeError):
            return 0

    def _probe_image_dim(self, image_path):
        """读取图片尺寸 [宽,高]（像素），失败返回 (0, 0)。"""
        if not self.ffprobe_path:
            return (0, 0)
        result = subprocess.run(
            [self.ffprobe_path, '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'stream=width:stream=height',
             '-of', 'csv=p=0', image_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=10,
            check=False,
            **no_window_kwargs(),
        )
        try:
            w, h = [p for p in result.stdout.strip().split(',')][:2]
            return (int(w), int(h))
        except (ValueError, TypeError):
            return (0, 0)

    def _append_vff_below(self, base_path, vff_path, output_path):
        """将 vff 图缩放到与开始图片相同宽度后拼接到下方（base_path 在上、vff 在下）。"""
        if not self.ffmpeg_path:
            raise RuntimeError(ffmpeg_missing_hint('ffmpeg'))
        if not os.path.exists(vff_path):
            raise RuntimeError(f'vff 图片不存在: {vff_path}')

        base_width = self._probe_image_width(base_path)
        if base_width <= 0:
            raise RuntimeError(f'无法读取开始图片宽度: {base_path}')

        _, base_ext = os.path.splitext(base_path)
        tmp_output_path = output_path + '.vfftmp' + (base_ext if base_ext else '.jpg')
        command = [
            self.ffmpeg_path,
            '-y',
            '-loglevel', 'error',
            '-i', base_path,
            '-i', vff_path,
            '-filter_complex', f"[1:v]scale={base_width}:ih[vff];[0:v][vff]vstack=inputs=2[out]",
            '-map', '[out]',
            '-frames:v', '1',
            '-update', '1',
            tmp_output_path,
        ]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=30,
            check=False,
            **no_window_kwargs(),
        )
        try:
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or 'ffmpeg vff 拼接失败')
            self._read_image_file(tmp_output_path, 'vff拼接结果')
            os.replace(tmp_output_path, output_path)
        finally:
            self._safe_remove_file(tmp_output_path)

    @staticmethod
    def _safe_remove_file(file_path):
        if not file_path:
            return
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError as exc:
            logger.debug(f"清理临时文件失败（可忽略）: {file_path}, {exc}")

    @staticmethod
    def _read_image_file(file_path, label):
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            raise RuntimeError(f'{label} 文件未生成或为空: {file_path}')
        with open(file_path, 'rb') as image_file:
            image_data = image_file.read()
        if not image_data:
            raise RuntimeError(f'{label} 文件内容为空: {file_path}')
        return image_data

    @staticmethod
    def _parse_alarm_time(raw_time):
        if not raw_time:
            return datetime.now()
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(str(raw_time), fmt)
            except ValueError:
                continue
        logger.warning(f"无法解析报警时间 '{raw_time}'，将回退使用当前时间。")
        return datetime.now()

    def _get_alarm_datetime(self, alarm, alarm_state):
        if alarm_state == 'start':
            alarm_time = alarm.get('start_time')
        else:
            alarm_time = alarm.get('end_time') or alarm.get('start_time')
        return self._parse_alarm_time(alarm_time)

    def _build_alarm_image_filename(self, alarm_dt, alarm_state, alarm_id=''):
        ts = alarm_dt.strftime('%Y%m%d_%H%M%S')
        centiseconds = alarm_dt.microsecond // 10000
        # 基于告警ID生成唯一文件名：同一时刻并发到达的多个告警（同一秒多个点位）必须各自独立成图，
        # 否则共享同一输出文件会被不同告警的 vff 在相同文件上反复叠加，导致多张 vff 被拼进同一张图。
        if alarm_id:
            return f"{self.boxid}_{ts}{centiseconds:02d}_{alarm_id}_{alarm_state}.jpg"
        return f"{self.boxid}_{ts}{centiseconds:02d}_{alarm_state}.jpg"
