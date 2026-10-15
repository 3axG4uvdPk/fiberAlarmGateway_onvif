# -*- coding: utf-8 -*-
"""
视频下载模块。

统一使用各厂家**私有 RTSP 回放协议**（ffmpeg 拉流）直接下载摄像机本地录像，
彻底抛弃海康 / 大华 / 宇视的私有 SDK（不再经由 NVR）：

- 海康 Hikvision：``/Streaming/tracks/<tracksID>``，时间参数为 **UTC**；
- 大华 Dahua：``/cam/playback``，时间参数为 **本地时间**（且参数顺序/数量严格）；
- 宇视 Uniview：``/media/record<N>``，时间参数为 **UTC**。

功能：
- 可配置的报警前后视频下载时长（按相机配置，见 cameras.<name>）
- 下载失败自动重试机制（持续重试直到成功，收到关闭信号时中断退出）
- 多条报警的视频任务并发下载；与抓图/PTZ 完全并行互不影响
"""
import os
import re
import time
import queue
import shlex
import shutil
import threading
import collections
import concurrent.futures
import contextlib
import subprocess
import logging
from datetime import datetime, timedelta
from urllib.parse import quote

logger = logging.getLogger("alarm_app.video")

from core.config import resolve_runtime_path
from core.ffmpeg_utils import (
    resolve_ffmpeg_path,
    run_command,
    no_window_kwargs,
    ffmpeg_missing_hint,
)

# 关闭信号事件：主程序停止时置位，正在执行的重试任务会尽快退出，
# 避免 thread_pool.shutdown(wait=True) 被无限重试的任务阻塞导致关闭卡死。
DOWNLOADER_SHUTDOWN_EVENT = threading.Event()

# 视频下载时长/延迟默认值（秒）。这些参数配置在 cameras.<name> 下，逐相机生效，
# 相机未配置时使用下列默认值。
DEFAULT_DOWNLOAD_DELAY_SECONDS = 0
DEFAULT_VIDEO_DOWNLOAD_BEFORE_SECONDS = 15
DEFAULT_VIDEO_DOWNLOAD_AFTER_SECONDS = 15
DEFAULT_VIDEO_DOWNLOAD_TIMEOUT_SECONDS = 30

# --- 私有 RTSP 回放协议常量 ---
RTSP_DEFAULT_PORT = 554
# 宇视/海康的时间参数按 UTC 解析（本地时间 - 8 小时），大华按本地时间解析（原样填）。
UTC_OFFSET_HOURS = 8
# RTSP socket 超时（微秒，5 秒）。新版 ffmpeg 用 -timeout，旧版用 -stimeout。
RTSP_SOCKET_TIMEOUT_MICROSECONDS = '5000000'
# ffmpeg 单次拉流超时：未配置 video_download_timeout 时按 时长*4、下限 60 秒计算
FFMPEG_TIMEOUT_MULTIPLIER = 4
MIN_FFMPEG_TIMEOUT_SECONDS = 60

# 日志中脱敏 RTSP URL 的密码
_RTSP_CREDENTIALS_RE = re.compile(r"(rtsp://[^:/@\s]+):[^@/\s]*@")

# ffmpeg stderr 中需要过滤掉的进度/横幅类噪声行
_FFMPEG_PROGRESS_RE = re.compile(r"(frame=|size=|bitrate=|speed=|Last message repeated)")
# 「有连接但没有录像数据」的 ffmpeg 报错特征
NO_RECORDING_HINTS = ('does not contain any stream', 'Output file is empty')

# 支持通过私有 RTSP 回放协议下载录像的相机厂家
SUPPORTED_MANUFACTURERS = ("hikvision", "dahua", "uniview")


def resolve_video_download_durations(cam_cfg):
    """从相机配置解析（下载延迟, 报警前时长, 报警后时长），单位秒。

    下载延迟以收到 MQ 报警消息的时刻为起点计算（见 run() 中的 executable_time）；
    报警前/后时长用于推算要下载的录像时间段，仍以报警消息中的 alarm_time 为基准。
    """
    cam_cfg = cam_cfg or {}
    return (
        cam_cfg.get('download_delay_seconds', DEFAULT_DOWNLOAD_DELAY_SECONDS),
        cam_cfg.get('video_download_before_seconds', DEFAULT_VIDEO_DOWNLOAD_BEFORE_SECONDS),
        cam_cfg.get('video_download_after_seconds', DEFAULT_VIDEO_DOWNLOAD_AFTER_SECONDS),
    )


# --- 工具函数 ---

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def mask_rtsp_url(url):
    """日志中脱敏 RTSP URL 的密码（rtsp://user:***@ip:554/...）。"""
    if not url:
        return url
    return _RTSP_CREDENTIALS_RE.sub(r"\1:***@", str(url))


def _positive_int(value, default):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _stream_index(cam_cfg, default=1):
    """规范化码流编号：1=主码流，2=子码流，3=第三码流。"""
    raw = cam_cfg.get('stream_type', default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value in (1, 2, 3) else default


def _rtsp_authority(cam_cfg):
    """拼接 RTSP URL 的 ``user:pwd@ip:port`` 部分（凭据做 URL 编码）。"""
    user = quote(str(cam_cfg.get('username') or ''), safe='')
    pwd = quote(str(cam_cfg.get('pwd') or cam_cfg.get('password') or ''), safe='')
    ip = cam_cfg.get('ip')
    if not ip:
        raise ValueError('相机配置缺少 ip，无法拼接 RTSP 回放 URL。')
    port = _positive_int(cam_cfg.get('rtsp_port'), RTSP_DEFAULT_PORT)
    credentials = f"{user}:{pwd}@" if user else ''
    return f"{credentials}{ip}:{port}"


def _utc_compact(dt):
    """本地时间 -> UTC 紧凑格式（宇视/海康要求的 ``YYYYMMDDTHHMMSSZ``）。"""
    return (dt - timedelta(hours=UTC_OFFSET_HOURS)).strftime('%Y%m%dT%H%M%SZ')


def _hikvision_tracks_id(cam_cfg, stream_index):
    """海康 tracksID：通道号*100 + 码流号（IPC 单通道主/子/第三码流 = 101/102/103）。

    现场 NVR/DVR 等特殊编号可用配置项 ``tracks_id`` 直接覆盖。
    """
    override = cam_cfg.get('tracks_id')
    if override not in (None, ''):
        return _positive_int(override, 101)
    channel = _positive_int(cam_cfg.get('channel'), 1)
    return channel * 100 + stream_index


def build_playback_rtsp_url(cam_cfg, start_dt, end_dt):
    """按厂家私有 RTSP 回放协议拼接指定时间段的录像回放 URL。

    时间语义是本协议最容易踩坑的地方（详见《私有RTSP回放协议说明.md》）：
    - 宇视：参数为 UTC（本地时间 - 8 小时），路径 ``/media/record<N>``；
    - 大华：参数为本地时间（**不减 8 小时**），路径 ``/cam/playback``，
      且参数顺序/数量严格：channel -> subtype -> starttime -> endtime，
      不得携带任何多余参数（type=/playback=/em= 等都会 404）；
    - 海康：参数为 UTC（本地时间 - 8 小时），路径 ``/Streaming/tracks/<tracksID>``。
    """
    manufacturer = str(cam_cfg.get('manufacturer', '')).lower()
    authority = _rtsp_authority(cam_cfg)
    stream_index = _stream_index(cam_cfg)

    if manufacturer == 'uniview':
        begin = _utc_compact(start_dt)
        end = _utc_compact(end_dt)
        return f"rtsp://{authority}/media/record{stream_index}?begintime={begin}&endtime={end}"

    if manufacturer == 'dahua':
        begin = start_dt.strftime('%Y_%m_%d_%H_%M_%S')
        end = end_dt.strftime('%Y_%m_%d_%H_%M_%S')
        channel = _positive_int(cam_cfg.get('channel'), 1)
        subtype = max(stream_index - 1, 0)
        return (
            f"rtsp://{authority}/cam/playback?channel={channel}&subtype={subtype}"
            f"&starttime={begin}&endtime={end}"
        )

    if manufacturer == 'hikvision':
        begin = _utc_compact(start_dt)
        end = _utc_compact(end_dt)
        tracks_id = _hikvision_tracks_id(cam_cfg, stream_index)
        return (
            f"rtsp://{authority}/Streaming/tracks/{tracks_id}"
            f"?starttime={begin}&endtime={end}"
        )

    raise ValueError(f"不支持的相机厂家，无法拼接 RTSP 回放 URL: {cam_cfg.get('manufacturer')!r}")


def format_command_for_log(command):
    """把参数列表格式化成便于直接复制执行的一行命令（仅用于日志展示）。

    ffmpeg 以**参数列表**方式直接执行（不经过 shell），因此 RTSP URL 中的
    ``?``、``&``、``=``、``@`` 等字符无需任何转义，也不会被 cmd.exe /
    PowerShell 二次解析——这比「整条 URL 加引号后交给 shell」更可靠，
    同时也避免了 Windows 下 cmd.exe 不识别 ``\\"`` 转义的问题。
    """
    parts = [str(part) for part in command]
    if os.name == 'nt':
        return subprocess.list2cmdline(parts)
    return shlex.join(parts)


def summarize_ffmpeg_output(stderr, limit=1200):
    """提取 ffmpeg 输出中的有效行（丢弃 banner/进度行），用于日志与错误上报。

    命令与协议说明保持一致（不加 ``-loglevel``），因此 stderr 里会包含版本横幅与
    进度统计；这里按 ``\\r``/``\\n`` 切分并过滤掉进度行，保留真正有用的报错与
    流信息，避免错误信息被进度行淹没。
    """
    if not stderr:
        return ''
    lines = []
    for raw_line in re.split(r'[\r\n]+', stderr):
        line_text = raw_line.strip()
        if not line_text or _FFMPEG_PROGRESS_RE.search(line_text):
            continue
        if line_text not in lines:
            lines.append(line_text)
    text = '\n'.join(lines)
    return text[-limit:] if len(text) > limit else text


def _no_recording_hint(cam_cfg, stderr):
    """当 ffmpeg 报「输出不含任何流」时，给出最可能的原因提示。

    实测：宇视摄像机的某路码流在该时间段没有录像时，回放 SDP 的 fmtp 为空
    （无 sprop 参数集），ffmpeg 解析不出编码参数而直接判定没有输出流。
    """
    if not any(hint in stderr for hint in NO_RECORDING_HINTS):
        return ''
    return (
        f"（该时间段内 stream_type={cam_cfg.get('stream_type', 1)} 对应的码流没有录像数据："
        f"请确认摄像机是否开启该码流录像；宇视 record1/2/3 = 主/子/第三码流，"
        f"大华 subtype 0/1 = 主/子码流，海康 tracks 101/102/103 = 主/子/第三码流）"
    )


def _ffmpeg_executable():
    """定位 ffmpeg 可执行文件。

    优先使用随项目分发的免安装包（``ffmpeg-win64/bin/ffmpeg.exe``），
    其次可执行文件同级目录、内嵌打包目录，最后才是系统 PATH；
    全部找不到时返回 None。
    """
    return resolve_ffmpeg_path()


def resolve_download_timeout(cam_cfg, duration_seconds):
    """解析单次 ffmpeg 拉流超时（秒）。

    优先使用相机配置 ``video_download_timeout``；未配置（或 <=0）时按
    拉流时长 * 4、下限 60 秒计算，避免慢设备被过早判定为超时。
    """
    configured = (cam_cfg or {}).get('video_download_timeout')
    if configured is not None:
        try:
            configured = float(configured)
        except (TypeError, ValueError):
            configured = None
    if configured and configured > 0:
        return configured
    return max(duration_seconds * FFMPEG_TIMEOUT_MULTIPLIER, MIN_FFMPEG_TIMEOUT_SECONDS)


def download_recording_by_rtsp(cam_cfg, start_dt, end_dt, save_path):
    """用 ffmpeg 从摄像机私有 RTSP 回放流拉取指定时间段的录像到本地文件。

    下载一经开始不中途中止（与抓图/PTZ 互不影响）；失败由上层无限重试。
    返回保存路径。
    """
    ffmpeg_path = _ffmpeg_executable()
    if not ffmpeg_path:
        raise RuntimeError(ffmpeg_missing_hint('ffmpeg'))

    rtsp_url = build_playback_rtsp_url(cam_cfg, start_dt, end_dt)
    duration_seconds = max((end_dt - start_dt).total_seconds(), 1.0)
    timeout_seconds = resolve_download_timeout(cam_cfg, duration_seconds)
    ensure_dir(os.path.dirname(os.path.abspath(save_path)) or '.')

    logger.debug(
        f"[RTSP] 下载计划: url={mask_rtsp_url(rtsp_url)}，"
        f"begin={start_dt.strftime('%Y-%m-%d %H:%M:%S')}，"
        f"end={end_dt.strftime('%Y-%m-%d %H:%M:%S')}，"
        f"duration={duration_seconds:g}s，timeout={timeout_seconds:g}s，"
        f"save_path={save_path}"
    )

    # -timeout / -stimeout 是 RTSP 解复用器的 socket 超时（微秒），不同 ffmpeg 版本
    # 支持的参数名不同：新版为 -timeout，旧版为 -stimeout。先按新版尝试，若 ffmpeg
    # 报 "Option timeout not found" 再自动换旧参数重试。
    last_error = None
    for timeout_option in ('-timeout', '-stimeout'):
        # 命令格式与《私有RTSP回放协议说明.md》完全一致：
        #   ffmpeg -nostdin -rtsp_transport tcp -timeout 5000000
        #          -i "<RTSP_URL>" -t <时长秒> -c copy -an -y "<输出.mp4>"
        # 以参数列表方式直接交给 ffmpeg（不经过 shell）：URL 中的 ? & = @ 天然
        # 作为一个完整参数传递，Windows 下不会被 cmd.exe 二次解析，
        # ffmpeg 路径与输出路径含空格/中文时也不会被误拆。
        command = [
            ffmpeg_path, '-nostdin',
            '-rtsp_transport', 'tcp',
            timeout_option, RTSP_SOCKET_TIMEOUT_MICROSECONDS,
            '-i', rtsp_url,
            '-t', f'{duration_seconds:g}',
            '-c', 'copy',
            '-an',
            '-y', save_path,
        ]
        logger.debug(f"[RTSP] 执行命令: {mask_rtsp_url(format_command_for_log(command))}")

        try:
            returncode, _stdout, stderr = run_command(command, timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"ffmpeg 拉取 RTSP 回放流超时（>{timeout_seconds:g} 秒）"
            ) from exc

        raw_stderr = stderr or ''
        stderr = raw_stderr.strip()
        if returncode == 0:
            if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                logger.debug(
                    f"[RTSP] 下载成功: {save_path}（{os.path.getsize(save_path)} 字节）"
                )
                return save_path
            raise RuntimeError('ffmpeg 执行成功但输出文件为空。')

        if 'not found' in stderr and timeout_option.lstrip('-') in stderr:
            logger.debug(f"[RTSP] 当前 ffmpeg 不支持 {timeout_option}，改用另一超时参数重试。")
            last_error = stderr
            continue

        summary = summarize_ffmpeg_output(raw_stderr)
        hint = _no_recording_hint(cam_cfg, raw_stderr)
        if hint:
            logger.error(
                f"[RTSP] 请求时间段内没有录像数据（url={mask_rtsp_url(rtsp_url)}）{hint}"
            )
        raise RuntimeError(
            f"ffmpeg 拉取 RTSP 回放流失败（返回码 {returncode}）: {summary}{hint}"
        )

    raise RuntimeError(f"ffmpeg 不支持 RTSP 超时参数: {last_error}")


# =====================================================================
# Video Downloader
# =====================================================================

class VideoDownloader(threading.Thread):
    """视频下载调度器，负责任务延迟派发和任务队列管理。

    采用小规模线程池并发下载（固定 20 个工作线程），多条报警的视频任务
    并发执行、不再逐条串行排队：
    - 录像下载统一通过厂家私有 RTSP 回放协议（ffmpeg）从相机自身拉取，经由
      CameraManager 的设备级操作门闩以 shared 模式执行，同一设备串行排队；
    - 视频下载与抓图/PTZ 是完全独立的任务线（下载走 RTSP、抓图/PTZ 走 ONVIF），
      互不阻塞、互不中止——下载一经开始绝不中途终止；抓图/PTZ 不经过门闩，
      多条报警同时到达时同时转预置位并行抓图；连接清理走 exclusive 独占；
    - 重试退避期间释放下载并发额度，防止单个无法成功的任务饿死其它任务；
    - 转码与 MinIO 上传在下载并发额度释放后进行，不占用下载并发额度。
    下载失败时持续重试，直到下载成功才释放该任务占用的线程资源；
    若设备持续不可用，后续任务会排队等待，视频证据任务不允许被放弃。
    MinIO 不可用时自动暂存失败上传，待连接恢复后自动重试。
    """

    RETRY_BASE_DELAY = 5       # 重试基础延迟（秒）
    RETRY_MAX_DELAY = 60       # 重试最大延迟上限（秒）
    MAX_PENDING_TASKS = 500    # 待派发任务队列上限，防止告警洪峰时任务无界堆积

    # 类级别：失败视频上传等待队列（供静态下载方法访问）
    MAX_PENDING_VIDEO_UPLOADS = 200
    _failed_video_uploads = collections.deque()
    _failed_video_lock = threading.Lock()

    def __init__(self, config, minio_mgr, video_queue, cam_mgr=None):
        super().__init__(daemon=True, name="VideoDownloader")
        self.config = config
        self.queue = video_queue
        self.minio_mgr = minio_mgr
        self.cam_mgr = cam_mgr
        self._is_running = True
        self.videos_dir = resolve_runtime_path(self.config.get('videos_dir', 'videos'))

        # 注册 MinIO 重连回调，用于重试失败的视频上传
        self.minio_mgr.register_reconnect_callback(self._retry_failed_video_uploads)

        # 视频下载时长/延迟按相机配置（cameras.<name>）逐任务解析，
        # 在调度与派发时通过 resolve_video_download_durations() 读取。

        # 视频下载并发工作线程数固定为 20。
        # 若保持单线程，一个因录像不存在等原因无限重试的任务会长期独占唯一工作线程，
        # 导致后续告警的视频下载任务被饿死（表现为日志中第二条报警不再触发下载）。
        # 配合“重试退避期间释放设备锁”的机制，既保留“视频证据不允许放弃”的无限重试，
        # 又能保证其它告警的视频任务能继续执行。
        max_workers = 20
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix='VideoWorker'
        )

        self.currently_processing = set()
        self.pending_tasks = []

    def stop(self):
        self._is_running = False
        self.queue.put(None)
        logger.info("正在关闭视频下载线程池，请稍候...")
        # 通知正在执行的重试任务立即退出，避免 shutdown(wait=True) 无限等待
        DOWNLOADER_SHUTDOWN_EVENT.set()
        self.thread_pool.shutdown(wait=True)
        logger.info("视频下载线程池已安全关闭。")

    def run(self):
        """主循环，作为任务调度器，将到期任务派发给线程池。"""
        logger.info("视频下载调度器已启动。")
        while self._is_running:
            try:
                # 接收新任务
                try:
                    new_task = self.queue.get(timeout=1.0)
                    if new_task is None:
                        self._is_running = False
                        continue
                    alarm_id = new_task['alarm_id']
                    delay_seconds, _, _ = resolve_video_download_durations(new_task.get('camera_config'))
                    # 下载延迟以【收到 MQ 报警消息】的时刻为起点计算：
                    # alarm_time 是上游上报的振动事件时间，与消息实际到达网关的时刻未必一致
                    # （上游侦测/上报耗时、设备与网关时钟偏差）。若以 alarm_time 为起点，
                    # 消息到达时 alarm_time + delay 往往已经过期，表现为“延迟不生效、收到
                    # 消息后立即下载录像”。取不到消息接收时间时退化为“调度器收到任务的时刻”。
                    received_at = new_task.get('_received_at')
                    if isinstance(received_at, (int, float)) and received_at > 0:
                        delay_base_dt = datetime.fromtimestamp(received_at)
                    else:
                        delay_base_dt = datetime.now()
                        logger.warning(
                            f"任务 {alarm_id} 缺少 MQ 消息接收时间，下载延迟将按当前时刻起算。"
                        )
                    new_task['executable_time'] = delay_base_dt + timedelta(seconds=delay_seconds)
                    new_task['retry_count'] = 0
                    self.pending_tasks.append(new_task)
                    if len(self.pending_tasks) > self.MAX_PENDING_TASKS:
                        dropped = self.pending_tasks.pop(0)
                        logger.error(
                            f"待派发视频任务数超过上限 ({self.MAX_PENDING_TASKS})，"
                            f"丢弃最旧任务: {dropped.get('alarm_id', 'unknown')}"
                        )
                except queue.Empty:
                    pass

                # 派发到期任务
                remaining_tasks = []
                now = datetime.now()
                for task in self.pending_tasks:
                    task_id = task['alarm_id']
                    if now >= task['executable_time'] and task_id not in self.currently_processing:
                        _, before_seconds, after_seconds = resolve_video_download_durations(
                            task.get('camera_config')
                        )
                        try:
                            alarm_time_dt = datetime.strptime(task['alarm_time'], '%Y-%m-%d %H:%M:%S')
                            start_dt = alarm_time_dt - timedelta(seconds=before_seconds)
                            end_dt = alarm_time_dt + timedelta(seconds=after_seconds)
                            time_range = f"{start_dt.strftime('%H:%M:%S')} 到 {end_dt.strftime('%H:%M:%S')}"
                        except Exception:
                            time_range = "时间段未知"

                        logger.debug(
                            f"调度器：任务 {task_id} 已到期 (下载时间段: {time_range})"
                        )
                        self.currently_processing.add(task_id)

                        task['videos_dir'] = self.videos_dir
                        task['minio_mgr'] = self.minio_mgr
                        task['cam_mgr'] = self.cam_mgr
                        task['before_seconds'] = before_seconds
                        task['after_seconds'] = after_seconds

                        future = self.thread_pool.submit(self._execute_download_task, task)
                        future.task_id = task_id
                        future.add_done_callback(self._task_done_callback)
                    else:
                        remaining_tasks.append(task)
                self.pending_tasks = remaining_tasks

            except Exception as e:
                logger.error(f"视频调度线程主循环发生未知错误: {e}", exc_info=True)
                time.sleep(5)
        logger.info("视频下载调度器已停止。")

    def _task_done_callback(self, future):
        """任务完成回调，清理状态。"""
        task_id = future.task_id
        if task_id in self.currently_processing:
            self.currently_processing.remove(task_id)
        exception = future.exception()
        if exception:
            logger.error(f"视频下载任务 {task_id} 因异常而失败: {exception}")
        else:
            logger.debug(f"视频下载任务 {task_id} 已处理完毕。")

    @classmethod
    def _add_failed_video_upload(cls, file_path, object_name, content_type='video/mp4'):
        """将上传失败的视频文件加入待重试队列。同 object_name 去重。"""
        upload_entry = {
            'file_path': file_path,
            'object_name': object_name,
            'content_type': content_type,
        }
        with cls._failed_video_lock:
            # 去重：同一 object_name 只保留一条
            for existing in cls._failed_video_uploads:
                if existing.get('object_name') == object_name:
                    logger.debug(
                        f"视频上传任务已在等待队列中，跳过重复添加: {object_name}"
                    )
                    return
            if len(cls._failed_video_uploads) >= cls.MAX_PENDING_VIDEO_UPLOADS:
                dropped = cls._failed_video_uploads.popleft()
                logger.error(
                    f"视频上传失败队列已满 ({cls.MAX_PENDING_VIDEO_UPLOADS})，"
                    f"丢弃最旧: {dropped.get('object_name', 'unknown')}"
                )
            cls._failed_video_uploads.append(upload_entry)
            logger.info(
                f"视频上传失败已加入等待队列: {object_name}，"
                f"当前待重试: {len(cls._failed_video_uploads)} 个。"
            )

    def _retry_failed_video_uploads(self):
        """重连回调：重新尝试所有等待队列中的视频上传任务。"""
        with VideoDownloader._failed_video_lock:
            if not VideoDownloader._failed_video_uploads:
                return
            pending = list(VideoDownloader._failed_video_uploads)
            VideoDownloader._failed_video_uploads.clear()

        if not self.minio_mgr.is_available:
            logger.warning(
                f"视频重连回调被触发但 MinIO 仍不可用，"
                f"{len(pending)} 个视频任务将保留在队列中。"
            )
            with VideoDownloader._failed_video_lock:
                for upload in reversed(pending):
                    VideoDownloader._failed_video_uploads.appendleft(upload)
            return

        logger.info(f"开始重试 {len(pending)} 个待处理的视频上传任务...")
        success_count = 0
        retry_failed = []

        for upload in pending:
            file_path = upload['file_path']
            object_name = upload['object_name']
            if not os.path.exists(file_path):
                logger.warning(f"待上传视频文件已不存在，跳过: {file_path}")
                continue

            if self.minio_mgr.upload_file_chunked(
                file_path, object_name, content_type=upload.get('content_type', 'video/mp4')
            ):
                success_count += 1
                logger.info(f"视频重试上传成功: {object_name}")
            else:
                retry_failed.append(upload)
                logger.warning(f"视频重试上传仍失败: {object_name}，将重新入队。")

        # 重试仍失败的任务重新入队
        if retry_failed:
            with VideoDownloader._failed_video_lock:
                for upload in reversed(retry_failed):
                    VideoDownloader._failed_video_uploads.appendleft(upload)
            logger.warning(
                f"{len(retry_failed)} 个视频任务重试失败，已重新加入等待队列。"
            )

        logger.info(
            f"视频上传重试完成: 成功 {success_count}/{len(pending)}，"
            f"仍失败 {len(retry_failed)}，当前队列剩余 {len(VideoDownloader._failed_video_uploads)} 个。"
        )

    @staticmethod
    def _execute_download_task(task):
        """工作函数：获取设备执行权，通过厂家私有 RTSP 回放协议下载录像。"""
        cam_cfg = task['camera_config']
        alarm_id = task['alarm_id']
        manufacturer = str(cam_cfg.get('manufacturer', '')).lower()

        # 下载目标即相机自身（NVR 已移除，统一从相机自身拉取其本地录像）
        device_ip = cam_cfg.get('ip')
        if not device_ip:
            logger.error(f"[Worker-{alarm_id}] 无法确定目标设备IP，任务终止。")
            return
        if manufacturer not in SUPPORTED_MANUFACTURERS:
            logger.error(f"[Worker-{alarm_id}] 不支持的设备厂家: '{manufacturer}'，任务终止。")
            return

        # 设备并发控制（使用 CameraManager 的设备级操作门闩，key 为设备 IP）：
        # 录像下载统一走 shared 共享模式，同一设备串行执行：设备侧可同时承载的
        # 回放码流数有限，多路并发时 RTSP SETUP 会被设备拒绝。多路报警依次排队
        # 下载，既不会互相挤占设备码流资源，也不会被放弃。
        # 抓图/PTZ 走 ONVIF、下载走私有 RTSP，两套协议栈完全独立，二者可真正并行、
        # 互不中止：下载一经开始绝不中途终止，抓图/PTZ 也不经过门闩。
        cam_mgr = task.get('cam_mgr')
        device_gate = cam_mgr.get_operation_gate(device_ip=device_ip) if cam_mgr else None
        if device_gate is None:
            logger.warning(
                f"[Worker-{alarm_id}] 未提供 cam_mgr，视频下载将不经过设备操作门闩执行。"
            )

        # 重试机制：不限制重试次数，持续重试直到下载成功，视频证据任务不允许被放弃。
        # 下载并发额度仅在单次下载尝试期间持有，重试退避等待时释放：
        # 一个因录像不存在等原因永远无法成功的任务若长期占用下载额度与工作线程，
        # 会饿死后续告警的视频下载任务（日志中表现为第二条报警不再触发下载）。
        # 程序收到关闭信号时（DOWNLOADER_SHUTDOWN_EVENT 置位）中断重试，保证线程池可正常关闭。
        retry_count = 0

        while True:
            if DOWNLOADER_SHUTDOWN_EVENT.is_set():
                logger.warning(
                    f"[Worker-{alarm_id}] 收到关闭信号，中断视频下载任务的重试。"
                )
                return
            raw_video_path = None
            try:
                if device_gate is not None:
                    # 下载走共享模式：同一设备串行下载，与抓图/PTZ 并行互不阻塞
                    download_scope = device_gate.shared()
                else:
                    download_scope = contextlib.nullcontext()
                with download_scope:
                    logger.debug(
                        f"[Worker-{alarm_id}] 已获得设备 {device_ip} 的下载并发额度，开始执行。"
                    )
                    raw_video_path = VideoDownloader._execute_rtsp_download(task, cam_cfg)

                # 下载并发额度已释放：转码与上传不依赖设备，放到额度外执行，
                # 缩短单个任务占用下载额度的时间，让后续下载任务尽快获得额度。
                if raw_video_path:
                    VideoDownloader._postprocess_downloaded_video(task, raw_video_path)
                else:
                    logger.warning(
                        f"[Worker-{alarm_id}] 下载函数未返回文件路径，跳过转码与上传。"
                    )

                logger.info(f"[Worker-{alarm_id}] 已释放设备 {device_ip} 的下载并发额度。")
                if retry_count > 0:
                    logger.info(f"[Worker-{alarm_id}] 下载成功（历经 {retry_count} 次重试后成功，总尝试 {retry_count + 1} 次）")
                else:
                    logger.info(f"[Worker-{alarm_id}] 下载成功（首次尝试即成功）")
                return

            except Exception as e:
                if raw_video_path and os.path.exists(raw_video_path):
                    try:
                        os.remove(raw_video_path)
                    except OSError:
                        pass
                retry_count += 1
                logger.error(
                    f"[Worker-{alarm_id}] 第 {retry_count} 次下载失败: {type(e).__name__}: {e}",
                    exc_info=(retry_count <= 2)
                )
                delay = min(
                    VideoDownloader.RETRY_BASE_DELAY * (2 ** (retry_count - 1)),
                    VideoDownloader.RETRY_MAX_DELAY
                )
                logger.warning(
                    f"[Worker-{alarm_id}] 将在 {delay} 秒后进行第 {retry_count + 1} 次尝试..."
                )
                # 用事件等待代替纯 sleep：收到关闭信号时立即中断，而不是干等满 delay 秒
                if DOWNLOADER_SHUTDOWN_EVENT.wait(timeout=delay):
                    logger.warning(
                        f"[Worker-{alarm_id}] 收到关闭信号，中断视频下载任务的重试。"
                    )
                    return

    @staticmethod
    def _postprocess_downloaded_video(task, raw_video_path):
        """转码并上传已下载的原始视频（在设备执行权释放后调用）。"""
        alarm_id = task['alarm_id']
        if not os.path.exists(raw_video_path) or os.path.getsize(raw_video_path) == 0:
            raise RuntimeError(f"下载的视频文件为空或不存在: {raw_video_path}")

        transcoded_path = os.path.abspath(
            os.path.join(os.path.dirname(raw_video_path), task['base_filename'])
        )
        if not transcode_video_static(raw_video_path, transcoded_path):
            raise RuntimeError("视频转码失败。")
        try:
            os.remove(raw_video_path)
        except OSError:
            pass

        # MinIO 上传（独立 try/except，异常不触发视频重下）
        try:
            minio_mgr = task['minio_mgr']
            if minio_mgr.is_available:
                if not minio_mgr.upload_file_chunked(transcoded_path, task['minio_object_name']):
                    logger.warning(
                        f"[Worker-{alarm_id}] MinIO 上传失败，任务已入队等待重试。"
                    )
                    VideoDownloader._add_failed_video_upload(
                        transcoded_path, task['minio_object_name']
                    )
            else:
                logger.warning(
                    f"[Worker-{alarm_id}] MinIO 不可用，视频上传任务已入队等待重试。"
                )
                VideoDownloader._add_failed_video_upload(
                    transcoded_path, task['minio_object_name']
                )
        except Exception as minio_exc:
            logger.warning(
                f"[Worker-{alarm_id}] MinIO 上传异常（不影响下载结果）: {minio_exc}，"
                f"任务已入队等待重试。"
            )
            VideoDownloader._add_failed_video_upload(
                transcoded_path, task['minio_object_name']
            )

    @staticmethod
    def _execute_rtsp_download(task, cam_cfg):
        """通过厂家私有 RTSP 回放协议下载指定时间段的录像（ffmpeg 拉流）。

        时间段的计算与历史版本一致：以报警时刻为基准，向前取
        video_download_before_seconds、向后取 video_download_after_seconds。
        URL 拼接由 ``build_playback_rtsp_url`` 按厂家完成，时间语义差异
        （宇视/海康为 UTC，大华为本地时间）在该函数内统一处理。
        """
        alarm_id = task['alarm_id']
        alarm_dt = datetime.strptime(task['alarm_time'], '%Y-%m-%d %H:%M:%S')
        before_seconds = task.get('before_seconds', DEFAULT_VIDEO_DOWNLOAD_BEFORE_SECONDS)
        after_seconds = task.get('after_seconds', DEFAULT_VIDEO_DOWNLOAD_AFTER_SECONDS)
        start_dt = alarm_dt - timedelta(seconds=before_seconds)
        end_dt = alarm_dt + timedelta(seconds=after_seconds)

        video_dir = os.path.join(task['videos_dir'], alarm_dt.strftime('%Y-%m-%d'))
        ensure_dir(video_dir)
        raw_video_path = os.path.abspath(
            os.path.join(video_dir, task['base_filename'].replace('.mp4', '_orig.mp4'))
        )

        logger.debug(
            f"[Worker-{alarm_id}][{cam_cfg.get('manufacturer')}] 开始 RTSP 回放下载："
            f"设备={cam_cfg.get('ip')}，通道={cam_cfg.get('channel')}，"
            f"码流={cam_cfg.get('stream_type', 1)}"
        )
        # 下载一经开始绝不中途中止，失败由上层无限重试。
        return download_recording_by_rtsp(cam_cfg, start_dt, end_dt, raw_video_path)

# =====================================================================
# 视频转码（独立函数）
# =====================================================================

def transcode_video_static(input_path, output_path):
    """使用 ffmpeg 转码视频为 web 兼容格式。"""
    logger.debug(f"开始转码视频: {input_path} -> {output_path}")

    ffmpeg_path = _ffmpeg_executable()
    if not ffmpeg_path:
        logger.warning("未找到 FFmpeg，将复制文件代替转码。")
        shutil.copy(input_path, output_path)
        return True

    command = [
        ffmpeg_path, '-y', '-i', input_path,
        '-c:v', 'copy', '-c:a', 'aac', '-movflags', '+faststart',
        output_path
    ]

    try:
        subprocess.run(
            command, check=True, capture_output=True, text=True,
            encoding='utf-8', errors='replace', timeout=300,
            **no_window_kwargs()
        )
        logger.debug(f"视频转码成功: {output_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg 转码失败: {e.stderr}")
    except Exception as e:
        logger.error(f"执行 ffmpeg 时发生未知错误: {e}")

    return False
