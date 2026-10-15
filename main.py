#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
振动光纤报警网关系统 - 主入口。
负责初始化所有模块、启动工作线程、管理生命周期。
"""
import os
import sys
import time
import queue
import threading
import subprocess
import logging
import signal

# 确保项目根目录在 sys.path 中
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.logging_config import setup_logging
from core.config import load_config, CONFIG_FILE
from core.system_status import SystemStatus

# 初始化日志
logger = setup_logging()


# 关闭信号统一处理：Ctrl+C(SIGINT) 与 systemd 的 SIGTERM 都触发优雅关闭。
# 重复信号只置位事件，不会打断正在执行的清理流程（避免清理中途被再次中断）。
SHUTDOWN_EVENT = threading.Event()


def _request_shutdown(signum, frame):
    """收到 SIGINT/SIGTERM 后请求优雅关闭，仅置位事件，不抛异常。"""
    SHUTDOWN_EVENT.set()


class ParentHeartbeatThread(threading.Thread):
    """内部心跳线程：定期更新心跳文件，用于看门狗进程监控。"""

    def __init__(self, heartbeat_file, interval=2):
        super().__init__(daemon=True, name="ParentHeartbeat")
        self.heartbeat_file = heartbeat_file
        self.interval = interval
        self._is_running = True

    def stop(self):
        self._is_running = False

    def run(self):
        logger.info(f"内部心跳线程已启动，将每 {self.interval} 秒更新文件: {self.heartbeat_file}")
        while self._is_running:
            try:
                with open(self.heartbeat_file, 'a'):
                    os.utime(self.heartbeat_file, None)
                # logger.debug(f"更新了心跳文件时间戳。")
            except Exception as e:
                logger.error(f"更新心跳文件时出错: {e}")
            time.sleep(self.interval)
        logger.info("内部心跳线程已停止。")


def _probe_one_camera(cam_mgr, cam_cfg, label):
    """登记并探测单台摄像机的 ONVIF 连通性（抓图/云台能力）。"""
    try:
        logger.info(f"预连接{label} ({cam_cfg.get('ip')}) ...")
        cam_mgr.get_client(cam_cfg)
        cam_mgr.probe(cam_cfg, cam_label=label)
    except Exception as e:
        logger.error(f"预连接{label}失败: {e}")


def _preconnect_cameras(cam_mgr, cfg):
    """预连接所有摄像机及其（可选的）定焦相机。"""
    cameras = cfg.get('cameras') or {}
    for cam_name, cam_cfg in cameras.items():
        if not isinstance(cam_cfg, dict):
            logger.warning(f"摄像机 '{cam_name}' 的配置不是字典，已跳过。")
            continue
        label = f"摄像机 {cam_name}"
        _probe_one_camera(cam_mgr, cam_cfg, label)

        # 定焦相机配置在云台相机内部（cameras.<name>.fixed_camera），可启用/禁用
        fixed_cfg = cam_cfg.get('fixed_camera')
        if not isinstance(fixed_cfg, dict):
            continue
        if not fixed_cfg.get('enabled', True):
            logger.info(f"{label} 的定焦相机已配置为禁用，跳过预连接。")
            continue
        if not cam_mgr.is_camera_configured(fixed_cfg):
            logger.warning(f"{label} 的 fixed_camera 配置字段不完整，已跳过定焦相机预连接。")
            continue
        _probe_one_camera(cam_mgr, fixed_cfg, f"{label} 的定焦相机")

    logger.info("所有摄像机连接尝试完成。")


def main():
    # ========== 加载配置 ==========
    cfg = load_config()
    if cfg is None:
        logger.critical("配置加载失败，应用退出。")
        sys.exit(1)

    logger.info("应用程序作为后台服务启动...")

    # ========== 看门狗环境检测（已临时注释）==========
    # watchdog_process = None
    # HEARTBEAT_FILE = f"/tmp/alarm_gateway_heartbeat_{os.getpid()}.tmp"
    # PARENT_HEARTBEAT_TIMEOUT = 5
    # IS_UNDER_SYSTEMD_WATCHDOG = 'WATCHDOG_USEC' in os.environ
    #
    # if IS_UNDER_SYSTEMD_WATCHDOG:
    #     logger.info("检测到 systemd 看门狗环境，将启动看门狗子进程。")
    #     try:
    #         with open(HEARTBEAT_FILE, 'w') as f:
    #             pass
    #         logger.info(f"已成功创建初始心跳文件: {HEARTBEAT_FILE}")
    #     except Exception as e:
    #         logger.error(f"创建初始心跳文件失败: {e}", exc_info=True)
    #
    #     if sys.platform == "linux":
    #         try:
    #             watchdog_script_path = os.path.join(PROJECT_ROOT, "watchdog_process.py")
    #             command = [
    #                 sys.executable,
    #                 watchdog_script_path,
    #                 "--heartbeat-file", HEARTBEAT_FILE,
    #                 "--pid", str(os.getpid()),
    #                 "--timeout", str(PARENT_HEARTBEAT_TIMEOUT)
    #             ]
    #             logger.info(f"正在启动看门狗子进程: {' '.join(command)}")
    #             watchdog_process = subprocess.Popen(command)
    #         except Exception as e:
    #             logger.error(f"启动看门狗子进程失败: {e}", exc_info=True)
    #     else:
    #         logger.warning("Systemd 看门狗环境仅在 Linux 上受支持。")
    # else:
    #     logger.info("未检测到 systemd 看门狗环境，以独立模式运行（无看门狗）。")

    # ========== 启动内部心跳线程（已临时注释）==========
    # parent_heartbeat = ParentHeartbeatThread(HEARTBEAT_FILE)
    # parent_heartbeat.start()

    # ========== 初始化核心组件 ==========
    processing_queue = queue.Queue()
    video_queue = queue.Queue()
    upload_queue = queue.Queue()

    from camera.camera_manager import CameraManager
    from storage.minio_manager import MinioManager

    cam_mgr = CameraManager(cfg)
    minio_mgr = MinioManager(cfg)
    system_status = SystemStatus()

    # ========== 初始化工作线程 ==========
    from messaging.heartbeat_worker import HeartbeatWorker
    from messaging.alarm_receiver import AlarmReceiver
    from messaging.status_receiver import StatusReceiver, SolarReceiver
    from messaging.upload_worker import UploadWorker
    from processing.processing_worker import ProcessingWorker
    from video.video_downloader import VideoDownloader

    heartbeat_worker = HeartbeatWorker(config=cfg, system_status=system_status)
    processing_worker = ProcessingWorker(
        cam_mgr=cam_mgr, minio_mgr=minio_mgr,
        processing_queue=processing_queue,
        video_queue=video_queue,
        upload_queue=upload_queue
    )
    alarm_receiver = AlarmReceiver(
        config=cfg,
        processing_queue=processing_queue,
        processing_worker=processing_worker,
    )
    status_receiver = StatusReceiver(
        config=cfg, system_status=system_status, heartbeat_worker=heartbeat_worker
    )
    solar_receiver = SolarReceiver(
        config=cfg, system_status=system_status, heartbeat_worker=heartbeat_worker
    )
    upload_worker = UploadWorker(minio_mgr=minio_mgr, upload_queue=upload_queue, config=cfg)
    video_downloader = VideoDownloader(
        config=cfg, minio_mgr=minio_mgr, video_queue=video_queue, cam_mgr=cam_mgr
    )

    # ========== 启动所有线程 ==========
    threads = [
        alarm_receiver, status_receiver, solar_receiver,
        processing_worker, video_downloader, heartbeat_worker, upload_worker
    ]
    for t in threads:
        t.start()

    # ========== 预连接摄像机 ==========
    preconnect_thread = threading.Thread(
        target=_preconnect_cameras, args=(cam_mgr, cfg), daemon=True
    )
    preconnect_thread.start()

    # ========== 主循环 ==========
    # 注册关闭信号处理：SIGINT(Ctrl+C) 与 SIGTERM(systemctl stop/restart) 统一走优雅关闭
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    try:
        logger.info("所有服务已启动，按 Ctrl+C 退出。")
        while True:
            if SHUTDOWN_EVENT.wait(timeout=10):
                logger.info("收到关闭信号，开始关闭程序...")
                break
            if not processing_worker.is_alive():
                logger.critical("核心处理线程已停止！正在关闭应用...")
                break
            # if watchdog_process and watchdog_process.poll() is not None:
            #     logger.critical("看门狗子进程意外退出！请检查日志。")
            #     break
    except KeyboardInterrupt:
        # 兜底：极端情况下信号处理器未生效时仍能走优雅关闭
        logger.info("收到关闭信号 (Ctrl+C)，开始关闭程序...")

    # ========== 优雅关闭 ==========
    logger.info("正在关闭服务，请稍候。")

    try:
        for t in threads:
            try:
                t.stop()
            except Exception as e:
                logger.warning(f"停止线程时出错: {e}")

        for t in threads:
            if t.is_alive():
                t.join(timeout=5)

        # if parent_heartbeat.is_alive():
        #     parent_heartbeat.stop()

        # 终止看门狗子进程（已临时注释）
        # if watchdog_process and watchdog_process.poll() is None:
        #     logger.info("正在终止看门狗子进程...")
        #     watchdog_process.terminate()
        #     try:
        #         watchdog_process.wait(timeout=2)
        #     except subprocess.TimeoutExpired:
        #         logger.warning("看门狗子进程在2秒内未终止，将强制杀死。")
        #         watchdog_process.kill()

        # 清理心跳文件（已临时注释）
        # try:
        #     if os.path.exists(HEARTBEAT_FILE):
        #         os.remove(HEARTBEAT_FILE)
        #         logger.info("已清理心跳文件。")
        # except Exception as e:
        #     logger.error(f"清理心跳文件时出错: {e}")

        # 清理相机与 SDK 资源（被信号打断时也不会以半清理状态退出）
        cam_mgr.cleanup()
    except Exception as exc:
        logger.error(f"关闭过程中发生异常: {exc}", exc_info=True)
    finally:
        logger.info("应用程序已安全关闭。")


if __name__ == "__main__":
    main()
