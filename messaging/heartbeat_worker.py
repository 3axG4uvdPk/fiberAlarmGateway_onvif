# -*- coding: utf-8 -*-
"""
HTTP心跳上报线程。
定期向平台发送设备状态（含激光雷达状态、电池电量等）。
"""
import json
import threading
import time
import logging
import requests
from datetime import datetime

logger = logging.getLogger("alarm_app.heartbeat")


class HeartbeatWorker(threading.Thread):
    """设备状态心跳上报线程。"""

    def __init__(self, config, system_status):
        super().__init__(daemon=True, name="HeartbeatWorker")
        self.config = config
        self.system_status = system_status
        self._is_running = True

        heartbeat_config = self.config.get('heartbeat', {})
        self.url = heartbeat_config.get('url')
        self.interval = heartbeat_config.get('interval_seconds', 60)
        self.timeout = heartbeat_config.get('status_timeout_seconds', 60)
        self.boxid = self.config.get('boxid', 'UNKNOWN_BOX')

        self.send_now_event = threading.Event()

        if not self.url:
            logger.error("配置文件中未找到 heartbeat.url，心跳上报功能将禁用。")
            self._is_running = False

    def stop(self):
        self._is_running = False
        self.send_now_event.set()

    def send_heartbeat_now(self):
        """触发立即发送一次心跳。"""
        self.send_now_event.set()

    def _get_system_status_payload(self):
        """组装心跳信息负载。"""
        current_status = self.system_status.check_and_get_status(self.timeout)
        return {
            "boxid": self.boxid,
            "status": 1,
            "rssi": "1",
            "sigtype": 1,
            "cpuup": 0.8,
            "cput": 36.0,
            "tvc": 12.0,
            "puv": 0.8,
            "tsc": 512.0,
            "sup": 0.6,
            "ddu": 1.0,
            "mdu": 1.0,
            "rbp": current_status.get("battery_percentage", 100.0),
            "bs": current_status.get("battery_status", 2),
            "cot": 1.0,
            "das": 1,
            "dmc": "1",
            "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "lidar_status": current_status.get("lidar_status", -1),
            "video_status": -1
        }

    def run(self):
        if not self._is_running:
            return
        logger.info(f"心跳上报线程已启动，间隔 {self.interval} 秒。")

        while self._is_running:
            triggered = self.send_now_event.wait(timeout=self.interval)

            if not self._is_running:
                break

            # if triggered:
            #     logger.debug("收到状态更新事件，立即发送心跳...")
            # else:
            #     logger.debug("发送周期性心跳...")

            self.send_now_event.clear()

            try:
                payload = self._get_system_status_payload()
                # logger.debug(f"[HTTP发送] URL: {self.url}")
                # logger.debug(f"[HTTP发送] 请求体: {json.dumps(payload, ensure_ascii=False)}")
                response = requests.post(self.url, json=payload, timeout=5)

                if response.status_code == 200:
                    logger.debug("心跳上报成功，服务器已响应!")
                else:
                    logger.warning(f"心跳上报失败，HTTP状态码: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logger.error(f"心跳上报时发生网络错误: {e}")
            except Exception as e:
                logger.error(f"心跳上报线程发生未知错误: {e}", exc_info=True)

        logger.info("心跳上报线程已停止。")
