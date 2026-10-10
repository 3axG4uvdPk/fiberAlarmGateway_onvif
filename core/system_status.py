# -*- coding: utf-8 -*-
"""
系统状态管理模块。
管理应变光纤状态和电池电量，提供线程安全的状态读写与超时检测。
"""
import time
import threading
import logging

logger = logging.getLogger("alarm_app")


class SystemStatus:
    """线程安全的系统状态管理。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._fiber_status = 1  # 1: 正常, -1: 异常
        self._battery_percentage = 100.0
        self._battery_status = 2
        self._last_status_update_time = time.time()

    def update_fiber_status(self, state_str):
        """根据状态字符串更新应变光纤状态。"""
        with self._lock:
            if state_str == "ok":
                self._fiber_status = 1
            else:
                self._fiber_status = -1
            self._last_status_update_time = time.time()
            logger.debug(f"应变光纤状态已更新为: {self._fiber_status} (原始状态: '{state_str}')")

    def update_lidar_status(self, state_str):
        """兼容激光雷达状态命名，内部沿用同一份状态值。"""
        self.update_fiber_status(state_str)

    def update_battery_status(self, percentage, status=None):
        """更新电池电量百分比与充放电状态。"""
        with self._lock:
            try:
                self._battery_percentage = float(percentage)
                logger.debug(f"电池电量已更新为: {self._battery_percentage}%")
            except (ValueError, TypeError):
                logger.warning(f"接收到的电池电量格式错误: {percentage}")
            if status is not None:
                try:
                    self._battery_status = int(status)
                    logger.debug(f"电池充放电状态已更新为: {self._battery_status}")
                except (ValueError, TypeError):
                    logger.warning(f"接收到的电池充放电状态格式错误: {status}")

    def check_and_get_status(self, timeout):
        """
        检查状态是否超时，返回当前所有状态的副本。
        如果超过 timeout 秒未更新，则将应变光纤状态设为 -1。
        """
        with self._lock:
            if time.time() - self._last_status_update_time > timeout:
                if self._fiber_status != -1:
                    logger.warning(f"超过 {timeout} 秒未收到光纤状态信息，将状态设置为 -1。")
                    self._fiber_status = -1
            return {
                "lidar_status": self._fiber_status,
                "fiber_status": self._fiber_status,
                "battery_percentage": self._battery_percentage,
                "battery_status": self._battery_status
            }
