# -*- coding: utf-8 -*-
"""
摄像机管理器模块。

统一通过 ONVIF 标准协议管理摄像机（抓图、云台预置位），
不再依赖海康 / 大华 / 宇视任一厂家的私有 SDK：

- 抓图 / 预置位：``onvif_camera``（onvif-zeep-async + httpx）；
- 录像下载：不在本模块内实现，由 ``video.video_downloader`` 使用
  各厂家私有 RTSP 回放协议（ffmpeg）直接拉取。

由于 ONVIF 是无状态的短连接协议，本模块不再维护长连接登录会话：
``get_client`` 仅做配置校验与轻量登记，真正的 ONVIF 会话由
``onvif_camera`` 在后台事件循环中按设备缓存与复用。
"""
import os
import logging
import threading
from contextlib import contextmanager

from . import onvif_camera

logger = logging.getLogger("alarm_app.camera")

# 支持的相机厂家（决定私有 RTSP 回放 URL 的拼接方式）
SUPPORTED_MANUFACTURERS = ("hikvision", "dahua", "uniview")


class DeviceOperationGate:
    """单设备操作门闩（按设备 IP 一把）。

    两类任务互不影响：
    - exclusive(): 连接清理（close_and_remove_client）等会话敏感操作，
      仅与其它独占操作互斥（清理操作之间串行），与抓图/PTZ 完全并行、互不阻塞。
    - shared(): 录像下载操作。同一设备的并发下载数受 max_shared 限制。
      设备侧可同时承载的回放码流数有限，并发数超出后 RTSP SETUP 会被设备拒绝，
      因此同一设备的录像下载默认串行执行（max_shared=1），多路报警依次排队下载，
      避免互相挤占设备码流资源导致下载失败。

    抓图/PTZ/重登不经过门闩：多条报警同时到达时同时转预置位、并行抓图，
    相互之间不做任何串行排队（ONVIF 抓图/PTZ 与 RTSP 下载使用完全独立的协议栈，
    可以真正并行）。
    """

    def __init__(self, max_shared: int = 1):
        self._cond = threading.Condition()
        self._exclusive_active = False
        self._max_shared = max(1, int(max_shared))
        self._shared_active = 0

    @contextmanager
    def exclusive(self):
        with self._cond:
            while self._exclusive_active:
                self._cond.wait()
            self._exclusive_active = True
        try:
            yield self
        finally:
            with self._cond:
                self._exclusive_active = False
                self._cond.notify_all()

    @contextmanager
    def shared(self):
        with self._cond:
            while self._shared_active >= self._max_shared:
                self._cond.wait()
            self._shared_active += 1
        try:
            yield self
        finally:
            with self._cond:
                self._shared_active -= 1
                self._cond.notify_all()


class CameraManager:
    REQUIRED_CAMERA_FIELDS = ("ip", "username", "pwd", "manufacturer")

    def __init__(self, config):
        self.clients = {}
        self.lock = threading.Lock()
        self.config = config
        # 设备级操作门闩（key 为设备 IP）：连接清理走 exclusive（自身串行），
        # 录像下载走 shared（同一设备串行，防止并发回放挤满设备码流资源）；
        # 两者与抓图/PTZ 完全并行互不阻塞
        self._device_operation_gates = {}
        self._device_operation_gates_guard = threading.Lock()
        self.ptz_capture_wait_seconds = self._parse_ptz_capture_wait_seconds(
            config.get("ptz_capture_wait_seconds", 10)
        )

    @staticmethod
    def _parse_ptz_capture_wait_seconds(raw_value):
        try:
            return max(float(raw_value or 0), 0.0)
        except (TypeError, ValueError):
            logger.warning(f"无效的 ptz_capture_wait_seconds 配置: {raw_value}，已回退为 10 秒。")
            return 10.0

    @classmethod
    def is_camera_configured(cls, cam_cfg):
        """判断相机配置是否完整可用。"""
        if not isinstance(cam_cfg, dict):
            return False
        return all(cam_cfg.get(field) not in (None, "") for field in cls.REQUIRED_CAMERA_FIELDS)

    def get_operation_gate(self, cam_cfg=None, device_ip=None):
        """返回设备级操作门闩（key 为设备 IP）。

        连接清理（close_and_remove_client）走 exclusive() 独占执行；录像下载走
        shared()（同一设备串行下载，避免并发回放挤满设备码流资源）。
        抓图/PTZ 不经过门闩：多条报警同时到达时同时转预置位、并行抓图，与下载、
        清理互不阻塞（抓图/PTZ 走 ONVIF，下载走私有 RTSP，两套协议栈完全独立）。
        """
        lock_key = device_ip or (cam_cfg or {}).get('ip')
        if not lock_key:
            lock_key = "__global__"

        with self._device_operation_gates_guard:
            gate = self._device_operation_gates.get(lock_key)
            if gate is None:
                gate = DeviceOperationGate()
                self._device_operation_gates[lock_key] = gate
            return gate

    def _find_point_cfgs(self, point):
        """根据报警点位匹配对应的摄像机配置列表。"""
        out = []
        points_cfg = self.config.get('points', {})
        cameras_cfg = self.config.get('cameras', {})
        for rng, info in points_cfg.items():
            try:
                lo, hi = map(int, rng.split('-'))
                if lo <= point <= hi:
                    cam_name = info.get('camera')
                    if cam_name in cameras_cfg:
                        cam_cfg = cameras_cfg[cam_name]
                        out.append((cam_name, cam_cfg, info['preset']))
                    else:
                        logger.warning(f"点位 {point} 配置的相机 '{cam_name}' 在相机列表中未定义。")
            except (ValueError, KeyError) as e:
                logger.error(f"解析点位配置 '{rng}' 失败: {e}")
        return out

    def get_client(self, cam_cfg):
        """获取或创建摄像机连接信息。

        ONVIF 为无状态短连接协议，这里不做真实网络交互，仅做配置校验与登记；
        真正的 ONVIF 会话由 ``onvif_camera`` 在首次抓图/PTZ 时按设备建立并复用。
        """
        key = (cam_cfg['ip'], cam_cfg['username'])
        with self.lock:
            if key in self.clients:
                return self.clients[key]

            mfr = str(cam_cfg.get('manufacturer', '')).lower()
            if mfr not in SUPPORTED_MANUFACTURERS:
                raise RuntimeError(f"不支持的相机厂家: {cam_cfg['manufacturer']}")

            connection_info = {
                'client': None,
                'mfr': mfr,
                'ip': cam_cfg['ip'],
                'config': cam_cfg,
            }
            self.clients[key] = connection_info
            logger.debug(f"相机 {cam_cfg['ip']} 已登记（ONVIF 会话按需建立）。")
            return connection_info

    def probe(self, cam_cfg, cam_label=None):
        """探测设备 ONVIF 连通性与能力（抓图/云台是否可用）。"""
        tag = cam_label or f"相机 {cam_cfg.get('ip')}"
        logger.info(f"正在探测 {tag} 的 ONVIF 能力 ...")
        return onvif_camera.probe(cam_cfg)

    def move_to_preset(self, connection_info, cam_cfg, channel, preset, cam_label=None):
        """控制云台移动到指定预置点。

        ONVIF 预置位控制失败（设备不支持 PTZ、预置点不存在等）不阻断抓图流程：
        记录警告后继续在当前视角抓图，避免因云台异常导致整条报警抓图重试失败。

        注意：本方法只下发 ONVIF PTZ GotoPreset（命令下发即返回，云台仍在转动），
        **不做**转到位后的稳定等待。调用方（processing_worker）必须在本方法返回后
        等待 ``ptz_capture_wait_seconds``（config.json 配置）再抓图，否则会拍到
        云台转动过程中的模糊画面。
        """
        tag = cam_label or f"相机 {cam_cfg['ip']}"
        if preset == -1:
            logger.info(f"{tag} 预置点为 -1，跳过 PTZ 操作。")
            return

        logger.debug(f"{tag}: 正在通过 ONVIF 移动到预置点 {preset}。")
        try:
            onvif_camera.goto_preset(cam_cfg, preset)
        except Exception as exc:
            logger.warning(
                f"{tag}: ONVIF 移动到预置点 {preset} 失败（{type(exc).__name__}: {exc}），"
                f"将在当前视角继续抓图。"
            )
            return
        logger.debug(f"{tag}: 移动到预置点 {preset} 完成。")

    def relogin_before_capture(self, connection_info, cam_cfg, cam_label=None):
        """统一预抓图登录流程。

        ONVIF 每次操作都是独立的短连接请求（底层会话按设备复用），
        不存在长期会话失效问题，因此无需预重登。
        """
        tag = cam_label or f"相机 {cam_cfg['ip']}"
        logger.debug(f"{tag} 使用 ONVIF 短连接，无需预重登。")

    def capture(self, connection_info, cam_cfg, channel, save_path, cam_label=None):
        """抓图到本地文件。"""
        tag = cam_label or f"相机 {cam_cfg['ip']}"
        logger.debug(f"{tag}: 正在通过 ONVIF 抓图。")
        image_data = self.capture_to_memory(connection_info, cam_cfg, channel, cam_label=tag)
        if not image_data:
            raise RuntimeError(f"{tag} ONVIF 抓图返回空数据。")
        save_path = os.path.abspath(save_path)
        os.makedirs(os.path.dirname(save_path) or '.', exist_ok=True)
        with open(save_path, 'wb') as image_file:
            image_file.write(image_data)
        logger.info(f"[OK] {tag} 抓图成功: {save_path}（{len(image_data)} 字节）")

    def capture_to_memory(self, connection_info, cam_cfg, channel, cam_label=None):
        """抓图到内存，返回图片二进制数据。"""
        tag = cam_label or f"相机 {cam_cfg['ip']}"
        logger.debug(f"{tag}: 正在通过 ONVIF 抓图到内存。")
        return onvif_camera.capture_to_memory(cam_cfg)

    def close_and_remove_client(self, cam_cfg, cam_label=None):
        """主动关闭并移除特定的相机连接，强制下次重新建立 ONVIF 会话。"""
        key = (cam_cfg['ip'], cam_cfg['username'])
        # 关闭连接在设备级门闩的独占模式内执行，与其它连接清理操作串行化，
        # 避免并发清理同一设备会话。
        device_gate = self.get_operation_gate(cam_cfg)
        with device_gate.exclusive():
            with self.lock:
                connection_info = self.clients.pop(key, None)

            tag = cam_label or f"相机 {cam_cfg['ip']}"
            logger.debug(f"检测到异常，正在清理{tag}的 ONVIF 会话: {cam_cfg['ip']}")
            try:
                onvif_camera.forget_device(cam_cfg)
            except Exception as e:
                logger.warning(f"清理相机 {cam_cfg['ip']} 的 ONVIF 会话时出错 (可能已断开): {e}")

            if connection_info is None:
                logger.debug(f"{tag} 无缓存连接条目，仅清理 ONVIF 会话。")

    def cleanup(self):
        """清理所有相机连接与 ONVIF 资源。"""
        logger.info("正在清理所有相机连接与 ONVIF 资源。")
        with self.lock:
            self.clients.clear()
        try:
            onvif_camera.shutdown()
        except Exception as exc:
            logger.error(f"关闭 ONVIF 资源失败: {exc}")
        logger.info("所有摄像机资源已清理。")
