# -*- coding: utf-8 -*-
"""
配置加载模块。
负责从 config.json 读取并验证系统配置，提供默认值。
"""
import os
import sys
import json
import logging

logger = logging.getLogger("alarm_app.config")

if getattr(sys, 'frozen', False):
    # 优先使用可执行文件同级目录下的 config/config.json（便于现场修改配置）
    CONFIG_FILE = os.path.join(os.path.dirname(sys.executable), 'config', 'config.json')
    # 若同级目录配置缺失，则回退到打包内嵌的配置文件（main.spec 中 datas 打包的 config/config.json），
    # 避免因外部配置文件缺失导致程序无法启动、无法接收告警消息。
    if not os.path.isfile(CONFIG_FILE) and hasattr(sys, '_MEIPASS'):
        embedded_config = os.path.join(sys._MEIPASS, 'config', 'config.json')
        if os.path.isfile(embedded_config):
            CONFIG_FILE = embedded_config
else:
    CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'config.json')

# 默认配置值
DEFAULT_RABBITMQ_OUT_CONFIG = {
    "url": "amqp://guest:guest@localhost:5672/",
    "exchange_out": "das.processed",
    "exchange_type": "fanout",
    "queue_name": "",
    "routing_key": "",
    "durable": True,
    "forward_enabled": True,
}

DEFAULT_HEARTBEAT_CONFIG = {
    "url": "http://localhost:9090/boxStatucService/boxStatus/add",
    "interval_seconds": 30,
    "status_timeout_seconds": 60,
}

# 云台相机默认配置（逐相机合并；cameras.<name> 下的同名字段优先）
DEFAULT_CAMERA_CONFIG = {
    "onvif_port": 80,   # ONVIF/HTTP 服务端口（抓图与预置位控制）
    "channel": 1,       # RTSP 回放通道号（大华/海康使用，1 起）
    "stream_type": 1,   # 码流编号：1=主码流，2=子码流，3=第三码流
    "rtsp_port": 554,   # 私有 RTSP 回放端口
}

# 定焦相机默认配置（挂在每个云台相机配置的 fixed_camera 下）
# enabled=False 时该云台相机不启用定焦抓图与拼接。
DEFAULT_FIXED_CAMERA_CONFIG = {
    "enabled": True,
    "onvif_port": 80,
    "channel": 1,
    "stream_type": 1,
}


def get_runtime_base_dir():
    """返回运行根目录：
    - 打包为可执行文件（PyInstaller frozen）时：可执行文件所在目录
    - 源码运行（python main.py）时：项目根目录
    """
    if getattr(sys, 'frozen', False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def normalize_runtime_path(path_value):
    """把配置中的存储路径规范为当前平台可直接使用的路径字符串。

    config.json 中允许以下三种写法，Windows 下全部可用：
    - Windows 盘符路径：``D:/data/media/snapshots`` 或 ``D:\\data\\media\\snapshots``；
    - UNC 网络共享：``\\\\server\\share\\media``；
    - 无盘符的根路径（如从 Linux 配置复制而来）：``/data/media/snapshots``，
      自动锚定到运行根目录所在盘符，即 ``D:\\data\\media\\snapshots``。

    非 Windows 平台原样返回（保持原有行为）。
    """
    if not path_value:
        return path_value
    text = str(path_value).strip()
    # 容忍配置中写成带引号的路径（如 "D:/data/media"）
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        text = text[1:-1]
    if not text:
        return text

    if os.name != 'nt':
        return text

    # Windows：统一分隔符，避免混用正/反斜杠导致的比较、日志与外部工具解析问题
    text = text.replace('/', '\\')
    # 「无盘符的根路径」：\data\media -> <运行根目录所在盘符>:\data\media
    if text.startswith('\\') and not text.startswith('\\\\'):
        drive, _ = os.path.splitdrive(get_runtime_base_dir())
        if drive:
            text = drive + text
    return os.path.normpath(text)


def resolve_runtime_path(path_value):
    """将配置中的路径解析为当前平台的绝对路径：
    - 绝对路径（Windows 盘符 / UNC / POSIX 根路径）规范后返回
    - 相对路径基于运行根目录（打包后可执行文件目录 / 源码运行时项目根目录）解析
    """
    if not path_value:
        return path_value
    normalized = normalize_runtime_path(path_value)
    if os.path.isabs(normalized):
        return normalized
    return os.path.abspath(os.path.join(get_runtime_base_dir(), normalized))


def _load_json_file(path):
    """读取 JSON 文件并返回字典。"""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        msg = f"配置文件 '{os.path.abspath(path)}' 未找到！"
        logger.critical(msg)
        return None
    except json.JSONDecodeError as e:
        msg = f"配置文件格式错误: {e}"
        logger.critical(msg)
        return None


def load_config(config_path=None):
    """
    加载配置文件，合并默认值，返回完整配置字典。
    """
    path = config_path or CONFIG_FILE
    cfg = _load_json_file(path)
    if cfg is None:
        return None

    # 合并默认值
    cfg.setdefault('boxid', 'UNKNOWN_BOX')
    cfg.setdefault('alarm_send_delay_seconds', 0)
    cfg.setdefault('ptz_capture_wait_seconds', 10)
    cfg.setdefault('forward_unconfigured_alarms', False)
    # 上传 MinIO 前 JPEG 图片压缩质量（1-100，越大越清晰体积越大；-1 或缺失表示不压缩按原图上传）
    cfg.setdefault('jpg_quality', -1)
    cfg.setdefault('snapshots_dir', 'snapshots')
    cfg.setdefault('videos_dir', 'videos')
    cfg.setdefault('vff_wait_timeout_seconds', 30)

    # heartbeat 默认值
    if 'heartbeat' not in cfg:
        cfg['heartbeat'] = DEFAULT_HEARTBEAT_CONFIG
    else:
        for k, v in DEFAULT_HEARTBEAT_CONFIG.items():
            cfg['heartbeat'].setdefault(k, v)

    # rabbitmq_out 默认值
    if 'rabbitmq_out' not in cfg:
        cfg['rabbitmq_out'] = DEFAULT_RABBITMQ_OUT_CONFIG
    else:
        for k, v in DEFAULT_RABBITMQ_OUT_CONFIG.items():
            cfg['rabbitmq_out'].setdefault(k, v)

    # 相机默认值：逐相机合并；每台云台相机可自带一个定焦相机（fixed_camera）
    cameras = cfg.get('cameras')
    if isinstance(cameras, dict):
        for cam_name, cam_cfg in cameras.items():
            if not isinstance(cam_cfg, dict):
                logger.warning(f"相机 '{cam_name}' 的配置不是字典，已跳过默认值合并。")
                continue
            for k, v in DEFAULT_CAMERA_CONFIG.items():
                cam_cfg.setdefault(k, v)
            fixed_cfg = cam_cfg.get('fixed_camera')
            if isinstance(fixed_cfg, dict):
                for k, v in DEFAULT_FIXED_CAMERA_CONFIG.items():
                    fixed_cfg.setdefault(k, v)

    return cfg
