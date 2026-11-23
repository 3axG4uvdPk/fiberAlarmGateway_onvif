#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
ONVIF 摄像头随机抓图测试 Demo（单文件，Windows / Linux 通用）

流程：
1. 按 IP / 端口直连摄像机 ONVIF 服务（无需设备发现）
2. GetProfiles 选媒体 Profile → GetSnapshotUri 取快照地址
3. httpx 拉取图片（digest → basic → 匿名 依次尝试）
4. 图片按「抓图时间」命名，保存到运行脚本时的当前目录

依赖：
    pip install onvif-zeep-async httpx

打包：
    pyinstaller -F --add-data "$(python3 -c 'import os,onvif;print(os.path.join(os.path.dirname(onvif.__file__),"wsdl"))'):onvif/wsdl" onvif_snapshot_demo.py

运行（Windows）：
    python onvif_snapshot_demo.py                # 存到当前目录
    python onvif_snapshot_demo.py -d "D:\snapshots" -n 10
    python onvif_snapshot_demo.py -d "\\192.168.1.10\share\snap"  # UNC 网络路径

运行（Linux / macOS）：
    python3 onvif_snapshot_demo.py -d /tmp/snapshots

Ctrl+C 退出
"""

import argparse
import asyncio
import logging
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path

import httpx
from onvif import ONVIFCamera

# ==================== 摄像头配置（写死在代码里） ====================
CAMERA = {
    "ip": "192.168.1.108",
    "username": "admin",
    "pwd": "HHkj1994",
    "onvif_port": 80,
    "channel": 1,        # 仅作记录：ONVIF 抓图由 Profile 决定，不使用通道号
    "stream_type": 1,    # 仅作记录：1=主码流 0=子码流，可用 onvif_profile 指定具体码流
    # "onvif_profile": "Profile_1",  # 可选：指定 Profile 的 token 或 Name，缺省用第一个
}

# ==================== 抓图配置 ====================
# 图片保存目录：默认脚本运行的当前路径；也可用命令行 -d/--save-dir 覆盖
SAVE_DIR = Path.cwd()

INTERVAL_RANGE = (5.0, 15000.0)   # 每张图之间的随机等待区间（秒）
MAX_SNAPSHOTS = 0                 # 抓图张数上限，0 表示一直抓直到 Ctrl+C（可用 -n 覆盖）
CONNECT_TIMEOUT = 15.0            # 建立 ONVIF 会话超时（秒）
SNAPSHOT_TIMEOUT = 25.0           # 单次抓图超时（秒），部分设备生成快照较慢
MAX_CONSECUTIVE_FAILURES = 5      # 连续失败多少次后重建 ONVIF 会话

# ★ 新增：强制把快照 URL 的 https:// 改成 http://
# 某些摄像头 HTTPS 实现不规范（TLS 握手失败），改走 HTTP 可绕开该问题。
# 设为 False 则按设备返回的原始协议下载。
FORCE_HTTP_SNAPSHOT = True

# ONVIF 库导入时会往 root logger 挂 INFO 级 handler，这里压到 WARNING 避免刷屏
for _logger_name in ("onvif", "zeep", "httpx", "httpcore", "asyncio"):
    logging.getLogger(_logger_name).setLevel(logging.WARNING)

# 去除 URL 里的 user:pass@（认证改由 httpx 的 auth 参数携带）
_URL_CRED_RE = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)(?P<user>[^:/@\s]+):(?P<pwd>[^@/\s]*)@"
)


def log(message):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}", flush=True)


def setup_console():
    """Windows 控制台默认使用本地代码页（如 GBK），统一切到 UTF-8 避免中文乱码。"""
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        except Exception:
            pass
    for stream in (sys.stdout, sys.stderr):
        if stream is None:  # pythonw.exe 下无控制台
            continue
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


def prepare_event_loop():
    """Windows 默认的 Proactor 事件循环对 Ctrl+C 响应较差，改用 Selector 策略。"""
    if sys.platform != "win32":
        return
    policy_cls = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if policy_cls is None:
        return
    try:
        asyncio.set_event_loop_policy(policy_cls())
    except Exception:
        pass


def parse_args():
    parser = argparse.ArgumentParser(description="ONVIF 摄像头随机抓图测试 Demo")
    parser.add_argument(
        "-d", "--save-dir", default=None,
        help='图片保存目录，默认当前目录。Windows 示例：-d "D:\\snapshots"',
    )
    parser.add_argument(
        "-n", "--count", type=int, default=None,
        help="抓图张数，0 表示不限制，默认取代码中的 MAX_SNAPSHOTS",
    )
    return parser.parse_args()


def strip_url_credentials(url):
    """去掉 URL 中的 user:pass@（认证改由 httpx 的 auth 参数携带）。"""
    return _URL_CRED_RE.sub(r"\g<scheme>", str(url), count=1)


def to_http(url):
    """★ 新增：把 https:// 快照地址降级为 http://（FORCE_HTTP_SNAPSHOT 开启时）。"""
    if FORCE_HTTP_SNAPSHOT and url.lower().startswith("https://"):
        new_url = "http://" + url[len("https://"):]
        log(f" 已将快照地址从 HTTPS 降级为 HTTP：{new_url}")
        return new_url
    return url


def format_error(exc):
    """整理异常信息；zeep 的 SOAP Fault 常把设备返回的 HTML 塞在 detail 里。"""
    message = str(exc) or type(exc).__name__
    detail = getattr(exc, "detail", None)
    if isinstance(detail, bytes):
        detail = detail.decode("utf-8", "ignore")
    if isinstance(detail, str) and detail.strip():
        text = " ".join(re.sub(r"<[^>]+>", " ", detail).split())
        if text and text not in message:
            message = f"{message} | 设备返回：{text}"
    return message


def print_troubleshooting(message):
    """针对常见失败原因给出排查提示。"""
    lower = message.lower()
    if "onvif integrate function is disabled" in lower or "refuse" in lower:
        log("提示：摄像头 Web 后台未开启 ONVIF 集成功能，请登录 "
            f"http://{CAMERA['ip']} 在「网络/系统设置 → ONVIF」中开启后重试。")
    elif "not authorized" in lower or "401" in lower or "unauthorized" in lower:
        log("提示：ONVIF 认证失败，请确认用户名/密码，以及该账号是否有 ONVIF 权限。")
    elif "connection" in lower or "timeout" in lower or "timed out" in lower:
        log(f"提示：无法连接 {CAMERA['ip']}:{CAMERA['onvif_port']}，请检查网络、IP 与 ONVIF 端口。")
    elif "handshake" in lower or "ssl" in lower:
        log("提示：TLS 握手失败，摄像头 HTTPS 实现不规范；可设置 FORCE_HTTP_SNAPSHOT=True "
            "改用 HTTP 下载快照，或检查摄像头 Web 后台是否开启了 HTTP 服务。")


# ==================== ONVIF 会话 ====================
def resolve_wsdl_dir():
    """定位 ONVIF WSDL 目录，兼容源码运行与 PyInstaller 打包（onefile / onedir）。"""
    candidates = []
    env_dir = os.environ.get("ONVIF_WSDL_DIR")
    if env_dir:
        candidates.append(Path(env_dir))

    meipass = getattr(sys, "_MEIPASS", None)  # PyInstaller 运行时的资源解压目录
    if meipass:
        candidates.append(Path(meipass) / "onvif" / "wsdl")

    # 手动把 onvif/wsdl 目录放在可执行文件旁边的情况
    candidates.append(Path(sys.executable).resolve().parent / "onvif" / "wsdl")

    # 源码运行：直接使用 onvif 包自带的 wsdl
    try:
        import onvif as onvif_pkg
        candidates.append(Path(onvif_pkg.__file__).resolve().parent / "wsdl")
    except Exception:
        pass

    for candidate in candidates:
        try:
            if (candidate / "devicemgmt.wsdl").is_file():
                return candidate
        except OSError:
            continue

    sep = ";" if sys.platform == "win32" else ":"
    raise RuntimeError(
        "未找到 ONVIF WSDL 文件（devicemgmt.wsdl）。打包时必须带上 onvif/wsdl 目录，例如："
        f'pyinstaller -F --add-data "<site-packages>/onvif/wsdl{sep}onvif/wsdl" '
        "onvif_snapshot_demo.py，或使用仓库中的 onvif_snapshot_demo.spec 重新打包。"
    )


async def close_camera(camera):
    if camera is None:
        return
    try:
        await camera.close()
    except Exception:
        pass


async def connect_onvif(cam_cfg):
    """建立 ONVIF 会话，返回 (camera, profile_token)。"""
    host = cam_cfg["ip"]
    port = int(cam_cfg.get("onvif_port") or 80)
    user = cam_cfg.get("username") or ""
    pwd = cam_cfg.get("pwd") or ""

    wsdl_dir = resolve_wsdl_dir()
    log(f"连接 ONVIF 摄像机 {host}:{port} ...")
    log(f" 使用 WSDL 目录：{wsdl_dir}")

    camera = ONVIFCamera(host, port, user, pwd, wsdl_dir=str(wsdl_dir))
    try:
        await asyncio.wait_for(camera.update_xaddrs(), timeout=CONNECT_TIMEOUT)
        media = await camera.create_media_service()
        profiles = await media.GetProfiles()
    except BaseException:
        await close_camera(camera)
        raise

    if not profiles:
        await close_camera(camera)
        raise RuntimeError("摄像机未返回任何媒体 Profile（GetProfiles 为空）")

    for profile in profiles:
        log(f"可用 Profile: token={getattr(profile, 'token', '')!r} "
            f"name={getattr(profile, 'Name', '')!r}")

    token = _pick_profile_token(profiles, cam_cfg)
    log(f"连接成功，使用 Profile={token!r}")
    return camera, token


def _pick_profile_token(profiles, cam_cfg):
    """选择 Profile：优先用配置的 onvif_profile（token 或 Name），否则取第一个。"""
    wanted = cam_cfg.get("onvif_profile")
    if wanted:
        for profile in profiles:
            if str(wanted) in (str(getattr(profile, "token", "")),
                               str(getattr(profile, "Name", ""))):
                return str(profile.token)
        log(f"配置的 onvif_profile={wanted!r} 不在设备列表中，直接按 token 使用")
        return str(wanted)
    return str(profiles[0].token)


# ==================== 抓图 ====================
async def fetch_snapshot(uri, user, pwd):
    """下载快照图片字节，认证方式按 digest → basic → 匿名 回退。"""
    request_url = to_http(strip_url_credentials(uri))

    attempts = []
    if user:
        attempts.append(("digest", httpx.DigestAuth(user, pwd or "")))
        attempts.append(("basic", httpx.BasicAuth(user, pwd or "")))
    attempts.append(("none", None))

    last_error = "未知错误"
    async with httpx.AsyncClient(
        timeout=SNAPSHOT_TIMEOUT,
        follow_redirects=True,
        verify=False,
    ) as client:
        for mode, auth in attempts:
            try:
                response = await client.get(request_url, auth=auth)
            except httpx.HTTPError as exc:
                last_error = f"{mode}: {type(exc).__name__}: {exc}"
                log(f" 快照请求异常（认证={mode}）：{last_error}")
                continue
            if response.is_success and response.content:
                log(f" 快照下载成功（认证={mode}，{len(response.content)} 字节）")
                return response.content
            last_error = f"{mode}: HTTP {response.status_code}"
            log(f" 快照请求失败（认证={mode}）：HTTP {response.status_code}")

    raise RuntimeError(f"拉取快照失败：{last_error}（{request_url}）")


async def capture_once(camera, token, cam_cfg):
    """执行一次 ONVIF 抓图，返回图片字节。"""
    uri = await asyncio.wait_for(camera.get_snapshot_uri(token), timeout=SNAPSHOT_TIMEOUT)
    if not uri:
        raise RuntimeError("GetSnapshotUri 返回空快照地址")
    log(f" 快照地址：{uri}")
    return await fetch_snapshot(uri, cam_cfg.get("username") or "", cam_cfg.get("pwd") or "")


def image_suffix(data):
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    return ".jpg"


def save_image(data, shot_time, save_dir):
    """按抓图时间命名并保存图片，返回保存路径（Windows 路径由 pathlib 处理）。"""
    try:
        save_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(f"创建保存目录失败：{save_dir}（{exc}）") from exc

    stem = shot_time.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # 毫秒级
    suffix = image_suffix(data)
    path = save_dir / f"{stem}{suffix}"
    index = 1
    while path.exists():  # 极端情况下同毫秒重名，追加序号
        path = save_dir / f"{stem}_{index}{suffix}"
        index += 1

    try:
        path.write_bytes(data)
    except OSError as exc:
        raise RuntimeError(f"图片写入失败：{path}（{exc}）") from exc
    return path


# ==================== 主循环 ====================
async def main(save_dir=None, max_snapshots=None):
    cam_cfg = CAMERA
    save_dir = Path(save_dir).expanduser() if save_dir else SAVE_DIR
    max_snapshots = MAX_SNAPSHOTS if max_snapshots is None else max(0, int(max_snapshots))

    log(f"图片保存目录：{save_dir.resolve()}")
    log(f"随机间隔：{INTERVAL_RANGE[0]:g} ~ {INTERVAL_RANGE[1]:g} 秒，"
        f"张数上限：{'不限' if not max_snapshots else max_snapshots}")

    camera = None
    try:
        camera, token = await connect_onvif(cam_cfg)
        saved = 0
        failures = 0
        while True:
            if max_snapshots and saved >= max_snapshots:
                log(f"已达到抓图上限 {max_snapshots} 张，结束。")
                break

            # 第一张立即抓，之后每张间隔一段随机时间
            if saved or failures:
                wait = random.uniform(*INTERVAL_RANGE)
                log(f"随机等待 {wait:.1f} 秒后抓图 ...")
                await asyncio.sleep(wait)

            shot_time = datetime.now()
            log(f"开始抓图（{shot_time:%Y-%m-%d %H:%M:%S.%f}）")
            try:
                data = await asyncio.wait_for(
                    capture_once(camera, token, cam_cfg),
                    timeout=SNAPSHOT_TIMEOUT + 10,
                )
            except Exception as exc:
                failures += 1
                log(f"抓图失败（连续第 {failures} 次）：{format_error(exc)}")
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    log("连续失败过多，重建 ONVIF 会话 ...")
                    await close_camera(camera)
                    camera, token = await connect_onvif(cam_cfg)
                    failures = 0
                continue

            failures = 0
            saved += 1
            path = save_image(data, shot_time, save_dir)
            log(f"第 {saved} 张已保存：{path}")
    finally:
        await close_camera(camera)


if __name__ == "__main__":
    setup_console()
    args = parse_args()
    prepare_event_loop()
    try:
        asyncio.run(main(args.save_dir, args.count))
    except KeyboardInterrupt:
        log("收到 Ctrl+C，退出。")
    except Exception as exc:
        message = format_error(exc)
        log(f"程序异常退出：{message}")
        print_troubleshooting(message)
        sys.exit(1)
