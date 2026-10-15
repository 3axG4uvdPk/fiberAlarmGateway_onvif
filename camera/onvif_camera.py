# -*- coding: utf-8 -*-
"""
ONVIF 摄像机操作模块（抓图 / 云台预置位）。

统一通过 ONVIF 标准协议操作摄像机，不再依赖任何厂家私有 SDK：
- 抓图：``GetProfiles`` → ``GetSnapshotUri`` → 用 **httpx** 拉取图片字节流
  （依次尝试 Digest 认证、Basic 认证、匿名）；
- 云台预置位：``PTZ GotoPreset``；
- 会话探测：``GetProfiles``（用于启动期预连接与诊断）。

线程模型
--------
``onvif-zeep-async``（zeep / aiohttp）的客户端对象**不线程安全**且绑定创建它的
事件循环。本模块因此维护「一个后台事件循环线程 + 每设备一份 ONVIF 会话」：
业务线程通过 ``asyncio.run_coroutine_threadsafe`` 把协程投递到该循环同步等待结果，
所有 ONVIF 调用天然串行在同一个循环线程上执行，避免并发竞态。
事件循环在首次使用时惰性创建，进程退出时由 :func:`shutdown` 关闭。
"""
import asyncio
import concurrent.futures
import logging
import re
import threading
from typing import Any

logger = logging.getLogger("alarm_app.camera.onvif")

# ONVIF 服务端口默认值（摄像机 ONVIF/HTTP 服务端口）
DEFAULT_ONVIF_PORT = 80
# 单次 ONVIF 操作（建链/取流地址/PTZ）的整体超时（秒）。
# 取值需要留足余量：部分摄像机（实测大华定焦机）动态生成一张 1920x1080 快照
# 就要 4~7 秒，加上建链与 GetSnapshotUri，20 秒容易踩到超时。
DEFAULT_ONVIF_OPERATION_TIMEOUT_SECONDS = 30.0
# 抓图 HTTP 请求超时（秒）
DEFAULT_SNAPSHOT_TIMEOUT_SECONDS = 20.0
# 单次 ONVIF 会话建立（update_xaddrs）超时（秒）
DEFAULT_ONVIF_CONNECT_TIMEOUT_SECONDS = 15.0

# 日志里脱敏 RTSP/HTTP URL 中的密码：scheme://user:pass@ -> scheme://user:***@
_URL_CREDENTIALS_RE = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)(?P<user>[^:/@\s]+):(?P<pwd>[^@/\s]*)@")


class OnvifError(RuntimeError):
    """ONVIF 操作失败。"""


# 惰性导入的 ONVIFCamera 类（首次使用时导入，避免未使用时的导入开销）
_ONVIF_CAMERA_CLASS = None
# onvif-zeep-async 在导入时向 root logger 添加的 handler，导入后需移除
_ONVIF_IMPORT_LOCK = threading.Lock()


def _onvif_camera_class():
    """惰性导入 ``ONVIFCamera`` 并抑制库自身对日志系统的副作用。

    ``onvif.client`` 模块在导入时会调用 ``logging.basicConfig(level=INFO)``，
    这会向 root logger 挂一个输出到 stderr 的 StreamHandler。本程序有独立的日志
    配置（``alarm_app`` logger），这里在导入前后比对并移除新增的 root handler，
    并把库日志级别收敛到 WARNING，避免出现两套格式的重复日志。
    """
    global _ONVIF_CAMERA_CLASS
    if _ONVIF_CAMERA_CLASS is not None:
        return _ONVIF_CAMERA_CLASS

    with _ONVIF_IMPORT_LOCK:
        if _ONVIF_CAMERA_CLASS is not None:
            return _ONVIF_CAMERA_CLASS

        root_handlers_before = list(logging.root.handlers)
        from onvif import ONVIFCamera

        for handler in list(logging.root.handlers):
            if handler not in root_handlers_before:
                logging.root.removeHandler(handler)
        for name in ("onvif", "zeep", "aiohttp", "httpx", "httpcore", "asyncio"):
            logging.getLogger(name).setLevel(logging.WARNING)

        _ONVIF_CAMERA_CLASS = ONVIFCamera
        return _ONVIF_CAMERA_CLASS


def mask_url_credentials(url):
    """将 URL 中的密码替换为 ``***``，便于安全打印日志。"""
    if not url:
        return url
    return _URL_CREDENTIALS_RE.sub(r"\g<scheme>\g<user>:***@", str(url))


# =====================================================================
# 后台事件循环线程
# =====================================================================

_runtime_lock = threading.Lock()
_loop = None
_loop_thread = None
# 设备会话缓存：device_key -> {'camera','media','ptz','profile_token'}
_sessions = {}


def _run_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _ensure_loop():
    """返回后台事件循环（惰性创建并启动专用线程）。"""
    global _loop, _loop_thread
    with _runtime_lock:
        if _loop is None or _loop.is_closed():
            loop = asyncio.new_event_loop()
            thread = threading.Thread(target=_run_loop, args=(loop,), name="OnvifLoop", daemon=True)
            _loop = loop
            _loop_thread = thread
            thread.start()
            logger.debug("ONVIF 后台事件循环线程已启动。")
        return _loop


def _submit(coro, timeout, operation):
    """把协程投递到后台事件循环并同步等待结果。"""
    loop = _ensure_loop()
    try:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
    except Exception:
        coro.close()
        raise
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError as exc:
        future.cancel()
        raise OnvifError(f"{operation} 超时（>{timeout:g} 秒）") from exc


# =====================================================================
# 设备会话
# =====================================================================

def _device_key(cam_cfg):
    """设备会话缓存键：(ip, ONVIF端口, 用户名)。"""
    return (
        str(cam_cfg.get('ip') or ''),
        _onvif_port(cam_cfg),
        str(cam_cfg.get('username') or ''),
    )


def _onvif_port(cam_cfg):
    raw_port = cam_cfg.get('onvif_port')
    if raw_port in (None, ''):
        raw_port = cam_cfg.get('port')
    try:
        return int(raw_port) if int(raw_port) > 0 else DEFAULT_ONVIF_PORT
    except (TypeError, ValueError):
        return DEFAULT_ONVIF_PORT


def _password(cam_cfg):
    return cam_cfg.get('pwd') or cam_cfg.get('password') or ''


def _device_tag(cam_cfg):
    return f"相机 {cam_cfg.get('ip')}"


async def _create_session(cam_cfg):
    """建立 ONVIF 会话（连接 + 媒体/云台服务）。"""
    ONVIFCamera = _onvif_camera_class()

    host = cam_cfg.get('ip')
    port = _onvif_port(cam_cfg)
    user = cam_cfg.get('username') or ''
    pwd = _password(cam_cfg)
    tag = _device_tag(cam_cfg)

    camera = ONVIFCamera(host, port, user, pwd)
    try:
        await asyncio.wait_for(
            camera.update_xaddrs(), timeout=DEFAULT_ONVIF_CONNECT_TIMEOUT_SECONDS
        )
        media = await camera.create_media_service()
        try:
            ptz = await camera.create_ptz_service()
        except Exception as exc:  # 部分设备不暴露 PTZ 服务，不影响抓图
            logger.warning(f"{tag} ONVIF 不支持云台服务（PTZ），预置位控制将被跳过: {exc}")
            ptz = None
    except BaseException:
        # 建链中途失败（如连接被拒、超时）时必须回收已创建的 zeep/aiohttp 连接，
        # 否则每次失败都会泄漏一个 ClientSession（表现为 asyncio 的
        # "Unclosed client session" 告警）。
        await _close_session({'camera': camera})
        raise

    logger.info(f"{tag} ONVIF 会话已建立（{host}:{port}）。")
    return {
        'camera': camera,
        'media': media,
        'ptz': ptz,
        'profile_token': None,
        # 上次抓图成功的 HTTP 认证方式（digest/basic/none），下次优先复用
        'snapshot_auth': None,
    }


async def _get_session(cam_cfg):
    """返回设备 ONVIF 会话（不存在时先建立）。"""
    key = _device_key(cam_cfg)
    session = _sessions.get(key)
    if session is None:
        session = await _create_session(cam_cfg)
        _sessions[key] = session
    return session


async def _resolve_profile_token(session, cam_cfg):
    """确定要使用的 ONVIF 媒体 Profile Token。

    优先使用配置项 ``onvif_profile``（可填 Profile 的 token 或 Name）；
    未配置时取设备返回的第一个 Profile（通常是主码流）。
    """
    if session.get('profile_token'):
        return session['profile_token']

    media = session['media']
    profiles = await media.GetProfiles()
    if not profiles:
        raise OnvifError(f"{_device_tag(cam_cfg)} 未返回任何媒体 Profile，无法抓图/控制云台。")

    wanted = cam_cfg.get('onvif_profile')
    token = None
    if wanted not in (None, ''):
        for profile in profiles:
            profile_token = str(getattr(profile, 'token', '') or '')
            profile_name = str(getattr(profile, 'Name', '') or '')
            if str(wanted) in (profile_token, profile_name):
                token = profile_token
                break
        if token is None:
            token = str(wanted)
            logger.warning(
                f"{_device_tag(cam_cfg)} 配置的 onvif_profile={wanted!r} 未在设备 Profile 列表中，"
                f"将直接按 token 使用。"
            )

    available = [str(getattr(p, 'token', '') or '') for p in profiles]
    if token is None:
        token = available[0]
    session['profile_token'] = token
    logger.info(
        f"{_device_tag(cam_cfg)} ONVIF 使用 Profile={token}，设备可用 Profile={available}。"
    )
    return token


async def _close_session(session):
    """关闭单个 ONVIF 会话，释放 zeep/aiohttp 连接。"""
    if not session:
        return
    camera = session.get('camera')
    if camera is None:
        return
    try:
        await camera.close()
    except Exception as exc:  # 关闭失败不影响业务
        logger.debug(f"关闭 ONVIF 会话时出现异常（可忽略）: {exc}")


async def _close_all_sessions():
    for key, session in list(_sessions.items()):
        await _close_session(session)
        _sessions.pop(key, None)


def forget_device(cam_cfg):
    """丢弃指定设备的 ONVIF 会话，下次操作时重新建立。

    抓图/PTZ 失败后调用（见 ``CameraManager.close_and_remove_client``），
    避免失效的底层连接被后续操作反复复用。
    """
    key = _device_key(cam_cfg)
    with _runtime_lock:
        session = _sessions.pop(key, None)
        loop = _loop
    if session is None or loop is None or loop.is_closed():
        return
    try:
        future = asyncio.run_coroutine_threadsafe(_close_session(session), loop)
        future.result(timeout=5)
    except Exception as exc:
        logger.debug(f"丢弃 ONVIF 会话时出现异常（可忽略）: {exc}")


def shutdown():
    """关闭所有 ONVIF 会话并停止后台事件循环线程（进程退出时调用）。"""
    global _loop, _loop_thread
    with _runtime_lock:
        loop, thread = _loop, _loop_thread
        _loop = None
        _loop_thread = None

    if loop is None:
        with _runtime_lock:
            _sessions.clear()
        return

    try:
        future = asyncio.run_coroutine_threadsafe(_close_all_sessions(), loop)
        future.result(timeout=10)
    except Exception as exc:
        logger.debug(f"关闭 ONVIF 会话时出现异常（可忽略）: {exc}")
        _sessions.clear()

    loop.call_soon_threadsafe(loop.stop)
    if thread is not None and thread.is_alive():
        thread.join(timeout=5)
    try:
        loop.close()
    except Exception as exc:
        logger.debug(f"关闭 ONVIF 事件循环时出现异常（可忽略）: {exc}")
    logger.info("ONVIF 后台事件循环线程已停止。")


# =====================================================================
# 对外操作
# =====================================================================

async def _probe(cam_cfg):
    session = await _get_session(cam_cfg)
    media = session['media']
    profiles = await media.GetProfiles()
    token = await _resolve_profile_token(session, cam_cfg)
    return {
        'profile_token': token,
        'profiles': [
            {
                'token': str(getattr(p, 'token', '') or ''),
                'name': str(getattr(p, 'Name', '') or ''),
            }
            for p in profiles
        ],
        'ptz_supported': session.get('ptz') is not None,
    }


def probe(cam_cfg, timeout=DEFAULT_ONVIF_OPERATION_TIMEOUT_SECONDS):
    """探测设备 ONVIF 能力，返回 Profile 与 PTZ 支持情况。"""
    tag = _device_tag(cam_cfg)
    try:
        result: dict[str, Any | str | list[dict[str, str]] | bool] = _submit(_probe(cam_cfg), timeout, f"{tag} ONVIF 连接探测")
    except OnvifError:
        # 探测失败时丢弃可能已损坏的会话，避免污染后续操作
        forget_device(cam_cfg)
        raise
    except Exception as exc:
        forget_device(cam_cfg)
        raise OnvifError(
            f"{tag} ONVIF 连接探测失败: {type(exc).__name__}: {exc}"
        ) from exc
    logger.info(f"{tag} ONVIF 连接探测成功: Profile={result['profile_token']}，"
        f"云台支持={'是' if result['ptz_supported'] else '否'}。")
    return result


async def _goto_preset(cam_cfg, preset):
    session = await _get_session(cam_cfg)
    ptz = session.get('ptz')
    if ptz is None:
        raise OnvifError(f"{_device_tag(cam_cfg)} 未提供 ONVIF 云台服务，无法执行预置位控制。")
    token = await _resolve_profile_token(session, cam_cfg)

    request = ptz.create_type('GotoPreset')
    request.ProfileToken = token
    request.PresetToken = str(preset)
    await ptz.GotoPreset(request)
    return token


def goto_preset(cam_cfg, preset, timeout=DEFAULT_ONVIF_OPERATION_TIMEOUT_SECONDS):
    """云台移动到指定预置位（ONVIF PTZ GotoPreset）。"""
    tag = _device_tag(cam_cfg)
    try:
        token = _submit(_goto_preset(cam_cfg, preset), timeout, f"{tag} 移动到预置位 {preset}")
    except OnvifError:
        forget_device(cam_cfg)
        raise
    except Exception as exc:
        forget_device(cam_cfg)
        raise OnvifError(
            f"{tag} ONVIF 移动到预置位 {preset} 失败: {type(exc).__name__}: {exc}"
        ) from exc
    logger.info(f"{tag} ONVIF 预置位 GOTO({preset}) 成功，Profile={token}。")
    return token


def _strip_url_credentials(url):
    """去掉 URL 中的 ``user:pass@`` 片段（认证改由 httpx auth 参数携带）。"""
    return _URL_CREDENTIALS_RE.sub(r"\g<scheme>", url, count=1)


async def _fetch_snapshot(uri, user, pwd, preferred=None, timeout=DEFAULT_SNAPSHOT_TIMEOUT_SECONDS):
    """用 httpx 拉取快照图片字节流，返回 ``(图片数据, 认证方式)``。

    认证方式按 ``preferred``（上次成功的方式）→ digest → basic → 匿名 依次尝试。
    实测部分摄像机生成一张快照本身就要数秒，因此缓存上次成功的认证方式，
    避免每次抓图都把整条回退链走一遍而撞上整体超时。
    """
    import httpx

    request_url = _strip_url_credentials(uri)
    order = []
    if preferred:
        order.append(preferred)
    if user:
        order.extend(('digest', 'basic'))
    order.append('none')

    last_error = None
    tried = set()
    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=True, verify=False
    ) as client:
        for mode in order:
            if mode in tried:
                continue
            tried.add(mode)
            auth = None
            if mode == 'digest' and user:
                auth = httpx.DigestAuth(user, pwd or '')
            elif mode == 'basic' and user:
                auth = httpx.BasicAuth(user, pwd or '')
            try:
                response = await client.get(request_url, auth=auth)
            except httpx.HTTPError as exc:
                last_error = f"HTTP 请求异常: {exc}"
                continue
            if response.is_success and response.content:
                return response.content, mode
            last_error = f"HTTP {response.status_code}"
    raise OnvifError(f"拉取快照失败（{mask_url_credentials(request_url)}）: {last_error}")


async def _snapshot(cam_cfg):
    session = await _get_session(cam_cfg)
    token = await _resolve_profile_token(session, cam_cfg)
    camera = session['camera']
    uri = await camera.get_snapshot_uri(token)
    if not uri:
        raise OnvifError(f"{_device_tag(cam_cfg)} 未返回快照地址（GetSnapshotUri）。")
    data, mode = await _fetch_snapshot(
        uri,
        cam_cfg.get('username') or '',
        _password(cam_cfg),
        preferred=session.get('snapshot_auth'),
    )
    if not data:
        raise OnvifError(f"{_device_tag(cam_cfg)} 快照返回空数据。")
    if session.get('snapshot_auth') != mode:
        session['snapshot_auth'] = mode
        logger.info(f"{_device_tag(cam_cfg)} ONVIF 快照认证方式命中: {mode}。")
    return data


def capture_to_memory(cam_cfg, timeout=DEFAULT_ONVIF_OPERATION_TIMEOUT_SECONDS):
    """ONVIF 抓图到内存，返回图片二进制数据。"""
    tag = _device_tag(cam_cfg)
    try:
        data = _submit(_snapshot(cam_cfg), timeout, f"{tag} ONVIF 抓图")
    except OnvifError:
        forget_device(cam_cfg)
        raise
    except Exception as exc:
        forget_device(cam_cfg)
        raise OnvifError(
            f"{tag} ONVIF 抓图失败: {type(exc).__name__}: {exc}"
        ) from exc
    logger.info(f"{tag} ONVIF 抓图到内存成功（{len(data)} 字节）。")
    return data
