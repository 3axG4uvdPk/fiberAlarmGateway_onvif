# -*- coding: utf-8 -*-
"""
ffmpeg / ffprobe 定位与子进程调用工具。

本程序运行在 Windows 环境，ffmpeg 免安装包随项目一起分发（默认位于
``ffmpeg-win64/bin``），因此**不再依赖系统 PATH 中存在 ffmpeg**：

- 源码运行（``python main.py``）：使用项目根目录下的 ``ffmpeg-win64/bin``；
- 打包运行（PyInstaller）：优先使用可执行文件同级目录，其次使用打包内嵌目录；
- 仍可通过环境变量 ``ALARM_FFMPEG_PATH`` / ``ALARM_FFMPEG_DIR`` 显式覆盖；
- 最后才回退到系统 PATH（``shutil.which``）。
"""
import os
import shutil
import subprocess
import sys
import logging

from core.config import get_runtime_base_dir, resolve_runtime_path

logger = logging.getLogger("alarm_app.ffmpeg")

# 随项目分发的 ffmpeg 免安装包目录（相对运行根目录），按优先级排列
# - Windows：ffmpeg-win64/bin（含 ffmpeg.exe / ffprobe.exe 及 DLL）
# - Linux  ：ffmpeg-linux64/bin（含 ffmpeg / ffprobe 及 .so）
# 二者均放在项目根目录下，由对应平台的构建脚本随包分发。
BUNDLED_FFMPEG_DIRS = (
    os.path.join('ffmpeg-win64', 'bin'),
    os.path.join('ffmpeg-linux64', 'bin'),
    os.path.join('ffmpeg', 'bin'),
    'ffmpeg-win64',
    'ffmpeg-linux64',
    'ffmpeg',
    'bin',
)

# 环境变量：显式指定 ffmpeg 目录（_DIR）或可执行文件路径（_PATH）
_ENV_DIR_KEYS = ('ALARM_FFMPEG_DIR', 'FFMPEG_DIR')
_ENV_FILE_KEYS = ('ALARM_FFMPEG_PATH', 'FFMPEG_PATH')

# 定位结果缓存（binary_name -> 绝对路径 或 None），避免重复扫描磁盘
_RESOLVE_CACHE = {}


def no_window_kwargs():
    """返回执行外部子进程时使用的平台附加参数（防止弹出控制台窗口）。

    - Windows：``creationflags=CREATE_NO_WINDOW``，打包为窗口/服务方式运行时
      不会为每次 ffmpeg 调用闪出控制台窗口；
    - 其它平台：``start_new_session=True``，让子进程独立于父进程会话，
      便于超时后连同整个进程组一起终止。
    """
    if os.name == 'nt':
        return {'creationflags': subprocess.CREATE_NO_WINDOW}
    return {'start_new_session': True}


def _exe_name(binary_name):
    return f"{binary_name}.exe" if os.name == 'nt' else binary_name


def _candidate_dirs():
    """返回需要尝试的 ffmpeg 所在目录（按优先级）。"""
    dirs = []
    for key in _ENV_DIR_KEYS:
        value = os.environ.get(key)
        if value:
            dirs.append(resolve_runtime_path(value))

    roots = []
    try:
        roots.append(get_runtime_base_dir())
    except Exception:
        pass
    if getattr(sys, 'frozen', False):
        roots.append(os.path.dirname(os.path.abspath(sys.executable)))
    meipass = getattr(sys, '_MEIPASS', None)
    if meipass:
        roots.append(meipass)

    for root in roots:
        if not root:
            continue
        for rel in BUNDLED_FFMPEG_DIRS:
            dirs.append(os.path.join(root, rel))
        # 可执行文件 / ffmpeg 直接平铺在运行根目录的情况
        dirs.append(root)

    # 去重但保持顺序
    unique_dirs = []
    for item in dirs:
        if item not in unique_dirs:
            unique_dirs.append(item)
    return unique_dirs


def resolve_binary(binary_name):
    """定位 ffmpeg/ffprobe 可执行文件，返回绝对路径；找不到返回 None（结果缓存）。"""
    if binary_name in _RESOLVE_CACHE:
        return _RESOLVE_CACHE[binary_name]

    exe_name = _exe_name(binary_name)

    # 1. 环境变量直接指定的可执行文件
    for key in _ENV_FILE_KEYS:
        value = os.environ.get(key)
        if value and os.path.isfile(value):
            resolved = os.path.abspath(value)
            _RESOLVE_CACHE[binary_name] = resolved
            logger.info(f"已通过环境变量 {key} 定位 {binary_name}: {resolved}")
            return resolved

    # 2. 项目内置免安装包 / 运行根目录
    for dir_path in _candidate_dirs():
        candidate = os.path.join(dir_path, exe_name)
        if os.path.isfile(candidate):
            resolved = os.path.abspath(candidate)
            _RESOLVE_CACHE[binary_name] = resolved
            logger.info(f"已定位 {binary_name}: {resolved}")
            return resolved

    # 3. 系统 PATH
    found = shutil.which(binary_name)
    if found:
        resolved = os.path.abspath(found)
        _RESOLVE_CACHE[binary_name] = resolved
        logger.info(f"已从系统 PATH 定位 {binary_name}: {resolved}")
        return resolved

    _RESOLVE_CACHE[binary_name] = None
    return None


def resolve_ffmpeg_path():
    """返回 ffmpeg 可执行文件绝对路径；找不到返回 None。"""
    return resolve_binary('ffmpeg')


def resolve_ffprobe_path():
    """返回 ffprobe 可执行文件绝对路径；找不到返回 None。"""
    return resolve_binary('ffprobe')


def ffmpeg_missing_hint(binary_name='ffmpeg'):
    """返回「未找到 ffmpeg/ffprobe」时便于现场排查的提示文本。"""
    expected = os.path.join(
        'ffmpeg-win64' if os.name == 'nt' else 'ffmpeg-linux64',
        'bin', _exe_name(binary_name)
    )
    try:
        expected_path = os.path.join(get_runtime_base_dir(), expected)
    except Exception:
        expected_path = expected
    return (
        f"未找到 {binary_name}，将无法执行相关操作。"
        f"请确认已将 ffmpeg 免安装包放在运行根目录下的 '{expected}'"
        f"（当前期望路径: {expected_path}），"
        f"或通过环境变量 ALARM_FFMPEG_PATH / ALARM_FFMPEG_DIR 指定。"
    )


def run_command(command, timeout_seconds):
    """执行参数列表形式的命令，返回 ``(returncode, stdout, stderr)``。

    统一在子进程中隐藏控制台窗口，并以 UTF-8 + 容错方式解码输出，
    避免 Windows 默认 GBK 代码页解码 ffmpeg 输出时报 UnicodeDecodeError。
    """
    kwargs = {
        'stdin': subprocess.DEVNULL,
        'stdout': subprocess.PIPE,
        'stderr': subprocess.PIPE,
    }
    kwargs.update(no_window_kwargs())
    process = subprocess.Popen(
        command,
        text=True,
        encoding='utf-8',
        errors='replace',
        **kwargs,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        terminate_process_tree(process)
        raise
    except BaseException:
        terminate_process_tree(process)
        raise
    return process.returncode, stdout, stderr


def terminate_process_tree(process):
    """终止子进程及其派生的整个进程树（确保 ffmpeg 不被残留）。

    Windows 没有 ``os.killpg``，改用 ``taskkill /F /T`` 结束整棵进程树；
    POSIX 下沿用 killpg(SIGKILL) 的原有实现。
    """
    if os.name == 'nt':
        try:
            subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(process.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW,
                timeout=10,
            )
        except Exception:
            pass
        try:
            process.kill()
        except Exception:
            pass
    else:
        try:
            import signal
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
    try:
        process.communicate(timeout=5)
    except Exception:
        pass
