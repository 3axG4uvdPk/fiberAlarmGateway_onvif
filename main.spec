# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

# ONVIF WSDL 文件随 onvif-zeep-async 包一起打包，否则运行时无法解析服务定义
onvif_wsdl_datas = collect_data_files('onvif', includes=['wsdl/**'])

# ffmpeg 免安装包随程序一起打包，运行时由 core.ffmpeg_utils 从该目录定位，
# 不依赖系统 PATH。按当前构建平台选择对应的二进制目录：
# - Windows：ffmpeg-win64/bin（ffmpeg.exe / ffprobe.exe 及其依赖 DLL）
# - Linux  ：ffmpeg-linux64/bin（ffmpeg / ffprobe 及其依赖 .so）
# 若对应目录不存在（例如仅在 Windows 上分发 Windows 包），则跳过打包，
# 运行时仍可回退到系统 PATH / 环境变量 ALARM_FFMPEG_PATH。
import os as _os
import sys as _sys

_ffmpeg_bin_dir = (
    'ffmpeg-win64/bin' if _sys.platform == 'win32' else 'ffmpeg-linux64/bin'
)
ffmpeg_datas = (
    [(_ffmpeg_bin_dir, _ffmpeg_bin_dir)] if _os.path.isdir(_ffmpeg_bin_dir) else []
)

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config/config.json', 'config'),
    ] + onvif_wsdl_datas + ffmpeg_datas,
    hiddenimports=[
        'camera',
        'camera.camera_manager',
        'camera.onvif_camera',
        'core',
        'core.config',
        'core.ffmpeg_utils',
        'core.logging_config',
        'core.system_status',
        'messaging',
        'messaging.alarm_receiver',
        'messaging.heartbeat_worker',
        'messaging.status_receiver',
        'messaging.upload_worker',
        'processing',
        'processing.processing_worker',
        'storage',
        'storage.minio_manager',
        'video',
        'video.video_downloader',
        'onvif',
        'httpx',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='fiber',  # 输出文件名
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # 保留控制台方便调试
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
