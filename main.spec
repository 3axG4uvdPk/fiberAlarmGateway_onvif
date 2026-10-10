# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

# ONVIF WSDL 文件随 onvif-zeep-async 包一起打包，否则运行时无法解析服务定义
onvif_wsdl_datas = collect_data_files('onvif', includes=['wsdl/**'])

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config/config.json', 'config'),
    ] + onvif_wsdl_datas,
    hiddenimports=[
        'camera',
        'camera.camera_manager',
        'camera.onvif_camera',
        'core',
        'core.config',
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
