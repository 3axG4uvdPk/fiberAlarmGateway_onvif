# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['onvif_snapshot_demo.py'],
    pathex=[],
    binaries=[],
    datas=[('/home/linaro/.local/lib/python3.10/site-packages/onvif/wsdl', 'onvif/wsdl')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='onvif_snapshot_demo',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
