#!/bin/bash

# install_service.sh - FiberAlarmGateway systemd 服务脚本（使用系统默认 Python）

if [ "$(id -u)" -ne 0 ]; then
    echo "请使用 sudo 或以 root 用户运行此脚本"
    exit 1
fi

SERVICE_NAME="fiber-alarm-gateway"
USER_NAME="linaro"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "脚本所在目录: ${SCRIPT_DIR}"

PYTHON_SCRIPT="${SCRIPT_DIR}/main.py"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

# 检查系统 Python
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到 python3，请先安装 Python 3"
    exit 1
fi

SYSTEM_PYTHON=$(command -v python3)

# 检查 ffmpeg / ffprobe 路径
FFMPEG_DIR="/opt/sophon/sophon-ffmpeg-latest/bin"
FFMPEG_BIN="${FFMPEG_DIR}/ffmpeg"
FFPROBE_BIN="${FFMPEG_DIR}/ffprobe"
if [ -f "$FFMPEG_BIN" ] && [ -f "$FFPROBE_BIN" ]; then
    echo "找到 ffmpeg: ${FFMPEG_BIN}"
    echo "找到 ffprobe: ${FFPROBE_BIN}"
    FFMPEG_PATH="${FFMPEG_DIR}"
else
    if [ ! -f "$FFMPEG_BIN" ]; then
        echo "警告: 未找到 ffmpeg，视频转码与图片拼接功能将不可用，请确保已安装"
    fi
    if [ ! -f "$FFPROBE_BIN" ]; then
        echo "警告: 未找到 ffprobe，图片宽度探测功能将不可用，请确保已安装"
    fi
    FFMPEG_PATH=""
fi

UNINSTALL=false
CLEARLOGS=false

for arg in "$@"; do
    case $arg in
        --uninstall|-u) UNINSTALL=true ;;
        --clearlogs|-c) CLEARLOGS=true ;;
    esac
done

if [ "$UNINSTALL" = "true" ]; then
    systemctl stop "${SERVICE_NAME}.service" 2>/dev/null
    systemctl disable "${SERVICE_NAME}.service" 2>/dev/null
    rm -f "$SERVICE_FILE"
    systemctl daemon-reload
    systemctl reset-failed
    echo "服务已卸载"
    exit 0
fi

if [ "$CLEARLOGS" = "true" ]; then
    journalctl -u "${SERVICE_NAME}.service" --rotate
    journalctl -u "${SERVICE_NAME}.service" --vacuum-time=1s
    echo "日志已清理"
    exit 0
fi

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "错误: $PYTHON_SCRIPT 不存在"
    exit 1
fi

# 检查 config 目录
if [ ! -d "${SCRIPT_DIR}/config" ]; then
    echo "警告: 未找到 config 目录，请确保配置文件存在"
fi

# ====== systemd ExecStart ======
EXEC_START="${SYSTEM_PYTHON} ${PYTHON_SCRIPT}"

# 构建环境变量（保留原有环境变量，添加 PATH）
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=${SERVICE_NAME} Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER_NAME}
Group=${USER_NAME}
WorkingDirectory=${SCRIPT_DIR}
ExecStart=${EXEC_START}
ExecStop=/bin/kill -SIGINT \$MAINPID
Restart=always
RestartSec=10s
# ====== 看门狗配置 ======
WatchdogSec=10s          # systemd 看门狗超时时间
Restart=on-watchdog      # 看门狗超时时自动重启
# 启动超时（适应慢启动场景）
# TimeoutStartSec=120

StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

Environment=PYTHONUNBUFFERED=1
Environment=PYTHONIOENCODING=utf-8
Environment=LC_ALL=C.UTF-8
Environment=LANG=C.UTF-8
EOF

# 如果有 ffmpeg，添加 PATH 环境变量
if [ -n "$FFMPEG_PATH" ]; then
    cat >> "$SERVICE_FILE" <<EOF
# 添加 Sophon ffmpeg/ffprobe 到 PATH
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${FFMPEG_PATH}
EOF
fi

cat >> "$SERVICE_FILE" <<EOF
# 资源限制
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

chmod 644 "$SERVICE_FILE"

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"
systemctl start "${SERVICE_NAME}.service"

systemctl status "${SERVICE_NAME}.service" --no-pager

echo "服务已安装完成"
echo "使用系统 Python: ${SYSTEM_PYTHON}"
if [ -n "$FFMPEG_PATH" ]; then
    echo "FFmpeg/ffprobe 路径已添加: ${FFMPEG_PATH}"
fi
echo "日志查看:"
echo "  journalctl -u ${SERVICE_NAME}.service -f"
echo "  tail -f -n 100 ${SCRIPT_DIR}/logs/"
echo "其他命令:"
echo "  停止服务:   sudo systemctl stop ${SERVICE_NAME}"
echo "  重启服务:   sudo systemctl restart ${SERVICE_NAME}"
echo "  服务状态：  sudo systemctl status  ${SERVICE_NAME}"
echo "  卸载服务:   sudo ./install_service.sh --uninstall"
echo "  清理日志:   sudo ./install_service.sh --clearlogs"