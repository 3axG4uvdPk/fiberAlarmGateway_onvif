#!/bin/bash

# install_service.sh - FiberAlarmGateway systemd 服务安装脚本
#
# 用法:
#   sudo ./install_service.sh               # 使用 conda 虚拟环境 fag 中的 python 运行 main.py
#   sudo ./install_service.sh --exe         # 运行打包生成的可执行程序 ./fiber
#   sudo ./install_service.sh --uninstall   # 卸载服务（--uninstall / -u）
#   sudo ./install_service.sh --clearlogs   # 清理服务日志（--clearlogs / -c）
#
# 可选环境变量:
#   CONDA_ENV_NAME  conda 环境名，默认 fag
#   CONDA_ENV_PATH  conda 环境绝对路径，默认自动探测
#                   例: sudo -E CONDA_ENV_PATH=/home/linaro/minianaconda3/envs/fag ./install_service.sh

if [ "$(id -u)" -ne 0 ]; then
    echo "请使用 sudo 或以 root 用户运行此脚本"
    exit 1
fi

SERVICE_NAME="fiber-alarm-gateway"
USER_NAME="linaro"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "脚本所在目录: ${SCRIPT_DIR}"

PYTHON_SCRIPT="${SCRIPT_DIR}/main.py"
MAIN_BIN="${SCRIPT_DIR}/fiber"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

CONDA_ENV_NAME="${CONDA_ENV_NAME:-fag}"
CONDA_ENV_PATH="${CONDA_ENV_PATH:-}"

USE_EXE=false
UNINSTALL=false
CLEARLOGS=false

for arg in "$@"; do
    case $arg in
        --exe|-e) USE_EXE=true ;;
        --uninstall|-u) UNINSTALL=true ;;
        --clearlogs|-c) CLEARLOGS=true ;;
        --help|-h)
            echo "用法: sudo $0 [--exe] [--uninstall] [--clearlogs]"
            echo "  (无参数)      使用 conda 环境 ${CONDA_ENV_NAME} 运行 main.py"
            echo "  --exe, -e     运行可执行程序 ${MAIN_BIN}"
            echo "  --uninstall   卸载服务"
            echo "  --clearlogs   清理日志"
            exit 0
            ;;
        *)
            echo "错误: 未知参数 $arg"
            echo "使用 sudo $0 --help 查看用法"
            exit 1
            ;;
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

# 检查 config 目录
if [ ! -d "${SCRIPT_DIR}/config" ]; then
    echo "警告: 未找到 config 目录，请确保配置文件存在"
fi

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

# ====== 查找 conda 虚拟环境（仅 --exe 以外的模式需要） ======
find_conda_env() {
    local name="$1"
    local candidates=()
    local conda_bin root home_dir c

    # 1) 显式指定 CONDA_ENV_PATH
    if [ -n "$CONDA_ENV_PATH" ] && [ -x "${CONDA_ENV_PATH}/bin/python" ]; then
        echo "$CONDA_ENV_PATH"
        return 0
    fi

    # 2) 当前已激活的环境正好是目标环境
    if [ -n "$CONDA_PREFIX" ] && [ "$(basename "$CONDA_PREFIX")" = "$name" ] \
        && [ -x "${CONDA_PREFIX}/bin/python" ]; then
        echo "$CONDA_PREFIX"
        return 0
    fi

    # 3) 通过 conda 命令反推安装根目录
    conda_bin="${CONDA_EXE:-}"
    if [ -z "$conda_bin" ]; then
        conda_bin="$(command -v conda 2>/dev/null)"
    fi
    if [ -n "$conda_bin" ]; then
        root="$(dirname "$(dirname "$conda_bin")")"
        candidates+=("${root}/envs/${name}")
    fi

    # 4) 常见安装位置（minianaconda3 / miniconda3 / anaconda3）
    for home_dir in "/home/${USER_NAME}"; do
        candidates+=(
            "${home_dir}/minianaconda3/envs/${name}"
            "${home_dir}/miniconda3/envs/${name}"
            "${home_dir}/anaconda3/envs/${name}"
        )
    done
    if [ -n "$SUDO_USER" ] && [ "$SUDO_USER" != "$USER_NAME" ] && [ -d "/home/${SUDO_USER}" ]; then
        candidates+=(
            "/home/${SUDO_USER}/minianaconda3/envs/${name}"
            "/home/${SUDO_USER}/miniconda3/envs/${name}"
            "/home/${SUDO_USER}/anaconda3/envs/${name}"
        )
    fi
    candidates+=(
        "/opt/minianaconda3/envs/${name}"
        "/opt/miniconda3/envs/${name}"
        "/opt/anaconda3/envs/${name}"
        "/usr/local/minianaconda3/envs/${name}"
        "/usr/local/miniconda3/envs/${name}"
        "/root/minianaconda3/envs/${name}"
        "/root/miniconda3/envs/${name}"
        "/root/anaconda3/envs/${name}"
    )

    for c in "${candidates[@]}"; do
        if [ -x "${c}/bin/python" ]; then
            echo "$c"
            return 0
        fi
    done

    return 1
}

BASE_PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
SERVICE_PATH="$BASE_PATH"
if [ -n "$FFMPEG_PATH" ]; then
    SERVICE_PATH="${SERVICE_PATH}:${FFMPEG_PATH}"
fi

if [ "$USE_EXE" = "true" ]; then
    # ====== 可执行程序模式 ======
    if [ ! -f "$MAIN_BIN" ]; then
        echo "错误: $MAIN_BIN 不存在，请先确认已打包并放入该目录"
        exit 1
    fi
    if [ ! -x "$MAIN_BIN" ]; then
        chmod +x "$MAIN_BIN"
    fi

    EXEC_START="${MAIN_BIN}"
    CONDA_ENV_DIR=""
    # 打包后的二进制内部不含 watchdog_process.py，无法向 systemd 上报心跳，
    # 因此 --exe 模式不启用 WatchdogSec，仅依赖 Restart=always 守护。
    WATCHDOG_SEC=""
    RUN_MODE_DESC="可执行程序 ${MAIN_BIN}"
else
    # ====== conda 虚拟环境模式 ======
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        echo "错误: $PYTHON_SCRIPT 不存在"
        exit 1
    fi

    if ! CONDA_ENV_DIR="$(find_conda_env "$CONDA_ENV_NAME")"; then
        echo "错误: 未找到 conda 虚拟环境 '${CONDA_ENV_NAME}'"
        echo "已尝试路径: /home/${USER_NAME}/{minianaconda3,miniconda3,anaconda3}/envs/${CONDA_ENV_NAME} 等"
        echo "请确认环境已创建，或手动指定环境路径，例如:"
        echo "  sudo -E CONDA_ENV_PATH=/home/${USER_NAME}/minianaconda3/envs/${CONDA_ENV_NAME} $0"
        exit 1
    fi
    CONDA_PYTHON="${CONDA_ENV_DIR}/bin/python"
    echo "找到 conda 环境: ${CONDA_ENV_DIR}"

    PY_VERSION="$("${CONDA_PYTHON}" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null)"
    if [ -z "$PY_VERSION" ]; then
        echo "错误: ${CONDA_PYTHON} 无法正常运行，请检查该 conda 环境"
        exit 1
    fi
    echo "环境 Python 版本: ${PY_VERSION}"

    # 使用 conda 环境中的 python 运行，并把该环境的 bin 放到 PATH 最前面
    EXEC_START="\"${CONDA_PYTHON}\" -u \"${PYTHON_SCRIPT}\""
    SERVICE_PATH="${CONDA_ENV_DIR}/bin:${SERVICE_PATH}"
    WATCHDOG_SEC="10s"
    RUN_MODE_DESC="conda 环境 ${CONDA_ENV_NAME} (${CONDA_PYTHON})"
fi

# ====== systemd 服务文件 ======
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
# 启动超时（适应慢启动场景）
# TimeoutStartSec=120

StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

Environment=PYTHONUNBUFFERED=1
Environment=PYTHONIOENCODING=utf-8
Environment=LC_ALL=C.UTF-8
Environment=LANG=C.UTF-8
Environment=PATH=${SERVICE_PATH}
EOF

if [ -n "$CONDA_ENV_DIR" ]; then
    cat >> "$SERVICE_FILE" <<EOF
# conda 虚拟环境信息
Environment=CONDA_PREFIX=${CONDA_ENV_DIR}
Environment=CONDA_DEFAULT_ENV=${CONDA_ENV_NAME}
Environment=CONDA_ENV_NAME=${CONDA_ENV_NAME}
EOF
fi

if [ -n "$WATCHDOG_SEC" ]; then
    cat >> "$SERVICE_FILE" <<EOF
# ====== 看门狗配置 ======
WatchdogSec=${WATCHDOG_SEC}
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
systemctl restart "${SERVICE_NAME}.service"

systemctl status "${SERVICE_NAME}.service" --no-pager

echo "服务已安装完成"
echo "运行方式: ${RUN_MODE_DESC}"
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
