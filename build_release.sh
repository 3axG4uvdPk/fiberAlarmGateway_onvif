#!/bin/bash
# ============================================================================
# build_release.sh - 一键打包 fiber-alarm-gateway 发布 tar 包
#
# 功能:
#   1. 使用 pyinstaller main.spec 打包成单文件可执行程序 (dist/fiber)
#   2. 将 config 配置文件夹复制到部署目录
#   3. 附带针对二进制的 systemd 安装脚本 (install_service.sh)
#   4. 将上述内容打成 tar.gz，方便在其他算法盒子上直接部署
#
# 用法:
#   bash build_release.sh            # 版本号默认取当前时间
#   bash build_release.sh 1.2.3      # 指定版本号
#
# 产物:
#   release/fiber-alarm-gateway-<版本>-<架构>.tar.gz
# ============================================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${PROJECT_DIR}"

VERSION="${1:-$(date +%Y%m%d_%H%M%S)}"
ARCH="$(uname -m)"
PKG_NAME="fiber-alarm-gateway-${VERSION}-${ARCH}"
RELEASE_DIR="${PROJECT_DIR}/release"
STAGE_DIR="${RELEASE_DIR}/${PKG_NAME}"
TARBALL="${RELEASE_DIR}/${PKG_NAME}.tar.gz"

echo "=========================================================="
echo " 构建发布包: ${PKG_NAME}"
echo "=========================================================="

# ---------- 0. 检查环境 ----------
if ! command -v python3 >/dev/null 2>&1; then
    echo "错误: 未找到 python3，请先安装 Python 3"
    exit 1
fi
if ! python3 -c "import PyInstaller" >/dev/null 2>&1; then
    echo "错误: 未安装 PyInstaller，请先执行: pip3 install pyinstaller"
    exit 1
fi
if [ ! -f main.spec ]; then
    echo "错误: 未找到 main.spec，请在项目根目录下运行本脚本"
    exit 1
fi

# ---------- 1. PyInstaller 打包单文件 ----------
echo ""
echo "==> [1/4] PyInstaller 打包单文件 (main.spec) ..."
python3 -m PyInstaller main.spec --noconfirm --clean --log-level=INFO

if [ ! -f dist/fiber ]; then
    echo "错误: 打包失败，未生成 dist/fiber"
    exit 1
fi
echo "    打包完成: $(du -h dist/fiber | cut -f1)"

# ---------- 2. 准备部署目录 ----------
echo ""
echo "==> [2/4] 准备部署目录 ..."
rm -rf "${STAGE_DIR}"
mkdir -p "${STAGE_DIR}/config"

cp -f dist/fiber "${STAGE_DIR}/fiber"
chmod +x "${STAGE_DIR}/fiber"

# 复制 config 文件夹（config.json 及配置说明文档）
if [ -d config ]; then
    cp -rf config/. "${STAGE_DIR}/config/"
    echo "    已复制 config 配置文件夹"
fi

# 附带适配二进制的 systemd 安装脚本
if [ -f deploy_install_service.sh ]; then
    cp -f deploy_install_service.sh "${STAGE_DIR}/install_service.sh"
    chmod +x "${STAGE_DIR}/install_service.sh"
    echo "    已附带 install_service.sh 部署脚本"
fi

# ---------- 3. 生成 MD5 校验 ----------
echo ""
echo "==> [3/4] 生成 MD5 校验文件 ..."
(cd "${STAGE_DIR}" && md5sum fiber > fiber.md5)
echo "    已生成 fiber.md5"

# ---------- 4. 打包 tar.gz ----------
echo ""
echo "==> [4/4] 打包 tar.gz ..."
mkdir -p "${RELEASE_DIR}"
rm -f "${TARBALL}"
tar -czf "${TARBALL}" -C "${RELEASE_DIR}" "${PKG_NAME}"
rm -rf "${STAGE_DIR}"

echo ""
echo "=========================================================="
echo " 打包完成: ${TARBALL}"
echo " 大小: $(du -h "${TARBALL}" | cut -f1)"
echo "=========================================================="
echo " 部署方式:"
echo "   tar -xzf ${PKG_NAME}.tar.gz"
echo "   cd ${PKG_NAME}"
echo "   ./fiber                # 直接前台运行"
echo "   sudo bash install_service.sh   # 或安装为 systemd 服务"
echo "=========================================================="
