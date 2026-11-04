#!/usr/bin/env bash
# 一键构建并安装 Proxy Pool 到 systemd 守护。
# 用法（在仓库根目录执行）：sudo bash deploy/install.sh
set -euo pipefail

APP_DIR="/opt/proxy-pool"
BIN="proxy-pool"

echo "==> 构建静态二进制（CGO_ENABLED=0）..."
CGO_ENABLED=0 go build -ldflags="-s -w" -o "${BIN}" ./cmd/proxy-pool

echo "==> 安装到 ${APP_DIR} ..."
mkdir -p "${APP_DIR}"
install -m 0755 "${BIN}" "${APP_DIR}/${BIN}"
# 首次部署时拷贝配置（已存在则不覆盖，避免丢失用户已改的配置）
if [ ! -f "${APP_DIR}/config.yaml" ]; then
  cp config.yaml "${APP_DIR}/config.yaml"
fi

echo "==> 安装 systemd 服务 ..."
cp deploy/proxy-pool.service /etc/systemd/system/proxy-pool.service
systemctl daemon-reload
systemctl enable proxy-pool
systemctl restart proxy-pool

echo "==> 完成。常用命令："
echo "    systemctl status proxy-pool       # 查看状态"
echo "    journalctl -u proxy-pool -f       # 跟踪日志"
echo "    systemctl restart proxy-pool      # 重启"
echo "访问入口："
echo "    Web 控制台：http://<服务器IP>:9999"
echo "    代理网关：  http://用户名:密码@<服务器IP>:1024"
