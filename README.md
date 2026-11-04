# Proxy Pool — 多平台代理池网关

高性能多供应商代理池调度服务。统一管理多个代理平台的**动态隧道**、**粘性隧道**、**代理提取链接**与**公开免费代理**，自动健康检查、加权调度、主备故障转移，为爬虫侧提供毫秒级取代理 API，并内置一个现代化的 Web 管理控制台。

## 核心特性

- **多平台统一接入**：一个系统接入任意数量的代理平台，通过 `providers` 配置或 Web 界面即可扩展
- **四类代理源**：
  - `tunnel` 动态隧道型 —— 平台侧自动轮换出口 IP 的固定网关
  - `sticky` 粘性隧道型 —— 粘性会话，时效内同一客户端固定同一 IP
  - `ip_pool` 提取链接型 —— 定期请求一个提取 URL 批量拉取 IP:Port
  - `free` 免费代理源 —— 自动采集公共免费列表，延迟达标才入池、失效即删
- **高性能**：内存快照池 + 无锁读路径（atomic 快照切换），单机可支撑上千爬虫并发
- **自动健康检查**：后台协程定期通过代理探测目标站点，失败自动降权、恢复自动回池
- **失效检测与轮换**：调度分组支持「主池 + 多级备用池」分层，主池存活率跌破阈值自动切换备用池，恢复后自动切回；Provider 掉线后按周期强制刷新自愈
- **平滑加权轮询（SWRR）**：组内池化型 Provider 按权重均匀分配，叠加延迟感知权重（快代理多承担流量）
- **账号密码登录 + 管理员权限体系**：Web 登录换取 token、API 用 Bearer token；admin 可管理全部，user 仅可用被授权的分组/自己的资源；支持开放注册（开关可控）
- **Web 全量配置管理**：Provider、调度分组、账户、告警（Webhook/邮件）、系统设置（注册开关、站点名、主题、健康检查参数与检测 URL）全部在 Web 界面实时修改并持久化，无需改配置文件
- **HTTP 代理网关**：按分组凭证鉴权，`curl -x 用户名:密码@主机:1024` 直连组内代理，主备切换对客户端透明
- **告警通知**：Provider 失效/恢复、池耗尽、刷新失败时经 Webhook（钉钉/飞书/Slack）与 SMTP 邮件通知，内置去重
- **调用统计**：按账户记录调用次数与成功率
- **结果反馈闭环**：爬虫上报成功/失败，实时调整单个代理状态

## 架构

```
                    ┌─────────────────────────────┐
   爬虫端 (1000+)    │        Proxy Pool API        │
 ─────────────────► │  GET /api/v1/proxy           │
   取代理 / 上报结果  │  GET /api/v1/proxies         │
                    │  POST /api/v1/proxy/{id}/report
                    │                             │
                    │   ┌─────────┐  ┌─────────┐  │
                    │   │ 内存快照池 │  │ 加权调度器 │  │
                    │   │(无锁读)  │  │(故障转移) │  │
                    │   └─────────┘  └─────────┘  │
                    │   ┌─────────┐  ┌─────────┐  │
                    │   │ 健康检查  │  │ 刷新协程  │  │
                    │   └─────────┘  └─────────┘  │
                    └─────────────────────────────┘
                          │          │
                    ┌─────▼─────┐ ┌──▼───────────┐
                    │ Tunnel     │ │ IP Pool       │
                    │ Provider   │ │ Provider      │
                    └───────────┘ └───────────────┘
```

## 持久化

重构后**无需 MySQL/Redis**，采用单二进制 + 文件持久化：

| 数据 | 存储位置 |
|------|---------|
| 账户 | `data/accounts.json` |
| Provider 配置 | `data/providers.json` |
| 调度分组 | `data/groups.json` |
| 系统设置 | `data/settings.json` |
| 告警配置 | `alerts.json` |
| 调用统计 | `usage.db`（SQLite） |

`data_dir`（默认 `data`）可在 `config.yaml` 配置；留空则退回纯内存模式。所有运行时修改都会实时写文件、跨重启保留。

## 快速开始

### 本地运行

```bash
go build -o proxy-pool ./cmd/proxy-pool
./proxy-pool -config config.yaml
```

默认端口：Web 控制台 + API `:9999`，代理网关 `:1024`。

### 服务器部署（systemd）

```bash
sudo bash deploy/install.sh
# 或手动：
CGO_ENABLED=0 go build -ldflags="-s -w" -o proxy-pool ./cmd/proxy-pool
sudo mkdir -p /opt/proxy-pool && sudo cp proxy-pool config.yaml /opt/proxy-pool/
sudo cp deploy/proxy-pool.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now proxy-pool
```

## 配置示例

```yaml
server:
  listen: ":9999"            # Web 控制台 + API
  gateway_listen: ":1024"    # HTTP 代理网关

data_dir: "data"

health_check:
  interval_s: 60
  timeout_ms: 6000
  concurrency: 256
  check_urls:
    - { name: "ipapi-cc", url: "http://ip-api.com/line/?fields=countryCode", enabled: true }
    - { name: "ipify",    url: "https://api.ipify.org", enabled: true }
  max_fails: 3

accounts:
  - name: "admin"
    password: "admin123"     # 首次登录后自动加盐哈希
    token: "replace-with-random-token"
    role: "admin"
    enabled: true
    groups: []

settings:
  site_name: "Proxy Pool"
  registration_enabled: false
  theme_default: "dark"

providers: []                 # 在 Web 中添加
groups: []                    # 在 Web 中创建
```

## 接入代理平台

在 Web「代理平台」页点击「添加 Provider」，支持四种类型：

| 类型 | 用途 | 关键字段 |
|------|------|---------|
| `tunnel` | 动态隧道（平台自动轮换出口 IP） | `gateway`/`port`/`scheme`/账号 |
| `sticky` | 粘性隧道（会话内固定 IP） | `extract_url` + `sticky_seconds` |
| `ip_pool` | 提取链接（请求 URL 批量取 IP） | `extract_url`/`format`/刷新间隔 |
| `free` | 爬取公开免费代理 | `feed_url`/延迟阈值 |

## 取代理 API

```bash
# 登录换取 token
curl -X POST http://主机:9999/api/v1/auth/login -H "Content-Type: application/json" \
  -d '{"name":"admin","password":"你的密码"}'

# 取一个代理
curl -H "Authorization: Bearer <token>" "http://主机:9999/api/v1/proxy?group=my-group"

# 粘性获取（会话内固定 IP）
curl -H "Authorization: Bearer <token>" "http://主机:9999/api/v1/proxy?sticky_seconds=300&client_id=spider-1"

# 批量取
curl -H "Authorization: Bearer <token>" "http://主机:9999/api/v1/proxies?count=10"

# 上报结果（反馈闭环）
curl -X POST "http://主机:9999/api/v1/proxy/<id>/report" -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" -d '{"success":true,"latency_ms":120}'
```

### 代理网关直连

```bash
curl -x 用户名:密码@主机:1024 http://目标URL
```

完整 API 文档见 Web 控制台「API 文档」页。

## 目录结构

```
.
├── cmd/proxy-pool/        # 服务入口
├── deploy/                # systemd 单元 + 一键安装脚本
├── examples/
│   └── mock-env/          # 模拟代理供应商环境（联调用）
├── internal/
│   ├── alert/             # 告警分发（Webhook + 邮件 + 监控自愈）
│   ├── auth/              # 账户鉴权（登录、token、角色、密码哈希）
│   ├── config/            # 配置加载与校验
│   ├── gateway/           # HTTP 代理网关
│   ├── geo/               # IP 归属查询
│   ├── health/            # 健康检查器（支持热更新参数）
│   ├── model/             # 核心数据模型
│   ├── persist/           # JSON 文件持久化（账户/分组/Provider）
│   ├── pool/              # 代理池（快照、分层调度、SWRR、粘性会话）
│   ├── provider/          # 供应商抽象（隧道/IP池/粘性/免费源）
│   ├── server/            # HTTP API + 内嵌 Web 控制台 + 系统设置
│   └── store/             # SQLite 异步调用记录
└── config.yaml            # 运行配置
```

## 测试

```bash
go test ./...
```
