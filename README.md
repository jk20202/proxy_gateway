# Proxy Pool

高性能多供应商代理池调度服务。统一管理多个代理平台的隧道型（Tunnel）与 IP 池型（IP Pool）代理，自动健康检查、加权调度与故障转移，为爬虫侧提供毫秒级响应的取代理 API。

## 核心特性

- **多平台统一管理**：一个系统接入多个代理平台的套餐，通过 `providers` 配置即可扩展
- **隧道型 + IP 池型 + 长效静态型 + 免费源四支持**：隧道型代理作为固定通道（平台侧自动轮换出口 IP），IP 池型代理定期从提取 API 拉取 IP 列表，长效静态型（sticky）在粘性时效内为同一客户端固定同一 IP，免费代理源（free）自动采集公共免费列表（延迟低于 `max_speed_ms` 的才收）
- **高性能**：内存快照池 + 无锁读路径（atomic 快照切换），实测单机 6.5 万 QPS、平均 15 微秒/请求，轻松支撑上千爬虫 × 20 并发
- **自动健康检查**：后台协程定期通过代理探测目标站点，失败自动降权、恢复自动回池
- **故障转移**：某个平台异常时自动将其代理移出可用池，爬虫侧无感知地切换到其他平台
- **加权随机调度**：按平台配置权重分配流量，可用作套餐流量调配
- **优先级主备（严格兜底）**：通过 `priority` 指定各平台优先级，高优先级平台存活率跌破 `min_alive_ratio` 阈值（如 0.1 = 90% 失效）才启用低优先级兜底，平时兜底平台零流量；存活率恢复后自动切回
- **调度分组 + 备用池分层**：通过 `groups` 定义命名分组（主池 + 多级备用池），主池存活率跌破组阈值自动依次切换备用池，恢复自动切回；组内全静态/池化型 provider 使用平滑加权轮询（SWRR），含隧道型则加权随机
- **故障自动恢复**：provider 掉线后按 `recover_interval_s` 周期强制刷新尝试自愈，无需人工干预
- **账户鉴权**：配置 `accounts` 后，Web 控制台与消费/管理 API 均需登录（Web 用账号密码换取 token，API 用 `Authorization: Bearer <token>`），每账户独立 token，可限定可用分组
- **调用统计**：消费 API 每次取代理由系统异步记录到 SQLite（不阻塞请求路径），Web 端按账户查看调用次数与成功率
- **Webhook + 邮件告警**：provider 失效/恢复、池耗尽、刷新失败时通过 Webhook（钉钉/飞书/Slack 等）与 SMTP 邮件及时通知，内置去重避免告警风暴
- **结果反馈闭环**：爬虫侧上报成功/失败，实时调整单个代理状态
- **MySQL + Redis 持久化存储**：MySQL 存低频设置（账户/分组/Provider 配置），Redis 缓存高频代理状态（延迟/国家/存活），配置改动能跨重启保留，全部由 `config.yaml` 驱动
- **HTTP 代理网关**：按分组凭证鉴权，`curl -x 用户名:密码@主机:10000` 直连组内代理；纯隧道型分组支持 `/direct` 直连端点，把上游地址下发给客户端、数据流量绕过本机（省本机带宽）
- **一键部署**：`deploy/docker-compose.yml` 一条命令拉起 MySQL + Redis + 应用；也提供 Supervisor 进程守护方式（详见「部署」）

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
                    │ (隧道网关)  │ │ (提取 API)    │
                    └───────────┘ └───────────────┘
```

## 部署

支持三种方式，按需选择：

| 方式 | 适合场景 | MySQL/Redis |
|------|---------|-------------|
| **Docker Compose 一键部署**（推荐） | 新服务器快速上线 | 由 Compose 自动拉起（MySQL + Redis），开箱即用 |
| **Supervisor 进程守护** | 已有 MySQL/Redis，仅需托管应用进程 | 复用你现有的本机/线上数据库 |
| **裸进程运行** | 本地开发调试 | 默认本机 127.0.0.1，可在 `config.yaml` 配置 |

详见下文「[Docker Compose 一键部署](#docker-compose-一键部署)」与「[Supervisor 部署](#supervisor-部署)」。

### Docker Compose 一键部署

`deploy/` 目录提供完整的 Docker 一键部署：`docker compose` 会同时拉起 **MySQL + Redis + 应用** 三个容器，无需手动装数据库。

**前置要求**：服务器已安装 Docker 与 Docker Compose 插件（`docker compose version` 可用）。

**1. 克隆仓库**

```bash
git clone https://github.com/jk20202/proxy_gateway.git
cd proxy_gateway
```

**2. 修改配置（可选）**

编辑 `deploy/config.docker.yaml`，重点确认：

- `accounts`：默认 `admin/admin123`，上线前务必改掉
- `storage.mysql.pass` 与 `deploy/docker-compose.yml` 中 `MYSQL_PASSWORD` 保持一致（默认都为 `proxy_pass`）
- `storage.redis.addr` 与 `storage.mysql.addr` 指向 compose 内网服务名 `redis:6379` / `mysql:3306`（已默认配好）

**3. 一键启动**

```bash
cd deploy
docker compose up -d --build
```

等待镜像构建与数据库初始化（首次约 1-3 分钟）：

```bash
docker compose logs -f proxy-pool   # 观察应用启动日志
```

看到 `mysql storage enabled` 与 `redis storage enabled` 即完成。

**4. 访问**

| 入口 | 地址 |
|------|------|
| Web 管理控制台 | `http://服务器IP:8080` |
| 代理网关（HTTP 代理） | `http://用户名:密码@服务器IP:10000` |

**常用命令**

```bash
docker compose ps                        # 查看三容器状态
docker compose logs -f proxy-pool        # 跟踪应用日志
docker compose restart proxy-pool        # 修改配置后重启应用
docker compose down                      # 停止（数据保留在卷中）
docker compose down -v                   # 停止并清空所有数据（慎用）
```

**转线上数据库（不用本仓库的 MySQL/Redis）**

把 `deploy/config.docker.yaml` 中 `storage` 段改为线上地址，并在 `docker-compose.yml` 中注释掉 `mysql`、`redis` 两个服务即可：

```yaml
storage:
  mysql:
    addr: "你的线上MySQL:3306"     # 如 db.example.com:3306
    user: "proxy"
    pass: "你的密码"
    db: "proxy_pool"
  redis:
    addr: "你的线上Redis:6379"     # 如 redis.example.com:6379
    # password: "你的Redis密码"（如无认证则留空）
    # db: 0
```

> 数据库地址、账号、密码全部由 `config.yaml`（或 `config.docker.yaml`）驱动，无需改代码。

### Supervisor 部署

适用于已有 MySQL/Redis 的服务器：只需用 Supervisor 守护应用进程，数据库由你自行准备（本机或线上均可）。

**1. 安装 Supervisor**

```bash
# Debian / Ubuntu
apt-get update && apt-get install -y supervisor

# CentOS / RHEL
yum install -y supervisor
```

**2. 准备应用**

```bash
mkdir -p /opt/proxy-pool /var/log/proxy-pool
cd /opt/proxy-pool
git clone https://github.com/jk20202/proxy_gateway.git .
go build -o proxy-pool ./cmd/proxy-pool
```

> 也可在本地交叉编译后上传二进制：`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o proxy-pool ./cmd/proxy-pool`

**3. 配置数据库连接**

编辑 `/opt/proxy-pool/config.yaml` 的 `storage` 段，指向你的 MySQL / Redis：

```yaml
storage:
  mysql:
    addr: "127.0.0.1:3306"        # 本机 MySQL；线上则填域名或公网 IP
    user: "proxy"
    pass: "你的密码"
    db: "proxy_pool"
  redis:
    addr: "127.0.0.1:6379"
    # password: "你的Redis密码"
    # db: 0
```

> 本方式不安装 MySQL/Redis：请先在服务器上准备一个 MySQL 库（`CREATE DATABASE proxy_pool`，schema 由应用启动时自动创建）和一个 Redis 实例。

**4. 安装 Supervisor 配置**

`deploy/supervisor/proxy-pool.conf` 为示例配置，安装到 Supervisor：

```bash
mkdir -p /etc/supervisor/conf.d
cp deploy/supervisor/proxy-pool.conf /etc/supervisor/conf.d/proxy-pool.conf

# 重新加载并启动
supervisorctl reread
supervisorctl update
supervisorctl status proxy-pool
```

**5. 常用管理**

```bash
supervisorctl status proxy-pool     # 查看状态
supervisorctl restart proxy-pool    # 重启
supervisorctl stop proxy-pool       # 停止
supervisorctl tail -f proxy-pool    # 跟踪日志
```

### MySQL/Redis 存储配置说明

应用默认开启 MySQL + Redis 持久化存储（在 `config.yaml` 的 `storage` 段配置）：

| 组件 | 存储内容 | 失效影响 |
|------|---------|---------|
| **MySQL** | 账户、调度分组、Provider 配置（低频设置） | 运行时编辑的配置无法持久化，重启回落到 `config.yaml` |
| **Redis** | 代理延迟、国家、存活状态（高频状态） | 代理池重启后需重新健康检查回填，其余功能正常 |

两者均可选：把整个 `storage` 段留空即可退回纯内存模式（与早期版本一致）。MySQL 为权威存储：启动时若数据库已有数据则优先加载，否则用 `config.yaml` 首次播种。账户新增/分组增删/provider 编辑都会实时写库，跨重启保留。


## 快速开始

### 前置要求

- Go 1.25+（见 `go.mod`）
- MySQL 8+ 与 Redis 6+（可选；仅开启持久化存储时使用，默认本机 `127.0.0.1`）

### 构建

```bash
go build -o proxy-pool ./cmd/proxy-pool
```

### 配置

编辑 `config.yaml`：

```yaml
server:
  listen: ":8080"
  read_timeout_ms: 3000
  write_timeout_ms: 3000
  max_workers: 8192

health_check:
  interval_s: 60       # 健康检查周期（秒）：每分钟检测一轮
  timeout_ms: 6000     # 单次探测超时（需测出免费代理 3s 内的真实延迟）
  concurrency: 256     # 并发探测数（免费代理量级大，建议调高）
  check_url: "http://httpbin.org/ip"   # 单 URL 兜底（未配置 check_urls 且 provider 未指定时使用）
  # 5 个固定默认测试 URL（均为轻量 IP 检测服务，响应仅几字节，尽量少占用代理流量）：
  # 新增 Provider 时 Web 界面默认全部勾选，可取消勾选不使用；也可自定义新增多个测试 URL。
  # 任一启用的 URL 可达即判定该代理存活。其中 ip-api.com 会返回代理出口 IP 的国家代码，
  # 检测存活的同时顺带刷新代理国家；若检测网站不返回国家（如纯 IP 端点），则保留该代理
  # 上一轮已测出的国家，避免用空值覆盖。
  check_urls:
    - { name: "ipapi-cc",   url: "http://ip-api.com/line/?fields=countryCode",           enabled: true }
    - { name: "ipapi-json", url: "http://ip-api.com/json/?fields=countryCode,query",     enabled: true }
    - { name: "ipify",      url: "https://api.ipify.org",                                enabled: true }
    - { name: "ipsb",       url: "https://ip.sb",                                        enabled: true }
    - { name: "icanhazip",  url: "https://icanhazip.com",                                enabled: true }
  max_fails: 3          # 连续失败次数，达到后移出可用池（免费代理一次失败即删）
  rebuild_interval_ms: 200

alerts:
  dedup_seconds: 300          # 同一事件去重窗口，避免告警风暴
  monitor_interval_s: 15      # provider 状态监控周期
  webhooks:
    - url: "https://oapi.dingtalk.com/robot/send?access_token=xxx"  # 钉钉/飞书/Slack 均可
      events: ["provider_down", "provider_recovered", "pool_exhausted", "refresh_failed"]  # 留空表示全部
  email:
    smtp_host: "smtp.example.com"
    smtp_port: 587
    username: "alert@example.com"
    password: "your-password"
    from: "alert@example.com"
    to: ["ops@example.com"]
    use_tls: false
    events: ["provider_down", "provider_recovered"]

providers:
  # 隧道型代理：固定网关 + 认证，平台侧自动轮换出口 IP
  # priority=10：高优先级（主）；min_alive_ratio=0.1：存活率跌破 10%（90% 失效）才降级用兜底
  # 调度权重已迁移到分组主池的 primary_weights（使用率百分比），此处不再配置
  - name: "aliyun-tunnel"
    type: "tunnel"
    enabled: true
    priority: 10
    min_alive_ratio: 0.1
    tunnel:
      scheme: "http"
      gateway: "tunnel1.proxy.example.com"
      port: 8080
      username: "user1"
      password: "pass1"

  # IP 池型代理：调用平台提取 API 拿到一批 IP:Port，作为兜底（priority=0 最低）
  - name: "proxy-cn-ip"
    type: "ip_pool"
    enabled: true
    priority: 0
    min_alive_ratio: 0
    ip_pool:
      extract_url: "https://api.proxycn.com/get"
      format: "text"              # text=每行 ip:port；json=常见 JSON 结构
      refresh_interval_s: 60       # 拉取间隔
      expire_after_s: 600          # IP 过期时间
      max_proxies: 500             # 单 provider 最大代理数

  # 长效静态型代理（粘性）：同客户端在 sticky_seconds 时效内固定同一 IP
  - name: "static-us"
    type: "sticky"
    enabled: true
    priority: 0
    sticky_seconds: 300           # 粘性时长（秒）：60=1分钟、300=5分钟，可任意设置
    ip_pool:
      extract_url: "https://api.staticproxy.com/get-static"
      format: "text"
      refresh_interval_s: 300
      expire_after_s: 3600
      max_proxies: 200

  # 免费代理源：采集自公共免费源，只保留上报延迟低于 max_speed_ms 的 HTTP 代理。
  # 不设上限、不过期：只要代理还能用、延迟达标就一直保留，每分钟检测自动淘汰超标者。
  - name: "charlespikachu-free"
    type: "free"
    enabled: true
    priority: 0
    min_alive_ratio: 0
    check_url: "http://www.gstatic.com/generate_204"   # 免费代理的真实连通性检测目标（204 轻量探测）
    # 每个 provider 也可配置多个测试 URL（check_urls，含自定义与全局默认快照，每项 enabled 独立开关）：
    # check_urls:
    #   - { name: "gstatic", url: "http://www.gstatic.com/generate_204", enabled: true }
    #   - { name: "baidu",   url: "https://www.baidu.com/",              enabled: true }
    free:
      feed_url: "https://charlespikachu.github.io/freeproxy/proxies.json"  # 免费代理 JSON 源
      refresh_interval_s: 600     # 每 10 分钟重新采集一次
      max_speed_ms: 3000          # 只收上报延迟低于 3 秒的（源站自己的测速）
      delete_latency_ms: 3000     # 每分钟实测延迟超过 3 秒的直接删除
```

## 调度语义（分组/优先级 + 权重）

### 显式分组（推荐）

配置 `groups` 后，消费 API 可用 `?group=NAME` 指定分组；不指定 group 时用第一个分组。

```
groups:
  - name: "residential"          # 分组名（消费 API 用 ?group=residential 指定）
    type: "static"               # 任意标签，仅用于展示
    min_alive_ratio: 0.5         # 主池存活率阈值：低于 50% 才切备用池
    primary: ["static-a", "pool-b"]          # 主池引用的 provider 名
    backups:                     # 备用池（有序，首个可用即停，不再看后面的层）
      - name: "tunnel-fallback"
        min_alive_ratio: 0
        providers: ["tunnel-c"]
      - name: "last-resort"
        min_alive_ratio: 0
        providers: ["pool-d"]
```

- 每个分组 = 主池（`primary`）+ 按顺序的多级备用池（`backups`）。
- 只有当前层的存活率 ≥ 该层 `min_alive_ratio` 时才使用该层；否则依次尝试下一层。
- 主池恢复后自动切回主池。
- **调度算法**：某一层内的 provider 全部为非隧道型（`ip_pool` / `sticky` / `free`）时使用**平滑加权轮询（SWRR）**按权重均匀分配；只要该层含隧道型 provider 则退化为加权随机。SWRR 额外叠加**延迟感知权重**（快代理承担更多流量，配额钳制在 0.5x~2x 避免单点热点），无延迟样本时退化为纯 SWRR。完整调度机制见 [`docs/scheduling.md`](docs/scheduling.md)。
- 同一 provider 可被多个分组引用；未配置 groups 时退化为下面的优先级模式。

**主池权重 = 使用率百分比**（`primary_weights`）

`primary_weights` 表示主池各 provider 的负载占比（使用率），勾选项总和恒为 100%：

```
groups:
  - name: "residential"
    primary: ["static-a", "pool-b", "tunnel-c"]
    primary_weights:
      static-a: 60        # 60% 流量
      pool-b: 20          # 20%
      tunnel-c: 20        # 20%
```

- Web 控制台编辑分组时自动维护：手动指定某项后锁定，其余自动均分剩余占比（保留整数），总和保持 100%。
- 未指定 `primary_weights` 时组内 provider 等权均分。
- 为全部主池 provider 指定权重时，后端校验总和必须为 100。

### 免费代理分组（free）

`type: "free"` 的 provider 自动采集公共免费代理源（`free.feed_url`，默认 `https://charlespikachu.github.io/freeproxy/proxies.json`），语义与付费代理不同：

- **延迟达标才入池**：采集时只保留上报延迟低于 `free.max_speed_ms`（默认 3000ms / 3 秒，取源站自己的测速）的 HTTP/HTTPS 代理，SOCKS-only 与超速代理直接丢弃。
- **不设上限、不过期**：不配置 `max_proxies` / `expire_after_s`（或设为 0），有多少收多少；只要代理还能用、延迟达标，不管何时入池都一直保留，不做按时间的自动淘汰。
- **每分钟检测、超标即删**：健康检测默认每 60 秒跑一轮，免费代理实测延迟超过 `free.delete_latency_ms`（默认 3000ms / 3 秒）或一次检测失败，直接从池中删除（不累积失败次数），下一轮采集（`free.refresh_interval_s`，默认 10 分钟）再补充新代理，实现"测一批删一批"的自维护轮换。
- **分组命名**：按"域名相关 + free"命名（如 `charlespikachu-free`），后续接入更多免费源时同样追加 `xxx-free` 分组。
- **同样支持兜底**：免费分组与其他分组一样，可在 `backups` 配置备用池，无可用时自动切换付费/隧道代理兜底。示例：

```
groups:
  - name: "charlespikachu-free"
    type: "free"
    min_alive_ratio: 0
    primary: ["charlespikachu-free"]
    backups:
      - name: "fallback-paid"
        min_alive_ratio: 0
        providers: ["aliyun-tunnel"]   # 免费代理全挂时兜底到付费隧道
```

### 优先级主备（无 groups 时）

1. 所有 provider 按 `priority` 降序分组，从最高优先级组开始。
2. 若该组存活代理比例 ≥ 该组 `min_alive_ratio`（取组内 provider 的最小值），则在该组内按 `weight` 加权随机返回。
3. 若存活率跌破阈值，降级到下一优先级组，直到有可用的组。
4. 存活率恢复后自动切回高优先级组。

| 配置 | 含义 |
|------|------|
| `priority: 0`（默认） | 所有 provider 同一组，行为等同于纯加权随机 |
| `priority: 10` + `min_alive_ratio: 0` | 严格主备：只要主平台还有 1 个存活代理就用它，兜底零流量 |
| `priority: 10` + `min_alive_ratio: 0.1` | 主平台 90% 代理失效才启用兜底，恢复后自动切回 |
| `type: sticky` + `sticky_seconds: 60` | 长效静态 IP：同客户端在 60 秒时效内固定同一 IP，到期轮换 |

### 运行

```bash
./proxy-pool -config config.yaml
```

### 网关直连（省流量）

HTTP 代理网关（`curl -x 用户名:密码@域名:10000 <URL>`）默认由本机中转所有数据流量，会消耗本机带宽。对**纯隧道型分组**（组内所有 provider 均为 `type: tunnel` 且指向同一上游网关），本机无需中转，可直接把上游隧道地址下发给客户端，让数据流量直达隧道服务商、完全绕过本机：

```bash
# 先向网关请求直连地址（复用分组凭证鉴权）
curl -s -x 用户名:密码@域名:10000 http://域名:10000/direct
# 返回示例：{"direct":"http://upuser:uppass@tunnel.example.com:3128"}
# 混合型/非隧道型分组返回 {"direct":null,"reason":"..."}

# 然后改用上游地址直连，流量不再经过本机
curl -x "http://upuser:uppass@tunnel.example.com:3128" <目标URL>
```

- 混合分组（含 IP 池/免费/静态代理）无法直连，`/direct` 会返回 `direct:null` 并说明原因，仍需中转。
- 直连模式不改变现有网关行为：普通 `curl -x 域名:10000` 仍走本机中转。

## API 文档

### 鉴权

配置了 `accounts` 后，所有消费与管理 API 都需要 token，两种方式任选：

```
Authorization: Bearer <token>
X-Api-Token: <token>
```

未配置 accounts（留空）时鉴权关闭，所有接口匿名可访问。

Web 控制台登录接口（换取 token，匿名可用）：

```
POST /api/v1/auth/login
{"name": "admin", "password": "admin123"}
```

响应：`{"ok": true, "token": "...", "name": "...", "role": "admin", "admin": true}`

### 获取单个代理

```
GET /api/v1/proxy?group=residential
GET /api/v1/proxy?group=charlespikachu-free    # 免费代理分组
```

- `group`（可选）：指定调度分组名；缺省用第一个分组。

响应：

```json
{
  "ok": true,
  "proxy": {
    "id": "mock-tunnel:127.0.0.1:10001",
    "scheme": "http",
    "host": "127.0.0.1",
    "port": 10001,
    "username": "",
    "password": "",
    "provider": "mock-tunnel",
    "type": "tunnel",
    "addr": "127.0.0.1:10001"
  }
}
```

无可用代理时返回 `503 {"error":"no available proxy"}`。

### 批量获取

```
GET /api/v1/proxies?count=10&group=residential
```

返回去重后的多个代理，`count` 上限 100。

### 粘性获取（长效静态 IP）

```
GET /api/v1/proxy?sticky_seconds=300&client_id=your-crawler-id
```

- 携带 `client_id` + `sticky_seconds` 时，系统在粘性时效内为该客户端固定返回**同一个代理**（IP 不变）
- 时效到期后再请求会重新分配（可能换 IP）；时效内其他客户端不受影响
- `sticky_seconds` 支持任意时长：60（1 分钟）、300（5 分钟）、1800（30 分钟）等
- 适合需要"会话内保持同一出口 IP"的场景（登录态保持、风控规避等）
- 粘性代理失效时自动重新分配一个可用代理，不中断

### 告警通知

当以下事件发生时，通过配置的 Webhook 与邮件通知：

| 事件 | 触发条件 |
|------|---------|
| `provider_down` | provider 存活率跌破其 `min_alive_ratio`（如 90% 代理失效）或全部失效 |
| `provider_recovered` | provider 存活率恢复至阈值以上 |
| `pool_exhausted` | 取代理时无任何可用代理 |
| `refresh_failed` | provider 提取/刷新接口调用失败 |

Webhook 为 HTTP POST JSON：

```json
{
  "type": "provider_down",
  "provider": "aliyun-tunnel",
  "message": "provider aliyun-tunnel is down (alive 0/10)",
  "data": {"alive": 0, "total": 10, "min_alive_ratio": 0.1},
  "timestamp": "2026-08-04T15:47:57.214Z"
}
```

- 内置去重：同一事件在 `dedup_seconds`（默认 300s）内只发送一次，避免告警风暴
- `provider_recovered` 会重置 `provider_down` 的去重记录，使新一轮故障能再次告警
- 邮件经 SMTP 发送（支持 TLS/STARTTLS），`use_tls: true` 用于 465 端口直连 TLS

### 上报使用结果（反馈闭环）

```
POST /api/v1/proxy/{id}/report
Content-Type: application/json

{"success": true, "latency_ms": 320}
```

- `success: true` 重置失败计数，恢复正常
- `success: false` 失败计数 +1，连续失败达 `max_fails` 后移出可用池

通用上报接口（二选一即可）：

```
POST /api/v1/feedback
{"id": "...", "success": true, "latency_ms": 320}
```

### 状态查询

```
GET /api/v1/status
```

响应：

```json
{
  "ok": true,
  "total": 11,
  "alive": 6,
  "by_provider": {
    "providers": [
      {"provider": "mock-tunnel", "type": "tunnel", "total": 1, "alive": 1, "enabled": true, "weight": 100, "priority": 10, "min_alive_ratio": 0.1},
      {"provider": "mock-ippool", "type": "ip_pool", "total": 10, "alive": 5, "enabled": true, "weight": 50, "priority": 0, "min_alive_ratio": 0}
    ]
  },
  "groups": [
    {"group": "residential", "type": "static", "tiers": [
      {"name": "primary", "alive_total": 3, "alive_count": 2, "min_alive_ratio": 0.5, "usable": true},
      {"name": "tunnel-fallback", "alive_total": 1, "alive_count": 1, "min_alive_ratio": 0, "usable": true}
    ]}
  ]
}
```

### 健康检查

```
GET /healthz
```

## Web 管理控制台

服务内置管理界面，浏览器访问 `http://<host>:<port>/` 即可：

- 全局指标（代理总数 / 存活 / 启用 Provider 数）
- Provider 管理：启用/停用开关、权重与优先级实时调整、手动刷新、删除、动态新增（隧道/IP 池字段按类型切换）
- 代理明细：搜索、分页、状态/延迟/权重展示、手动删除
- 调度分组：查看每个分组各层级的存活率、阈值与可用状态
- 账户管理：新增/删除账户、查看每账户 token 与可用分组（admin 可操作）
- 调用统计：按账户查看调用次数、成功数、成功率
- 手动健康检查按钮
- 配置 `accounts` 后首次打开需登录（Web 登录换取 token，存 localStorage 用于后续请求）

## 管理 API

```
GET    /api/v1/admin/providers                       # 列出所有 provider
POST   /api/v1/admin/providers                       # 新增 provider（body 为 ProviderCfg JSON）
DELETE /api/v1/admin/providers?name=xxx              # 删除 provider 及其代理
POST   /api/v1/admin/providers/{name}/enable         # 启用
POST   /api/v1/admin/providers/{name}/disable        # 停用（清空其代理）
POST   /api/v1/admin/providers/{name}/weight         # body {"weight": N}
POST   /api/v1/admin/providers/{name}/priority       # body {"priority": N, "min_alive_ratio": 0.1}
POST   /api/v1/admin/providers/{name}/refresh        # 手动刷新该 provider
GET    /api/v1/admin/proxies                         # 列出全部代理明细
DELETE /api/v1/admin/proxies/{id}                    # 删除单个代理
POST   /api/v1/admin/health/check                    # 手动触发一轮健康检查
GET    /api/v1/admin/groups                          # 查看各调度分组各层级状态
GET    /api/v1/admin/accounts                        # 列出账户（密码不回显）
POST   /api/v1/admin/accounts                        # 新增账户，body {"name","password","token","role","enabled","groups"}
DELETE /api/v1/admin/accounts?name=xxx               # 删除账户
GET    /api/v1/admin/usage                           # 按账户统计调用次数；?account=xxx 只看单个账户
GET    /api/v1/admin/alerts                          # 查看当前告警配置（webhooks/email/dedup/监控间隔/恢复间隔）
POST   /api/v1/admin/alerts/webhooks                 # 添加 webhook，body {"url": "...", "events": ["provider_down"]}
DELETE /api/v1/admin/alerts/webhooks?url=xxx         # 删除指定 webhook
POST   /api/v1/admin/alerts/email                    # 更新邮件配置，body 为 EmailConfig；password 留空保留原值
POST   /api/v1/admin/alerts/dedup                    # 更新去重窗口，body {"dedup_seconds": 300}
POST   /api/v1/admin/alerts/monitor                  # 更新监控间隔，body {"monitor_interval_s": 15}
POST   /api/v1/admin/alerts/recover                  # 更新恢复自检间隔，body {"recover_interval_s": 3600}
```

> 管理 API 需 admin 角色 token（配置 accounts 后）；Provider 列表会返回完整配置（含密码等字段），请勿暴露到公网。

### 告警配置持久化

Web 控制台的「告警配置」页可以实时增删改 Webhook 与邮件配置，修改后立即生效并写入 `-alerts-file`（默认 `alerts.json`）。启动时如该文件存在，将覆盖 `config.yaml` 中的 `alerts` 段，实现"页面改、重启不丢"。

## 爬虫侧接入示例

```python
import requests

API = "http://127.0.0.1:8080"
# 配置 accounts 后需要携带 token（管理后台/配置文件中获取）
TOKEN = "replace-with-your-token"  # 留空则无需鉴权
HEADERS = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}

# 1. 获取代理（可用 ?group=residential 指定调度分组）
resp = requests.get(f"{API}/api/v1/proxy", headers=HEADERS, timeout=1)
data = resp.json()
proxy = data["proxy"]

proxies = {
    "http": f"{proxy['scheme']}://{proxy['addr']}",
    "https": f"{proxy['scheme']}://{proxy['addr']}",
}
if proxy.get("username"):
    proxies["http"] = f"{proxy['scheme']}://{proxy['username']}:{proxy['password']}@{proxy['addr']}"

# 2. 通过代理请求目标
try:
    r = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=10)
    # 上报成功
    requests.post(f"{API}/api/v1/proxy/{proxy['id']}/report",
                  json={"success": True, "latency_ms": int(r.elapsed.total_seconds() * 1000)},
                  headers=HEADERS)
except Exception as e:
    # 上报失败，调度器会降低该代理权重并触发故障转移
    requests.post(f"{API}/api/v1/proxy/{proxy['id']}/report",
                  json={"success": False}, headers=HEADERS)
    # 失败后重新取一个代理重试
    resp = requests.get(f"{API}/api/v1/proxy", headers=HEADERS, timeout=1)
```

## 性能

| 测试项 | 结果 |
|--------|------|
| 内存快照读（handler 直调） | 41.5 万 rps，平均 2.4 微秒/请求 |
| 真实 HTTP 端到端 | 6.5 万 rps，平均 15.4 微秒/请求 |

## 多平台对冲策略落地对照

| 场景 | 系统行为 |
|------|---------|
| 某平台代理 IP 全被用过 | 该平台 IP 池节点健康检查持续失败，自动移出可用池 |
| 某平台突发异常无响应 | 健康检查将其代理降级，爬虫自动取其他平台代理 |
| 主平台 90% 代理失效 | `min_alive_ratio` 触发降级，自动启用低优先级兜底平台，恢复后切回 |
| 主池存活率跌破组阈值 | `groups` 分层调度启用第一个可用备用池，恢复后自动切回主池 |
| 某平台套餐到期/流量用完 | 配置中 `enabled: false` 停用，或该平台代理持续失败自动降级 |
| 新增一个平台 | 在 `providers` 增加一项配置，重启服务即可 |

## 端到端联调

`examples/mock-env/` 提供了一个模拟代理供应商环境（隧道网关 + IP 提取 API + 目标站点 + 故障注入管理接口），用于本地验证故障转移与恢复：

```bash
# 终端 1：启动模拟环境
go run ./examples/mock-env/ -gateway-port 10001 -target-port 10002 -api-port 10003 -admin-port 10004

# 终端 2：启动代理池（使用根目录 config.yaml，已指向 mock 环境）
# 如需启用鉴权，先在 config.yaml 的 accounts 段配置账户
go run ./cmd/proxy-pool -config config.yaml -alerts-file alerts.json

# 模拟某平台故障
curl http://127.0.0.1:10004/gateway/fail
# 恢复
curl http://127.0.0.1:10004/gateway/recover

# 观察状态（含分组层级）
curl http://127.0.0.1:8080/api/v1/status
# 取代理（指定分组）
curl -H "Authorization: Bearer <token>" "http://127.0.0.1:8080/api/v1/proxy?group=residential"
# 浏览器打开管理台（配置 accounts 后需登录）
# http://127.0.0.1:8080/
```

## 测试

```bash
go test ./...
```

## 目录结构

```
.
├── cmd/proxy-pool/        # 服务入口
├── deploy/
│   ├── docker-compose.yml # Docker 一键部署（MySQL + Redis + 应用）
│   ├── config.docker.yaml # Docker 部署用配置（数据库地址指向 compose 内网服务名）
│   └── supervisor/        # Supervisor 进程守护示例配置
├── Dockerfile             # 多阶段构建镜像
├── examples/
│   ├── config.yaml        # 配置示例
│   └── mock-env/          # 模拟代理供应商环境（联调用）
├── internal/
│   ├── alert/             # 告警分发（Webhook + 邮件 + provider 状态监控 + 故障自愈）
│   ├── auth/              # 账户鉴权（登录、token、角色、分组权限）
│   ├── config/            # 配置加载与校验
│   ├── gateway/           # HTTP 代理网关（按分组中转/隧道直连）
│   ├── health/            # 健康检查器
│   ├── model/             # 核心数据模型（Proxy）
│   ├── persist/           # MySQL/Redis 持久化存储（账户/分组/provider + 代理状态）
│   ├── pool/              # 代理池（快照、优先级/分组分层调度、SWRR、粘性会话）
│   ├── provider/          # 供应商抽象（隧道型/IP池型/长效静态型/免费源）
│   ├── server/            # HTTP API + 内嵌 Web 管理控制台
│   └── store/             # SQLite 异步调用记录（每账户统计）
└── config.yaml            # 运行配置（联调模式）
```

## 自定义 Provider

实现 `internal/provider.Provider` 接口（`Name` / `Kind` / `Weight` / `CheckURL` / `Initial` / `Refresh`），在 `provider.New` 中注册类型，即可接入任意自有代理平台。

创建代理对象时，把该 provider 配置中的启用测试 URL 一并写入 `model.Proxy.CheckURLs`（`cfg.EnabledCheckURLs()`），健康检测会对全部启用 URL 并行探测，任一可达即判定存活；未配置时回退到单 `CheckURL` 或全局默认 `health_check.check_urls`。
