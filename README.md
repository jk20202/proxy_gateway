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

## 在线预览（临时部署地址）

当前部署实例（实时可访问）：

- **Web 控制台**：https://8080-6bfeb97dcb0b6b86.monkeycode-ai.online
- **取代理 API 示例**：
  - 免费代理：`GET https://8080-6bfeb97dcb0b6b86.monkeycode-ai.online/api/v1/proxy?group=charlespikachu-free`
  - 需带 `Authorization: Bearer <token>`（默认账户见 `config.yaml` 的 `accounts`）
- 控制台内「API 文档」页签内含全部接口的调用说明与 curl 示例。

> 注：此为开发环境的临时公网预览地址，重启/重建后可能变化，以实际部署为准。

## 快速开始

### 前置要求

- Go 1.21+

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
  check_url: "http://httpbin.org/ip"
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
  - name: "aliyun-tunnel"
    type: "tunnel"
    enabled: true
    weight: 100
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
    weight: 50
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
    weight: 30
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
    weight: 10
    priority: 0
    min_alive_ratio: 0
    check_url: "http://www.gstatic.com/generate_204"   # 免费代理的真实连通性检测目标（204 轻量探测）
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
- **调度算法**：某一层内的 provider 全部为非隧道型（`ip_pool` / `sticky` / `free`）时使用**平滑加权轮询（SWRR）**按权重均匀分配；只要该层含隧道型 provider 则退化为加权随机。
- 同一 provider 可被多个分组引用；未配置 groups 时退化为下面的优先级模式。

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
├── examples/
│   ├── config.yaml        # 配置示例
│   └── mock-env/          # 模拟代理供应商环境（联调用）
├── internal/
│   ├── alert/             # 告警分发（Webhook + 邮件 + provider 状态监控 + 故障自愈）
│   ├── auth/              # 账户鉴权（登录、token、角色、分组权限）
│   ├── config/            # 配置加载与校验
│   ├── health/            # 健康检查器
│   ├── model/             # 核心数据模型（Proxy）
│   ├── pool/              # 代理池（快照、优先级/分组分层调度、SWRR、粘性会话）
│   ├── provider/          # 供应商抽象（隧道型/IP池型/长效静态型）
│   ├── server/            # HTTP API + 内嵌 Web 管理控制台
│   └── store/             # SQLite 异步调用记录（每账户统计）
└── config.yaml            # 运行配置（联调模式）
```

## 自定义 Provider

实现 `internal/provider.Provider` 接口（`Name` / `Kind` / `Weight` / `CheckURL` / `Initial` / `Refresh`），在 `provider.New` 中注册类型，即可接入任意自有代理平台。
