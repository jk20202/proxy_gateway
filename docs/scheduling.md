# 代理调度与负载均衡机制

本服务把来自多个代理平台的 `Provider` 汇总成统一的内存快照池，对外提供"拿一个可用代理"的接口。选路流程在 `internal/pool` 内实现，本文档说明其分层、权重与延迟感知策略。

## 总览

调度分两层：

1. **分组（group）层**：多个命名分组，每组有主池 + 多级备用池，从主池到备用池依次降级。
2. **层内（layer）选路**：在某一层的多个存活代理中，用平滑加权轮询（SWRR）或加权随机选出一个。

未配置 `groups` 时退化为**优先级主备**模式：按 `priority` 降序分组，高优先级存活率不足时降级。

```
取代理请求
  │
  ├─ 有 groups ──► 指定分组(或缺省第一个分组)
  │                 └─► 主池 ─► 备用池1 ─► 备用池2 ...   (存活率不足则降级)
  │                        └─► 层内选路(SWRR / 加权随机)
  │
  └─ 无 groups ──► 优先级主备: 按 priority 降序, ∝ 存活率降级到下一组
```

## 分层降级（group → backups）

每个分组定义：

```yaml
groups:
  - name: "residential"
    primary: ["static-a", "pool-b"]      # 主池 provider
    primary_weights:                     # 主池各 provider 使用率(%)
      static-a: 60
      pool-b: 40
    backups:
      - name: "fallback-paid"
        providers: ["aliyun-tunnel"]
    min_alive_ratio: 0                   # 主池阈值
```

- 当前层的**存活率**（`alive_count / alive_total`）达到该层 `min_alive_ratio` 才使用该层。
- 主池存活率跌破阈值 → 依次尝试备用池；主池恢复后自动切回主池。
- 该组 `min_alive_ratio` 取组内各 provider 配置的最小值。

## 层内选路（SWRR / 加权随机）

某一层的 provider 集合确定后，按该层是否含隧道型代理决定算法：

| 层内代理类型 | 算法 |
|------|------|
| 全为非隧道型（`ip_pool` / `sticky` / `free`） | 平滑加权轮询（SWRR） |
| 含隧道型（`tunnel`） | 加权随机 |

- **SWRR**：`smooth weighted round-robin`，按权重平滑分配，避免短时间连选同一高权重节点，实现按权重精确分流。
- **加权随机**：按 CDF（累积权重）均匀随机采样，权重越大被选概率越高，适合少量隧道型节点。

权重来源：每个代理自身的 `weight`；当分组配置了 `primary_weights` 时，以该 provider 的使用率百分比覆盖其默认权重。

## 延迟感知（latency-aware SWRR）

SWRR 在基础权重之上叠加**延迟因子**，让更快的代理承担更多流量，同时保留每个代理在轮转中以避免单点热点：

```
动态权重 = 基础权重 × factor
factor   = clamp(ref / latency, 0.5, 2.0)
ref      = 该层所有已有延迟样本的代理的平均延迟(ms)
```

- `latency` 为该代理健康检查上报的 `LatencyMS`。
- 平均延迟 `< latency` 的代理 `factor > 1`（更高概率）；平均延迟 `> latency` 的代理 `factor < 1`（更低概率）。
- `factor` 被钳制在 `[0.5, 2.0]`：既不会让慢代理彻底饿死，也不会让单个快代理独占（防止把流量集中到单一节点造成其成为新的瓶颈）。
- `latency = 0`（尚无样本）视为中性，`factor = 1`，行为退化为纯 SWRR。

参考延迟 `ref` 在**每次选路时动态重算**（而非构建快照时固化），以便实时跟随健康检查结果。

## 优先级主备（无 groups）

所有 provider 按 `priority` 降序分为若干组；从最高优先级开始：

1. 该组存活率 ≥ 组内 `min_alive_ratio` → 在组内按权重选一个。
2. 存活率跌破阈值 → 降级到下一优先级组。
3. 存活率恢复后自动切回高优先级组。

- `priority: 0`（默认）：所有 provider 同组，等价纯加权随机。
- `priority: 10` + `min_alive_ratio: 0`：严格主备，主平台只要有 1 个存活就用它。

## 粘性会话（sticky）

`sticky` 型 provider 为同一 `client_id` 在 `sticky_seconds` 内固定同一代理。选路流程：

1. 先查该 `client_id` 是否已有未过期的粘性会话，有则复用。
2. 无会话或过期 → 走正常 `NextInGroup`/`Next` 选路，若选出的代理支持粘性则记录新会话。

## 故障转移与结果反馈

- 健康检查失败 → 代理标记为不可用（移出存活集）。
- `free` 型代理单次失败或延迟超限 → 直接删除（不累积失败）。
- 客户端上报 `POST /api/v1/proxy/{id}/report` 成功/失败，实时调整该代理状态与延迟，从而影响延迟感知权重。

## 多账户权限与调度

调度分组与 Provider 都支持归属控制，普通账户只能使用自己拥有（或全局）的资源：

- **Provider**：`owner` 为空 = 全局（所有账户可见可用）；`owner` 非空 = 归属某账户，默认私有，可开启 `public` 对外共享。
- **分组**：`owner` 为空 = 全局分组（所有账户可用，通常由 admin 创建）；`owner` 非空 = 私有分组，仅创建者可用、可编辑。
- 取代理时按账户校验其可用分组，私有分组只能被 owner 使用；非 owner 越权操作返回 `403`。

以上权限校验在 `internal/server` 完成，调度层本身无账户概念，只按分组名路由。
