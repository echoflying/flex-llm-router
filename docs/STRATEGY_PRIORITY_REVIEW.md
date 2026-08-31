# Router 策略优先级审计与修改计划

> 本文是对当前实现的审计和后续修改计划。写入本文时不修改请求调度代码、不重启服务。

## 1. 总原则

Router 的策略不是一组平级开关，而是一个可以被事件抢占的状态机：

> 高优先级事件出现时，低优先级约束让位；低优先级规则仍保留在 Trace 和状态中，条件恢复后才重新生效。

“让位”只作用于当前请求和当前阶段，不代表删除事实，也不代表取消所有仍有价值的上游任务。

## 2. 优先级层次

| 级别 | 规则/事件 | 抢占对象 | 约束 |
|---|---|---|---|
| P0 | 下游断开、进程停止、明确取消 | 全部调度与重试 | 立即停止当前请求，取消或脱离上游任务 |
| P1 | 首活动硬截止、流式最终截止 | 等待、Affinity、Hedge | 必须结束并返回 504，不能无限等待 |
| P2 | 已向下游输出可见 SSE | 中途切换模型 | 保持响应完整性，不拼接另一个模型的输出 |
| P3 | 内容政策限制、已登记协议兼容错误 | Affinity、成本、普通重试 | 首个可见输出前按明确顺序回退；清除不可信 Affinity |
| P4 | RPM/TPM 限流的第一阶段重试 | 普通调度、Affinity | 短暂固定触发错误的 Channel，按指数退避重试 |
| P5 | RPM/TPM 升级为冷却 | 原有选路偏好 | 新请求避开该 Channel；当前请求按恢复策略决定等待或切换 |
| P6 | 连接失败、5xx、空响应、响应/首 SSE 超时 | Affinity、普通调度 | 首输出前允许重试/Hedge，失败后降低或清除 Affinity |
| P7 | Channel 状态筛选 | Scheduler | 禁用、能力不匹配、上下文超限、冷却通道不得选入候选 |
| P8 | Session Affinity | 轮询、成本排序 | 仅是可用候选中的首选提示，不得阻止 P0-P7 |
| P9 | cost-aware、round-robin、tier | — | 只在高优先级规则没有接管时生效 |
| P10 | 统计、日志、UI | — | 只记录，不改变请求结果 |

Affinity 的正式定义是“软约束”：只有成功的 Channel 才能建立或刷新；发生失败、超时、协议错误、政策限制或流式提前中断时，应降低或清除该粘性。

## 3. 事件清单和目标处理

### 3.1 请求进入与候选筛选

1. 解析外部模型名到 Runner/直连 Channel。
2. 按能力、上下文窗口、请求格式筛选。
3. 排除禁用、冷却和本地窗口保护的 Channel。
4. 在剩余候选中先看有效的 Session Affinity，再执行 Runner Scheduler。

未知模型、非法 JSON、缺少 messages 等是本地 400/404，不应进入上游重试。

### 3.2 RPM/TPM 两阶段状态机

RPM 和 TPM 使用独立计数、独立退避和独立冷却原因：

```text
正常
  └─ 收到真实 RPM/TPM
       └─ 第一阶段：短退避重试原 Channel
            ├─ 成功 → 清除临时冷却，刷新成功状态
            └─ 再次失败/超过 retry_policy.max_retries
                 └─ 第二阶段：升级为 Channel 冷却
                      ├─ 新请求避开该 Channel
                      ├─ 当前请求按 on_exhausted=failover/wait/fail
                      └─ 冷却到期或真实请求成功 → 恢复
```

第一阶段优先级高于普通调度，因为它验证的是刚刚产生限流的原 Channel；但它低于下游断开、硬截止、协议错误和内容政策回退。

第二阶段不应继续伪装成普通重试。必须在 Trace 中明确记录：

- `limit_retry_started`：第几次短退避；
- `limit_escalated`：何时升级为冷却、RPM 还是 TPM；
- `limit_cooldown_wait`：预计恢复时间和剩余时间；
- `limit_failover` / `limit_final_failure`：最终切换或失败原因。

### 3.3 其他上游事件

- **协议兼容错误**：只对配置表中明确登记的 Provider/Model/HTTP/文本组合切换；普通 400 不盲目重试。
- **内容政策错误**：识别 `content_filter`、`content_policy_blocked`、`data_inspection_failed` 等明确标记；首个可见输出前按 Runner 内标记通道、再按全局 fallback 顺序处理。
- **连接错误、5xx、空响应**：首个输出前允许按重试策略和 Hedge 接管；产生输出后不切换当前响应。
- **响应对象已返回但没有首个 SSE**：仍属于“无首活动”，必须继续执行 6/9/12 分钟 Hedge/截止计时。
- **首个 SSE 后长时间没有后续 SSE**：重新开始流式空闲计时，按当前 Runner Hedge 阶段处理，最终硬截止。
- **客户端断开**：最高优先级，取消当前流和等待中的 Hedge；不得继续占用上游额度。
- **本地五小时窗口**：这是本地保护计数，不等同于供应商真实配额；原请求验证与其他新请求选路必须分开。
- **相同请求重叠**：目前只观察和统计，不默认去重；后续如启用，必须以明确指纹、时间窗和“已有结果”作为条件，不能丢弃未知结果的在途请求。

## 4. 当前代码对照

| 能力 | 当前实现 | 结论 |
|---|---|---|
| 候选能力/上下文筛选 | `compatibility()` 和 `state.eligible()` | 已实现 |
| Affinity 只在可用候选中优先 | 先筛候选，再命中 `affinity_channel()` | 已实现 |
| 失败时清除 Affinity | 多数异常路径调用 `forget_affinity()` | 基本实现，需补齐所有流式结束路径 |
| RPM/TPM 第一阶段退避 | 独立 `retry_steps`、TPM 4 秒/RPM 8 秒指数序列 | 已实现 |
| RPM/TPM 第二阶段冷却 | 写入 Channel 冷却；支持 `selection.rpm_limit.on_exhausted=failover/wait/fail`，并记录升级事件 | 已实现，待真实流量回归 |
| 内容政策 fallback | Runner 标记优先、全局顺序其次 | 已实现 |
| 协议兼容错误 | `protocol_error_rules` 窄匹配并清除 Affinity | 已实现 |
| 6/9/12 分钟 Hedge | 按 Channel 数量或显式 stages 生成 | 已实现；流式空闲读取使用分离任务，生命周期不依赖 SDK 取消清理 |
| 单 Channel 6 分钟重试/9 分钟截止 | 自动 Hedge 计划 | 已实现 |
| 首 SSE 后空闲 Hedge | 流式消费者内实现 | 已实现；超时立即夺回控制权并分离取消底层读取 |
| 响应对象已返回但消费者未进入 | Hedge 任务先挂到 watchdog handoff，流式消费者进入后接管 | **已修复，待回归验证** |
| 下游断开 | 7800 核心监听并取消/脱离上游任务 | 已实现，需增加竞态测试 |
| 普通 400 | 不在协议表时直接返回 | 已实现 |
| 配额/引擎验证 | 原 Channel 定时验证，不后台空探测 | 已实现，但应与第二阶段冷却统一展示 |
| 配置变更生效 | 保存后需重启核心 | 现行约束，文档需保持明确 |

### 已确认的生命周期缺陷

当 LiteLLM 返回了 response object，但 ASGI 流式消费者尚未真正进入时，旧代码会执行 `watchdog_handoff` 并清空 `on_hedge` 回调。此后 watchdog 只能记录 `watchdog_hedge_due`，不能启动实际 Hedge，最终直接在硬截止返回 504。当前实现已改为在 handoff 间隙先启动并暂存 Hedge 任务，流式消费者进入后接管这些任务；仍需通过真实异步回归测试验证取消竞态。

## 5. 修改计划（先文档，后代码）

### 阶段 A：明确数据模型和策略配置

1. 增加统一的限流阶段状态：`observed → retrying → cooled → recovered/exhausted`。
2. 为 Runner 使用限流升级后的明确策略（已支持），例如：
   - `on_exhausted: failover`：优先业务连续性；
   - `on_exhausted: wait`：优先同模型一致性；
   - `on_exhausted: fail`：直接返回限流错误。
3. 保持 RPM/TPM 独立，不共享计数器和截止时间。

### 阶段 B：修复 watchdog/流式阶段抢占

1. 在收到首个有效 SSE 前，`watchdog_handoff` 不得关闭 Hedge 能力。
2. 只有流式消费者确认已启动，并且首个 SSE 已到达，才停止“无首活动” Hedge。
3. response object 无首 SSE 时，6/9 分钟仍应实际启动后续 Channel。
4. 硬截止必须是最终兜底，不能因回调缺失而提前变成单通道等待。

### 阶段 C：统一错误优先级执行器

1. 将下游断开、硬截止、已输出内容、政策/协议、限流阶段、普通故障、Affinity、Scheduler 编码为显式状态转换。
2. 所有抢占都写入统一 Trace 事件，并记录被抢占的低优先级规则。
3. 防止高优先级切换后，旧的 Affinity、冷却验证或 Hedge 定时器再次把请求拉回原路径。

### 阶段 D：验证和回归

- 首选 Affinity 通道在 6 分钟无首 SSE，必须实际出现第二 Channel 的 `upstream_request`。
- response object 已返回但消费者未进入时，仍能在 Hedge 阶段切换。
- RPM/TPM 第一次错误只短退避；达到次数后出现明确冷却事件。
- `on_exhausted=failover` 能切换，`wait` 不切换，`fail` 返回 429。
- 已输出 SSE 后任何错误都不拼接另一模型响应。
- 下游断开不会留下未标记的活跃 attempt。
- 内容政策和协议错误会清除 Affinity；普通 400 不重试。
- 单 Channel、双 Channel、三 Channel Runner 的截止时间和 Hedge 计划分别正确。

## 6. 本文后的执行顺序

本文完成后，下一步才进入代码修改。代码修改应先实现阶段 A/B，再实现阶段 C，最后补充阶段 D 测试；每个阶段单独提交、静态检查、运行时测试和同步，默认不自动重启核心服务。
