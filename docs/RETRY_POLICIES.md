# Flex LLM Router 重试、退让与超时策略

本文是所有请求恢复策略的唯一现行说明。架构和数据模型见
[`ARCHITECTURE.md`](ARCHITECTURE.md)。策略按单个请求执行；其他新请求仍可根据
Channel 状态选择同一 Runner 的备用通道。

## 1. 优先级等级

策略不是一组平级开关，而是一个可以被事件抢占的状态机：高优先级事件出现时，
低优先级约束让位；被让位的规则仍保留在 Trace 和状态中，条件恢复后才重新生效。
“让位”只作用于当前请求和当前阶段，不代表删除事实或取消所有仍有价值的上游任务。

| 级别 | 规则/事件 | 可抢占对象 | 处理约束 |
|---|---|---|---|
| P0 | 下游断开、明确取消、核心停止 | 全部调度与重试 | 立即停止当前请求，取消或脱离上游任务 |
| P1 | 首活动硬截止、流式最终截止 | 等待、Affinity、Hedge | 必须结束并返回 504/终止 SSE，不能无限等待 |
| P2 | 已向下游输出有效 SSE | 中途切换模型 | 保持响应完整性，不拼接另一模型的输出 |
| P3 | 内容政策限制、协议兼容错误 | Affinity、成本、普通重试 | 首个可见输出前按明确顺序回退；清除不可信粘性 |
| P4 | RPM/TPM 第一阶段真实限流 | 普通调度、Affinity | 固定触发错误的原 Channel，按指数退避重试 |
| P5 | RPM/TPM 升级为 Channel 冷却 | 原有选路偏好 | 新请求避开冷却通道；当前请求按 `on_exhausted` 决定等待/切换/失败 |
| P6 | 连接失败、5xx、空响应、响应/首 SSE 超时 | Affinity、普通调度 | 首输出前允许重试/Hedge，失败后降低或清除粘性 |
| P7 | Channel 状态筛选 | Scheduler | 禁用、能力不匹配、上下文超限、冷却通道不得进入候选 |
| P8 | Session Affinity | 轮询、成本排序 | 仅是可用候选中的首选提示，不得阻止 P0–P7 |
| P9 | `cost_aware`、`round_robin`、tier | — | 只在高优先级规则没有接管时生效 |
| P10 | 统计、日志、UI | — | 只记录，不改变请求结果 |

Affinity 始终是软约束。只有成功的 Channel 才能建立或刷新；发生失败、超时、协议
错误、政策限制或流式提前中断时，应降低或清除该粘性。切换后首个 SSE 只建立临时
粘性，完整成功后才确认最终粘性。

## 2. 本地拒绝与候选筛选

未知外部模型返回 404；缺少 `messages`、非法 JSON 或不支持的本地参数返回 400。
Channel 先按流式、工具、JSON、上下文窗口和启用/冷却状态筛选。没有合格 Channel
时等待最早可恢复状态，超过对应等待上限返回 503 `no_eligible_channel`。

## 3. RPM / TPM 限流

只有真实上游限流才触发退避，不预先主动占用额度。RPM 与 TPM 独立计数、独立退避、
独立冷却原因：

- TPM：4、8、16、32 秒……；
- RPM：8、16、32、64 秒……；
- 累计等待分别受 `FLEX_QUEUE_TPM`、`FLEX_QUEUE_RPM` 限制。

第一阶段固定触发错误的原 Channel，Trace 写 `limit_retry_started`。达到
`retry_policy.max_retries` 后升级为 Channel 冷却并写 `limit_escalated`；此时新请求
避开该 Channel。`cost_aware` 可按 `selection.rpm_limit.on_exhausted=failover` 切换
下一 tier；`wait` 继续等待原 Channel；`fail` 直接返回 429。其它策略默认等待。
冷却等待写 `limit_cooldown_wait`，最终切换/失败写 `limit_failover` 或
`limit_final_failure`。普通 RPM/TPM 不因 Affinity 而永久固定在故障 Channel。

## 4. 配额、引擎与协议兼容

- `allocated quota exceeded`：原请求固定原 Channel，按 1、2、4 分钟验证，之后每
  10 分钟复验；没有通用健康检查。若该 Channel 进入配额冷却且回切探测开启，后台
  恢复循环可按 §10 发一次最小验证请求。其它新请求可选备用。
- `engine is not available temporarily`（兼容常见拼写）：原 Channel 按 15、45、
  120 秒验证，之后每 5 分钟复验；其它 HTTP 400 不进入该流程。
- 协议兼容错误（例如明确登记的 `reasoning_content` 400）：首个输出前清除 Affinity，
  按规则切换兼容 Channel，并记录协议错误统计；普通 400、鉴权和模型不存在不重试。

## 5. CHN Content Policy fallback

仅识别明确的 `content_filter`、`content_policy_blocked`、`data_inspection_failed` 等
上游信号。首个可见输出前，先尝试当前 Runner 中按顺序标记
`chn_content_policy_fallback: true` 的其它 Channel；没有可用标记通道时，再按
`global_fallback.chn_content_policy` 的 Channel ID 顺序尝试（通常第一项为
`agnes-flash`）。已输出 SSE 后不切换；全部兜底仍被阻断时返回
`content_policy_blocked`。普通拒答文本没有明确政策信号时不触发。

## 6. 无首活动 Hedge

核心 7800 watchdog 独立每秒观察响应对象或有效 SSE 活动，按 Runner Channel 数量
生成副本计划：

| Channel 数量 | 时间线 | 首活动硬截止 |
|---|---|---|
| 3 个或更多 | T=0 原请求；T=6 分钟第二 Channel；T=9 分钟第三 Channel | T=12 分钟 |
| 2 个 | T=0 原请求；T=6 分钟第二 Channel | T=9 分钟 |
| 1 个 | T=0 原请求；T=6 分钟同一 Channel 新连接 | T=9 分钟 |

每次尝试另有默认 180 秒“响应对象/首 SSE”安全边界，超时推进下一阶段。最先产生
有效响应活动的尝试获胜，其他副本取消或脱离；迟到结果不再转发。`watchdog_handoff`
不能吞掉 Hedge 能力：response object 已返回但流式消费者尚未进入时，副本仍会被
启动并在消费者进入后接管。

## 7. 流式空闲与下游断开

首个有效 SSE 到达后重新初始化 6/3/3 分钟空闲时钟：连续 6 分钟无后续有效 SSE
启动下一阶段，之后 3 分钟再启动下一阶段，再过 3 分钟硬终止。空事件、心跳和仅
元数据事件不刷新计时；每个有效正文、reasoning、tool-call 或协议事件都会重置。
SDK 取消清理不能阻塞 watchdog；必要时底层读取任务会被分离回收。

HTTP disconnect 是标准 OpenAI 协议下可靠的取消信号：首活动前记录
`client_disconnected_before_first_token`，流式期间记录 `client_disconnected`/
`cancelled`，并取消全部 Hedge。核心硬截止在未发响应头时返回 HTTP 504，已进入 SSE
时发送终止 SSE 错误事件，不能只把内部 Trace 留在 `running`。

## 8. 追踪与验证

每次尝试都会记录 `upstream_task_started`、`upstream_response_received`、
`upstream_first_sse`、`upstream_error_received`、等待、冷却、切换、取消和最终结果。
RPM/TPM、协议、政策、Hedge 和下游断开都必须在 Trace 中可区分。不做与请求无关的
通用健康检查；仅对已进入配额/五小时冷却且满足回切条件的通道执行 §10 所述恢复探测。
Channel 自检和 Responses 检测只有管理员显式点击时执行。

## 9. 参数参考：Channel 级别

### `retry_policy`

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `max_retries` | `3` | 初始请求之外，当前 Channel 允许的普通重试次数；`0` 表示不重试。RPM/TPM 达到该值后进入第二阶段处理。 |
| `backoff.base_seconds` | `5` | 普通连接/超时/5xx 重试的首个等待秒数。 |
| `backoff.max_seconds` | `60` | 普通重试单次等待的上限。 |
| `backoff.exponential` | `true` | 是否按 `base × 2^n` 增长；关闭时每次使用 base。 |
| `retry_on` | `rpm_limit, connection_error, timeout, server_error` | 普通重试错误类型。`tpm_limit`、配额和引擎不可用由专门状态机处理，不依赖此列表。 |

普通重试的等待公式为：

```text
wait_n = min(base_seconds × (2^n if exponential else 1), max_seconds)
```

其中 `n=0` 表示第一次重试。`max_retries` 是“重试次数”而不是历史调用总数；
每次切换到另一个 Channel 后会重新计算该 Channel 的预算。

### `limits`

| 参数 | 默认值 | 作用与注意 |
|---|---:|---|
| `rpm` | `null` | 本地每分钟请求保护闸门；不是 Provider 宣布的真实上限。 |
| `tpm` | `null` | 本地每分钟 Token 保护闸门；未知时保持 `null`，由真实错误学习。 |
| `local_cooldown_seconds` | `300` | 本地保护触发后的最短冷却时间。 |
| `window_seconds` | `18000` | 本地五小时滑动窗口长度，单位秒。 |
| `max_requests_per_window` | `null` | 五小时窗口本地最大调用数；达到后按配额验证流程，不代表账户余额。 |
| `quota_cooldown_seconds` | `3600` | 确认总量配额异常时的 Channel 冷却参考值。 |
| `busy_threshold` | `3` | `busy_window_minutes` 内达到多少次瞬时限流/忙错误后进入 busy 状态。 |
| `busy_window_minutes` | `5` | busy 计数的滑动观察窗口。 |
| `busy_cooldown_seconds` | `300` | busy 状态对新请求的短冷却参考值。 |

`rpm`/`tpm` 只影响本地选路保护；真正收到上游 429 后，Router 还会保存窗口内
请求数、Token、错误类型和恢复结果，用于逐步逼近安全值。旧数据库中的
`rate_limit` 仅在读取时规范化为 `rpm_limit`。

## 10. 参数参考：Runner 级别

### `selection`

| 参数 | 可选值/默认 | 说明 |
|---|---|---|
| `strategy` | `cost_aware` | `round_robin` 按顺序轮转；`cost_aware` 按 tier 从低到高；`quota_paced_priority` 结合五小时窗口节奏。 |
| `tiers` | 每个 Channel 必须有整数 | 写在 Runner 上，数字越小越优先；可以有 `0,0,1,2`，不绑定模型名称。 |
| `fallback.order` | `cost_ascending` | 当前调度按 tier 升序寻找下一个可用 Channel。 |
| `fallback.trigger` | `quota_exhausted,busy_persistent,failure` | 允许触发下切的事件集合；普通参数/鉴权错误不会因该字段被重试。 |
| `fallback.max_fallback_tiers` | `2` | 记录允许的 tier 深度；实际候选还必须通过能力、上下文、启用和冷却筛选。 |
| `retry_next_channel_on` | `[]` | 旧配置兼容字段；只有显式包含错误类型时才允许普通非流式请求切换。新配置优先使用 `fallback.trigger`。 |
| `rpm_limit.on_exhausted` | cost-aware=`failover`，其它=`wait` | RPM/TPM 第一阶段预算耗尽后的动作：`failover` 切换、`wait` 固定原 Channel、`fail` 返回 429。字段名保留 rpm_limit，实际同时作用于 TPM。 |

### `selection.fallback.reattach`

该组参数控制已冷却 Channel 的恢复探测：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `probe_before_switch_back` | `true` | 是否在回切前发送一次最小真实请求验证。 |
| `probe_cooldown_seconds` | `600` | 普通配额/故障探测失败后的最小再次探测间隔。 |
| `quiet_window_seconds` | `1200` | busy/静默恢复的参考窗口，供状态记录和回切判断使用。 |
| `quota_recover_seconds` | `3600` | 配额恢复的参考时间；真实恢复仍以请求或探测结果为准。 |
| `failure_retry_after` | `300` | 普通故障再次尝试的参考间隔。 |

恢复探测不是通用健康检查：后台循环默认每 `FLEX_PROBE_INTERVAL=120` 秒运行，
只针对 `quota_exhausted` 或本地五小时窗口冷却项；五小时项探测间隔固定为 1800 秒，
并且若原请求正在验证则跳过，避免重复消耗。RPM/TPM 冷却不会被后台空探测撞击，
通常在下一次真实请求中自然恢复。直连单 Channel 的兼容探测失败节流可由
`FLEX_PROBE_COOLDOWN`（默认 120 秒）控制。

### `context_policy` 与 `session_affinity`

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `context_policy.reserve_output_tokens` | `8192` | 未指定 `max_tokens` 时为能力/上下文筛选预留的输出空间。 |
| `session_affinity.enabled` | `false` | 是否优先保持同一对话的 Channel。 |
| `session_affinity.idle_seconds` | `1200` | Runner 自身的粘性闲置窗口；Setup 全局值可覆盖。 |
| `session_affinity.minimum_messages` | `2` | 至少积累多少条消息后建立普通会话粘性。 |
| `selection.stickiness.min_stable_seconds` | `3600` | 成本平衡相关的稳定参考时间；不覆盖故障、协议错误或硬截止。 |

## 11. 全局运行参数（Setup / 环境变量）

| 参数 | 默认值 | 当前示例配置 | 作用 |
|---|---:|---:|---|
| `FLEX_QUEUE_TPM` | `60` 秒 | `300` 秒 | 单请求 TPM 退避累计上限。 |
| `FLEX_QUEUE_RPM` | `300` 秒 | `900` 秒 | 单请求 RPM 退避累计上限。 |
| `FLEX_UPSTREAM_RESPONSE_TIMEOUT` | `180` 秒 | `180` 秒 | 单次 LiteLLM 调用等待 response object 的安全边界；超时会脱离底层任务。 |
| `FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT` | `180` 秒 | `180` 秒 | response object 后等待首个有效 SSE 的安全边界。 |
| `FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT` | `900` 秒 | `900` 秒 | 请求级首活动全局上限；按 Runner Channel 数量自动收紧为 9 分钟或 12 分钟。 |
| `FLEX_SESSION_AFFINITY_IDLE_SECONDS` | `3600` 秒 | 由 Setup 覆盖 | 普通会话粘性闲置时间，范围 60–86400 秒。 |
| `FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS` | `3600` 秒 | 由 Setup 覆盖 | 协议兼容回退后的独立粘性闲置时间，范围 60–86400 秒。 |
| `FLEX_PROBE_INTERVAL` | `120` 秒 | 未写入 setup.conf | 配额恢复后台循环的检查频率，不等于每个 Channel 的健康检查频率。 |
| `FLEX_PROBE_COOLDOWN` | `120` 秒 | 未写入 setup.conf | 直连冷却探测失败后的节流间隔。 |

`FLEX_HEDGE_FIRST_SECONDS` 和 `FLEX_HEDGE_SECOND_SECONDS` 是历史环境变量；当前
自动计划固定为 6/9/12 分钟，只有 Runner 的 `selection.hedge.stages` 能显式给出
目标阶段和 Channel。健康接口 `/healthz` 返回最终生效的自动截止与安全边界，排查时
应以它为准。

## 12. 状态转换与典型时序

### RPM/TPM

```text
正常 → 收到真实 rpm_limit/tpm_limit
     → limit_retry_started
     → 原 Channel 指数退避（4/8… 或 8/16…）
     ├─ 成功 → channel_recovered，清除临时冷却
     └─ 预算耗尽 → limit_escalated
          ├─ failover → 清除本次固定关系，按 Runner tier 重新选路
          ├─ wait     → 原 Channel 继续等待，达到累计上限后返回 429
          └─ fail     → 立即向下游返回 429
```

### 无首活动

```text
T=0 原请求
  ├─ 3+ Channels：T=6m 第二，T=9m 第三，T=12m 504
  ├─ 2 Channels ：T=6m 第二，T=9m 504
  └─ 1 Channel  ：T=6m 同 Channel 重试，T=9m 504
```

每个阶段都可能先受到单次 180 秒 response/首 SSE 安全边界影响；安全边界推进
阶段，但不会把更晚的请求重新计为一次初始请求。赢者确定后，输家只记录取消或脱离，
不会向下游拼接第二个模型的输出。
