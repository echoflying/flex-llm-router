# Router 韧性与错误处理策略（现行实现）

本文描述 `src/flex_llm_router/app.py` 的实际请求生命周期。除特别注明外，策略都作用于单个请求；其他新请求可以根据 Channel 的冷却状态选择其它可用通道。

## 基本原则

- 只有明确识别的临时错误才自动重试；普通参数、鉴权、模型或工具格式错误不盲目重试。
- 不做与用户请求无关的后台“空探测”。配额和引擎验证由原始请求承担。
- 每个请求的每次上游尝试、等待、冷却、切换、恢复、取消和最终结果都写入 Trace。
- 7800 核心 watchdog 独立负责 Hedge、首活动截止和流式空闲截止；7801 UI 不参与正确性。

## 错误分类与处理

### 1. 请求进入前的本地拒绝

- 未知外部模型名：立即返回 `404`。
- 缺少 `messages` 或请求 JSON 不合法：返回 `400`。
- Channel 不具备所需能力（流式、工具、JSON 等）：该 Channel 被跳过，继续选择其它可用 Channel。
- 预计输入加输出超过上下文窗口：该 Channel 被跳过，继续选择其它可用 Channel。
- 所有 Channel 都不可用：对正在冷却的临时状态等待最早恢复；超过对应等待上限后返回 `503 no_eligible_channel`。

### 2. 内容政策限制

识别 `content_policy_blocked`、`content_filter`、`data_inspection_failed`（例如
`Input text data may contain inappropriate content`）等明确上游信号。

- 非流式：先按当前 Runner 中标记 `chn_content_policy_fallback: true` 的 Channel 顺序尝试；Runner 没有可用标记通道时，按 `global_fallback.chn_content_policy` 顺序尝试。
- 流式且尚未向下游输出可见内容：可以切换到下一个政策兜底 Channel。
- 已经输出内容后：不再切换，记录 `content_policy_blocked_after_output` 并结束流。
- 所有兜底通道都失败：返回 `502 content_policy_blocked`。
- 单纯的拒答文本不会自动判定为政策错误，必须有明确的 finish reason 或错误标记。

### 3. 协议兼容错误

`protocol_error_rules` 只匹配明确的 Provider/Model、HTTP 状态和错误签名。例如 `reasoning_content` 必须返回的 `400`：

- 首个 SSE/输出之前：清除当前会话粘性，尝试下一个可用 Channel，并记录协议错误统计。
- 流式首个 SSE 阶段发生同类错误：同样可以切换到下一个 Channel。
- 已经向下游输出后：不再切换，直接结束。
- 其它普通 `400` 不适用此策略，直接返回。

### 4. 配额异常 `allocated quota exceeded`

这是套餐/总配额类错误，不是普通瞬时限流。

- 当前请求固定回到发生错误的原 Channel。
- 等待并验证：`1 分钟 → 2 分钟 → 4 分钟`；三次仍失败后每 `10 分钟`复验。
- 直到成功、达到客户端总超时或客户端断开；不切换 Channel，不发送后台空探测。
- 其它新请求可以使用 Runner 中的其它 Channel。

### 5. 临时引擎不可用 `engine is not available temporarily`

仅对明确包含该错误文本（兼容 `avaiable` 拼写）的 HTTP 400 使用专门策略：

- 当前请求固定原 Channel；
- `15 秒 → 45 秒 → 120 秒`，之后每 `5 分钟`复验；
- 其它 HTTP 400 不进入此流程。

### 6. RPM / TPM 瞬时限流

只有收到真实上游限流后才进入退避，不预先占用或主动探测额度。RPM 与 TPM 使用独立计数和退避。
对外错误类型统一写作 `rpm_limit`（Requests Per Minute）和 `tpm_limit`（Tokens Per
Minute）；`rate_limit` 仅作为旧数据库记录的兼容读法，展示和新写入均使用 `rpm_limit`。
限流处理分为两个阶段：第一次真实限流后的短退避重试（Trace 为 `limit_retry_started`），以及重试预算耗尽后升级为 Channel 冷却（Trace 为 `limit_escalated`）。升级后不再把冷却状态伪装成普通重试。

- **TPM**：`4s → 8s → 16s → 32s → ...`。
- **RPM**：`8s → 16s → 32s → 64s → ...`。
- 累计等待分别受 `FLEX_QUEUE_TPM`、`FLEX_QUEUE_RPM` 限制（代码默认 TPM 60 秒、RPM 300 秒，Setup 可覆盖）。
- `cost_aware` Runner：先在原 Channel 按其 `retry_policy.max_retries` 进行上述指数退避；达到次数后才按 tier 成本顺序切换到下一个可用 Channel。
- 可在 Runner 的 `selection.rpm_limit.on_exhausted` 指定第二阶段动作：`failover`（切换备用 Channel）、`wait`（继续等待原 Channel）或 `fail`（立即返回 429）。未配置时保持兼容默认：`cost_aware` 等价 `failover`，其它策略等价 `wait`。
- `wait` 模式下当前请求在整个退避序列中固定触发错误的原 Channel；`failover` 模式达到重试预算后清除固定关系并重新选路。
- 其它新请求仍可避开处于冷却的 Channel，使用同一 Runner 的其它通道。
- 达到累计上限后，向调用方返回原始限流错误（通常为 HTTP 429）。

### 7. 其它上游故障

包括连接错误、响应超时、HTTP 5xx、空响应或 malformed stream 等：

- 非流式：先按 Channel 的 `retry_policy` 重试；达到该 Channel 的重试次数后，如果 Runner 的 failure 策略允许，再切换到下一个 Channel。
- 流式且已开始输出：不重新生成另一条响应，记录错误并结束当前流。
- 首个响应/SSE 尚未到达时：由无首活动 Hedge 机制决定是否启动其它 Channel。
- LiteLLM 已返回 response object 但流式消费者尚未进入时，仍属于无首活动阶段；watchdog 会先启动并暂存 Hedge，消费者进入后接管，不能因为 `watchdog_handoff` 而跳过备用 Channel。
- 若 watchdog 在该交接窗口内达到硬截止，响应层会独立发送终止信号：尚未发出响应头时返回 HTTP 504，已发出流式响应头时发送终止 SSE 错误事件；Trace 内部失败记录不再作为唯一反馈。

### 8. 流式空闲与首活动截止

- 未收到任何响应对象或 SSE：
  - 3 个以上 Channel：原始请求、6 分钟第二 Channel、9 分钟第三 Channel，12 分钟硬截止；
  - 2 个 Channel：6 分钟第二 Channel，9 分钟硬截止；
  - 1 个 Channel：6 分钟对同一 Channel 发起一次重试，9 分钟硬截止（全局设置只允许进一步缩短截止时间）。
- 已收到 SSE 后，进入流式空闲计时；连续没有后续 SSE 时按 6/3/3 分钟阶段启动空闲 Hedge，最终硬截止。
- 每收到新的有效 SSE，空闲计时器重新开始。
- 最先产生有效响应的副本获胜，其余上游任务取消。

### 9. 下游断开与取消

Router 监听 HTTP disconnect：

- 首活动前断开：取消上游任务，Trace 标记 `client_disconnected_before_first_token`。
- 流式期间断开：取消当前流和所有 Hedge，Trace 标记 `client_disconnected` / `cancelled`。
- 不依赖标准 OpenAI 请求中的任务 ID；仅依据真实连接状态处理。

### 10. 本地五小时滑动窗口

`max_requests_per_window` 是本地保护阈值，不代表供应商账户余额：

- 当前请求固定在触发保护的原 Channel；
- 验证间隔：`1、5、10、20 分钟`，之后每 `30 分钟`；
- 不发送后台空探测；其它新请求可以使用其它 Channel。

## 会话粘性与回切

会话粘性只决定正常请求的优先 Channel，不覆盖上述固定原 Channel 的验证流程。普通会话粘性闲置时间由 Setup 的 `FLEX_SESSION_AFFINITY_IDLE_SECONDS` 控制；协议兼容错误切换后使用独立的 `FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS`，避免已验证兼容的备用 Channel 很快被切回不兼容通道。两者均为“无新请求后的闲置时间”，不是会话总时长；每次成功请求都会刷新对应记录。上游 429、400、连接/响应超时或流式提前中断时会清除旧粘性，避免下一次请求继续命中故障通道。切换后首个 SSE 会建立临时粘性，完整成功后才确认最终粘性。

## 观测与上限

- Trace 默认保留最近 3 天、最多 1000 条。
- 完整请求留存独立于普通 Debug，默认关闭，并受时长、条数和空间上限约束。
- `/healthz` 返回版本、构建特性、Hedge/截止策略、watchdog 活跃数和启动恢复数。
- 所有错误分类、限流样本和恢复结果进入 SQLite 统计，可在 Dashboard 查看。
