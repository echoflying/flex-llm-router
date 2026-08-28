# Router 韧性与退让策略（当前实现）

本文以当前 `src/flex_llm_router/app.py`、`state.py` 和 `config/pools.yaml` 为准。目标是在上游无响应、限流或临时故障时保持调用可靠，同时不让 UI 进程承担核心正确性。

## 基本原则

- 只有明确识别的临时错误才自动重试；普通 HTTP 400、模型名错误、参数/工具格式错误立即返回。
- 没有后台“空”探测：配额异常和引擎不可用的验证由原始请求承担。
- 冷却、配额和学习状态按 Channel 保存；其它 Runner 请求可选择可用备用 Channel。
- 所有等待、重试、冷却、恢复、取消和 Hedge 事件都会写入 Trace。
- 7800 核心 watchdog 独立于 7801 UI 和浏览器页面，负责长请求截止和收口。

## 上游错误

### `allocated quota exceeded`

这类错误标记为 `quota_exhausted`。原请求固定回到发生异常的同一 Channel 验证：1 分钟、2 分钟、4 分钟；三次仍失败后每 10 分钟复验，直到成功或客户端结束。其它新请求优先选择 Runner 中的其它可用 Channel，不制造后台探测。

### `engine is not available temporarily`

只有包含该精确临时引擎错误的 HTTP 400 才进入专门验证流程：15 秒、45 秒、120 秒，之后每 5 分钟复验。其它 HTTP 400 不重试。

### RPM / TPM

真实限流发生后才进入 RPM/TPM 退让；两类使用独立指数退避，并受 Setup 中的单请求累计等待上限约束。未收到真实上游限流前，Router 不主动预留或消耗 RPM/TPM。

### 五小时窗口

`max_requests_per_window` 是本地滑动窗口保护，不代表账户余额。达到阈值后，原请求固定在该 Channel，按 1、5、10、20 分钟验证，之后每 30 分钟验证一次；尚未发出的本地拒绝不计数。

## 无首活动 Hedge

只有在没有上游响应对象或任何 SSE 活动时触发；一旦已有首活动，不切换响应源。

| Runner Channel 数量 | Hedge 时间线 | 最终截止 |
|---|---|---|
| 3 个或更多 | T=0 原始；T=6 分钟第二 Channel；T=9 分钟第三 Channel | T=12 分钟 |
| 2 个 | T=0 原始；T=6 分钟第二 Channel | T=9 分钟 |
| 1 个 | T=0 原始 | `FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT` |

目标按当前首选 Channel 和 Runner 配置顺序计算，不绑定 Provider/Model 名称。`selection.hedge.stages` 可以显式覆盖目标，但最终截止仍按 Runner Channel 数量计算。

每个实际 LiteLLM 尝试另有默认 180 秒响应对象和 180 秒首 SSE 安全边界；单次超时会推进下一阶段。最先产生有效响应对象或 SSE 的副本获胜，其余副本取消。Trace 会记录 `watchdog_hedge_due`、`hedge_started`、`hedge_won`、`hedge_cancelled` 和 `watchdog_deadline_due` 等事件。

## 下游断开与取消

Router 监听真实 HTTP `disconnect`。首活动前或流式期间断开，会取消原始上游流和未完成 Hedge，并以 `client_disconnected`/`cancelled` 收口。标准 OpenAI 协议没有逻辑任务取消 ID，因此不能仅凭请求内容判断取消。

如果上游 HTTP 客户端不及时响应取消，Router 会将任务脱离主请求清理；Trace 和调用方仍按截止时间结束，不会永久显示进行中。

## 会话粘性与回切

启用 `session_affinity` 时，同一对话优先保持同一 Channel，避免上下文缓存因频繁切换失效。限流、配额或故障时可以向 Runner 中的下一个 tier 回退；恢复探测和回切由配置的 `selection.fallback.reattach` 控制。

会话粘性不是永久绑定：上游发生 429、400、连接/响应超时或流式提前中断时，Router 会清除该对话的粘性记录，避免下一次重发再次命中已失败的 Channel。切换后的 Channel 一旦收到首个 SSE，会先写入临时粘性（事件 `session_affinity_provisional`）；只有完整流结束后才记为最终成功。这样既能让重复请求跟随已经开始响应的 Channel，也不会把未完成的请求误记为完整成功。

## 观测与诊断

- `/traces` 展示请求级 Trace、每次上游尝试和完整事件序列。
- `/statistics` 展示调用、错误、成功/重试和按小时趋势。
- 可选的三小时全文留存独立于 `FLEX_DEBUG`，默认关闭并受条数/空间上限约束。
- `/healthz` 返回构建特性、当前自动 Hedge/截止策略、watchdog 活跃数和启动恢复数。
- 客户端、模型、Runner、Channel、Provider 均记录在请求元数据中；凭据只从本机环境变量读取。
