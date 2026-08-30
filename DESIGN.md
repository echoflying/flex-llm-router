# Flex LLM Router 设计说明（当前实现）

## 1. 目标

Flex 是运行在本地的 LLM 逻辑路由层。它向 Hermes 等标准 OpenAI 客户端提供稳定的 `/v1` 接口，在不暴露 Provider 凭据的前提下，根据能力、上下文、配额、限流和会话粘性选择上游 Channel。LiteLLM 负责实际 Provider 调用，Flex 负责策略、状态和可观测性。

## 2. 资源抽象

- **Provider**：配置 URL/API Key 的环境变量名，密钥不落 YAML。
- **Channel**：恰好一个 Provider + 一个 LiteLLM Model，附带能力、上下文窗口、限额和重试参数。可标记 `chn_content_policy_fallback: true`，表示它可作为中国内容政策兜底通道。
- Channel 的 `externally_exposed` 控制是否作为独立模型出现在 `/v1/models`；关闭时仍可作为 Runner 的内部候选。
- **Runner**：稳定的外部模型名，包含一个或多个 Channel，并复用原 Pool 的策略字段。
- **links**：兼容旧客户端名称的别名映射；旧 `pools`/`connections` 在加载时一次迁移为 Runner/links。

同一 Provider + Model 不允许配置多个 Channel 来区分凭据、区域、参数或额度；这些差异应通过不同 Provider/Runner 设计表达。Channel 选择不会执行周期性主动健康检查，状态只在加入 Runner、真实流量或上游故障时更新。

## 3. 选路与会话

`selection.strategy` 支持 `round_robin`、`cost_aware` 和 `quota_paced_priority`。`tiers` 属于 Runner，用于表达池内相对优先级，不绑定具体模型名称。启用 `session_affinity` 后，同一对话优先保持原 Channel，减少上下文缓存失效；只有异常、冷却或不可用时才向后回退。异常会清除该对话粘性；切换 Channel 收到首个 SSE 后先写入临时粘性，完整流结束后再确认最终成功，避免重发再次命中失败通道。

### 协议兼容错误回退

`protocol_error_rules` 是按 Provider + Model + HTTP 状态 + 上游错误文本匹配的窄规则表，
不读取用户消息内容。只有明确登记的协议兼容错误（当前包含
`reasoning_content` 必须返回）才允许在首个 SSE 之前切换到其它 Channel；普通 400（参数、鉴权、模型不存在）仍直接返回。命中会清除会话粘性并写入
`protocol_error_observations`，可通过 `/api/statistics/protocol-errors` 查看各规则、Provider、Model 和 Channel 的出现次数及回退请求最终是否成功。

## 4. 限额与学习

Channel 的 `limits` 包含 RPM、TPM、本地冷却、五小时滑动窗口、配额冷却以及忙阈值。真实 429 才会触发限流分类、指数退避和学习样本；本地窗口是保护阈值，不等同于供应商账户余额。状态存储在 SQLite 中，并用于 Dashboard、统计和 `/healthz`。

RPM/TPM 触发后，TPM 基数为 4 秒、RPM 基数为 8 秒并按 2 倍递增。`cost_aware` 先在原 Channel 按其 `retry_policy.max_retries` 重试，达到次数后才按 tier 成本顺序回退；其它策略固定原 Channel 直到 `FLEX_QUEUE_TPM` / `FLEX_QUEUE_RPM` 累计上限。其它新请求仍可改走同一 Runner 的其它 Channel。

### CHN Content Policy fallback

`global_fallback.chn_content_policy` 是按顺序排列的 Channel ID（建议
`agnes-flash` 作为第一项）。当被标记的 Channel 返回明确的
`finish_reason=content_filter` 或等价的 `content_policy_blocked` 信号时，
如果当前 Runner 还有其他标记为 CHN Content Policy Fallback 的 Channel，则优先按 Runner
顺序尝试它们；Runner 没有可用标记 Channel 时才按全局列表逐个请求；列表耗尽后返回
`content_policy_blocked`。普通拒答文本没有明确政策信号时不触发此路径。

## 5. 无首活动 Hedge

核心 watchdog 每秒观察请求是否收到上游响应对象或 SSE 活动：

| Runner Channel 数量 | Hedge | 首活动最终截止 |
|---|---|---|
| 3 个或更多 | 6 分钟第二 Channel；9 分钟第三 Channel | 12 分钟 |
| 2 个 | 6 分钟第二 Channel | 9 分钟 |
| 1 个 | 6 分钟同一 Channel 重试 | 9 分钟 |

策略按当前首选 Channel 和 Runner 配置顺序计算，也可由 `selection.hedge.stages` 显式覆盖目标。响应对象/首 SSE 安全边界默认各 180 秒；最先产生有效活动的尝试获胜，输家取消并从客户端视角隐藏。

首个 SSE 到达后会重新初始化流式空闲计时器：连续 6 分钟无后续 SSE 时发起第一阶段 Hedge，再过 3 分钟发起下一阶段，最终再过 3 分钟终止该流。每次收到后续 SSE 都会重置这组 6/3/3 分钟计时。

## 6. 请求生命周期

请求进入后生成 Trace，记录客户端、模型、Runner、上下文消息统计和请求指纹。Router 完成能力/上下文/状态筛选，调用 LiteLLM，并记录每个 attempt 的开始、响应、首 SSE、错误、取消、TTFT、完成耗时和最终结果。下游断开时取消未完成的上游任务。非流式成功响应可在短窗口内安全回放，避免传输丢失导致重复调用；流式或工具调用不进入该回放。

## 7. 数据与隐私

- Trace 默认限制最近 3 天、最多 1000 条。
- 三小时全文上行留存独立开关，默认关闭，并受条数/空间上限限制。
- 长期分析只保留统计事实、规模桶和错误类型，不保存 Prompt 或输出正文。
- API Key/Base URL 只从本机环境变量读取，错误日志会做敏感字段脱敏。

## 8. 运行边界

7800 是核心 API、调度器和 watchdog；7801 是独立 UI。UI 重启不会替代核心 watchdog；代码或配置修改后需重启核心才加载新版本。项目的本地正式副本是 `D:\\home.it\\flex-llm-router`，提交并推送后通过 `scripts/sync_to_mac.ps1` 同步 Mac；Mac 端运行时 `.env`、整个 `config/`、数据库和日志不参与同步，避免覆盖 UI 保存的运行配置。
