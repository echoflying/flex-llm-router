# Router 韧性与退让策略

本文件描述 Flex Router 在上游异常、长时间无首字与下游连接断开时的行为。目标是保留真实对话、避免无意义探测，并让同一池中的其他请求继续工作。

## 基本原则

- 只把已明确识别的上游临时错误当作可重试错误；普通请求格式错误仍立即返回。
- 没有后台“空”探测：配额异常和引擎不可用的验证均由原始挂起对话承担。
- 一个发生异常的 Channel 在冷却中会从池的正常调度中排除；其它池内请求可选择备用 Channel。
- 所有等待、验证、冷却、恢复与取消写入 Trace。
- 下游在上游首字/响应前断开时，Router 会取消仍在等待的上游任务，并记录 `client_disconnected_before_first_token`；首字后则由流式断开监听处理。标准 OpenAI 协议没有逻辑任务取消 ID，因此只有真实 HTTP 断开可被可靠感知。

## `allocated quota exceeded`

这类 429 记为 `quota_exhausted`，但在低频场景下可能是上游子额度、模型分配或临时路由异常，而不一定是账户总余额耗尽。

原请求固定回到发生异常的同一 Channel 进行验证：

1. 首次异常后等待 1 分钟；
2. 再失败后等待 2 分钟；
3. 再失败后等待 4 分钟；
4. 三次验证仍失败后，每 10 分钟复验一次，直到原请求成功或调用方总超时。

成功会清除临时状态。等待中的事件为 `quota_retry_wait`，恢复事件为 `channel_recovered`。其它新请求不成为探测请求；池模式会优先走其它可用 Channel。

## 本地五小时调用额度

Sensenova Flash Lite 的本地保护额度为五小时 500 次。它是路由器根据已发出的上游尝试计算的保护阈值，成功和实际发出的失败尝试都会计数；尚未发出的本地拒绝不会计数。

达到阈值时，Router 不再把原请求立即返回为 503：原请求固定在同一 Channel，依次等待 1、5、10、20 分钟，之后每 30 分钟验证一次，直至上游给出响应或调用方自行超时/断开。该验证请求可越过本地计数一次，但不绕过其它上游错误或冷却规则；同时其它新请求仍被本地额度保护，不会并发冲击上游。若没有仍在等待的原请求，后台同样至多每 30 分钟发送一次最小探测；若原请求在等待，后台不会并发探测。Trace 事件为 `five_hour_quota_retry_wait`。

## `engine is not available temporarily`

只有精确匹配该上游文本的 HTTP 400 被归为 `engine_unavailable`。其它 HTTP 400 一律仍为 `request_error`，不重试，避免掩盖模型名、参数或工具格式错误。

原请求在相同 Channel 上按以下间隔验证：15 秒、45 秒、120 秒；三次仍失败后每 5 分钟复验一次。等待事件为 `engine_retry_wait`，任一次成功同样记录 `channel_recovered` 并恢复调度。

## RPM / TPM

RPM 和 TPM 429 使用独立指数退避。每次真实 429 后记录学习样本与本轮冷却，等待受 Setup 中的单请求累计上限约束。Router 不会在未收到真实限流前预先阻止正常请求。

## 无首字的长请求（Hedge）

尚未收到上游响应对象时，原请求会在 6 分钟、12 分钟各启动一个同 Channel 的并行副本，最先返回响应对象者获胜，其余副本取消。每个尝试若先触发独立的响应对象超时，会立即记为该尝试失败并推进下一个 Hedge 阶段，不必再等到原定时间点；若响应对象已返回但尚未收到 SSE，剩余 Hedge 档位同样以最先收到上游事件者获胜。15 分钟仍无响应对象或 SSE 时，Router 取消所有等待副本、记录 `upstream_total_timeout` 并返回 504；已经开始输出的流不受该首活动时限截断。即使某个上游 HTTP 库未立即响应取消，Router 也不会继续等待它释放：Trace 与调用方会立即得到最终失败状态。该时钟由 7800 内部 watchdog 每秒驱动，和 7801 前端进程、页面是否打开完全无关；Trace 会显示 `watchdog_hedge_due` / `watchdog_deadline_due` 作为到期证据。

Pool 的 Hedge 阶段由 `selection.hedge.stages` 配置。每个 stage 的 `after_seconds` 指定等待时间、`channels` 指定并行目标；列表第一项先执行，后续项按顺序执行。任意一份先返回响应对象或 SSE 即获胜，所有其它副本取消；15 分钟仍没有活动则统一返回 504。Trace 的 Hedge 事件会标明实际目标 Channel。

每个 stage 会跳过已经有未完成请求的 Channel，并记录 `pre_response_hedge_skipped` / `hedge_skipped`。因此配置中重复出现同一 Channel 不会制造重复在途请求；只有原尝试已经结束后，后续普通重试才可能再次使用它。

每次 LiteLLM 调用还有独立的安全边界：默认 180 秒没有拿到响应对象，或拿到响应对象后 180 秒没有收到首个 SSE 事件，就记录 `upstream_response_timeout` 并把该尝试作为超时处理；若仍有可用 Hedge 阶段，会立即启动下一阶段。响应对象选定后，watchdog 会从“响应对象阶段”交接给“首 SSE 阶段”，不会再由前一阶段的回调重复派发 Hedge。超时的底层任务脱离主请求清理，避免 HTTP 客户端不响应取消而拖住调用方。可在 `setup.conf` 中用 `FLEX_UPSTREAM_RESPONSE_TIMEOUT` 和 `FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT` 调整；15 分钟仍是整个请求的最终首活动上限。

每次调用会写入 `upstream_task_started`、`upstream_response_received`、`stream_consumer_started`、`upstream_first_sse_wait_started`、`upstream_first_sse`、`upstream_error_received` 和 `upstream_cancel_requested` 等事件，区分“未拿到响应”“下游尚未开始消费”“拿到响应但无 SSE”“上游明确报错”和“取消未及时完成”。若响应对象已返回但流式消费者始终未进入，15 分钟截止由 watchdog 直接收尾并记录 `stream_deadline_observed`，不依赖队列消费者。Trace 列表查询使用 `attempts(trace_id)` 索引和一次性聚合，避免前端轮询拖慢核心 watchdog。

## CONFIG 中的池策略名称

CONFIG 会根据池当前配置显示中文策略名称：

- **配置驱动多阶段 Hedge**：按每个 Pool 的 `selection.hedge.stages` 执行，不绑定任何具体模型或服务商。
- **轮转均衡 + 同通道 Hedge**：按轮转选择 Channel，6 / 12 分钟向原 Channel 发副本。
- **成本优先 + 故障回退**：按 tier 优先低成本 Channel，异常时切换备用 Channel。
- **配额节奏优先 + 故障回退**：按配额消耗节奏安排 Channel，异常时依次回退。

Hedge 的实际顺序以 Pool 的 `selection.hedge.stages` 为准。每个 stage 使用 `after_seconds` 指定等待秒数、使用 `channels` 指定该阶段并行目标；列表顺序就是先后顺序。调整 YAML 后重启核心即可应用，不需要改程序代码。

## 下游客户端断开

流式响应并行监听 `http.disconnect`，包括等待首个上游 SSE 的阶段。断开后 Router 取消原始上游流和未完成 hedge，Trace 记录 `client_disconnected`，attempt 与 Trace 均以 `cancelled` 收尾。

## 重复在途请求观察

Setup 中的“重复在途请求观察”默认关闭。开启后只记录同一客户端、同一模型、短窗口内完全相同且仍在执行的请求。请求内容只以本机 HMAC 指纹比对，不保存全文。该功能永不取消、合并、延迟或改变路由；统计页独立展示样本，待积累后再决定是否启用去重策略。

## 短时结果恢复（幂等回放）

为处理调用方在上游已成功、但客户端未及时收到结果时发出的重复请求，Router 对**完全相同**的请求提供一个独立于“重复在途观察”的短窗口恢复机制：

- 仅适用于非流式（`stream: false`）、没有 `tools` 与 `tool_choice` 的请求；流式、工具调用、失败、被截断（`finish_reason=length`）及异常响应绝不缓存。
- 匹配条件同时包括调用方标识、对外模型名以及整份请求的本机 HMAC 指纹；不同客户端、模型、消息、参数均不会命中。
- 正常成功响应最多保存 **120 秒**，命中时直接回放完整 OpenAI JSON，返回 `X-Flex-Response-Replay: hit`，不再访问上游。Trace 会记录 `replay_hit`，因此这次调用仍可审计。
- 完整响应只放在本机 SQLite 的短时表中，到期即删除，最大 1 MiB；Prompt 从不保存到该表，也不进入长期分析数据。

这特别适合 Hermes 的命令批准类非流式请求：例如已得到 `APPROVE`，但调用方因为传输中断重发时，可避免重复占用同一模型。它不尝试对自然语言“相似”内容作判断，也不会合并仍在进行中的请求。

## 全文上行请求留存（可选诊断）

Setup 的“3 小时全文上行请求留存”与通用 `FLEX_DEBUG` 完全独立，默认关闭。开启后，Router 会在本机 SQLite 保存实际发给上游的完整请求 JSON，便于复现“特定 payload 无首字”的问题。它不保存 HTTP Header/API Key，也不额外保存流式模型输出。保留时长、最多条数和最大空间均可配置；任何一个限制到达时，最旧记录自动删除。Setup 会显示当前池的条数、空间、最早与最新记录时间。
