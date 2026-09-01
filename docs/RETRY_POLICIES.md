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
  10 分钟复验；不发送后台空探测，直到成功、客户端超时或断开。其它新请求可选备用。
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
RPM/TPM、协议、政策、Hedge 和下游断开都必须在 Trace 中可区分。无后台空探测；
Channel 自检和 Responses 检测只有管理员显式点击时执行。
