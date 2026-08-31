# 延迟副本（Hedged Retry）策略

目标：处理代理或上游网关已接收连接、但请求无声丢失且长期没有任何响应活动的情况；不以短总超时误杀 Hermes 的长任务。

## 时间线

这条时钟由 **7800 核心进程内的独立 watchdog** 每秒检查；不依赖管理界面（7801）、浏览器标签或前端刷新。watchdog 只发出到期信号，请求处理器负责启动副本及取消实际 LiteLLM 任务。

1. `T=0` 发起原始上游请求 A。
2. Runner 有 3 个或更多 Channel 时，`T=6 分钟` 发起第二个 Channel 的副本 B，`T=9 分钟` 发起第三个 Channel 的副本 C；`T=12 分钟` 仍无首活动则收口并返回 HTTP 504。
3. Runner 有 2 个 Channel 时，`T=6 分钟` 发起第二个 Channel 的副本 B；`T=9 分钟` 仍无首活动则收口并返回 HTTP 504。
4. 单 Channel Runner 在 `T=6 分钟` 对同一 Channel 建立一次新连接重试；`T=9 分钟` 仍无首活动则硬截止。全局 `FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT` 只能进一步缩短该截止。以上顺序按 Runner 配置顺序和当前首选 Channel 计算，不绑定模型或 Provider 名称。

每个实际尝试另外受两个可配置的 180 秒安全边界保护：LiteLLM 调用在 180 秒内没有返回响应对象，或响应对象返回后 180 秒内没有首个 SSE 事件，就记录超时并释放该尝试；如有后续 Hedge 阶段，会立即推进到下一阶段。该边界不改变按 Channel 数量计算的请求级最终 deadline。

如果所有预响应 Hedge Attempt 都失败，Router 会保留最后一次 Attempt 的异常、Channel 和耗时并向调用方返回真实错误；不会隐式返回 `None`，也不会把最初 Channel 错误地写成最终 Channel。

收到首个 SSE 后，后续每一次 SSE 读取也使用独立的可取消计时任务。上游迭代器即使不响应取消，Router 仍会在流式空闲阶段的 6/9/12 分钟时钟到期时立即切换或硬截止，不会被 SDK 的取消清理无限阻塞；迟到的上游任务会被分离并静默回收。

上游先返回响应对象即可结束“连接建立阶段”的 hedge；流式响应此后仍以任一可转发 SSE 事件（正文、reasoning、tool-call 或协议事件）决定赢家。非流式则以完整成功响应决定赢家；不是仅指普通文本 token。

## 获胜与取消

- 第一个有效响应为赢家，只有赢家的数据会转发给客户端。
- 其余副本立即取消；若上游不支持取消，仍会产生费用，但不会再向客户端转发。
- 一旦已向客户端转发任何 SSE 事件，不再创建副本，也不切换响应源，避免两份流混合。
- 客户端断开时取消全部仍在运行的副本。

## 约束与记录

- 每个客户端请求最多一个原请求和两个副本；普通多 Channel Runner 中，同一 Channel 已有未完成尝试时，后续 stage 会跳过它，避免重复并发。单 Channel Runner 的 6 分钟阶段是明确允许的同 Channel 重试例外。跳过会记录 `pre_response_hedge_skipped` 或 `hedge_skipped`。
- 响应对象尚未返回时，副本以 `pre_response_hedge_started`、`pre_response_hedge_won`、`pre_response_hedge_cancelled` 记录；流式首字阶段仍使用 `hedge_started`、`hedge_won`、`hedge_cancelled`。watchdog 到达 6/9/12 分钟（取决于 Runner Channel 数量）时还会先写入 `watchdog_hedge_due` 或 `watchdog_deadline_due`，便于区分“时钟已到”与上游调用本身的结果。预响应阶段的最终截止回调会同步写入 `upstream_cancel_requested`、`upstream_total_timeout` 和最终失败状态，不依赖请求协程是否及时完成 SDK 清理。每次真实上游调用都记录为独立 attempt。
- LiteLLM 生命周期记录为 `upstream_task_started`、`upstream_response_received`、`upstream_first_sse`、`upstream_error_received`、`upstream_response_timeout` 和 `upstream_cancel_requested`，可以区分 HTTP 错误、无响应和取消滞后。
- 对多 Channel Runner，副本优先使用下一个合格通道；单 Channel Runner 则在 6 分钟建立同一通道的新连接。
- 该机制只用于“没有任何上游活动”的疑似丢失请求，不替代 RPM/TPM、5 小时额度、普通错误重试或退让策略。
