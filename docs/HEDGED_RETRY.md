# 延迟副本（Hedged Retry）策略

目标：处理代理或上游网关已接收连接、但请求无声丢失且长期没有任何响应活动的情况；不以短总超时误杀 Hermes 的长任务。

## 时间线

这条时钟由 **7800 核心进程内的独立 watchdog** 每秒检查；不依赖管理界面（7801）、浏览器标签或前端刷新。watchdog 只发出到期信号，请求处理器负责启动副本及取消实际 LiteLLM 任务。

1. `T=0` 发起原始上游请求 A。
2. `T=6 分钟`：若尚未收到上游响应对象或任何有效 SSE 活动，发起副本 B，A 保持等待。
3. `T=12 分钟`：若 A、B 都未产生上述活动，发起副本 C；最多三路。
4. `T=15 分钟`：若仍未获得上游响应对象或任何 SSE 事件，Flex 由核心 watchdog 直接收口 Trace、关闭所有未完成 attempt，取消/脱离仍在等待的副本，并返回 HTTP 504。

每个实际尝试另外受两个可配置的 180 秒安全边界保护：LiteLLM 调用在 180 秒内没有返回响应对象，或响应对象返回后 180 秒内没有首个 SSE 事件，就记录超时并释放该尝试；如有后续 Hedge 阶段，会立即推进到下一阶段。该边界不改变 15 分钟的请求级最终 deadline。

上游先返回响应对象即可结束“连接建立阶段”的 hedge；流式响应此后仍以任一可转发 SSE 事件（正文、reasoning、tool-call 或协议事件）决定赢家。非流式则以完整成功响应决定赢家；不是仅指普通文本 token。

## 获胜与取消

- 第一个有效响应为赢家，只有赢家的数据会转发给客户端。
- 其余副本立即取消；若上游不支持取消，仍会产生费用，但不会再向客户端转发。
- 一旦已向客户端转发任何 SSE 事件，不再创建副本，也不切换响应源，避免两份流混合。
- 客户端断开时取消全部仍在运行的副本。

## 约束与记录

- 每个客户端请求最多一个原请求和两个副本；同一 Channel 已有未完成尝试时，后续 stage 会跳过它，避免对同一上游重复并发。跳过会记录 `pre_response_hedge_skipped` 或 `hedge_skipped`。
- 响应对象尚未返回时，副本以 `pre_response_hedge_started`、`pre_response_hedge_won`、`pre_response_hedge_cancelled` 记录；流式首字阶段仍使用 `hedge_started`、`hedge_won`、`hedge_cancelled`。watchdog 到达 6/12/15 分钟时还会先写入 `watchdog_hedge_due` 或 `watchdog_deadline_due`，便于区分“时钟已到”与上游调用本身的结果。预响应阶段的 15 分钟回调会同步写入 `upstream_cancel_requested`、`upstream_total_timeout` 和最终失败状态，不依赖请求协程是否及时完成 SDK 清理。每次真实上游调用都记录为独立 attempt。
- LiteLLM 生命周期记录为 `upstream_task_started`、`upstream_response_received`、`upstream_first_sse`、`upstream_error_received`、`upstream_response_timeout` 和 `upstream_cancel_requested`，可以区分 HTTP 错误、无响应和取消滞后。
- 对池，副本优先使用下一个合格通道；单通道则建立同一通道的新连接。
- 该机制只用于“没有任何上游活动”的疑似丢失请求，不替代 RPM/TPM、5 小时额度、普通错误重试或退让策略。
