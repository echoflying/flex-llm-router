# 分析数据保留

Flex 将可读的调用 Trace 限制为最近 3 天、最多 1000 条，以控制本机数据库大小并减少 prompt / 输出保留。

与此同时，Router 为持续策略学习保留两层不含 prompt、输出正文或请求指纹的分析数据：

- `request_outcomes`：请求级分析事实，保留 400 天。包含池、首/最终 Channel、请求模型、输入规模桶与 token 计数、TTFT、总耗时、尝试次数、回退次数、最终状态和错误类型。
- `daily_request_analytics` / `daily_error_analytics`：按天、池、Channel、模型和输入规模桶汇总；保存请求数、一次/重试成功、失败/取消、上游尝试数、token、TTFT/完成耗时及错误恢复数据。日聚合不保存内容，默认不自动删除。

历史短期 Trace 会在核心启动时幂等回填到日聚合。每个 Trace 只会计入一次，由 `analytics_rollup_marks` 防止重启或重复调用造成重复计数。

“短时结果恢复”另有独立的 `response_replays` 临时表：仅保存符合条件的完整非流式成功响应，最长 120 秒、单条最多 1 MiB，到期即删除；不保存 Prompt，也不参与上述长期分析数据。
