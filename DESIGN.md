# Flex LLM Router 设计说明（当前实现）

## 1. 目标

Flex 是运行在本地的 LLM 逻辑路由层。它向 Hermes 等标准 OpenAI 客户端提供稳定的 `/v1` 接口，在不暴露 Provider 凭据的前提下，根据能力、上下文、配额、限流和会话粘性选择上游 Channel。LiteLLM 负责实际 Provider 调用，Flex 负责策略、状态和可观测性。

## 2. 资源抽象

- **Provider**：配置 URL/API Key 的环境变量名，密钥不落 YAML。
- **Channel**：恰好一个 Provider + 一个 LiteLLM Model，附带能力、上下文窗口、限额和重试参数。
- **Runner**：稳定的外部模型名，包含一个或多个 Channel，并复用原 Pool 的策略字段。
- **links**：兼容旧客户端名称的别名映射；旧 `pools`/`connections` 在加载时一次迁移为 Runner/links。

同一 Provider + Model 不允许配置多个 Channel 来区分凭据、区域、参数或额度；这些差异应通过不同 Provider/Runner 设计表达。Channel 选择不会执行周期性主动健康检查，状态只在加入 Runner、真实流量或上游故障时更新。

## 3. 选路与会话

`selection.strategy` 支持 `round_robin`、`cost_aware` 和 `quota_paced_priority`。`tiers` 属于 Runner，用于表达池内相对优先级，不绑定具体模型名称。启用 `session_affinity` 后，同一对话优先保持原 Channel，减少上下文缓存失效；只有异常、冷却或不可用时才向后回退。

## 4. 限额与学习

Channel 的 `limits` 包含 RPM、TPM、本地冷却、五小时滑动窗口、配额冷却以及忙阈值。真实 429 才会触发限流分类、指数退避和学习样本；本地窗口是保护阈值，不等同于供应商账户余额。状态存储在 SQLite 中，并用于 Dashboard、统计和 `/healthz`。

## 5. 无首活动 Hedge

核心 watchdog 每秒观察请求是否收到上游响应对象或 SSE 活动：

| Runner Channel 数量 | Hedge | 首活动最终截止 |
|---|---|---|
| 3 个或更多 | 6 分钟第二 Channel；9 分钟第三 Channel | 12 分钟 |
| 2 个 | 6 分钟第二 Channel | 9 分钟 |
| 1 个 | 不派副本 | `FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT` |

策略按当前首选 Channel 和 Runner 配置顺序计算，也可由 `selection.hedge.stages` 显式覆盖目标。响应对象/首 SSE 安全边界默认各 180 秒；最先产生有效活动的尝试获胜，输家取消并从客户端视角隐藏。

## 6. 请求生命周期

请求进入后生成 Trace，记录客户端、模型、Runner、上下文消息统计和请求指纹。Router 完成能力/上下文/状态筛选，调用 LiteLLM，并记录每个 attempt 的开始、响应、首 SSE、错误、取消、TTFT、完成耗时和最终结果。下游断开时取消未完成的上游任务。非流式成功响应可在短窗口内安全回放，避免传输丢失导致重复调用；流式或工具调用不进入该回放。

## 7. 数据与隐私

- Trace 默认限制最近 3 天、最多 1000 条。
- 三小时全文上行留存独立开关，默认关闭，并受条数/空间上限限制。
- 长期分析只保留统计事实、规模桶和错误类型，不保存 Prompt 或输出正文。
- API Key/Base URL 只从本机环境变量读取，错误日志会做敏感字段脱敏。

## 8. 运行边界

7800 是核心 API、调度器和 watchdog；7801 是独立 UI。UI 重启不会替代核心 watchdog；代码或配置修改后需重启核心才加载新版本。项目的本地正式副本是 `D:\\home.it\\flex-llm-router`，提交并推送后通过 `scripts/sync_to_mac.ps1` 同步 Mac，运行时 `.env`、数据库和日志不参与同步。
