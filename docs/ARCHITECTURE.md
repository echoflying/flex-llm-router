# Flex LLM Router 架构与运行说明

本文是项目当前实现的架构基线。重试、退让、Hedge、超时和错误处理统一见
[`RETRY_POLICIES.md`](RETRY_POLICIES.md)；面向使用者的介绍见根目录
[`README.md`](../README.md)。

## 1. 定位与边界

Flex 是运行在本地的逻辑路由层，向 Hermes 等标准 OpenAI 客户端提供稳定的
`/v1` 接口。Flex 负责模型名解析、能力/上下文筛选、选路、限额保护、会话粘性、
重试状态和可观测性；LiteLLM 只负责把已选 Channel 的请求转换并发送给上游 Provider。
Provider 凭据只从本机环境变量读取，不进入 YAML、API 响应或页面。

## 2. 资源模型

- **Provider**：保存上游 Base URL 和 API Key 对应的环境变量名，不保存密钥值。
- **Channel**：恰好一个 Provider + 一个 LiteLLM Model，附带能力、上下文窗口、
  限额、启用状态、协议检测结果和错误学习状态。同一 Provider + Model 不允许配置
  多个 Channel 来区分凭据、区域或参数。
- **Runner**：稳定的外部模型名，包含一个或多个按顺序排列的 Channel，并复用
  `selection`、`tiers`、会话粘性和 Hedge 策略。Channel 数量改变不会改变 Runner
  的外部身份。
- **links**：旧客户端名称到 Runner/Channel 的兼容别名。旧 `pools`、`connections`
  配置在加载时迁移为 `runners`、`links`；旧 API 路径继续兼容。

Channel 的 `externally_exposed` 字段控制它是否作为独立模型出现在 `/v1/models`；
关闭时仍可作为 Runner 的内部候选。当前 Config 页面主要编辑 Channel 的 Provider、
Model、别名、启用和 CHN Content Policy Fallback 标记；外部暴露字段仍可由结构化
配置/API 管理。

## 3. 选路模型

`selection.strategy` 支持：

- `round_robin`：按 Runner Channel 顺序轮转；
- `cost_aware`：按 Runner 的 `tiers` 从低成本到高成本选择；
- `quota_paced_priority`：结合本地五小时窗口和 Channel 状态平滑使用配额。

能力、上下文窗口、流式/工具/JSON 要求、启用状态和冷却状态先筛选，再在候选中
应用会话粘性和策略。会话粘性是软优先级，不得阻止故障、协议不兼容、限流升级或
硬截止等更高优先级事件。普通和协议兼容回退分别使用 Setup 中的
`FLEX_SESSION_AFFINITY_IDLE_SECONDS` 与 `FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS`。

## 4. 配置与管理界面

`/config` 固定为 Runner → Channel → Model 三个标签页：

- Runner 管理外部名称、Channel 顺序、策略和 Base URL 展示；成员增删、上下移和
  策略选择确认后立即保存；Runner 名称只允许字母、数字、点、下划线和连字符，
  最多 64 个字符。
- Channel 按 Provider 分组，管理 Provider/Model、别名、启用状态、CHN Content
  Policy Fallback 标记、普通自检和 Responses 能力检测。检测只在人工点击时发起，
  不做周期性主动健康检查。
- Model 管理 Provider 及其 `.env` 变量名引用，只显示变量名、存在性和模型候选，
  不显示密钥值。

结构化保存先执行 `FlexConfig.model_validate`，成功后在 `config/pools.yaml` 旁创建
`pools.yaml.backup.YYYYMMDDHHMMSS`，仅保留最近 10 份，然后热更新当前核心进程的
内存配置。配置保存不自动重启核心；Python 代码、模板或进程环境变化需要手动重启。

## 5. API 与协议

核心接口包括：

- `/v1/models`、`/v1/chat/completions`：OpenAI 兼容入口；
- `/api/runners`、`/api/providers`、`/api/config/editor`：管理数据；
- `/api/traces`、`/api/requests`、`/api/statistics/*`：调用轨迹和统计；
- `/api/config/channels/{id}/test`、`responses-test`：人工测试和协议能力检测；
- `/healthz`：版本、构建特性、Hedge 截止策略、活跃 watchdog 和启动恢复数量；
- `/api/admin/restart`：macOS launchd 核心重启入口。

LiteLLM 的返回错误先由 Flex 归类；只有明确登记的协议兼容错误才允许切换到兼容
Channel，普通参数/鉴权/模型不存在错误不会盲目重试。Responses 能力检测结果持久化
在 Channel 的 `protocol_support.responses` 中。

## 6. 状态、统计与隐私

核心使用 SQLite `data/flex.db` 保存 Channel 状态、配额窗口、限流学习、请求尝试、
Trace 和统计。Trace 默认最近 3 天、最多 1000 条；三小时完整上行请求留存是独立
开关，另有条数和空间上限，默认关闭。长期分析只保留请求规模桶、Token、耗时、
错误类型和结果，不保存 Prompt 或输出正文；短时非流式成功响应回放最多 120 秒、
单条 1 MiB，流式和工具调用不进入回放。

## 7. 进程与同步

7800 是核心 API、调度器和 watchdog；7801 是独立 UI。UI 重启不影响核心正确性。
本地 checkout 是唯一编辑源；`scripts/sync_to_mac.ps1` 只同步已提交的代码、文档、
模板和测试，排除 Mac 运行时 `config/`、`.env`、数据库、日志、虚拟环境、缓存和备份。
同步脚本的 Mac 用户、主机和目录必须由调用者通过参数或本地环境变量提供，不写入仓库。

