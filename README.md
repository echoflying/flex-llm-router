# Flex LLM Router

Flex LLM Router 为 Hermes、OpenCode 及其他标准 OpenAI 客户端提供一个稳定的本地
LLM 入口。客户端只需要记住一个 Base URL 和模型名，Flex 会在后台管理多个 Provider
和 Channel，在不暴露密钥的前提下完成能力匹配、选路、重试和故障恢复。

## 用户能得到什么

- **更高的可用性**：一个 Runner 可以包含多个上游通道；单个 Provider 超时、限流、
  暂时不可用或协议不兼容时，按策略继续尝试合适的备用通道。
- **更少的配置变更**：客户端始终使用稳定的 Runner 名称，不需要知道真实 Provider、
  LiteLLM 模型名或当前使用的是哪条通道。
- **业务连续性优先**：会话粘性减少不必要的模型切换；遇到无响应、流式停滞或错误，
  watchdog、Hedge 和明确的退让策略避免请求无限挂起。
- **成本与配额可控**：每个 Channel 独立管理 RPM、TPM、五小时窗口和冷却状态；可按
  Runner 的 tier 与 `cost_aware` 策略优先使用低成本通道。
- **可诊断、可追溯**：每次尝试、等待、切换、首字时间、最终结果和错误类型都能在
  Trace 与统计页面中查看，便于区分上游故障、限流和客户端断开。
- **本地隐私**：API Key 只存在本机环境变量；普通 Trace 默认限时限量，完整上行内容
  需要单独开启并受条数/空间上限保护。
- **平滑扩展**：Provider、Channel、Runner 解耦；可以先用一个通道，之后按顺序增加
  备用通道，而不改变对外模型名。

## 工作方式

```text
标准 OpenAI 请求 → Flex（选路/状态/重试）→ LiteLLM → Provider
                         └→ SQLite Trace 与统计
```

LiteLLM 负责具体 Provider 协议适配，Flex 负责策略和运行状态。对外只展示 Runner；
Channel 是否作为独立模型出现在 `/v1/models` 由其 `externally_exposed` 字段控制，
未独立暴露的 Channel 仍可以作为 Runner 内部候选。

## 快速开始

```bash
cp .env.example .env          # 填写本机 API keys
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
flex-router                   # 核心默认监听 http://127.0.0.1:7800
```

将客户端指向 `http://<router-host>:7800/v1`，模型名填写 Config 页面中 Runner 的
对外模型名即可。`/help` 页面提供可直接复制的 Hermes 配置字段。

## 管理页面

| 页面 | 用途 |
|---|---|
| `/` | Runner/Channel 状态、调用指标和最近错误 |
| `/config` | Runner、Channel、Model 三标签页；结构化校验后立即保存并热应用 |
| `/setup` | `.env` 与系统环境变量来源、Override 及会话粘性窗口 |
| `/traces` | 调用轨迹、每次尝试和错误详情 |
| `/statistics` | 调用成功率、跨 Channel 结果、按 Channel/Pool 的响应时间（平均值/中位数/P95/TTFT）及趋势统计 |
| `/help` | Hermes Base URL、Provider 和模型名的接入提示 |

配置保存会在 `config/pools.yaml` 旁保留带时间戳的最近 10 份备份，不自动重启核心。
配置变更会热应用；Python 代码、模板或进程环境变更需要手动重启核心。7800 是核心
API 与 watchdog，7801 是独立 UI，UI 重启不会替代核心 watchdog。

## 现行策略与架构文档

- [架构与运行说明](docs/ARCHITECTURE.md)：Runner/Channel/Provider、API、数据留存、
  隐私、进程边界和同步规则。
- [重试、退让与超时策略](docs/RETRY_POLICIES.md)：RPM/TPM、配额、协议/内容政策回退、
  6/9/12 分钟 Hedge、流式空闲和下游断开处理。

## 本地与 Mac 同步

本地 checkout 是唯一编辑源。提交后使用 `scripts/sync_to_mac.ps1`，通过参数或本地
环境变量提供 Mac 用户、主机和目标目录；脚本不会同步 Mac 的 `config/`、`.env`、数据库、
日志、虚拟环境、缓存或备份，也不会自动重启服务。

## 主要接口

- `GET /v1/models`、`POST /v1/chat/completions`：OpenAI 兼容入口；
- `GET /healthz`：版本、构建特性、Hedge 截止和 watchdog 状态；
- `GET /api/runners`、`GET /api/providers`、`GET /api/config/editor`：管理数据；
- `GET /api/traces`、`GET /api/statistics/*`：诊断与统计；
- `POST /api/admin/restart`：macOS launchd 核心重启入口。重启先进入排空状态：新请求立即收到
  `503 router_restarting`；已建立的流式请求收到终止 SSE 错误，再等待 3 秒重启核心。
