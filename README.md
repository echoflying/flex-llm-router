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

## OS 层健康探针（独立于 Router）

Router 本身由 `com.weifeng.flex-llm-router` 经 `launchd KeepAlive=true` 托管——进程崩溃会立刻被拉起。
但有一种「假死」场景：主循环卡住、HTTP hang、日志停止写入，而进程 PID 仍在。此时只有
**外部 OS 级探针**能感知。

### 探针系统组成

```
scripts/probe_router.sh          # 纯 Bash, 不依赖 Python/uvicorn
scripts/com.weifeng.flex-probe.plist  # launchd agent (StartInterval=60s)
logs/probe.out.log               # 正常/警告输出
logs/probe.err.log               # CRITICAL 与 kickstart 结果
```

探测逻辑（每分钟执行一次）：

| 检查项 | 阈值 | 动作 |
|---|---|---|
| 端口 :7800 是否监听 (`lsof`) | 无 | 消失 → 立即 `kickstart -k` |
| `flex-router.log` 最后修改时间 (`stat`) | >5 分钟未写 | WARN 告警 |
| 僵尸 trace 数 (`sqlite3 status='running' AND started < now-15min`) | >5 条 | WARN; >10 条 + 日志停写 → 强制 kickstart |

### 安装与启用

探针 plist 已在仓库 `scripts/` 下，**需要你手动加载到 launchd**（DSH sandbox 禁止自动写入 `~/Library/LaunchAgents/`）：

```bash
# 1. 拷贝 plist 到 LaunchAgents (只需执行一次)
cp scripts/com.weifeng.flex-probe.plist ~/Library/LaunchAgents/
chmod 644 ~/Library/LaunchAgents/com.weifeng.flex-probe.plist

# 2. bootstrap 激活
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.weifeng.flex-probe.plist

# 3. 验证
launchctl list | grep flex-probe
tail -f logs/probe.out.log   # 应看到 "OK port 7800 is listening"
```

禁用：
```bash
launchctl bootout gui/$(id -u)/com.weifeng.flex-probe
rm ~/Library/LaunchAgents/com.weifeng.flex-probe.plist
```

### 与现有 Monitor 的关系

- **monitor** (`com.weifeng.flex-monitor`, 每 30 分钟): 读 DB + 生成 `data/monitor/report.md`，需要 router 的 SQLite 可访问性，但**依赖 Python 环境**。
- **probe** (`com.weifeng.flex-probe`, 每 60 秒): 只做 OS 级判断（端口、文件时间戳、SQLite 只读查询），**完全不依赖 router 进程存活**。

两层互补：router 真的死了 → probe 第一时间发现并 kickstart；router 假死 → monitor 报告异常 + probe 的 zombie 计数也会升高。

## Git 提交记录关键变更

- `src/flex_llm_router/app.py`: `/api/quota` 配额 API + 流式 finally 兜底终态 + `_stale_trace_sweeper` 后台任务
- `src/flex_llm_router/state.py`: `trace_status()`、`sweep_stale_traces()`、`cancel_interrupted_traces()` 补充 attempts 收尾
- `templates/dashboard.html`: 状态灯后加「距今时长」、Channel 名后加「1h 调用次数」徽标
- `templates/quota.html`: 新增配额仪表盘页面
- `templates/base.html`: 导航增加「配额仪表盘」链接
- `scripts/probe_router.sh` + `scripts/com.weifeng.flex-probe.plist`: OS 层健康探针（待用户手动 bootstrap）
