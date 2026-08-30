# 路由与流控现行说明

本文以当前代码和 `config/pools.yaml` 为准。对外资源统一叫 **Runner**；Pool、Link、connections 只作为旧配置/API 的兼容名称，不再是新的用户模型。

## 1. 资源结构

- **Provider**：只保存环境变量名，真实 URL/API Key 留在本机 `.env`。
- **Channel**：恰好一个 Provider + 一个 LiteLLM Model。Channel 是内部资源，不作为主要外部模型入口。
- **Runner**：稳定的外部模型名，包含一个或多个 Channel，并复用既有 `selection`、`tiers`、会话粘性和 Hedge 策略。
- **links**：旧外部别名到 Runner/单 Channel 的映射，用于保持既有客户端模型名和 URL 不变。

相同 Provider + Model 不允许配置成多个 Channel；不同凭据、地区、参数或限额不能用重复 Channel 表示。Provider 的模型候选由 `/api/providers/{provider}/models` 从配置读取，不执行周期性主动健康检查。

Channel 可设置 `chn_content_policy_fallback: true`，表示该通道可作为内容政策兜底。
此标志不代表它自身会拦截内容。上游明确返回的内容政策信号（例如
`finish_reason: content_filter`）触发后，如果当前 Runner 还有其他标记通道，则先按 Runner
顺序尝试；只有 Runner 没有可用标记通道时才按 `global_fallback.chn_content_policy`
中的 Channel ID 顺序全局兜底，建议第一项为 `agnes-flash`；所有兜底均阻断时返回
`content_policy_blocked`。

## 2. 选路

`selection.strategy` 当前支持：

- `round_robin`：按 Runner 中的 Channel 顺序轮转；
- `cost_aware`：按 Runner 的 `tiers` 从低到高选择，失败或不可用时向后回退；
- `quota_paced_priority`：按配额消耗节奏优先选择，并在异常时回退。

会话粘性开启时，同一对话优先保持已选 Channel，避免上下文缓存因频繁切换失效。能力、上下文窗口、启用状态、冷却和已观测限流都会参与可用性判断。

## 3. 错误、限流与配额

每个 Channel 的 `limits` 使用以下现行字段：

```yaml
limits:
  rpm: null
  tpm: null
  local_cooldown_seconds: 300
  window_seconds: 18000
  max_requests_per_window: null
  quota_cooldown_seconds: 3600
  busy_threshold: 3
  busy_window_minutes: 5
  busy_cooldown_seconds: 300
```

- `rpm`/`tpm` 是本地保护闸门；未撞到真实上游限流前，不主动制造探测流量。
- `max_requests_per_window` 是本地五小时滑动窗口保护，不等同于上游账户余额。
- 真实 429 会区分 RPM、TPM、配额或其它错误，写入 Trace、统计和学习数据；退避按错误类别执行指数策略。
- RPM/TPM 只影响当前请求所在的原 Channel：当前请求在该 Channel 上退避重试，不切换到同一 Runner 的其它 Channel。其它新请求仍可避开冷却通道，使用备用 Channel。
- TPM 退避基数为 4 秒，RPM 退避基数为 8 秒，均按 2 倍递增；累计等待分别受 `FLEX_QUEUE_TPM` / `FLEX_QUEUE_RPM` 上限约束。
- `allocated quota exceeded` 由原请求固定回到原 Channel 做 1/2/4 分钟验证，之后每 10 分钟复验；不使用后台空探测。
- `engine is not available temporarily` 仅按专门策略验证；其它 HTTP 400 立即返回，不自动重试。

## 4. 无首活动 Hedge

仅当上游没有返回响应对象或任何 SSE 活动时触发；一旦已有首活动，不再切换响应源。

| Runner Channel 数量 | 时间线 | 最终截止 |
|---|---|---|
| 3 个或更多 | T=0 原始；T=6 分钟第二 Channel；T=9 分钟第三 Channel | T=12 分钟 |
| 2 个 | T=0 原始；T=6 分钟第二 Channel | T=9 分钟 |
| 1 个 | T=0 原始 | `FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT` |

顺序按当前首选 Channel 和 Runner 配置顺序计算，不绑定具体模型或 Provider。也可用 `selection.hedge.stages` 显式指定阶段目标。

收到首个 SSE 后，流式阶段重新开始 6/3/3 分钟计时：6 分钟无后续 SSE
发起第一阶段 Hedge，之后 3 分钟发起下一阶段，再过 3 分钟硬终止；每个
后续 SSE 都会重置计时器。

每次实际尝试另有默认 180 秒响应对象/首 SSE 安全边界；超时会推进下一个 Hedge 阶段。核心 7800 内 watchdog 负责到期、取消和 Trace 收口，7801 UI 不是正确性依赖。

## 5. 观测与数据

- Trace 默认保留最近 3 天、最多 1000 条；可选的完整上行请求留存默认关闭，并有独立的时长、条数和空间上限。
- SQLite 保存请求尝试、Channel 状态、配额窗口、限流学习、调用统计和错误统计。
- `/healthz` 返回版本、构建特性、当前 Hedge/截止策略和 watchdog 状态。
- `/traces`、`/statistics`、`/config`、`/setup`、`/help` 提供本地管理和诊断界面。

## 6. 生效与同步

修改 YAML 或 Python 后，先在 `D:\home.it\flex-llm-router` 提交并推送 Git，再运行：

```powershell
powershell -File scripts/sync_to_mac.ps1
```

脚本只同步已提交的代码、文档和测试；明确排除 Mac 端整个 `config/`（包括 `pools.yaml`、`setup.conf` 及备份）、`.env`、数据库、日志、虚拟环境和缓存，避免覆盖 Mac UI 新增的 Channel/Runner 或运行开关。同步后需手动重启 Mac 核心服务，代码修改才会加载；UI 进程是否刷新不影响核心 watchdog。
