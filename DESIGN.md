# Flex LLM Router：配额、限流与自适应调度设计

本文定义 `deepseek-v4-flash` 池的目标调度行为。LiteLLM 只负责向具体供应商发请求；配额、成本、可用性判断和选择顺序均由 Flex 负责。

## 1. 目标与渠道角色

当前逻辑模型池对 Hermes 暴露为 `flex-deepseek-v4-flash`，YAML 池键与该模型名相同；具体 channel 可映射到不同的真实上游模型。

| 渠道 | 角色 | 目标 |
| --- | --- | --- |
| `opencode-go-deepseek` | 主力、免费额度 | 尽量承载主流量；从真实调用中学习其 RPM、TPM 与 429 恢复规律。 |
| `sensenova-deepseek` | 有限免费额度 | 5 小时最多 500 次，调度目标为 90%，即最多 450 次；应平滑使用，避免免费额度闲置。 |
| `deepseek-official` | 付费兜底 | 正常选择中永不主动使用；只有其他可兼容免费渠道均不可用时才使用。 |

三个渠道当前均为相同模型，因此能力与上下文筛选规则相同；以后混入不同模型时，仍先执行上下文窗口、tools、JSON、流式等兼容性筛选，再进入本设计的调度算法。

## 2. SenseNova：5 小时滑动窗口而非固定冷却

SenseNova 的额度按滑动窗口理解：过去 300 分钟内的调用次数不得超过目标额度 450。每条调用在自己的发生时刻满 300 分钟后释放一个名额，额度逐笔恢复，不会在 5 小时后一次性恢复。

```text
calls_5h(t) = count(request.started >= t - 300 minutes)
remaining_5h(t) = 450 - calls_5h(t)
next_release_at = 最早一条仍在窗口中的 started + 300 minutes
```

当 `remaining_5h <= 0` 时，SenseNova 仅冷却到 `next_release_at`。一旦最早请求移出窗口即可再次安排，而不是固定休息 5 小时。

### 2.1 平滑消耗（quota pacing）

450 次 / 300 分钟约为 `1.5 次/分钟`。在有足够真实需求时，SenseNova 应围绕此节奏被使用，而不是短时间耗尽。

对当前滚动窗口，调度器保存 SenseNova 的请求时间序列，并计算目标使用轨迹：

```text
目标速率 = 450 / 300 requests per minute
目标使用量 = 目标速率 × 已观测窗口时长
```

若实际使用量低于目标、仍有可用额度且未触发 RPM/TPM 风险，SenseNova 获得临时优先级；若已接近目标或窗口额度紧张，则选择 OpenCode。真实业务流量低于 1.5 次/分钟时，不制造虚假调用来消耗额度。

## 3. 选择顺序

每个请求按下面顺序处理：

1. 根据上下文长度与请求能力，剔除不兼容渠道。
2. 剔除被人工禁用、处于上游明确冷却期、或本地滑动窗口已达到硬限制的渠道。
3. 如果 SenseNova 落后其配额节奏并且其 RPM/TPM 安全窗口允许，优先 SenseNova。
4. 否则优先 OpenCode。
5. 两个免费渠道都无资格时，才选择 `deepseek-official`。
6. 一次请求失败后，只在同类请求允许重试且存在其他合格渠道时切换；流式响应开始后不切换，避免混合输出。

`routing.type: fallback_only` 的渠道不参加 round robin、权重选择、低延迟选择或额度补偿。

## 4. 已知与未知限制

限制分两类处理：

| 类型 | 例子 | 数据来源 | 行为 |
| --- | --- | --- | --- |
| 已知硬限制 | SenseNova 500 次 / 5h | 配置 | 使用精确滑动窗口计数；实际调度只使用 450。 |
| 未知或不完整限制 | OpenCode RPM、TPM、并发、429 恢复时间 | 真实请求和上游反馈 | 建立每个 channel 独立的估算值与置信度，保守调度并持续学习。 |

未知渠道绝不因一次 429 被永久认定为“5 小时限额”。429 可能代表 RPM、TPM、并发、账户窗口限制或上游临时保护。

## 5. 需要持久化的观测数据

对每一条实际请求（包含人工 Test）记录：

- channel、模型、开始时间、结束时间、成功或失败；
- 输入 token、输出 token、总 token（以供应商返回的 usage 为准）；
- 请求参数中影响能力选择的元数据，不保存 prompt 内容；
- HTTP 状态、脱敏后的上游错误摘要、`Retry-After` / reset header（若有）；
- 发生 429 时的 60 秒请求数、60 秒 token 数、5 小时调用数、当时并发数。

每个 channel 另存一份学习状态：

```text
safe_rpm                 当前保守安全 RPM
safe_tpm                 当前保守安全 TPM
rpm_lower_bound          已观察到的最高稳定成功请求速率
rpm_upper_bound          429 推断出的最小可能 RPM 边界
tpm_lower_bound / upper_bound
last_429_at
last_429_evidence        发生时的窗口快照与错误摘要
confidence               样本数量、近期性、推断一致性
```

这些状态必须按 channel 隔离，不能把一个供应商的估值应用到另一个供应商。

## 6. RPM / TPM 的滑动窗口与学习算法

### 6.1 基础窗口

对每个 channel 保留最近 60 秒成功和失败请求的事件队列：

```text
rpm(t) = count(request.started >= t - 60 seconds)
tpm(t) = sum(request.total_tokens where request.started >= t - 60 seconds)
```

每一笔请求/Token 在自己的时间戳满 60 秒后自然释放容量。不能使用“发生 429 后固定等待 N 分钟”的粗粒度模型。

### 6.2 429 分类证据

发生 429 时按以下优先级判断：

1. 上游明确的 `Retry-After`、重置时间或错误文案：最高优先级。
2. 触发时请求数接近最近稳定上限、token 较低：更像 RPM。
3. 触发时 token 接近最近稳定上限、请求数不高：更像 TPM。
4. 接近已知 5 小时窗口额度：更像配额窗口限制。
5. 三者均不明显：标记为 `unknown_429`，可能是并发、账户级限制或上游临时保护。

分类结果附带置信度，避免仅凭一次样本得出确定结论。

### 6.3 保守逼近

初始估值使用低保守值。真实请求连续成功时缓慢抬高允许速率；429 时快速收紧。

```text
成功稳定窗口：safe_limit 向已观测成功上界缓慢增加
触发 429：candidate_limit = 触发前窗口值
           safe_limit = min(旧 safe_limit, 0.80 × candidate_limit)
恢复后：仅以 safe_limit 的 80%～90% 参与正常调度
```

若上游给出 `Retry-After`，严格遵守；否则对 RPM/TPM 推断等待至最早一笔请求（或 token 批次）移出 60 秒窗口后，以一笔真实的低风险业务请求恢复。`unknown_429` 使用短退避开始，例如 30 秒；再次失败则指数增加，成功后逐步降低退避。

不发送人为探测流量来故意撞上限；估值只从人工 Test 和真实业务请求中学习。因此算法在安全性与收敛速度之间偏向安全。

## 7. 可观测性

管理页与 API 应展示：

- 每 channel 的 5 小时已用 / 剩余次数、下一次释放时间；
- 当前 `safe_rpm`、`safe_tpm`、置信度和最近 429 分类；
- 近 60 秒请求数、token 数、并发数；
- 选择该 channel 的理由：`quota_pacing`、`primary`、`fallback`、`recovery_probe` 等；
- 最近测试结果、延迟与脱敏错误摘要。

日志不得记录 API Key、Authorization、prompt 正文或完整响应；错误摘要必须脱敏并截断。

## 8. 实施顺序

1. 引入 `quota_paced_priority` 与 `fallback_only` 配置，固定 Official 的兜底地位。
2. 用滑动窗口替代现有固定 5 小时冷却；SenseNova 设为 500 次硬上限、450 次调度目标。
3. 持久化真实 token usage、请求窗口和 429 证据。
4. 实现 per-channel 的 RPM/TPM 估算、置信度与恢复探测。
5. 在管理页展示估值、释放时间和“为什么选中该 channel”。
6. 用真实、低频业务流量验证并微调安全裕量；不使用大量人工压测来学习上限。
