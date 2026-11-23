# `/api/fleet-status` 接口说明

本文档描述 flex-llm-router 的 `GET /api/fleet-status` 接口：它返回什么、每个字段
的含义、以及外部系统（如 point-farm 采集脚本）在派发请求前可以怎样使用。

> 本接口是**只读总览**，不产生任何主动探测：只读取路由已积累的事实（当前冷却、
> 自检结果、学习到的限流、今日预算消耗）。调用方可把它当作派发前的"一瞥"。

## 1. 为什么需要它

在派发一个 LLM 请求前，调用方通常只知道自己"想用什么模型"，不知道：

- 哪些底层通道此刻正在冷却/封禁（撞上去会等一个完整超时窗口）；
- 哪些通道自检失败、实际上不可用；
- 每个通道今日的预算还剩多少。

`/api/fleet-status` 把这些问题一次回答完，让调用方**跳过明显不可用的通道**，
而不是撞上之后再等超时。这是 2026-11-22 死锁问题（大窗成员被阻塞但对外仍宣传
大窗）之后新增的防线之一。

## 2. 请求

```text
GET /api/fleet-status
```

无参数。返回 JSON，`Content-Type: application/json`。

## 3. 返回结构（三层）

```text
{
  object, generated_at, router_version, now_local,   ← 顶层元信息
  fleet,                                             ← 全局汇总
  runners: [...],                                    ← 每个对外模型的可用窗口
  channels: [...]                                    ← 每个底层通道的明细
}
```

### 3.1 顶层元信息

| 字段 | 类型 | 含义 |
|---|---|---|
| `object` | string | 恒为 `"fleet_status"` |
| `generated_at` | string | 生成时刻（本地，`YYYY-MM-DDTHH:MM:SS`） |
| `router_version` | string | 路由版本 |
| `now_local` | string | 本地时间（`HH:MM:SS`），用于判断新鲜度 |

### 3.2 `fleet` 全局汇总

| 字段 | 类型 | 含义 |
|---|---|---|
| `channels_total` | int | 通道总数 |
| `channels_enabled` | int | 启用的通道数 |
| `channels_cooldown` | int | **正在冷却/封禁中的通道数**（派发前应跳过） |
| `channels_selftest_failed` | int | 最近一次自检失败、实际不可用的通道数 |
| `runners_total` | int | 对外模型（runner）总数 |
| `runners_usable` | int | 当前真正有可用成员通道的 runner 数 |
| `daily_budget_tokens_declared` | int | 全池今日声明的 token 预算（所有通道 `tokens_budget` 之和） |
| `daily_budget_tokens_remaining` | int | 剩余 token 预算 |
| `daily_budget_tokens_consumed` | int | 已消耗 token 预算 |

一句话用法：先看 `channels_cooldown` / `channels_selftest_failed` 是否非零，再看
`daily_budget_tokens_remaining` 是否还够——不够就提前走免费兜底，而不是硬发。

### 3.3 `runners[]` 每个对外模型

| 字段 | 类型 | 含义 |
|---|---|---|
| `name` | string | runner 名（配置里的键） |
| `public_model` | string | 对外暴露的模型 id（即 `/v1/models` 里看到的） |
| `strategy` | string | 选择策略（`round_robin` / `cost_aware` / `budget_aware` 等） |
| `advertised_window` | int | **对外宣传**的上下文窗口（成员最大值，`/v1/models` 报的就是这个） |
| `usable_window` | int | **真实可用**窗口（剔除禁用/冷却/自检失败后，成员最大值） |
| `usable_channels` | string[] | 当前真正可用的成员通道 id 列表 |
| `member_count` | int | 成员通道总数 |
| `tiers` | object | `{channel_id: tier}` 成员分级 |

> **关键判据**：`usable_window` 与 `advertised_window` 不一致，就意味着大窗成员
> 当前被阻塞——对外仍"宣传"大窗，但实际用不了。调用方应据 `usable_window`
> 而非 `advertised_window` 判断能否承载请求；`usable_channels` 是可直接用的通道名单。

### 3.4 `channels[]` 每个底层通道

| 字段 | 类型 | 含义 |
|---|---|---|
| `id` | string | 通道 id |
| `enabled` | bool | 是否启用 |
| `provider` | string | 提供商 |
| `model` | string | LiteLLM 模型串（`openai/...`） |
| `public_model` | string | 对外模型 id |
| `runners` | string[] | 挂载该通道的 runner 列表 |
| `context_window_tokens` | int | 上下文窗口（token） |
| `max_output_tokens` | int | 最大输出（token） |
| `capabilities` | string[] | 能力（`chat`/`streaming`/`tools`/`vision`/`json`/`reasoning`/`responses`） |
| `supported_params` | string[] | 支持的参数 |
| `cooldown` | object\|null | **冷却状态**；`null` 表示未冷却 |
| `learned` | object\|null | 学习到的限流信息；`null` 表示无 |
| `last_test` | object\|null | 最近一次自检结果；`null` 表示无 |
| `daily_budget` | object | 今日预算消耗 |
| `declared_limits` | object\|null | 声明的 `rpm`/`tpm` 上限 |

**`cooldown`（null 或对象）**：

```json
{ "until": 1795536000.0, "reason": "quota_daily", "pool": "bulk-lite",
  "seconds_left": 59073, "until_local": "2026-11-25T00:00:00" }
```

| 字段 | 含义 |
|---|---|
| `until` | 冷却到期（Unix 秒） |
| `reason` | 原因（`quota_daily` / `rpm_limit` / `tpm_limit` / `busy` 等） |
| `pool` | 触发冷却的 pool |
| `seconds_left` | 剩余秒数（>0 表示仍冷却中） |
| `until_local` | 到期本地时间（人类可读） |

**`learned`（null 或对象）**：

```json
{ "rpm": 2, "tpm": 931772, "confidence": 1282, "kind": "quota_exhausted" }
```

| 字段 | 含义 |
|---|---|
| `rpm` | 安全每分钟请求数（学习到的） |
| `tpm` | 安全每分钟 token 数（学习到的） |
| `confidence` | 学习置信度（越大越可信） |
| `kind` | 最近一次 429 类型（`quota_exhausted` / `rate_limit` 等） |

**`last_test`（null 或对象）**：

```json
{ "outcome": "success", "error_type": null, "at": 1795392180.6 }
```

| 字段 | 含义 |
|---|---|
| `outcome` | `success` / `failure` |
| `error_type` | 失败类型（如 `connection_error`；成功为 `null`） |
| `at` | 测试时刻（Unix 秒） |

**`daily_budget`**：

```json
{ "day": "2026-11-24", "reset": "local_midnight", "reset_at": 1795536000.0,
  "seconds_to_reset": 59073, "tokens_used": 7715, "calls_used": 62,
  "healthy": false, "ratio": 0.61425, "tokens_remaining": 12285,
  "calls_remaining": 338, "label": "每日 1 积分…",
  "tokens_budget": 20000, "calls_budget": 400 }
```

| 字段 | 含义 |
|---|---|
| `day` | 当前预算日 |
| `reset` | 重置策略（`local_midnight` / `utc_midnight`） |
| `reset_at` | 下次重置时刻（Unix 秒） |
| `seconds_to_reset` | 距重置秒数 |
| `tokens_used` / `calls_used` | 今日已用 token / 调用次数 |
| `healthy` | 今日预算是否健康（由最近自检判定） |
| `ratio` | **剩余比例**：1.0 未消耗，0.0 耗尽，`null` 表示未声明预算 |
| `tokens_remaining` / `calls_remaining` | 剩余 token / 调用次数（未声明时为 `null`） |
| `label` | 预算说明（如"每日 1 积分…"） |
| `tokens_budget` / `calls_budget` | 声明的今日上限（仅声明时出现） |

> `ratio` 是**剩余**比例（不是消耗比例）。`0.0` 表示某一维度已耗尽——即使另一
> 维度还有余量，也会因绑定维度耗尽而无法再发请求。调度时优先看 `ratio`。

**`declared_limits`**：

```json
{ "rpm": null, "tpm": null }
```

声明的 rpm/tpm 上限；`null` 表示未声明。

## 4. 典型使用模式

### 4.1 派发前跳过冷却通道

```python
status = get("/api/fleet-status")

if status["fleet"]["channels_cooldown"] > 0:
    # 至少有一个通道在冷却，选路时要避开
    pass

# 找一个预算健康、自检通过、未冷却的通道
candidates = [
    c for c in status["channels"]
    if c["cooldown"] is None
    and c["last_test"] and c["last_test"]["outcome"] == "success"
    and c["daily_budget"]["ratio"] not in (None, 0.0)
]
```

### 4.2 判断一个 runner 是否真能承载请求

```python
for r in status["runners"]:
    if r["usable_window"] < needed_tokens:
        continue          # 真实可用窗口不够，即使 advertised 够大
    if r["usable_channels"]:
        pick = r["usable_channels"][0]   # 从真实可用通道里挑
```

`usable_window` 与 `advertised_window` 的分歧正是 2026-11-22 死锁的根因：大窗成员
被阻塞但对外仍宣传大窗。**永远以 `usable_window` 为准**。

### 4.3 预算余量与"付费/免费"切换

```python
for c in status["channels"]:
    b = c["daily_budget"]
    if b.get("tokens_remaining") is not None and b["tokens_remaining"] < threshold:
        fall_back_to_free_tier()   # 付费通道额度将尽，切免费兜底
```

## 5. 与 `/api/bulk-lite` 的关系

- `/api/bulk-lite` 回答"**这个大小该用哪个通道**"（按体量分档 + 预算感知推荐）。
- `/api/fleet-status` 回答"**现在整个池子是什么状态**"（冷却/自检/预算余量）。

两者互补：`fleet-status` 是全局概览，`bulk-lite` 是定向推荐。外部系统可先用
`fleet-status` 过滤掉明显不可用的通道，再用 `bulk-lite` 拿推荐。

## 6. 注意

- **`daily_budget.tokens_remaining` 是路由池的 token 预算余量**，不是某个上游
  账户的人民币余额。若调用方需要"账户余额自动切换付费/免费"，须另行接入上游
  官方余额接口（如 DeepSeek `/user/balance`）。
- `channels_selftest_failed` 统计的是最近一次自检失败；自检是路由自行发起的，
  不代表上游一定停机，但调用方应优先避开。
- 本接口不主动探测，返回的是路由已积累的事实；如需实时探针，应配合路由的
  自检周期或主动调一次 `/v1/models` 再取状态。
