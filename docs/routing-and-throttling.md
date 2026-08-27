# 路由与流控设计文档（Flex LLM Router）

> 状态：Step 1 实施中（本文档先于代码落地，作为唯一事实来源）
> 最后整理：2026-08-20

---

## 0. 背景与约束

- 我们**不是直连 DeepSeek 官方**，而是走 **Sensenova 通道**代理的 DeepSeek。
  Sensenova 这一层有自己的 RPM / TPM / 每5小时总量配额限制，且都返回 **HTTP 429**。
- 还使用 **opencode-go 通道**代理的 DeepSeek，以及 **deepseek-official 直连**（兜底）。
- 另外存在 **腾讯云（代理侧）忙** 类 429（服务器忙，也是 429）。
- 关键痛点：**上下文缓存（KVCache）跨通道不稳定**，所以"尽量黏住 sensenova、避免频繁切换"
  是首要目标。实际使用中已因中途切换导致缓存失效。

---

## 1. 核心概念：两类 429 必须区分

所有限流信号都是 HTTP 429，但处置策略完全不同。**一切调度逻辑建立在"先分类"之上。**

| 类别 | 名称 | 含义 | 典型来源 | 处置 |
|---|---|---|---|---|
| **A 类** | 总量配额耗尽（quota exhausted） | 套餐/免费额度池、每5小时总量用完 | Sensenova 每5h 总量（deepseek-v4-flash 500、sensenova-6.7-flash 1500） | **立即向下切**到 opencode-go；长冷却后试回 |
| **B 类** | 瞬时限流 / 服务器忙（busy） | RPM 超限、TPM 超限、腾讯云忙 | Sensenova RPM/TPM、腾讯云忙、opencode-go 频繁 RPM/TPM | **设门槛**才向下切；短冷却后积极试回 |

### 1.1 区分依据（错误体关键字匹配，已用真实日志校准）

来源：Hermes 系统日志 `state.db` 的 **P.AAAA**（`20260808_152910_2a7bac`）/ **P.WANGYUYAN**（`20260804_171234_e442a8`）session，真实 Sensenova 通道 429 错误体：

- **A 类（套餐/总量配额耗尽）**：`HTTP 429: Allocated quota exceeded, please increase your quota limit.`
  → 关键字：`allocated quota exceeded` / `quota exceeded` / `insufficient_quota` / `free allocated` / `exceeded your quota` / `额度` / `配额`
  → 长冷却 `quota_cooldown_seconds(3600)`，等额度恢复
- **B 类（瞬时限流/忙）**：
  - `HTTP 429: rpm exhausted`（每分钟请求数超限）
  - `HTTP 429: inference tpm exhausted`（每分钟 token 消耗太多）
  - 其他 `rate limit` / `requests per minute` / `tokens per minute`
  → 统一归 `rate_limit`，busy 窗口计数达阈值才向下切

> ⚠️ **关键**：`rpm exhausted` / `tpm exhausted` 是**瞬时窗口限流（B类）**，不是总量。`exhausted` 字样不能归 A 类——只有 `allocated quota exceeded` 才是 A 类（总量）。误判会导致"本该长冷却等额度"被当瞬时限流频繁来回切。

旧版风险已消除：Step 1 已用真实日志定稿关键字表（原猜测 `quota` 词过于宽泛，已收窄为 `allocated quota exceeded` 等精确词）。

### 1.2 为什么 B 类不直接切、设门槛

B 类是瞬时的（服务器忙、偶发超频）。偶发一次不该切走（切走反而丢缓存）。
只有**短时间窗口内频繁出现**（达 `busy_threshold`）才认为该通道此刻持续受限，才向下切。

---

## 2. 三类流控（配置字段统一命名）

现有 `Limits` 字段命名混乱（`cooldown_on_rpm_seconds` / `cooldown_on_429_seconds` 等混在一起，
且 `cooldown_on_429_seconds` 是**死字段未接线**）。统一为三组，配置里一眼可分：

```yaml
limits:
  # ① 自我流控（本地闸门，不依赖上游报错，防自己打爆上游）
  rpm: int | None            # 自限速 / 分钟（如 Sensenova 5/min）
  tpm: int | None            # 自限速 / 分钟令牌（官方无数据 → 由 Step2 估算写入，见 §5）
  local_cooldown_seconds: int   # 本地闸门触发后的冷却时长

  # ② 5小时总量控制（A 类硬限制，滑动窗口）
  window_seconds: int           # 滑动窗口长度（18000 = 5h）
  max_requests_per_window: int | None   # 窗口内最大请求数（Sensenova 给的配额）
  quota_cooldown_seconds: int    # A 类配额耗尽后的冷却（长，等额度恢复）

  # ③ 被流控后退让（B 类瞬时限流）
  busy_threshold: int           # B 类窗口内达到几次才向下切
  busy_window_minutes: int      # B 类统计窗口（分钟）
  busy_cooldown_seconds: int    # B 类切走后的冷却（短，20min 静默即试回）
```

**删除的冗余字段**：`cooldown_on_rpm_seconds`、`cooldown_on_429_seconds`、`requests_per_5_hours`（别名，统一为 `max_requests_per_window`）、`quota_window_seconds`（统一为 `window_seconds`）。

---

## 3. 调度策略（Step 1 目标行为）

### 3.1 优先级（固定，不变）
`sensenova-deepseek-v4-flash` (primary) → `opencode-go-deepseek-v4-flash` (fallback) → `deepseek-official-deepseek-v4-flash` (fallback)

### 3.2 A 类（配额耗尽）流程
> 切回完整流程见 §3.6（以 §3.6 为准）。
1. sensenova 命中 A 类 429 → 标记 `quota_exhausted`，冷却 `quota_cooldown_seconds`(3600)
2. 冷却期间 scheduler 跳过 sensenova，按 `cost_aware` 选下一个可用 tier（当前 max_fallback_tiers=2 为全链式：0尽→1尽→2）
3. 冷却到期后，通过 §3.6 的异步探测确认恢复 → 切回低 tier

### 3.3 B 类（忙）流程
1. sensenova 命中 B 类 429 → 累加该通道 busy 计数
2. 窗口内（`busy_window_minutes`）达 `busy_threshold` → 标记 `busy`，本轮选 opencode-go
3. opencode-go 自身 B 类忙 → **本地降速，停在 opencode-go，不继续下切**
4. **向上切回（短）**：满足任一即解除 `busy`、放回首选：
   - (a) sensenova 连续静默 ≥ `busy_cooldown_seconds`(20min) 无任何调用 → 直接回
   - (b) 距切走 ≥ 1h 且检测到 ≥10min 调用空档 → 在空档回
5. 向上切回前**先探测测试** sensenova（发测试请求看是否恢复），恢复才正式切回

### 3.4 回切时机要点（保缓存）
- 回切必须落在**调用者空闲窗口**（session_affinity 空闲判定），避免对话中途切导致 KVCache 失效。
- 探测测试请求本身不计入用户对话上下文，仅探活。

### 3.5 session_affinity
保持开启，建议 `idle_seconds` 加长到 3600，减少中途跳通道。

### 3.6 切回策略与完整流程（下切的对称闭环）

切回（switch-back）与下切（fallback）是**对称**的：下切是"当前通道不可用 → 选更高 tier 通道"，
切回是"更低 tier 通道恢复 → 重新选它"。两者都由 `cost_aware` 策略统一驱动（始终选 `cost_tier` 最小的可选通道）。

#### 3.6.1 切回触发条件（对应下切的三类 trigger）
| 下切原因 | 冷却标记 | 切回触发（到期/条件满足） | 回切参数 |
|---|---|---|---|
| A类 额度耗尽 | `quota_exhausted` | 冷却到期（`quota_cooldown_seconds`=3600）→ 探测恢复 | `quota_recover_seconds: 3600` |
| B类 持续忙 | `busy` | 连续静默 ≥ `busy_cooldown_seconds`(1200) → 探测恢复 | `quiet_window_seconds: 1200` |
| 故障 | `failure`（或 busy 标记） | 间隔 ≥ `failure_retry_after`(300) → 探测恢复 | `failure_retry_after: 300` |

> 注意：**短期 B类（未达 busy 阈值）不下切，也无"切回"概念**——它只在本地降速，通道始终可选。

#### 3.6.2 切回核心机制（Y 方案：先探测再切回）
1. 冷却中的低 tier 通道，在接近冷却到期（`probe_window_before`=60s 前）或满足条件时，
   router 通过 `asyncio.create_task` **异步**发起轻量探测请求（不阻塞当前用户请求）。
2. 探测成功 → `state.clear_cooldown()` 清除该通道冷却 → 后续请求 `cost_aware` 自然选回低 tier（正式切回）。
3. 探测失败 → `state.record_probe(success=False)` 记录 `last_probe_at`，在 `probe_cooldown_seconds`(600) 内**不再探测**（避免频繁探测打 upstream）。
4. 探测请求不计入用户对话上下文，仅探活。

#### 3.6.3 切回的层级方向
- 从当前 tier 回切到**更低 tier 的可用通道**（如 opencode-go(tier1) 回切 sensenova(tier0)）。
- 若更低 tier 仍冷却/不可用，则留在当前 tier（不强行切回）。
- 与下切共用 `max_fallback_tiers`：下切深度和回切深度一致（如链式 0→1→2，回切 2→1→0）。

#### 3.6.4 保缓存约束（切回不能破坏 KVCache）
- 切回必须落在**调用者空闲窗口**（session_affinity 空闲判定），避免对话中途切导致缓存失效。
- 探测本身不污染用户上下文。
- `stickiness.min_stable_seconds`(3600)：即便低 tier 恢复，若当前通道稳定运行未达最小稳定时长，
  **不为了"切回更便宜"而主动切换**（避免为省钱频繁切、抖动缓存）。即：切回只在"当前通道自然结束/空闲"时发生。

#### 3.6.5 完整状态机（单通道维度）
```
        正常服务
           │
     ┌─────┴──────┐ 触发下切(quota/busy/failure)
     │            ▼
   [冷却中] ──── 冷却到期/条件满足 ──┐
     │  (标记 quota_exhausted/busy)  │ 异步探测(reattach)
     │                              ▼
     │                        [探测中] ──成功──► clear_cooldown ──► 回切低tier(正常服务)
     │                              │
     │                          失败│ record_probe(节流 probe_cooldown)
     └──────────────────────────────┘ 下一周期再探
```
关键点：**下切是同步的（请求时立即选更高 tier），切回是异步的（探测成功后下一请求自然回）**，
两者通过 `cost_aware` 策略 + 冷却状态解耦，无独立"切回开关"。

#### 3.6.6 与旧版说明的差异
- §3.2/§3.3 的旧描述（"不继续向下切到 deepseek-official"、"20min静默/1h空档10min"）已过时：
  当前 `max_fallback_tiers: 2` 为**全链式下切**（0尽→1尽→2）；回切参数统一在 `selection.fallback.reattach` 配置。
  以本节（§3.6）为准。

---


## 4. 各 Channel 配额事实表（数据正确性）

| Channel | Provider | Model | 5h 总量配额 | RPM | TPM | 备注 |
|---|---|---|---|---|---|---|
| sensenova-deepseek-v4-flash | sensenova | deepseek-v4-flash | **500** / 5h | **5** / min | 官方无数据（估算） | 滑动窗口 |
| sensenova-agnes-2.5-flash | sensenova | sensenova-6.7-flash | **1500** / 5h | 待确认 | 官方无数据（估算） | 滑动窗口 |
| opencode-go-deepseek-v4-flash | opencode-go | deepseek-v4-flash | 各自供应商额度（未给） | 待确认 | 待确认 | fallback |
| deepseek-official-deepseek-v4-flash | deepseek-official | deepseek-v4-flash | 直连额度（未给） | 待确认 | 待确认 | 兜底，仅前两者 down 时用 |

> ⚠️ **重要**：`max_requests_per_window` 按**每个 channel 各自供应商**填，不能把 Sensenova 的 500 套到 opencode-go / official。
> Sensenova 给的数只填 sensenova-* 两个 channel；opencode-go / official 暂留 None（不限制），回头看日志补。

---

## 5. TPM 估算机制（Step 2 规划，本次不做实现）

### 5.1 问题
Sensenova / opencode-go 的 TPM 官方无公开数据。无法硬编码 `tpm`。

### 5.2 方案：per-(provider, model) 滑动窗口统计 + 反推
- 建立统计模块（建议 `stats.py`）：对每个 (provider, model) 维护最近 1/5/60 min 的
  请求数、输出 token 量、429 类型分布。
- 撞到 **TPM 类 429** 时，取**前 1 分钟输出 token 量**作为该通道 TPM 实际上限的下界，
  写入 learned `safe_tpm`（乘 0.8 留余量），并递减。
- 与现有 `observe_429` 的 RPM 估算逻辑对齐扩展（目前 `observe_429` 已对 rpm/tpm 做 `safe_*=.8` 估算，
  但**未真正记录"前1分钟输出量"反推 TPM 上限**，Step 2 补这块）。

### 5.3 统计表结构（草案）
```
stats_window(provider, model, window_seconds) -> {requests, output_tokens, tpm_429_count, quota_429_count}
```
用于：① 估算 TPM 上限 ② 支撑 A/B 判定校准 ③ Dashboard 展示真实用量。

---

## 6. 待确认 / 待日志校准项

| 项 | 状态 | 计划 |
|---|---|---|
| Sensenova TPM 上限 | 无官方数据 | Step 2 估算；先不填 `tpm` |
| opencode-go / official 的 RPM/TPM/5h 配额 | 未给 | 留 None，看日志补 |
| agnes-2.5-flash 的 RPM | 未给 | 看日志补 |
| A/B 错误体真实样例 | 无 | 先模糊关键字匹配，撞到后从日志 `error_detail` 校准 `error_type` |
| `busy_threshold` / `busy_window_minutes` | 默认 3次/5min | 看日志调 |
| `busy_cooldown_seconds`(20min) / 1h 回切空档(10min) | 默认 | 看日志调 |
| 回切探测测试失败后的等待 | 继续等下一个周期 | 实施时定 |

---

## 7. 实施步骤

- [x] **文档先行**：本文件（Step 0 之前完成）
- [x] **Step 1**：config 字段统一重命名 + 删死字段；pools.yaml 填入已知数（§4）；
        error_type 新增 `quota` 类型；A/B 类分流 + 调度（§3）；回切探测；session_affinity 加长
- [x] **验证**：pytest + 真实调用日志核对 A/B 分类（已用 P.AAAA/P.WANGYUYAN 日志校准关键字）
- [ ] **Step 2**：per-(provider,model) 统计模块 + TPM 反推估算

---

## 6.5 层级抽象（cost-tier）— 可组合的下切模型

把"免费+收费1+收费2"这个具体三元组抽象为 **tier 升序 + 与层级解耦的下切规则**，
未来 `免费+免费+收费+收费` / `免费+收费1` 等组合只需改 YAML，核心调度代码不变。

> **tier 配在 POOL 侧，不配在 Channel 上**。tier 是"通道在池内的成本排位"（相对属性），
> 同一通道在不同池排位可能不同；且支持 `0 0 1 2` 组合（多个免费并列）。

### Pool 配置（tier 在池侧声明）
```yaml
pools:
  flex-deepseek-v4-flash:
    channels: [sensenova-deepseek-v4-flash, opencode-go-deepseek-v4-flash, deepseek-official-deepseek-v4-flash]
    tiers:
      sensenova-deepseek-v4-flash: 0        # 免费层
      opencode-go-deepseek-v4-flash: 1      # 收费1
      deepseek-official-deepseek-v4-flash: 2 # 收费2
    # 0 0 1 2 组合示例:
    # tiers: {sensenova-a: 0, sensenova-b: 0, opencode-go: 1, deepseek-official: 2}
    selection:
      strategy: cost_aware           # 按 tiers 升序优先
      fallback:
    order: cost_ascending
    trigger: [quota_exhausted, busy_persistent, failure]  # 什么错误触发下切
    max_fallback_tiers: 2        # 0用尽->1用尽->2 全链式下切
    reattach:
      probe_before_switch_back: true
      quiet_window_seconds: 1200     # B类回切: 静默窗口
      quota_recover_seconds: 3600    # A类回切: 等额度
      failure_retry_after: 300       # 故障回切: 定时重试间隔
  stickiness:
    min_stable_seconds: 3600     # 单通道至少稳定跑这么久才因"平衡"切换(防频繁切, 保缓存)
```

### 四类流控统一框架（按时间尺度）
| 尺度 | 触发 | 行为 | 配置映射 |
|---|---|---|---|
| 短期 | B类瞬时限流(短暂) | 本地降速, 不切 | busy 计数未达阈值 |
| 中期① | B类持续无好转 | 下切一层 | busy 达阈值 → `busy_persistent` |
| 中期② | A类 5h额度 (`allocated quota exceeded`) | 下切, 长冷却 | `quota_exhausted` |
| 长期 | 故障 (connection/timeout/5xx) | 下切(=用完), 定时重试 | `failure` trigger |
| 更长期 | 全部额度完 | 同 A类, 节奏略有区别 | 待日志区分(目前合并) |

### 关键约束（缓存稳定性）
- **A 和负载均衡一样不能频繁切**：选了通道要稳定跑 `min_stable_seconds` 才考虑因平衡切换
- **同 tier 顺序优先**（不主动负载均衡），且保持单通道稳定
- **故障 = 用完 + 定时重试**（非永久放弃）
- **不无限下切**：`max_fallback_tiers` 控制下切深度（当前=2 全链式：0尽→1尽→2）

### 已落地代码
- `config.py`: Channel 移除 cost_tier/paid(tier 改配 Pool.tiers); Pool.tiers 校验覆盖所有 channel; Pool.selection 文档化 fallback/stickiness
- `scheduler.py`: `cost_aware` 策略（按 Pool.tiers 升序、同 tier 按池列表顺序优先，支持 0 0 1 2 组合）
- `pools.yaml`: 三通道 tier 0/1/2 + cost_aware + fallback 块
- `app.py`: select 传 selection; failure trigger 驱动下切

## 8. 现状代码对照（实施 Step 1 时的改动点）

- `config.py` `Limits`：字段重命名（见 §2）
- `app.py` `error_type()`：新增 `quota` 判定（看 `error_detail` 关键字）
- `app.py` chat 主流程：`observe_429` 调用需区分 A/B（目前都传 `rate_limit` 触发，需按分类传）
- `state.py` `observe_429`：新增 `kind` 参数或内部判定 A/B；A 类走长冷却、B 类走短冷却+计数
- `state.py` 新增：busy 计数表 / quota_exhausted 标记 / 回切探测状态
- `scheduler.py`：按 `primary` + 跳过 `busy`/`quota_exhausted` 通道；fallback 不无限下切
- `pools.yaml`：填入 §4 已知数；`selection.retry_next_channel_on` 移除 `rate_limit`（B 类不再靠 retry 切，改靠 busy 阈值切）
