# REPORT-03 — Collector 复核：P0 修复确认报告

**审阅对象**：`src/flex_llm_router/app.py`、`tests/test_routing_integration.py`
**前置报告**：`review/REPORT-01-collector.md`、`review/REPORT-02-default.md`、`review/REPORT-FINAL.md`
**审阅人**：@collector
**日期**：2026-08-21

---

## 1. 总体结论

**✅ 三个 P0 修复通过复核，可合入**。测试 32/32 passed 属实（本地复跑确认），三个 P0 修复点（重试计数语义、A 类 429 冷却、回切探测）在源码中均落地且路径正确。存在 **1 处 P1 遗漏**（`/api/pools/{name}/channels/{id}/test` 端点只处理 `rate_limit`，未同步 `quota_exhausted` 分支）与 **1 处 P2 可优化**（startup 用 deprecated `on_event` 且未捕获 probe 探测自身的 429 分类，探测失败一律记为 `False`）。不影响 P0 修复本身，建议 @default 自行决定一并补或留在 P1 批次。

---

## 2. 分维度评分（5 星制）

| 维度 | 评分 | 备注 |
|---|---|---|
| 设计 | ★★★★ | Y 方案（回切探测）完整实现，与 selection.reattach 开关对齐 |
| 正确性 | ★★★★★ | 主循环 `retries < rp.max_retries` + `retries+=1` 语义正确；observe_429 两分支 `ch.limits` 无误 |
| 安全 | ★★★★★ | 无密钥外泄；`on_event` 内层 try/except 兜底；`asyncio.create_task` 在 running loop 下安全 |
| 测试 | ★★★★ | 三 P0 用例真实命中修复路径；但 `test_p0_2` 未新增（回切探测仍无集成测试） |
| 运维 | ★★★★ | 冷却默认 3600s / probe 默认 600s 合理；`on_event` 有 deprecation warning 但非阻断 |

---

## 3. 三个 P0 修复逐项复核

### ✅ P0-1 — 重试计数语义修复（`app.py:377`）

**修复前**：`while attempt < rp.max_retries` — 拿 DB 自增 row id 当计数器，history 一长就永久跳过重试。
**修复后**（源码第 354 / 377 行）：

```python
tried=set(); last='no_eligible_channel'; rejected=[]; retries=0
...
if typ in rp.retry_on and retries < rp.max_retries:
    ...
    retries += 1   # 第 380 行，continue 前递增
    continue
```

**判定**：✅ 正确。`retries` 是请求级局部变量，每次新请求归零；`attempt` 仍只在第 369 行 `state.start(...)` 作为 DB 主键返回，语义分离干净。streaming 分支（第 391-405 行 `events()`）没有重试循环，不需要重试语义——**同步正确**。

**测试命中**：`test_p0_1_retry_uses_per_request_counter_not_rowid` 预灌 5 行历史（row id ≥3），然后发 429→429→OK 三次响应序列，期望第三次成功即 `status_code==200`。旧逻辑 row id=6 > max_retries=3 会直接跳过重试走 fallback，新逻辑按 `retries` 计数走 2 次重试后成功。**测试真实命中修复路径** ✅

---

### ✅ P0-3 — A 类 429（`quota_exhausted`）冷却接入（`app.py:374` / `402`）

**修复前**：`if typ=='rate_limit': state.observe_429(...)` — 只处理 B 类瞬时限流，A 类 `quota_exhausted` 裸奔，每次 502 反复打。
**修复后**（源码第 374、402 行）：

```python
if typ in ('rate_limit','quota_exhausted'):
    state.observe_429(key,ch.id,detail,limits=ch.limits)
```

**关键核实 `limits=ch.limits` 是否传对**：
- `ch` 是第 369 行 `scheduler.select(key,available,...)` 返回的**当前选中通道**（外层 `tried.add(ch.id)` 之后），是**本轮实际发起请求**的通道。
- 主循环外层 `for c in channels` 迭代变量是 `c`，内层 `ch=...or scheduler.select(...)` 是选定通道。
- `limits` 传入的是 **`ch.limits`（选定通道）**，正确——因为要冷却的正是刚刚返回 429 的通道，不是任意通道 `c`。
- 非流式分支 `app.py:374`、流式 `events()` 分支 `app.py:402` **两处同步**，无回归。

**下游链路核实**（`state.py:76-111`）：
- `observe_429(..., limits=None)`：`limits` 参数只用在第 104 行 `_cool(pool,ch_id, now+self._quota_cooldown_seconds(limits), 'quota_exhausted')` 与第 129 行 `_observe_busy(...)`。
- `app.py:374/402` 传入 `limits=ch.limits` → 冷却时长走通道配置（默认 3600s），正确。
- `kind` 参数由 `app.error_type(e)` 直接决定（`quota_exhausted`/`rate_limit`），不走 `observe_429` 的 `kind=None` 兜底推断；两条链路独立正确。

**判定**：✅ 正确且同步。

**测试命中**：
- `test_p0_3_quota_exhausted_triggers_cooldown`：发 A 类 429（`Allocated quota exceeded`）→ 期望 `states` 表写入 `reason='quota_exhausted'`。修复前为空。**真实命中** ✅
- `test_p0_3_observe_429_recovers_channel_after_cooldown_expiry`：先触发冷却，手动 `UPDATE states SET until=0` 模拟到期，再发请求 → 期望 `200`。**测试验证冷却不会永久坏** ✅

---

### ✅ P0-2 — 回切探测 `_probe_loop`（`app.py:408-444`）

**实现**：
- `async def _probe_loop(interval_seconds=120)`（第 408 行）：无限 `while True` + `await asyncio.sleep`；遍历 `config.pools`；读 `pool.selection.reattach.probe_before_switch_back` 开关（默认 `True`，config.py:75 已定义）；只探 `cooldown_reason in ('busy','quota_exhausted')` 的通道；`should_probe` 节流（默认 600s）；最小探测 `max_tokens=1`；成功 `clear_cooldown`，失败 `record_probe`；最外层 `try/except` 兜底日志。

**startup 拉起核实**：
```python
@app.on_event('startup')
async def _start_probe_loop():
    asyncio.create_task(_probe_loop())
```

- 在 FastAPI `startup` 事件中调用时，当前协程已在 **running loop** 上执行（uvicorn 已经启动事件循环），`asyncio.create_task(_probe_loop())` 安全挂载 ✅
- **未捕获异常风险**：第 439-440 行 `except Exception as exc` 包了整个循环体，日志 `logger.error('probe loop error: %s', exc)` 后继续下一轮——**不会拖垮进程** ✅
- 单轮 probe 里的 `litellm.acompletion(...)` 异常也在第 436-438 行 `except Exception` 被吞掉 → `record_probe(..., success=False)`，不向上传 **✅**

**判定**：✅ 正确实现。

**测试覆盖**：`test_routing_integration.py` **未新增 P0-2 集成测试**（用户说"新增 3 个集成测试"分别对应 P0-1 / P0-3 / P0-3 恢复，**不含 P0-2**）。回切探测逻辑目前只在 `state.py` 单元级间接覆盖。

---

## 4. 遗漏 & 可改进点

### 🟠 P1 — 遗漏分支：`/api/pools/{name}/channels/{id}/test` 端点未同步 `quota_exhausted`

**文件**：`src/flex_llm_router/app.py:320`

```python
if typ=='rate_limit': state.observe_429(channel_id,ch.id,detail)
```

**问题**：这条是"通道测试"端点的 429 处理，仍只写 `rate_limit` 分支——与 `app.py:374/402`（业务主流程）**不同步**。虽然此端点是管理员点按测试、不会被高并发打爆，但语义上仍然漏：如果手动 test 一个通道刚好撞 A 类 429，会走 502 直接返回、不写冷却。

**建议修复**：
```python
if typ in ('rate_limit','quota_exhausted'):
    state.observe_429(channel_id, ch.id, detail, limits=ch.limits)
```

另外注意此处调用 `observe_429(channel_id, ...)` 而主流程是 `observe_429(key, ...)`——`channel_id` 直接做 pool 也 OK（StateStore 内部不依赖 pool 命名一致性，看 `channel_tests` 表是 `pool,channel` 双主键，但 pool 命名不影响查询），但建议顺手对齐成 `observe_429(key, ch.id, detail, limits=ch.limits)` 更整洁。

---

### 🟡 P2 — 可优化：startup 使用 deprecated `on_event`

**文件**：`src/flex_llm_router/app.py:442`

```python
@app.on_event('startup')
async def _start_probe_loop():
```

pytest 运行报出 `DeprecationWarning: on_event is deprecated, use lifespan event handlers instead.`（25 条 warning 之一）。FastAPI 已把 `@app.on_event` 标为 deprecated，建议迁移到 `@asynccontextmanager` + `@app lifespan` 模式。**不影响功能**，但 @default 后续升级 FastAPI 版本时可能会变成 error。

---

### 🟡 P2 — 可优化：`probe` 探测自身 429 一律算失败，不分类

**文件**：`src/flex_llm_router/app.py:430-438`

```python
try:
    base, key_ = channel_credentials(ch, config.providers)
    await litellm.acompletion(model=ch.litellm_model, messages=[...], ..., max_tokens=1)
    state.clear_cooldown(pool_name, ch_id)
except Exception:
    state.record_probe(pool_name, ch_id, now=now, success=False)
```

探测本身如果返回 `quota_exhausted`（A 类），理应**保持冷却而不是重试探测**——现在 `record_probe(success=False)` 只是延长节流窗口，本身 OK；但如果探测返回 `rate_limit`（B 类，可能是上游瞬时忙），理论上也不应该 `clear_cooldown` 而应该继续冷却。当前"探测非 2xx 一律记失败"的策略是**保守可接受的**，不影响 P0 功能；仅在运维精细化上有优化空间。

---

### 🟡 P2 — config 中 `RetryPolicy.retry_on` 默认值不含 `quota_exhausted`

**文件**：`src/flex_llm_router/config.py:35`

```python
retry_on: list[str] = Field(default=['rate_limit', 'connection_error', 'timeout', 'server_error'])
```

**@default 的疑虑**：P0-3 只靠 `observe_429` 接冷却、不靠 `retry_on`，是否足够？

**结论**：✅ **足够，不需要改默认值**。原因：
1. `quota_exhausted`（A 类）是**总量配额耗尽**，需要"提额"或"等窗口滑动"才能恢复——**重试同通道是浪费**，不应该在 `retry_on` 里。
2. `rate_limit`（B 类，rpm/tpm）是**瞬时窗口限流**，等几十秒窗口滑动后可重试——所以在 `retry_on` 里。
3. 主流程第 374 行 `if typ in ('rate_limit','quota_exhausted'): observe_429(...)` **已经覆盖 A 类**：写冷却→下一次请求 `eligible` 检查冷却→不 eligible→切下游通道（`fallback.trigger: ['quota_exhausted', ...]` 已包含）。A 类处理路径是：冷却 + 下游切换 + 回切探测，**不需要走同通道重试**。

**结论**：`retry_on` 默认值无需补 `quota_exhausted`。补上反而会让主流程在 A 类 429 后重试同通道浪费窗口。

---

## 5. 安全/敏感信息检查

- ✅ `.env` 未读、未写入对话
- ✅ `channel_credentials` 从 env 取，未硬编码
- ✅ `error_detail` 有 `re.sub(...redacted)` 脱敏（`app.py:58`）
- ✅ `config.py` 未暴露 api_key

---

## 6. 测试覆盖缺口（区分已有测试盲区）

- ✅ P0-1 已覆盖（预灌 row id，验证重试生效）
- ✅ P0-3 已覆盖（冷却写入 + 冷却到期恢复）
- ❌ **P0-2 回切探测无集成测试**——`test_routing_integration.py` 只有 3 个用例，没有探测试用。建议 @default 补一条：模拟通道先 `quota_exhausted` 冷却→等待/注入探针成功→期望 `clear_cooldown` 后下一个请求回到该通道。
- ❌ P1（`/test` 端点 A 类 429）未测试——如果修，建议配一条 `test_p0_3_test_endpoint_quota_exhausted_also_cools`

---

## 7. 亮点

- 主循环 `tried` / `retries` / `attempt` 三个变量职责分离清晰，语义干净
- `error_type` 函数把 A/B 类 429 分类（`app.py:21-37`）作为单一入口，app 层与 state 层都可复用
- `_probe_loop` 用 `try/except` 双层包裹（循环体 + 单次探测），异常不扩散、不拖垮进程，**运维友好**
- `RetryPolicy.retry_on` 默认值保持纯 B 类瞬时限流，**设计意图正确**

---

## 8. 下一步建议

1. **合入 P0 修复** ✅（三处改动均通过复核）
2. **可选 P1 一并补**：`app.py:320` 同步 `quota_exhausted` 分支
3. **可选 P2 记录**：`on_event` deprecation → 后续迁移 lifespan
4. **建议新增 `test_p0_2_probe_loop_reattaches_after_probe_success`**，把回切探测纳入回归保护
5. P1/P2 批次可以独立推进，不阻塞 P0 合入

---

**最终判定**：@default 三个 P0 修复**通过复核**，测试 32/32 passed 属实，源码无回归。
