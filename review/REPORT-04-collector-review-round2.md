# REPORT-04 — Collector 复核：第 2 轮修复确认

**审阅对象**：`src/flex_llm_router/app.py`、`tests/test_routing_integration.py`
**前置报告**：`review/REPORT-01-collector.md`、`review/REPORT-02-default.md`、`review/REPORT-FINAL.md`、`review/REPORT-03-collector-review-fix.md`
**审阅人**：@collector
**日期**：2026-08-21

---

## 1. 总体结论

**✅ 第 2 轮 5 项改动全部通过复核，33/33 passed 属实（本地复跑确认）**。P0 三兄弟（重试计数 / A 类 429 冷却 / 回切探测）至此全闭环；上轮 P1 遗漏补上；非阻塞建议两条落地；回切探测集成测试首次补齐。可合入。

无新增 P0。P2 死配置（`on_event` deprecation / 探测 429 分类）可按你节奏留在后续批次，不阻塞本轮。

---

## 2. 分维度评分

| 维度 | 评分 | 备注 |
|---|---|---|
| 设计 | ★★★★★ | 三项 P0 全部落地且接线一致 |
| 正确性 | ★★★★★ | `app.state.probe_tasks` 位置正确、`retries=0` 重置语义正确、`FLEX_PROBE_INTERVAL` 默认回退正确 |
| 安全 | ★★★★★ | 无敏感外泄 |
| 测试 | ★★★★★ | 4 个 P0 用例全部覆盖（P0-1 / P0-3 / P0-3 恢复 / P0-2 回切探测） |
| 运维 | ★★★★ | `on_event` 仍 deprecated（P2 遗留）；probe 探测自身 429 不分类（P2 遗留） |

---

## 3. 五项改动逐项复核

### ✅ 改动 1 — `app.py:320` test 端点同步 `quota_exhausted`

```python
# 改前
if typ=='rate_limit': state.observe_429(channel_id,ch.id,detail)
# 改后
if typ in ('rate_limit','quota_exhausted'): state.observe_429(channel_id,ch.id,detail,limits=ch.limits)
```

**核实**：`app.py:320/374/402` 三处 `observe_429` 现在**三处分支一致**，`limits=ch.limits` 正确。全局 grep `if typ=='rate_limit'` 与 `if typ in ('rate_limit','quota_exhausted')` 结果：
- `app.py:320` ✅ 同步
- `app.py:374` ✅ 同步
- `app.py:402` ✅ 同步
- `app.py:385` ❌ 不是 observe_429 调用点（是 fallback 切换判定），不涉及
- **无遗漏分支** ✅

`state.py` 中 `observe_429` 只有一个定义 + `quota_exhausted` 只在内部分支判断，无需改动。

---

### ✅ 改动 2 — `app.state.probe_tasks=set()` 位置正确

**源码 `app.py:94`**：
```python
state=StateStore(...); app=FastAPI(title='Flex LLM Router',version='0.2.0'); app.state.probe_tasks=set()
```

**位置核实**：在 `create_app` 函数体同一语句块、`app = FastAPI(...)` 之后立即赋值，先于：
- `render(...)` 定义（第 95 行）
- 所有 `@app.get`/`@app.post` 路由
- `@app.on_event('startup')`（第 442 行）

**影响评估**：FastAPI 的 `app.state` 是 `StateTrio` 对象，允许动态加属性，`app.state.probe_tasks=set()` 是常规用法——**不影响已有逻辑** ✅。

**startup 内 `_start_probe_loop` 使用**（第 444-445 行）：
```python
t = asyncio.create_task(_probe_loop())
app.state.probe_tasks.add(t); t.add_done_callback(app.state.probe_tasks.discard)
```

- `add_done_callback(discard)` 保证 task 完成后从 set 移除，**不泄漏 task 引用**
- 生产意义：进程收到 shutdown signal 时可 `await asyncio.gather(*app.state.probe_tasks)` 优雅等待。虽然当前 `restart()` 端点用 `launchctl kickstart -k` 硬重启不需要，但属良好实践。✅

---

### ✅ 改动 3 — fallback 切换 `retries=0` 重置

**源码 `app.py:385`**：
```python
if not stream and (typ in retry_on or (failure_trigger and typ in ('connection_error','timeout','server_error'))):
    retries=0; continue  # 切下一通道时重置本请求重试预算，per-channel 独立
```

**语义核实**（回答你的疑问②）：

- 主流程 `retries` 变量在第 354 行初始化为 0，每发一次请求（`attempt = state.start(...)` 第 369 行）后失败才可能被 `retries+=1`（第 380 行）推进。
- 第 385 行是"本次请求在本通道 retry 预算耗尽后，因 `fallback.retry_next_channel_on` 命中而切下一通道"的路径。
- 切下一通道时**重置 `retries=0`** → 下一个通道**拥有独立的重试预算**（最多再 `rp.max_retries` 次），而不是"本请求已用了 3 次重试，再切通道就直接 502"。
- 语义正确性：**每个通道的重试预算独立** 是合理的（每个通道失败模式不同，一个通道 429 三次不代表下一个通道也会 429）。

**确认不会破坏"重试耗尽"语义**：
- 同一通道耗尽：第 377 行 `retries < rp.max_retries` 判定，retries 达到上限后不走重试，直接进第 382-385 行 fallback 判定。
- 重置只作用于**切换通道之后**，已耗尽的通道已被 `tried.add(ch.id)` 排除（第 369 行），不会重试。
- ✅ 逻辑正确。

**小注（P2 边缘）**：`retry_on` 是从 pool.selection 取的，如果 pool 未配置 `retry_next_channel_on` 默认为 `[]`（`config.py:75`），此时会走第 386 行直接 `raise HTTPException`，行为与之前一致。无回归。

---

### ✅ 改动 4 — `_probe_loop` 探测间隔可配置

**源码 `app.py:408/412`**：
```python
async def _probe_loop(interval_seconds: int | None = None):
    ...
    await asyncio.sleep(interval_seconds or int(os.getenv('FLEX_PROBE_INTERVAL', '120')))
```

**核实**：
- 默认 `interval_seconds=None` → 走 `os.getenv('FLEX_PROBE_INTERVAL','120')` → 生产默认 120s 不变 ✅
- 测试可通过环境变量压到 0，便于驱动一轮 ✅
- 边界：`FLEX_PROBE_INTERVAL='0'` 时 `int('0')` = 0，`interval_seconds or 0` = 0，`asyncio.sleep(0)` 直接让出事件循环（等价于 `asyncio.sleep(0)`），不会阻塞 ✅
- 边界：`FLEX_PROBE_INTERVAL` 非数字 → `int()` 抛 `ValueError` → 被 `except Exception as exc`（第 439 行）吞掉，记录日志后进入下一轮 `sleep`（此时 `interval_seconds` 仍为 None 走 env 读取，会再次抛）——**这是一个潜在的死循环打日志**。但生产环境不会随便设非数字值，且 `FLEX_PROBE_INTERVAL` 只由运维/测试设置，不算阻断。属 P2 边缘可优化。

---

### ✅ 改动 5 — `test_p0_2_probe_recovery_clears_cooldown` 实现

**测试结构**（`test_routing_integration.py:118-157`）：

1. 设置 `FLEX_STATE_DB` + `FLEX_PROBE_INTERVAL=0`
2. 用 `monkeypatch.setattr(app_mod.asyncio, 'create_task', _capture_create_task)` 拦截 `asyncio.create_task`，把探测 task 抓进 `captured['task']`
3. `with client:` 上下文触发 FastAPI startup 事件（`create_task` 被调用 → `captured['task']` 赋值）
4. 预置某通道 `quota_exhausted` 冷却（`until=9e9` 保证冷却未到期）
5. 用 `_asyncio.run_coroutine_threadsafe(_asyncio.wait_for(captured['task'], timeout=2), loop)` 在主 loop 外部驱动探测 task 跑一轮（探测成功 → `clear_cooldown`）
6. 断言 `states` 表该条目被删

**回答你的疑问③**：

- ✅ `run_coroutine_threadsafe` + `wait_for` + `timeout=2` 组合是驱动"while True + sleep(0)"循环跑一轮的**正确模式**。因为 FLEX_PROBE_INTERVAL=0，sleep(0) 立即让出，第一轮循环体执行完毕后下一次 sleep 又让出，2 秒超时足够探测多次。
- ✅ 探测 `acompletion` 被 `_make_client` 的 `fake_acompletion` mock 为 `_ok_response()`（成功），所以 `clear_cooldown` 必然被调用。
- ✅ `assert row is None` 断言直接查 DB，确认 `clear_cooldown` 的 `DELETE FROM states` 生效。
- ⚠️ **一处小风险**：`_make_client` 里 `fake_acompletion` 只拦截 `litellm.acompletion`，但测试内 `probe` 探测调用的是同一个函数——**同一 mock 会被探测循环消费**（`fake_acompletion` 的 `responses` 列表只有 `[_ok_response()]`，`idx['i']` 递增到 1 后走 `responses[-1]` 复用）。因此探测不会因 mock 用尽而抛异常。**安全**。
- ✅ `except (_asyncio.TimeoutError, Exception) as exc: pass` 吞掉超时 + 可能的 task cancelled 异常，避免污染断言。

**测试真实命中修复路径**：
- 拦截 `asyncio.create_task` → 确认 startup 事件真的触发了探测循环（`captured.get('task') is not None` 断言第 137 行）—— 这是**"接线"** 层面的回归保护
- `clear_cooldown` 删除 states 条目 → 确认**"探测成功 → 回切"** 逻辑层回归保护
- 两层保护齐全 ✅

---

## 4. 全局 `observe_429` 调用点扫描（回答疑问④）

`src/flex_llm_router/` 内所有 `observe_429` 出现：

| 文件:行 | 代码 | 状态 |
|---|---|---|
| `app.py:320` | `if typ in ('rate_limit','quota_exhausted'):state.observe_429(...)` | ✅ |
| `app.py:374` | `if typ in ('rate_limit','quota_exhausted'):state.observe_429(...)` | ✅ |
| `app.py:402` | `if typ in ('rate_limit','quota_exhausted'):state.observe_429(...)` | ✅ |
| `state.py:76` | `def observe_429(self,...)` | 定义，不动 |

**无 `if typ=='rate_limit'` 遗漏分支**。✅ 三处分支完全对齐。

---

## 5. 安全 / 敏感信息检查

- ✅ `.env` 未读
- ✅ 无硬编码密钥
- ✅ `error_detail` 脱敏保留

---

## 6. 测试覆盖

| 测试 | 覆盖 | 状态 |
|---|---|---|
| `test_p0_1_retry_uses_per_request_counter_not_rowid` | P0-1 重试计数 | ✅ |
| `test_p0_3_quota_exhausted_triggers_cooldown` | P0-3 A 类 429 冷却 | ✅ |
| `test_p0_3_observe_429_recovers_channel_after_cooldown_expiry` | P0-3 冷却到期恢复 | ✅ |
| `test_p0_2_probe_recovery_clears_cooldown` | P0-2 回切探测接线 + 清除 | ✅ **新增** |

4 个 P0 用例全覆盖 ✅

---

## 7. 遗留 P2（不阻塞本轮）

1. `@app.on_event('startup')` deprecation → 后续迁移 `lifespan` 上下文管理器
2. `_probe_loop` 内 `FLEX_PROBE_INTERVAL` 非数字时 `int()` 抛 → 建议加 `try: ... except (ValueError, TypeError): interval_seconds = 120`
3. 探测自身 429 分类：`except Exception: record_probe(..., success=False)` 不区分 A/B 类（保守可接受）

---

## 8. 亮点

- 三项 P0 接线完全对齐（三处 `observe_429` 语义、`limits` 参数、分支一致）
- `app.state.probe_tasks` 用 `set` + `done_callback.discard` 是优雅任务管理的正确姿势
- 测试 `monkeypatch asyncio.create_task` 抓 task + `run_coroutine_threadsafe` 驱动探测，是驱动后台循环测试的标准技巧，**测试技法好**
- `retries=0` 重置位置在第 385 行 `continue` 前，不破坏已有"重试耗尽"判定

---

## 9. 下一步建议

1. **合入第 2 轮 5 项改动** ✅
2. **P0 全闭环确认** ✅
3. P1/P2 死配置（deprecation / 类型守卫 / 探测分类）等用户与你定节奏

---

**最终判定**：@default 本轮 5 项改动**全部通过复核**，33/33 passed 属实，源码无遗漏分支、无回退、无回归。**P0 三兄弟全闭环**。
