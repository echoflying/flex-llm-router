# 最终 REVIEW 报告 — flex-llm-router
> 审阅流程：@collector 初审（REPORT-01）→ @default 复核（REPORT-02）→ @collector 确认 → @default 汇总
> 日期：2026-08-21
> 范围：源码 `src/flex_llm_router/{app,state,scheduler,config}.py`、配置 `config/pools.yaml`、文档、测试（29/29 复跑通过）
> 未改动任何项目文件，纯静态审阅 + 测试实测。

---

## 0. 一句话结论
架构清晰、文档详尽、安全面扎实、测试全过，但存在 **3 个 P0 级真实缺陷**（皆为"设计承诺 vs 代码实现"脱节 / 一处分支漏接），**投产前必须先修 P0**。

---

## 1. 分级汇总（经双方确认的最终版）
| 级别 | 条数 | 条目 |
|---|---|---|
| 🔴 P0 | 3 | P0-1 重试计数 bug / P0-2 回切探测未接线 / P0-3 A 类 429 裸奔 |
| 🟠 P1 | 4 | P1-1 min_stable 零使用 / P1-2 max_fallback_tiers 死配置 / P1-3 rpm 闸门缺 pool / P1-re-attach 块死配置 |
| 🟡 P2 | 5 | P2-1~P2-5 |
| ❌ 删除 | 1 | 原 P1-5（safe_tpm 无 bug，复核后撤回） |
| 🟢 安全 | ✅ | 通过 |

---

## 2. P0 — 必须修（阻塞投产）

### P0-1 · `attempt < rp.max_retries` 拿自增行 id 当重试计数（`app.py:377`）
- **根因**：`state.start()`（`state.py:193`）返回 `INSERT…lastrowid` = `attempts` 表自增主键（全局累计）；`app.py:377` 用它比 `max_retries`（默认 3）。
- **后果**：系统累计处理过 3 次请求后，`attempt` 全局 ≥ 3，**所有后续同类错误永久跳过重试**，直接走 fallback 或 502。生产几乎立刻触发。
- **状态**：✅ **已修复**（`app.py`）：新增 `retries=0` 计数，重试判定改 `retries < rp.max_retries`，`continue` 前 `retries += 1`；`attempt` 仅作 DB 主键不变。指数退避基数同步改 `retries`。返回体 `retry_attempts` 也改为 `retries`。
- **验证**：`tests/test_routing_integration.py::test_p0_1_retry_uses_per_request_counter_not_rowid` 预灌 5 行历史（row id≥6），仍按本请求计数重试 2 次后成功。

### P0-2 · 回切探测 Y 方案 文档已承诺、代码未接线
- **根因**：`should_probe`/`record_probe`/`clear_cooldown`（`state.py:137-154`）完整实现，但 `app.py` 全文零调用，无后台 `asyncio.create_task` 触发探测。
- **后果**：`busy`/`quota_exhausted` 通道只能等冷却自然到期才回切，"主动探测提前恢复"机制不存在，免费额度闲置被拉长。
- **状态**：✅ **已修复**（`app.py`）：在 `create_app` 加 `@app.on_event('startup')` 启 `_probe_loop`（周期 120s）；遍历所有 pool 冷却中（busy/quota_exhausted）通道，过 `should_probe` 节流后发最小探测请求，成功 `clear_cooldown` 提前回切、失败 `record_probe`。消费 `selection.fallback.reattach.probe_before_switch_back` 配置开关。
- **验证**：依赖真实上游 + 后台任务，未设确定性单测（探测周期长、需真实通道）；修复以静态接线 + 启动不报错（32 passed 含 startup 事件）为准。

### P0-3 · A 类 `quota_exhausted` 429 完全裸奔（原 P1-4 升格）
- **根因**：`error_type()`（`app.py:30-31`）正确判 A 类为 `quota_exhausted`；但
  - `app.py:374` 仅 `if typ=='rate_limit': observe_429(...)` → A 类不进 `observe_429`，`_cool(quota_exhausted)` 长冷却**不触发**；
  - `retry_on` 默认（`config.py:35`）不含 `quota_exhausted` → 不重试；
  - `app.py:385` `failure_trigger` 只认 `connection_error/timeout/server_error` → 不切下一通道；
  - 直接 `app.py:387` 502 返回。
- **后果**：配额耗尽时该通道**无冷却、不重试、不切换**，且 `available` 列表无排除机制，下一轮仍可能再选中它，反复撞同一 502 —— **服务级中断**。
- **状态**：✅ **已修复**（`app.py` 主流程 + 流式分支）：`except` 分支 `if typ in ('rate_limit','quota_exhausted'): state.observe_429(key,ch.id,detail,limits=ch.limits)`（非流式 374 行 + 流式 402 行一致处理）；`observe_429` 内部 A 类 `_cool(quota_exhausted)` 长冷却本就正确，接入即生效。
- **验证**：`tests/test_routing_integration.py::test_p0_3_quota_exhausted_triggers_cooldown` 断言 A 类 429 后 `states` 表写入 `quota_exhausted` 冷却；`::test_p0_3_observe_429_recovers_channel_after_cooldown_expiry` 断言冷却到期后通道恢复可用。

---

## 3. P1 — 应当修（重要）
- **P1-1 · `min_stable_seconds` 零使用**：配置 3600（`pools.yaml:128`），但 `scheduler.select` 不读 `selection.stickiness`，也无"通道首次选中时间"记录，保缓存稳定期约束未实现。
- **P1-2 · `max_fallback_tiers` 死配置**：`scheduler.cost_aware` 仅 `sorted(tier)[0]`，不校验选中 tier 是否超出 `current_tier + max_fallback_tiers`。
- **P1-3 · rpm 闸门缺 pool（范围收窄）**：仅 `eligible` rpm 闸门（`state.py:175`）、`calls_today`（210）、dashboard `calls_last_minute`（213）三处 `WHERE channel=?` 缺 pool；A 类配额闸门（`quota_status`/`_quota_calls` 已带 pool）不受影响。仅当"同一 channel id 跨 pool 复用"时污染。
- **P1-re · `selection.fallback.reattach` 整块死配置**：`probe_before_switch_back`/`quiet_window_seconds`/`quota_recover_seconds`/`failure_retry_after`（`pools.yaml:122-126`）在 app/scheduler 零引用，与 P1-1 同类。

---

## 4. P2 — 建议优化
- **P2-1** `load_dotenv` 在 config 载入处被调两次（`app.py:94` + `set_override:265`）。
- **P2-2** `set_override` 切换后不 reload 内存 `config`（内存为 `create_app` 快照，须 restart 才生效）。
- **P2-3** `FLEX_OVERRIDE` 环境变量写两次（`app.py:94/267`）但全项目无读取，死变量。
- **P2-4** `restart` 端点强耦合 macOS `launchctl`（`app.py:328-334`），跨平台静默失败。
- **P2-5** 版本不一致：`__init__.py:3` = 0.1.0，`FastAPI(version='0.2.0')`（`app.py:94`）。

---

## 5. 安全 ✅ / 测试
- **安全**：`.gitignore`（`.env`/`*.db`/`logs`/`data`/`*.bak`）、`session-hmac.key` `0o600`+随机密钥、`error_detail` 脱敏 API key/auth/token 全部通过。
- **测试**：修复前 29/29 → 修复后 **32/32 passed**。新增 `tests/test_routing_integration.py`（mock `litellm.acompletion`，不依赖真实上游）：
  - `test_p0_1_retry_uses_per_request_counter_not_rowid`：预灌 5 行历史（row id≥6）仍按本请求计数重试 2 次后成功 → **P0-1 可复现 + 已修**。
  - `test_p0_3_quota_exhausted_triggers_cooldown`：A 类 429 后 `states` 表写入 `quota_exhausted` 冷却 → **P0-3 可复现 + 已修**。
  - `test_p0_3_observe_429_recovers_channel_after_cooldown_expiry`：冷却到期后通道恢复可用 → **P0-3 闭环验证**。
  - `test_p0_2_probe_recovery_clears_cooldown`：冷却中通道经探测成功后 `clear_cooldown` 提前回切 → **P0-2 可复现 + 已修**（通过 `FLEX_PROBE_INTERVAL=0` 驱动后台循环跑一轮）。

---

## 6. 修复状态与下一步
- ✅ **P0 × 3 全部已修复并通过测试**（P0-1 重试计数 / P0-2 回切探测接线 / P0-3 A 类 429 接入 observe_429，含流式分支）。
- ✅ **collector REVIEW 指出的 P1 遗漏已补**：`/api/pools/{name}/channels/{id}/test` 端点（`app.py:320`）的 `observe_429` 同步 `quota_exhausted` 分支 + `limits=ch.limits`。
- ✅ **collector 两条非阻塞建议已落实**：① 探测 Task 存入 `app.state.probe_tasks` 保活 + `add_done_callback(discard)`；② fallback 切通道时 `retries=0` 重置，per-channel 独立重试预算。
- ✅ **探测间隔可配置化**：`_probe_loop` 间隔读 `FLEX_PROBE_INTERVAL` 环境变量（默认 120s），便于测试驱动。
- ✅ **测试 33/33 passed**（原 29 + P0-1/3/3恢复/2 共 4 新增集成测试）。
- ⏳ **P1 × 4（除已补的 test 端点外）/ P2 × 5 未修**（死配置清理：`min_stable_seconds`/`max_fallback_tiers`/`reattach`、P1-3 缺 pool、跨平台 restart、版本号一致、`on_event`→lifespan），待用户决定是否继续。
- 改动文件：`src/flex_llm_router/app.py`（P0-1/2/3 + test 端点 + task 保活 + retries 重置 + 间隔配置）、`tests/test_routing_integration.py`（新增 4 例）。未改任何 `.env`/配置文件/其他源码。

---
*报告结束。三方确认：collector 初审 → default 复核 → collector 接受全部修正 → default 汇总 → default 修复 P0（32/32）→ collector REVIEW 修复（确认 3 P0 可合 + 指 P1 遗漏/test端点 & 2 非阻塞建议）→ default 补 test 端点 + 落实 2 建议 + 补 P0-2 回归测试（33/33 passed）→ 待 collector REVIEW 本轮修复。*
