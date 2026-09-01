# REVIEW 报告 02 — DEFAULT 对 COLLECTOR 初审的复核
> 历史审计快照：结论对应报告日期的代码版本，不是当前实现规范；请以根目录 README、DESIGN 和 docs/ROUTER_RESILIENCE.md 为准。
> 复核对象：`review/REPORT-01-collector.md`
> 复核人：@default（主会话）· 时间：2026-08-21
> 方法：逐条对照源码 `app.py` / `state.py` / `scheduler.py` / `config.py` / `pools.yaml`，并复跑 `pytest`（29 passed）确认测试基线。未改动任何项目文件。

---

## 0. 总体结论
COLLECTOR 的初审**大方向正确、质量高**，尤其 P0-1（重试计数 bug）、P0-2（回切探测未接线）抓得准。但有三处需要修正/精确化：

- **一处误判（P1-5 不成立）**：`safe_tpm` 经 `max(1, …)` 后不可能为 0，"未学(NULL)"与"学到值(≥1)"语义清晰，**无 bug**，应删除该条。
- **一处低估（P1-4 实为高危）**：A 类 `quota_exhausted` 429 实际是**完全裸奔**——既不重试、也不切通道、也不触发任何冷却，直接 502 且通道持续被打。比 collector 描述的"只影响 learned_limits 学习"严重得多，建议**升为 P0-3**。
- **一处需精确化（P1-3 范围写偏）**：真正缺 pool 限定的只有 `eligible` 的 rpm 闸门、及两处 dashboard 计数；A 类配额闸门（`quota_status`/`_quota_calls`）已带 pool，不受影响。

---

## 1. 对 COLLECTOR 各条的逐项裁定

### P0-1 · `attempt < rp.max_retries` 拿行 id 当重试计数 — ✅ 确认属实，真 bug
- `state.start()`（`state.py:193`）返回 `INSERT … .lastrowid` = attempts 表自增主键，从 1 累计，非本请求重试计数。
- `app.py:377` `if typ in rp.retry_on and attempt < rp.max_retries` 用它比 `max_retries`（默认 3，`config.py:33`）。
- **影响量化**：系统累计处理过 3 次请求（任意通道、任意成败）后，`attempt` 全局 ≥ 3，所有后续同类错误**永久跳过重试**，直接走 fallback 或 502。生产环境几乎立刻触发。这是最可能在真实流量里踩的雷，collector 排第一正确。

### P0-2 · 回切探测 Y 方案未接线 — ✅ 确认属实
- `should_probe`/`record_probe`/`clear_cooldown`（`state.py:137-154`）**完整实现**，但 `app.py` 全文零调用，也无后台 `asyncio.create_task` 触发探测。
- 后果：`busy`/`quota_exhausted` 通道只能等冷却定时器自然到期才回切，"主动探测提前恢复"机制不存在。确认。

### P0-3（由 P1-4 升格）· A 类 `quota_exhausted` 429 完全裸奔 — ⚠️ 升级，比原描述严重
- `error_type()`（`app.py:30-31`）把"allocated quota exceeded"类判为 `'quota_exhausted'`。
- 主流程 `app.py:374` 仅 `if typ=='rate_limit': state.observe_429(...)` —— A 类 typ 是 `'quota_exhausted'`，**不进 observe_429**，所以 `observe_429` 内部的 `_cool(quota_exhausted)` 长冷却**根本不触发**。
- 且 `retry_on` 默认（`config.py:35`）= `['rate_limit','connection_error','timeout','server_error']`，**不含 `quota_exhausted'`** → `app.py:377` 不重试；`app.py:385` 也不切下一通道 → 走 `app.py:387` 直接 `raise HTTPException`。
- **最终后果**：配额耗尽时，该通道**无冷却、不重试、不切通道**，持续被选中并持续 502。collector 说"只影响 learned_limits 学习、无兜底"写浅了——实际是**通道在该窗口内彻底不可用且反复被打**的服务级中断。建议与 P0-1/P0-2 同列 P0。
- 注：`observe_429` 自身逻辑是对的（传入 kind=None 时会按 detail 文本把"quota exceeded"推为 `quota_exhausted` 并 `_cool`），**根因在 app.py 没把它接入 A 类分支**，补一个 `if typ in ('rate_limit','quota_exhausted'): state.observe_429(...)` 即可修。

### P1-1 · `min_stable_seconds` 零使用 — ✅ 确认属实
- 配置存在（`pools.yaml:128`、`config.py:73` 默认值 3600），但 `scheduler.select`（`scheduler.py:13-40`）完全不读 `selection.stickiness`，也无"通道首次选中时间"记录。保缓存稳定期约束未实现。确认。
- **补充**：同属"文档/配置有、代码无"的还有 `selection.fallback.reattach` 整块（`probe_before_switch_back` / `quiet_window_seconds` / `quota_recover_seconds` / `failure_retry_after`，`pools.yaml:122-126`）——`app.py` 的 fallback 处理（`app.py:382-385`）只读 `trigger`/`retry_next_channel_on`，**整块 reattach 配置未被消费**。见下方新增 P2-7。

### P1-2 · `max_fallback_tiers` 未裁剪 — ✅ 确认属实（措辞微调）
- `scheduler.select` 的 `cost_aware` 仅 `sorted(enabled, key=tier)[0]`，不校验选中 tier 是否超出 `current_tier + max_fallback_tiers`。`max_fallback_tiers` 是死配置。确认。
- 微调：回切/下切的"深度"实际在 `app.py` 主循环里靠 `eligible` 排除冷却通道隐式实现，并非显式按 `max_fallback_tiers` 限制，collector 把它归到 scheduler 略有错位，但"字段未被校验"结论正确。

### P1-3 · RPM 闸门查询未带 pool — ⚠️ 方向对，范围需精确化
- **确实缺 pool 的三处**：`state.eligible` 的 rpm 本地闸门（`state.py:175` `WHERE channel=? AND started>=?`）、`calls_today`（`state.py:210`）、`channels_state` 的 `calls_last_minute`（`state.py:213`）。
- **已带 pool、不受影响**：`window_metrics`（`state.py:71`）、`quota_status`/`_quota_calls`（`state.py:58`）—— A 类配额闸门正确带 pool。
- **collector 误差**：他说"observe_429 都未带 pool"，但 `observe_429` 根本不查 `attempts` 表（查 `learned_limits`），拉它进来是误导。核心 bug 是 rpm 本地闸门 + dashboard 计数缺 pool，且仅在"同一 channel id 跨 pool 复用"时才会污染（配置层面允许，属边缘情况）。维持 P1，但影响范围收窄到 rpm/计数，不波及配额。

### P1-4 → 见上方 P0-3（升格，描述修正）

### P1-5 · `safe_tpm` 用 0 冒充 NULL — ❌ 不成立，删除
- `observe_429`（`state.py:93`）`safe_tpm=max(1,int(...))` 保证学到值 **≥1，永不为 0**。
- "未学到"分支走 `else: safe_tpm=None`（行93 仅命中 tpm 关键词才赋值），即 NULL。
- 下游 `eligible`（`state.py:183`）`if learned['safe_tpm'] and …` 用 None/正整数均正确判性。列本身 `INTEGER` 可空，设计无误。
- **结论**：collector 假设 safe_tpm 可能为 0，但 `max(1,…)` 已排除；"未学(NULL)"与"学到值(≥1)"语义干净，**无 bug**。该条删除。

---

## 2. P2 项复核
- **P2-1**（load_dotenv 调两次）：✅ 属实（`app.py:94` 内 load_config → load_dotenv；`set_override:265` 再 load_dotenv）。但 collector 说"首次 override 读取时机晚于 dotenv"逻辑表述混乱——实际 `override_on` 在 `load_config` 前已从 `setup.conf` 读出，首次即正确。轻微，维持。
- **P2-2**（set_override 不 reload 内存 config）：✅ 属实。内存 `config` 是 `create_app` 快照，切换 override 后不刷新，须 restart。与 P2-4 关联。维持。
- **P2-3**（FLEX_OVERRIDE 死变量）：✅ 属实（设两次 `app.py:94/267`，全项目无读取）。维持。
- **P2-4**（restart 耦合 launchctl）：✅ 属实（`app.py:328-334`），跨平台静默失败。维持。
- **P2-5**（版本不一致）：✅ 属实（`__init__.py:3` = 0.1.0，`FastAPI(version='0.2.0')` `app.py:94`）。维持。
- **P2-6**（error_detail 脱敏）：✅ 良好。`error_detail`（`app.py:58`）正则脱敏 API key/auth/token，channel_tests 走同一路径。维持。
- **P2-7（新增）· `selection.fallback.reattach` 整块配置未被消费**：`pools.yaml:122-126` 的 `probe_before_switch_back`/`quiet_window_seconds`/`quota_recover_seconds`/`failure_retry_after` 在 `app.py` 与 `scheduler.py` 均无引用，与 `min_stable_seconds` 同类死配置。建议要么接进回切逻辑（配合 P0-2），要么从文档/配置显式标注"暂未实现"。

---

## 3. 安全 / 测试 复核
- **安全 ✅**：`.gitignore`（`.env` / `*.db` / `logs/` / `data/` / `*.bak`）齐全；`session-hmac.key` `0o600` + 随机密钥（`state.py:19-21`）；`error_detail` 脱敏。collector 判定通过，确认。
- **测试 ✅ 基线 29/29 passed（已复跑）**，但盲区属实：`test_app.py` 全测 HTML/API 静态路径，**未 mock `litellm.acompletion` 走真实路由主流程**——P0-1/P0-3 的 bug 路径、回切探测、streaming 错误处理均无覆盖。补充：`test_state.py` 测了 `observe_429` 单测，但**只覆盖 B 类（rpm/tpm）**，未测"A 类 quota_exhausted → observe_429 → `_cool`"这一本应成立、却被 app 层漏接的路径（可补一例证明 observe_429 内部正确，反衬 app 层漏接）。

---

## 4. 修正后的最终分级建议
| 级别 | 条 | 要点 |
|---|---|---|
| 🔴 P0 | 3 | P0-1 重试计数 bug；P0-2 回切探测未接线；**P0-3 A 类 429 裸奔（原 P1-4 升格）** |
| 🟠 P1 | 4 | P1-1 min_stable 零使用；P1-2 max_fallback_tiers 死配置；P1-3 rpm 闸门缺 pool（范围收窄）；P2-7 reattach 块死配置 |
| 🟡 P2 | 5 | P2-1~P2-5（维持） |
| ❌ 删除 | 1 | P1-5（safe_tpm 无 bug） |
| 🟢 安全 | ✅ | 通过 |

**修复优先级**：P0-1（改 `attempt` 为 per-request retry counter）+ P0-3（app.py:374 放行 `quota_exhausted` 进 observe_429）+ P0-2（app.py 接 should_probe/clear_cooldown 后台任务）先修，再补路由集成测试把三个 P0 变成可复现用例。

---
*复核结束。以上基于对 `~/projects/flex-llm-router` 源码的逐行对照与 pytest 复跑，未改动任何项目文件。待 @collector 确认后由 @default 汇总最终报告。*
