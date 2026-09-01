# REVIEW 报告 01 — COLLECTOR 初审
> 历史审计快照：结论对应报告日期的代码版本，不是当前实现规范；请以根目录 README、DESIGN 和 docs/ROUTER_RESILIENCE.md 为准。
> 审阅对象：本项目 checkout
> 审阅人：@collector · 时间：2026-08-21
> 范围：源码(`src/flex_llm_router`)、配置(`config/pools.yaml`)、设计文档(`DESIGN.md`/`docs/routing-and-throttling.md`)、测试(`tests/`)、模板(`templates/`)

---

## 0. 总体结论
项目架构清晰、设计文档详尽、A/B 429 分类与 cost-tier 调度模型合理，**29/29 测试全过**。但存在一个**严重问题**：设计文档承诺的核心机制（回切探测、缓存防抖稳定期、下切深度）在代码里**大量"只配未用"**，文档与实现严重脱节；另有一处疑似会导致线上事故的重试逻辑 bug。建议**先修 P0/P1 再投产**。

---

## 1. 评分（分维度）
| 维度 | 评分 | 说明 |
|---|---|---|
| 设计清晰度 | ★★★★☆ | 文档详尽，但文档与代码的对应关系需核实 |
| 代码正确性 | ★★★☆☆ | 1 处疑似 bug + 多处未实现功能 |
| 安全/敏感信息 | ★★★★★ | 脱敏、.gitignore、HMAC、文件权限都好 |
| 测试覆盖 | ★★★☆☆ | 单测全过，但缺重试/回切/流式/集成测试 |
| 可运维性 | ★★★★☆ | Dashboard/备份/restart 齐全，restart 强耦合 macOS |

---

## 2. P0 — 必须修（阻塞投产）

### P0-1 · `attempt < rp.max_retries` 逻辑错误（`app.py:377`）
```python
attempt=state.start(key,ch.id,ch.litellm_model, ...)   # ← 返回 SQLite 自增 row id
...
if typ in rp.retry_on and attempt < rp.max_retries:     # ← 拿 row id 当"重试次数"
```
`state.start()` 返回的是**数据库自增主键**，从 1 开始累计（不是本请求的 retry 计数）。
- 当前效果：只要历史请求总数 ≥ `max_retries`（如 3），**所有后续请求的同类错误都会永久跳过重试**，直接走 fallback 或报错。
- 修复方向：单独维护一个 per-attempt 的 retry counter，从 0 开始，每次 `continue` 重试时 +1。

### P0-2 · 回切探测 Y 方案 文档已承诺但代码未接线（`app.py`）
设计文档 §3.6.2 要求：冷却中的通道在到期前 `asyncio.create_task` 异步发探测 → 成功 `clear_cooldown` → 失败节流。
实际：`state.py` 里 `should_probe` / `record_probe` / `clear_cooldown` **都实现了**，但 **`app.py` 全程没有调用它们**，也没有后台任务触发探测。结果是 `busy`/`quota_exhausted` 通道只能靠**冷却定时器自然到期**回切，"主动探测 + 提前恢复"机制完全不存在。
- 风险：恢复时间被拖到最长冷却时长，免费额度闲置时间变长（与设计"平滑消耗额度"目标冲突）。

---

## 3. P1 — 应当修（重要）

### P1-1 · `min_stable_seconds`（缓存防抖）形同虚设
设计 §3.6.4 强调"选了通道要稳定跑 `min_stable_seconds` 才因平衡切换，保 KVCache"。配置默认 3600s。
但代码**没有记录"当前通道首次被选中时间"**，scheduler 与 affinity 都无法衡量"已稳定多久"，该字段**只在 config 里存在，零使用**。切回/切换的缓存抖动保护实际不存在。

### P1-2 · `max_fallback_tiers` 未做裁剪
设计 §3.6.3 用此字段控制下切深度（"不无限下切"）。配置 = 2（全链式 0→1→2）。
但 `scheduler.select` 只做 `min(tier)`，**不校验选中的 tier 是否超出 `current_tier + max_fallback_tiers`**。如果未来配置出错或 tiers 跨度过大，保护无效。

### P1-3 · RPM 本地闸门查询未带 pool 限定（`state.py:175/210/213`）
```python
n=self.db.execute('SELECT COUNT(*) FROM attempts WHERE channel=? AND started>=?',(ch_id,now-60)).fetchone()[0]
```
`attempts` 表有 `pool` 列，但 RPM / `calls_today` / dashboard 的 `calls_last_minute` 都**只按 channel id 查**。若未来**同一 channel id 出现在多个 pool**（配置允许 channel 跨 pool 复用），三个计数器会互相污染。`quota_status` / `window_metrics` / `observe_429` 等都已带 pool，这里不一致。

### P1-4 · `quota_exhausted` 429 未被 observe（`app.py:374/402`）
```python
if typ=='rate_limit': state.observe_429(key,ch.id,detail)
```
A 类配额耗尽 429 在 `error_type()` 里被正确判为 `quota_exhausted`，但主流程**只在 `typ=='rate_limit'` 时才 observe**。结果：A 类 429 不写 `learned_limits`、不触发自己的冷却逻辑（靠 `app.py` 的什么兜底？——**实际没有兜底**，A 类 429 只会走 retry/fallback 流程，"长冷却 3600s 等额度"的 A 类处置路径未落地）。
> 这是设计文档 §1 / §3.2 的核心要求，目前只实现了"B 类走 observe"的一半。

### P1-5 · `learned_limits.safe_tpm` 用 0 充当 NULL
`observe_429` 中 `safe_tpm=max(1, int(...))` 永远不会是 NULL，"未学到"和"学到 0"语义混淆。下游 `eligible` 用 `if learned['safe_tpm']:` 做判断，0 虽为 falsy 侥幸成立，但语义不干净。建议列类型明确 + 用 NULL。

---

## 4. P2 — 建议优化

### P2-1 · `load_dotenv` 在 config 载入时被调用两次
`config.load_config` 里 `load_dotenv(..., override=override)`；`app.py:92` 读 `setup.conf` 后又决定 `override_on`，但此时 config 已经加载过了——**首次 load_config 用的是硬编码的 `override=override_on` 读取时机晚于 dotenv 加载**。请核实 override 切换后是否需要 reload config。

### P2-2 · `set_override()` 切换后不 reload 内存里的 `config`
`/api/setup/override` 改的是 `load_dotenv(override=...)` 后进程 ENV，但 `create_app` 里捕获的 `config = load_config(...)` 是启动时快照。**override 切换后内存 config 不刷新**，除非重启。

### P2-3 · `FLEX_OVERRIDE` 环境变量写了但没人读
`os.environ['FLEX_OVERRIDE']='1' if override_on else '0'` 出现了两次（`app.py:94/267`），但项目内无任何代码读取它。死变量。

### P2-4 · `restart` 端点强耦合 macOS launchd
```python
subprocess.run(['/bin/launchctl','kickstart','-k', target])
```
跨平台会静默失败。建议至少加平台检测 + 明确错误提示，或提供 reload-config 的纯 Python 替代。

### P2-5 · 版本不一致
`__init__.py` 是 `0.1.0`，`FastAPI(version='0.2.0')` 是 `0.2.0`。

### P2-6 · `error_detail` 已脱敏，但 `channel_tests.error_detail` 走的是 `record_test`，同样走脱敏路径 ✓ —— 良好，无需改。

---

## 5. 安全 / 敏感信息检查 ✅ 通过
| 检查项 | 结果 |
|---|---|
| `.env` 在 `.gitignore` | ✅ |
| `*.db` / `data/` / `logs/` 不入库 | ✅ |
| `error_detail()` 脱敏 API key / auth / token | ✅ 正则替换为 `[redacted]` |
| 不记录 prompt 正文 | ✅ 仅记录 usage / error 摘要 |
| `session-hmac.key` 权限 `0o600` + 随机密钥 | ✅ |
| `.bak` 备份不含 key（只备份 yaml 结构） | ✅ |
| API key 通过 ENV 读取，不落盘 | ✅ |

---

## 6. 测试覆盖缺口（29/29 全过 ✓，但覆盖有盲区）
| 盲区 | 影响 |
|---|---|
| **重试 + fallback 主流程**：`app.py:376-387` 整段无测试（含 P0-1 bug 路径） | 高 |
| **回切探测**：无测试验证"探测成功后回切"（P0-2 的佐证） | 高 |
| **streaming 流式错误处理**（`app.py:391-406`） | 中 |
| **mock LiteLLM 的集成测试**：所有 app 测试只测 HTML/API 静态路径，未走真实路由 | 中 |
| `P1-4` A 类 429 observe 缺失：无测试会触发失败 | 中 |
| `restart` 端点（macOS-only） | 低 |

**建议**：补一组 `monkeypatch litellm.acompletion` 的路由集成测试，覆盖"主通道 429 → 重试 → fallback → 下一通道成功"，顺带把 P0-1 的 bug 变成可复现的测试用例。

---

## 7. 值得肯定的亮点
- 设计文档把 A/B 429 两类流控、cost-tier 抽象、保缓存约束讲得很清楚，是高质量的事实来源。
- `error_detail()` 的 litellm 前缀剥离 + `[redacted]` 脱敏写得细致。
- `session_affinity` 用 HMAC 前缀链做亲和，不记录 prompt，隐私友好。
- config 保存前 `.bak` 备份 + schema 校验，运维友好。
- `sessions/requests/errors` API 设计克制，符合"管理面最小化"。

---

## 8. 下一步建议（给 @default 复核用）
1. 先确认 **P0-1（attempt 重试 bug）** 是否如我所读——这是最可能在真实流量里踩到的雷。
2. 确认 **P0-2 / P1-1 / P1-4** 这三处"文档有、代码无"是**刻意为之（暂不做）**还是**遗漏**——这决定要不要更新设计文档或补实现。
3. 测试盲区：补 LiteLLM mock 路由集成测试。

---
*报告结束。以上基于静态审阅 + 29/29 pytest 实测，未改动任何项目文件。*
