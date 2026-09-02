# Router 事件记录 / 修复台账

本文件记录 flex-llm-router 运行中发现的失败/挂住事件、根因分析与已做修改，
由每 30 分钟的监控（launchd `com.weifeng.flex-monitor`）配套人工分析后更新。
最新实时报告见 `data/monitor/report.md`。

---

## 2026-09-03 事件 A：trace `r-da58cdd82ad547caa59a5aaa5563909d` 挂住（流式消费者阶段）

### 现象
- 请求 `mix-deepseek-v4-flash`，stream=true，消息内容为"重试"。
- 时间线（2026-09-02）：
  - `23:53:44` 选中 sensenova（session affinity hit），发起上游调用
  - `23:53:48` 收到响应对象（4.1s）→ handoff → 收到首条 SSE → `stream_consumer_started`
  - `23:54:20` `first_token`（TTFT 36339ms，即上游在返回 3 条"心跳/空帧"后思考了约 32 秒才产出首个可见 token）
  - **之后 30+ 分钟无任何事件**，status 一直 `running`、attempt 一直 `started`，未完成、未失败。
- 核心 `active_trace_count=3`，而 DB 中 `running` 仅 1 条 → **有 2 条 watch_record 泄漏**（未修复版问题 3 的现场）。

### 关键证据（用户怀疑"LLM 信息被 Router 丢了一些" —— 有直接支持）
- `debug_logs` 该 rid：有 3 条 `upstream_in`（23:53:48，sensenova 返回的 SSE chunk，HTTP 200）；
- 该 rid **`client_out` 为 0 条**（下游未收到任何输出，流未正常结束）。
- 全库最近 1 小时：`upstream_in=215` 条 vs `client_out=52` 条，差距明显。
- 结论：**上游内容确实到达 Router，但没有（完整地）转发给下游**，与"信息被吞"的怀疑一致。

### 根因分析
- 挂住点位于流式消费者 `events()`/`selected_items()` 阶段：上游在 `first_token` 后不再产出 SSE。
- 本应在 12 分钟（`stream_idle_hard_seconds = hedge_plan[-1][0]+180`）触发的流式空闲看门狗
  （`stream_idle_guard_task` / `watchdog_stream_deadline`）**未生效**，导致僵尸请求永久挂起。
- 与之前问题 1（pre-response 阶段 handler 未消费完成 task）属同一家族：**handler/消费者协程在
  某些时序下卡死，独立看门狗未能兜底**。当前核心（2026-09-02 22:05 启动，旧代码）没有加载修复。

### 已做修改（针对此事件及家族问题）
| 编号 | 修改 | 状态 |
|---|---|---|
| M1 | 504/499 错误改为 OpenAI 兼容格式 `{"error":{message,type,param,code}}`，避免 Hermes/OpenAI SDK 无法识别而挂 30 分钟 | 已改代码，待重启 |
| M2 | `watchdog_cancel_deadline` / `stream_deadline_hard_stop` 硬停时注销 `watch_record`，杜绝已结束请求继续触发 Hedge（修复 watch_record 泄漏） | 已改代码，待重启 |
| M3 | 在 pre-response 循环、task 消费、watchdog 信号、硬停、handoff、SSE 预取共 10 处加 `router_wait`/`router_task`/`router_hedge`/`router_deadline` debug 日志，进入现有 `debug_logs` 清理体系（1000 条上限 + 保留时间） | 已改代码，待重启 |
| M4 | 新增 `scripts/monitor_router.py` + launchd `com.weifeng.flex-monitor`（每 30 分钟）检查失败/挂住调用，写入 `data/monitor/report.md` | **已生效** |

### 下一步
- **重启核心 :7800 让 M1/M2/M3 生效**（顺带清理当前 3 条泄漏的 watch_record / 僵尸 trace）。
- 重启后若再出现同类挂住，`router_*` 日志可定位消费者卡在哪个环节（是否有 `WON`/`PREFETCH_FIRST_SSE`、
  最后几轮 `router_wait` 的 active/done 状态、是否走到 `router_deadline`）。
- 监控每 30 分钟自动产出报告，人工按需分析。

### 关闭（2026-09-03 00:28）
- 核心已重启（PID 69390，00:28:10，新代码）：`active_trace_count=0`，泄漏 watch_record 已清理；
  挂住 trace `r-da58cdd82ad547caa5` 被重启优雅排空标记为 `cancelled/router_restarted`。
- M1/M2/M3 已在运行的核心上验证生效：504 返回 OpenAI 兼容格式 `{"error":{...}}`；
  `FLEX_DEBUG=1` 已开启；`/api/config` 正常。
- 监控 launchd `com.weifeng.flex-monitor` 每 30 分钟运行，报告 `data/monitor/report.md`。

---

## 2026-09-03 事件 B：CONFIG 页创建 Runner 报“未知配置项” —— 路由冲突（已修复）

### 现象
- 在 CONFIG 页增加 Runner，选首个 Channel `sensenova-flash-lite`（其 LiteLLM model 为 `openai/sensenova-6.8-flash-lite`），创建时报 `创建失败：未知配置项`。

### 根因
- 新增的 Setup 配置项端点 `POST /api/config/{key}`（注册于 app.py 靠前位置）与 CONFIG 页的
  `POST /api/config/runners`、`/api/config/runners/{name}`、`/api/config/channels/{channel_id}` 等发生**路径冲突**。
- FastAPI 按注册顺序匹配，`POST /api/config/runners` 被 `/api/config/{key}` 拦截（key=`runners`），
  进入 `set_config_value` 返回 `未知配置项`（400）。

### 修复
- 配置项端点路径改为不冲突的 `POST /api/config/item/{key}`，前端 `setup.html` 同步改为 `/api/config/item/{key}`。
- 验证：`POST /api/config/runners` 修复前 400 未知配置项 → 修复后 200 正常创建；
  `POST /api/config/item/FLEX_QUEUE_TPM` 与 `GET /api/config` 均正常。
- 状态：已修复，核心重启后生效（PID 76549，2026-09-03 01:01）。测试 runner 已清理。

---

## 2026-09-03 事件 C：接入火山方舟 Code Plan（Volcengine） + 非流式 prefetch 修复

### 背景
- 用户购买火山方舟 **Code Plan**，key 仅能在 `https://ark.cn-beijing.volces.com/api/plan/v3` 使用（标准 `/api/v3` 返回 401）。
- plan 端点**不支持 GET /models**（404），因此 CONFIG 页无法像其他 provider 那样拉模型列表；经批量实测，
  **`doubao-seed-2.0-lite` 在该端点可用**（200，返回含 reasoning_content）。

### 配置（config/pools.yaml）
- channel `volcengine-doubao-seed-2.0-lite`：provider=VOLCENGIN，litellm_model=`openai/doubao-seed-2.0-lite`。
- runner `doubao-seed-2.0-lite`（public_model 同名）→ 调用 `model=doubao-seed-2.0-lite` 即可。

### 修复的预存 bug（M5）
- 非流式请求在 `await_initial_response` 里也会无条件创建 `prefetch_first_sse` 后台任务，把非流式
  `ModelResponse` 包进 `BufferedUpstreamStream` 报 `AttributeError: ... has no __aiter__`（“Task exception was never retrieved”）。
- 修复：仅 `stream=True` 时才创建 prefetch 任务与 `watchdog_handoff` 事件。

### 验证（核心 PID 96088，2026-09-03 01:50 重启后）
- 非流式 `POST /v1/chat/completions model=doubao-seed-2.0-lite` → 200，正常返回。
- 流式 → 200，SSE 正常（含 reasoning_content），`[DONE]` 正常，trace success/200。
- `/v1/models` 已列出 `doubao-seed-2.0-lite`。

---
