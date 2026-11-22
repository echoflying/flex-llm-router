# Router 事件记录 / 修复台账

本文件记录 flex-llm-router 运行中发现的失败/挂住事件、根因分析与已做修改，
由每 30 分钟的监控（launchd `com.weifeng.flex-monitor`）配套人工分析后更新。
最新实时报告见 `data/monitor/report.md`。

---

## 2026-11-22 事件：`context_exceeded` 导致"no eligible channel"误判

### 现象
- 时间：2026-11-22 09:35 左右
- 返回给客户端的错误：
  ```json
  {"detail":{"error":"no eligible channel for mix-lite","last_error":"no_eligible_channel",
  "rejected":[{"channel":"agnes-flash","reason":"context_exceeded:527460>524288"},
              {"channel":"sensenova-flash-lite","reason":"context_exceeded:527460>262144"}]}}
  ```
- 实际输入 token 约 527k，超过两个 channel 的上下文窗口（512k 与 256k）。

### 关键证据
- 错误来自 `compatibility()` 检查：路由在发起上游调用前就拒绝了请求。
- 但 `channels` 表里记录的 `context_window_tokens`（524288 / 262144）与 channel 实际支持的上限**不一致**——可能是历史遗留配置或不同版本 LiteLLM model spec 的差异。
- 客户端收到的是"无可用 channel"，而不是明确的"上下文超限"提示，导致难以区分是配额耗尽还是输入过大。

### 根因分析
1. **对外广播 vs 实际调用策略不一致**：`compatibility()` 用 `ch.context_window_tokens` 做硬拦截，但该字段不一定等于上游真实可接受的上限（某些 channel 可能通过 LiteLLM 的 `model` 参数隐式支持更大窗口）。
2. **错误语义模糊**：`context_exceeded` 被归类成 `no_eligible_channel`，调用方无法知道是"真的没 channel 可用"还是"我的输入太大了"。

### 待处置（不由小时巡检自动处理，需人工决策）
- [ ] 确认各 channel 的真实上下文窗口（查 LiteLLM model spec 或实测）。
- [ ] 决定策略：
  - A) 把 `context_window_tokens` 修正为准确值；
  - B) 放行大上下文请求交给上游报错（让上游返回明确的 context limit 错误）；
  - C) 在 router 层给出更精确的错误码（如新增 `context_too_large` error_type）。
- [ ] 更新 `docs/ROUTER_INCIDENTS.md` 最终处置结论。

### 关联改动
- 本次发现促使我们把 `context_exceeded` 加入**已知模式库**还是**待排查清单**，取决于上述决策。

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

---

## 2026-11-22 12:36 巡检：单通道池率限即 503（no_eligible_channel）

### 现象
近 1 小时 `request_traces` 中 `no_eligible_channel` 8 条（HTTP 503），分两组：

| 池 | 条数 | 拒绝原因 |
|---|---|---|
| `sensenova-deepseek-v4-flash` | 6 | 唯一成员 `sensenova-deepseek-flash` 处于 rpm_limit / tpm_limit |
| `mix-lite` | 2 | `agnes-flash` 与 `sensenova-flash-lite` 同时 quota_daily |

### 根因
1. `sensenova-deepseek-v4-flash` 是**单通道池**（`channels: [sensenova-deepseek-flash]`，无 fallback tier）。
   该通道一旦进入 RPM/TPM 冷却，池内无任何可退通道，Router 只能返回 503。
   单通道本身不是 bug，但把「上游瞬时率限」直接放大成「用户可见 503」，是容量配置问题。
2. `mix-lite` 两个成员同时 `quota_daily`（每日配额），属于成员配额同步耗尽，无第三个兜底。

### 关联发现（非本事件，记录备查）
- `AMD-deepseek-v4-flash` 的日额度极小：错误原文为
  `Daily usage limit exceeded: at most 1 pt per day (Beijing time) for this key`
  以及 `maximum $1 per 24 hours for this OneClick user`。
  历史上该类告警 1243 次。Router 已正确处理：`states` 表写入
  `quota_daily` 冷却至当日 23:59:59，不再反复打上游。该通道不适合作为主力通道。

### 处置建议（待人工决策，未擅自改配置）
- A) 给 `sensenova-deepseek-v4-flash` 池增加一个 tier-1 兜底通道（如 `AMD-deepseek-v4-flash`
  或新接入的 `nvidia-*`），把 503 降级为「换通道成功」；
- B) 给 `mix-lite` 增加第三个成员；
- C) 保持现状，接受率限期间返回 503（当前行为语义正确，只是不够韧）。

依据 `using-large-models` 对额度轴的分类：RPM/TPM 与「每日积分额度」是独立的一轴，
本次两条链路分别踩了「每分钟预算被打满」和「每日额度耗尽」，均属上游行为，非 Router 缺陷。

### 2026-11-22 13:00 巡检追加：agnes-flash 免费层日额度耗尽，单通道池整池停摆 14h

- **现象**：12:54 起 `sensenova-flash-plus` 池持续返回 429 `quota_daily`（近 1 小时 18 条，末条 13:03）。
- **上游原文**（free tier）：
  `You have used up today's text quota and are now limited to 1 request every 3 minutes. Try again after 04:03:23.`
  另一变体：`You've reached the API rate limit for free users. Upgrade to a Token Plan ...`（注意此变体**不匹配**当前
  `quota_429_policy.match` 的两条正则，属潜在漏配）。
- **Router 处理**：`quota_429_policy.mode=until_reset, reset_hint=error_text` 解析出重置时刻，写入
  `states: agnes-flash / quota_daily / until=2026-11-23 04:03:23`（约 14h），行为正确。
- **影响面**：
  | 池 / 入口 | 是否仍可用 |
  |---|---|
  | `sensenova-flash-plus`（唯一成员 agnes） | ❌ 整池停摆至次日 04:03 |
  | `hermes-backend` link（直接指向 agnes-flash channel） | ❌ 无 fallback，同步停摆 |
  | `mix-lite`、`M2-deepseek-v4-flash`、`agnes-flash` 池 | ⚠️ 降级到第二通道 |
- **建议（待决策）**：
  1. 给 `sensenova-flash-plus` 增加 tier-1 兜底（如 `sensenova-flash-lite`）；
  2. `hermes-backend` 改为指向多通道池，避免单通道直接绑定；
  3. 把 `You've reached the API rate limit for free users` 加入 agnes 的 `quota_429_policy.match`，
     否则该变体会落到 `rpm_limit` 短冷却，导致恢复后继续撞墙。

### 复发登记（仅追加，不新建事件）
- 2026-11-22 13:53:41 `sensenova-deepseek-v4-flash` 再 1 条 `no_eligible_channel`(503)，根因同 12:36 条目（单通道池 + 上游 rpm/tpm），累计该池 503：12:09–12:26 六条、13:25 一条、13:53 一条。
- 2026-11-22 13:52:55 同池 1 条 `stale_no_terminal`：`finally` 兜底正常收尾（客户端中断导致），非缺陷。
- `agnes-flash` 日额度降级持续：每 3 分钟一次 429 → Router 写入约 3 分钟 `quota_daily` 冷却后重试，循环往复；近 1 小时 agnes 成功 6 / 失败 4，通道处于「降级可用」而非「停摆」。
- 2026-11-22 14:07:17 / 14:12:20 / 14:18:08 `sensenova-deepseek-v4-flash` 再 3 条 `no_eligible_channel`(503)；本小时该池另有 2 条 `tpm_limit`。
  **归因修正**：该池是**有意**的 SenseNova-only 池（`links.sensenova-only -> sensenova-deepseek-v4-flash`），
  单通道并非疏漏，因此「SenseNova 限流即 503」属设计取舍，不应静默加异厂商 fallback 破坏 link 语义。
  若需韧性，应新建独立的多通道 Runner（如 `deepseek-mix`），而不是改这个池。
- `agnes-flash` 自 14:27:39 之后未再写入新冷却，本小时 6 成功 / 2 失败，降级中仍可用。
- 2026-11-23 00:57–01:00:40 `agnes-flash` **形态变化**：不再是「3 分钟 1 次的快速 429」，而是
  **挂起 90s 后超时**（00:58:39、00:59:00、01:00:23 三条 `upstream_response_timeout`），
  首个 429 原文（`You have used up today's text quota and are now limited to 1 request`）
  直到 **01:00:40** 才落地。`until_reset` 据此封禁 4 个池至 17:03:37（已验证持久化于 `states`）。
  结论：**1 小时内的巡检若落在 00:58–01:00:40 之间会看到「无冷却、但 agnes 请求全部挂起」**，
  这不是冷却丢失，而是上游由「快速拒绝」退化为「静默挂起」的窗口。
- 同窗 `sensenova-flash-plus`（刻意单通道，唯一成员 agnes）01:01:03 / 01:01:46 各 1 条
  `no_eligible_channel`，累计该 Runner 停摆中；恢复需等 17:03:37。按刻意单通道清单，只计数不重复建议。
- 2026-11-22 18:25–18:39 `mix-lite` 4 条 `stale_no_terminal`：客户端均为 `pi/1.5.2 (linux; node/v22.21.1)`，
  事件链显示「客户端断开 → `finally` 兜底补写 `cancelled`」，**兜底机制正常工作**，非 Router 缺陷
  （修复前这 4 条会永久 `running`）。其中 2 条 `handler_response_ready after 93798ms` —— 首 SSE 等到 93–94s
  （期间命中 60s pre-response hedge），已超出该客户端约 90s 超时，客户端先断。
  观察项：首字延迟 90s+ 会撞客户端超时，若需改善可下调 `FLEX_HEDGE_FIRST_SECONDS`（当前 60）。
- 2026-11-22 23:11:53 / 23:14:04 `sensenova-deepseek-v4-flash` 再 2 条 `no_eligible_channel`（`last_error: rpm_limit`）。
  **该池今日累计 15 条**，全部为「SenseNova 单通道 + 上游 rpm/tpm」的设计取舍，无需再逐条登记。

## 已确认的「刻意单通道 Runner」清单（巡检不再建议加成员）

以下 Runner 是**有意**只挂一条通道，单通道在通道限流时整池报错属预期行为，**不要**再建议加 tier-1 成员：

| Runner | 唯一成员 | 确认时间 |
|---|---|---|
| `sensenova-only` | `sensenova-deepseek-flash` | 2026-11-22（由 `links.sensenova-only` 语义决定） |
| `sensenova-flash-plus` | `agnes-flash` | 2026-11-22（用户明确：本身就是单 Channel Runner） |

巡检规则：对上述 Runner 只统计失败、不重复提「加 fallback」建议。

## 2026-11-22 23:50 NVIDIA `nvidia-nim` 全部 502：模型 ID 虚构 + LiteLLM provider 前缀缺失

**现象**：`nvidia-nim` / `nvidia-nim-free` 三个 channel 配额页显示「异常」，请求 502。

**根因（两个叠加，都与 Router 运行时无关）**：
1. **模型 ID 不存在**。最初配置的 `deepseek-ai/deepseek-v3.2`、`moonshotai/kimi-k2.5`、`THUDM/glm-5-1`
   是凭空猜测的 ID，NVIDIA 端点返回 `404 page not found`（非 LiteLLM 错误体，故曾误判为「502 网关问题」）。
   NVIDIA 的 `/v1/models` 只返回 33 个模型，且与文档 `docs.api.nvidia.com/nim/reference/llm-apis`
   的列表**不一致**——文档里列的 `deepseek-ai/deepseek-v4-flash`、`moonshotai/kimi-k2-instruct`、
   `openai/gpt-oss-20b` 等均已 `410 Gone`（end of life）。**`/v1/models` 才是权威清单。**
2. **LiteLLM 缺少 `openai/` 前缀**。custom_openai 通道必须写成 `openai/<model>`，
   否则报 `LLM Provider NOT provided`。旧配置无前缀时该错误被上游 404 掩盖，修好 ID 后才暴露。

**处置**：按 `/v1/models` 实盘替换为 6 个实测可出 token 的模型（均为 `openai/` 前缀）：

| Channel | litellm_model | 直连实测 TTFT |
|---|---|---|
| `nvidia-glm-5.3-flash` | `openai/z-ai/glm-5.3-flash` | 15.6s |
| `nvidia-minimax-m3.1-flash` | `openai/minimaxai/minimax-m3.1-flash` | 14.1s |
| `nvidia-mistral-large-4` | `openai/mistralai/mistral-large-4-1t-a52b` | 0.9s |
| `nvidia-nemotron-3.5-lightning` | `openai/nvidia/nemotron-3.5-lightning-30b-a3b` | 0.6s |
| `nvidia-step-5-preview` | `openai/stepfun-ai/step-5-preview` | 6.3s |
| `nvidia-poolside-laguna` | `openai/poolside/laguna-xs-2.1` | 0.6s |

验证：经 Router `nvidia-nim` 非流式/流式均 200，round-robin 依次命中 3 个不同 channel 全部成功。

**免费额度口径（重要，避免误解为「每日刷新 token」）**：
NVIDIA 官方 FAQ（`docs.api.nvidia.com/nim/docs/product`）只承诺
*「Developer Program 成员在**计划成员期内**可免费访问 NIM API 端点用于原型开发」*，
**没有**公布每日 token / 请求数配额；实测并发 8 时 48 个请求仅 3 个 429，未见配额响应头。
限制在于**用途**：仅原型/研发/测试，"production（服务真实终端用户）"需 NVIDIA AI Enterprise 授权。
故巡检不应假设它「每天有一笔免费额度」；429 已由 channel 级 `quota_429_policy mode=cooldown 120s` 承接。

**踩坑记录（勿再犯）**：
- 用 `open(path,'w')` + `yaml.safe_dump` 改写 `pools.yaml` 时，**文件在 dump 抛错前已被截断为 0 字节**，
  导致 `config/pools.yaml` 损坏。此后必须**先 dump 到临时文件、校验、再 `os.replace()`**。
- `yaml.safe_dump` 不接受 `collections.OrderedDict`，须用普通 dict（`sort_keys=True` 已保证顺序）。
