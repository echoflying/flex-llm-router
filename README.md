# Flex LLM Router

Policy-driven LLM routing backed by LiteLLM. Flex picks which channel handles
a request; LiteLLM makes the actual provider call.

Flex is designed to make LLM access stable and reliable: it keeps provider
credentials local, applies per-channel quota/RPM/TPM controls, preserves
conversation affinity, retries transient upstream failures, and records the
routing outcome for diagnosis. A single external Runner can therefore use
multiple providers without exposing that switching detail to the client.

## Quick start

```bash
cp .env.example .env          # set real API keys
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
flex-router                    # starts on http://127.0.0.1:7800
```

## Local/Mac synchronization

`D:\\home.it\\flex-llm-router` is the canonical editing checkout. Commit and
push local changes first, then run `powershell -File scripts/sync_to_mac.ps1`
to synchronize the committed code to Mac. The script intentionally excludes
`.env`, databases, logs, virtual environments, caches, and backups. Mac is a
runtime target, not a second editing source; restart its services manually
when a code or configuration change should take effect.

## Pages

| Path | Description |
|---|---|
| `/` | **Dashboard** — Runner/Channel 状态、调用指标与快速操作 |
| `/config` | **Config** — Runner、Channel、Model 三标签页，结构化编辑并校验 `config/pools.yaml` |
| `/setup` | **Environment vars** — .env vs system ENV, Override toggle |
| `/help` | **Hermes connection** — copy-paste provider URL into Hermes config |

### Config editor

`/config` 固定按 Runner → Channel → Model 显示三个标签页。Runner 编辑
成员和顺序；Channel 编辑 Provider、LiteLLM model、启用及
`externally_exposed`；Model 编辑 Provider 的 `.env` 变量名引用，不显示
密钥值。结构化保存先运行 `FlexConfig.model_validate`，成功后创建 `.bak`
并热更新进程内配置，不自动重启核心。旧的 `/api/config` 原文校验保存接口
仍兼容保留。

### Setup / Override

Shows all Provider-referenced environment variables from `pools.yaml`, their
presence in `.env` and system ENV, and which source is active. The list is
derived from the current Provider definitions; it is not a fixed channel count.

The **Override** toggle controls whether `.env` values override system ENV
(ON = dotenv wins, OFF = system ENV wins). Click toggles immediately via
`load_dotenv(override=…)` and persists to `config/setup.conf` for the next
server start.

## Templates

HTML pages live in `templates/` as standalone `.html` files. Modify them
with any text editor and refresh the browser — no server restart needed.
`base.html` provides the shared frame (navigation + page shell); each page
extends it via `{{ content }}` placeholder.

## Architecture

```
Request → Flex (FastAPI) → LiteLLM → upstream provider
              │
              └→ StateStore (SQLite)
                  ├ quota windows (5h sliding)
                  ├ RPM/TPM learning (60s sliding)
                  ├ session affinity (HMACed message prefixes)
                  └ channel tests & cooldowns
```

- **Scheduler**: `round_robin`, `cost_aware`, or `quota_paced_priority`, using Runner tiers and Channel state
- **Limits**: learned safe RPM/TPM, 429 classification, quota windows, exponential backoff
- **Config**: `config/pools.yaml` — Runners, internal Channels, limits, and routing policy
- **State file**: `data/flex.db` (SQLite, `.gitignore`d)

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Health check |
| GET | `/v1/models` | OpenAI-compatible model list |
| POST | `/v1/chat/completions` | Chat completion (non-stream + stream) |
| GET | `/` | Dashboard HTML |
| GET | `/config` | Config viewer/editor HTML |
| GET/POST | `/setup`, `/api/setup/override` | Env management |
| GET | `/help` | Hermes connection details |
| GET | `/api/runners` | Canonical Runner list |
| GET | `/api/runners/{name}/channels` | Runner Channel metrics |
| GET | `/api/providers` | Provider list and configured model counts |
| GET | `/api/providers/{provider}/models` | Configured candidates; `?refresh=1` explicitly queries that Provider's `/models` endpoint (no periodic probe) |
| GET | `/api/config/editor` | Config editor data (Runner/Channel/Provider env names; no secrets) |
| POST | `/api/config/runners/{name}` | Edit Runner membership/order and public model |
| POST | `/api/config/channels/{id}` | Edit Channel and external exposure toggle |
| POST | `/api/config/channels` | Add a new Provider + Model Channel |
| POST | `/api/config/channels/{id}/test` | Explicit self-test for one Channel |
| POST/DELETE | `/api/config/providers[/{name}]` | Add/update or remove an unreferenced Provider |
| GET | `/api/pools/{name}/channels` | Legacy-compatible Channel metrics path |
| GET | `/api/requests` | Recent attempt log |
| GET | `/api/traces` | Trace list |
| GET | `/api/traces/{trace_id}` | Trace detail and events |
| GET | `/api/traces/{trace_id}/full-request` | Full captured request when optional retention is enabled |
| GET | `/api/statistics/*` | Error, call, request, hourly and duplicate statistics |
| POST | `/api/pools/{name}/channels/{id}/test` | Channel test |
| POST | `/api/pools/{name}/channels/{id}/enabled` | Enable/disable |
| POST | `/api/pools/{name}/channels/{id}/reset` | Reset quota/cooldown |
| POST | `/api/config` | Validate & save config |
| POST | `/api/admin/restart` | Launchd restart (macOS) |

The Runner action endpoints under `/api/runners/{name}/channels/{id}/...` are
also available; the `/api/pools/...` action paths remain for compatibility.
