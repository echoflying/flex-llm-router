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
| `/` | **Dashboard** — channels, metrics, Test/Enable/Disable/Restart |
| `/config` | **YAML editor** — read/write `config/pools.yaml` with schema validation |
| `/setup` | **Environment vars** — .env vs system ENV, Override toggle |
| `/help` | **Hermes connection** — copy-paste provider URL into Hermes config |

### Config editor

Edit `config/pools.yaml` in the browser. Save runs schema validation
(`FlexConfig.model_validate`) before writing. Invalid YAML is rejected with
error detail. A `.bak` backup is created before overwrite, and the server
automatically restarts so changes take effect immediately.

### Setup / Override

Shows all required env vars (6 keys from `pools.yaml`: 3 channels × base+key),
their presence in `.env` and system ENV, and which source is active.

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

- **Scheduler**: round-robin with quota-pacing priority over fallback-only
- **Limits**: learned safe RPM/TPM, 429 classification, exponential backoff
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
| GET | `/api/pools/{name}/channels` | Channel metrics |
| GET | `/api/requests` | Recent attempt log |
| POST | `/api/pools/{name}/channels/{id}/test` | Channel test |
| POST | `/api/pools/{name}/channels/{id}/enabled` | Enable/disable |
| POST | `/api/pools/{name}/channels/{id}/reset` | Reset quota/cooldown |
| POST | `/api/config` | Validate & save config |
| POST | `/api/admin/restart` | Launchd restart (macOS) |
