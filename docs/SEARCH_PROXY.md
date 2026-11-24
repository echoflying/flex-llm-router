# 搜索代理（Search Proxy）

flex-llm-router 的统一搜索入口，聚合 point farm 用过的全部搜索通道，按可用性自动选择。

## 通道清单（10 个，实测评估 2026-11-24）

| 通道 | 名称 | 类目 | key | 实测 | 状态 |
|---|---|---|---|---|---|
| `searxng` | SearXNG 本地 | web | 无 | 602ms n=10 | ✅ 可用（本地最快） |
| `poi_360` | 360 地图 POI | poi | 无 | 1055ms n=10 | ✅ 可用（主源） |
| `tencent_poi` | 腾讯 POI | poi | `TENCENT_WEB_API_KEY` | 301ms n=1 | ✅ 可用（需 Referer 白名单） |
| `bing` | Bing 直爬 | web | 无 | 4426ms n=10 | ✅ 可用（慢） |
| `sogou` | 搜狗网页 | web | 无 | 996ms n=8 | ✅ 可用（移动版） |
| `socom` | 360 网页 | web | 无 | 1036ms n=14 | ✅ 可用 |
| `osm` | OSM Nominatim | geo | 无 | 738ms n=1 | ✅ 可用（免 key 地理源） |
| `tavily` | Tavily 付费 | web | `TAVILY_API_KEY` | 4772ms n=5 | ✅ 已配置 key，实测命中玉山聚贤大酒店 |
| `baidu_web` | 百度网页 | web | 无 | HTTP200 反爬 | ❌ 反爬（返回验证页） |
| `sogou_baike` | 搜狗百科 | wiki | 无 | HTTP404 | ❌ 接口失效 |

> **Tavily key**：2026-11-24 由用户提供并写入 `.env` 的 `TAVILY_API_KEY`（54 字符，gitignore 不落库）。
> 配置后自动启用（需重启服务加载新环境）。flow-llm-router 的 `search_provider_keys` 表仍 0 行——该侧未复用。

## API

- `GET /api/search/channels` — 通道清单 + 实时可用性（外部调用方先查这个）
- `POST /api/search/search` — 统一搜索
  - body：`{"query","kind","channels","city","district","limit","max_channels","force"}`
  - `kind` 可选 `web/poi/geo/wiki`；`force=true` 时强制试 blocked 通道
  - 结果：`results[]` 统一 `{channel,kind,title,url,snippet,latency_ms,poi}`；
    `channels_used` 命中通道，`skipped` 跳过原因
- `POST /api/search/test/{channel}` — 单通道真实探测（忽略 blocked 标记）
- `GET /api/search/stats` — 调用统计（`search_stats` 表）

## key 管理

对齐 `config.channel_credentials`：只存 **env var 名**（`key_env`），值由 `.env` 注入。
不落库、不落盘，避免 key 泄漏。Tavily/腾讯各自只需一行环境变量。

## 持久化

`search_stats` 表并入 `data/flex.db`（与 `gps_stats` 同库）。调用统计通过
`record_search_stat` / `search_statistics` / `search_calls` 读写。

## 已知局限

- **SearXNG 引擎质量差**：对中文长关键词返回拆词/垃圾结果（与 point farm 观察一致）。
  已**降级为佐证通道**（排在 `poi_360`/`tencent_poi` 之后）。POI 搜索优先 `poi_360`/`tencent_poi`；
  `searxng` 仅作文本佐证。
- 坐标系统：`poi_360`/`tencent_poi` 返回 GCJ-02，`osm` 返回 WGS-84（结果 `poi.coord_sys` 标注）。
- 5 个 `tests/test_routing_integration.py` 用例预先失败（`HTTP 429 inference tpm exhausted`，
  LLM 配额 mock 环境问题），与搜索代理无关（已用 git stash 验证改动前后均失败）。
