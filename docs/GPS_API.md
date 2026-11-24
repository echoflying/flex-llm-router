# `/api/gps/*` — GPS 坐标搜索 API

> 本接口把「店名 → 经纬度」搜索能力统一暴露给外部调用方（agent、脚本、下游），
> 类似 LLM 调用：POST JSON 请求体，返回 JSON。**调用方不用读点 farm 的 engine.mjs 源码**。
>
> 模块：`src/flex_llm_router/gps_search.py`（独立，不依赖点 farm 任何代码）。

## 能力概览

| 端点 | 方法 | 回答什么 |
|---|---|---|
| `/api/gps/sources` | GET | 四个源（360/百度/高德/腾讯）的可用性 + 优先级顺序 |
| `/api/gps/search` | POST | 店名 → 坐标（统一 WGS-84），带 Fallback 链 |
| `/api/gps/stats` | GET | 每源命中率 + Fallback 统计 |

## `/api/gps/sources`

```
GET /api/gps/sources
```

返回：
```json
{
  "object": "gps_sources",
  "sources": [
    {"id": "360",    "name": "360地图",  "quality": "high",   "available": true,  "reason": null},
    {"id": "baidu",  "name": "百度地图", "quality": "medium", "available": true,  "reason": null},
    {"id": "amap",   "name": "高德地图", "quality": "high",   "available": false, "reason": "AMAP年度额度耗尽(10044)"},
    {"id": "tencent","name": "腾讯地图", "quality": "medium", "available": false, "reason": "TENCENT key来源域名未授权(110)"}
  ],
  "order": {"normal": ["360","baidu","amap","tencent"], "high": ["360","amap"]}
}
```

## `/api/gps/search`

```
POST /api/gps/search
Content-Type: application/json
```

请求体：
```json
{
  "name": "聚贤楼",          // 必填：店名
  "city": "上饶市",           // 可选：城市名（行政区校验用）
  "district": "玉山县",       // 可选：区县名（行政区校验用）
  "sources": null,            // 可选：显式源列表，完全覆盖默认链
  "priority": "normal"        // 可选：normal|high
}
```

返回：
```json
{
  "object": "gps_search",
  "result": {
    "ok": true,
    "source": "360",
    "coord": {"lng": 117.993, "lat": 28.814},   // WGS-84（统一）
    "gcj02": {"lng": 118.0, "lat": 28.82},
    "matchSim": 1.0,
    "matchLevel": "strong",
    "poiName": "聚贤楼",
    "poiAddr": "...",
    "adcode": "361123",
    "attempts": [{"source":"360","outcome":"hit","sim":1.0}]
  },
  "sources_info": {"360": {"available": true, "reason": null}, "...": "..."}
}
```

**语义**：
- `sources` 指定时完全覆盖默认链；不指定则按 `priority` 档（`normal`=360→百度→高德→腾讯）。
- Fallback：前一源无有效数据 → 下一源继续，`attempts` 记录全链（`hit`/`miss`/`disabled`）。
- 坐标统一 WGS-84（`coord`）；高德/腾讯返回 GCJ-02 后自动转 WGS-84。
- 验证：`name` 必填（否则 400）；`priority` 非法（否则 400）。

## `/api/gps/stats`

```
GET /api/gps/stats
```

返回：
```json
{
  "object": "gps_stats",
  "stats": {
    "per_source": {"360": {"calls": 2, "hits": 2, "hit_rate": 1.0}},
    "fallback": {"total_calls": 2, "first_source_hit": 2,
                 "fallback_attempts": 0, "fallback_hit": 0, "fallback_rate": 0.0}
  }
}
```

统计存 `data/gps_stats.sqlite`（可用环境变量 `FLEX_GPS_STATS_DB` 覆盖）。

## 调用示例

```bash
# 先看可用源
curl http://127.0.0.1:7800/api/gps/sources

# 搜索
curl -X POST http://127.0.0.1:7800/api/gps/search \
  -H 'Content-Type: application/json' \
  -d '{"name":"聚贤楼","city":"上饶市","district":"玉山县"}'
```

## 与其他能力接口的关系

| 接口 | 回答什么 |
|---|---|
| `/api/help` | 全部端点目录（免读文档） |
| `/api/gps/sources` | GPS 哪些源可用、什么顺序 |
| `/api/gps/search` | 店名 → 坐标 |
| `/api/bulk-lite` | 这个大小该用哪个 LLM 通道 |
