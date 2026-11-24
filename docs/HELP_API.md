# `/api/help` — 能力目录接口

> 说明：本接口的作用是让外部调用方**免读文档**即可发现所有端点、参数与说明。
> 目录由 FastAPI 路由**自动自省生成**，与真实路由永远同步，无需手工维护。

## 用途

外部系统（agent、脚本、下游）想调用本 router 时，先 `GET /api/help` 拿一份能力清单：
每个端点的 `method`、`path`、`params`（query/path/body）、`description`（取端点 docstring 首行）。
不用读 `docs/*.md` 就能知道"有哪些接口、各要什么参数、返回什么语义"。

## 请求

```
GET /api/help
GET /api/help?verbose=true    # 附上参数类型/默认值/必填
```

## 返回结构

```json
{
  "object": "help",
  "router_version": "0.2.1",
  "generated_at": "2026-11-24T09:30:00",
  "endpoint_count": 75,
  "endpoints": [
    {
      "method": "GET",
      "path": "/api/bulk-lite",
      "params": [{"name": "size", "in": "query"}],
      "description": "按任务体量推荐该调哪个 model, 调用方不必认识资源池内部."
    }
  ],
  "groups": {
    "routing":   ["/v1/chat/completions", "/v1/models"],
    "advice":    ["/api/bulk-lite"],
    "status":    ["/api/fleet-status", "/api/dashboard", "/api/quality", "/healthz"],
    "config":    ["/api/config", "/api/config/view", "/api/config/runners", "/api/config/channels", "/api/config/providers"],
    "statistics": ["/api/statistics/*"],
    "traces":    ["/api/traces/*", "/api/errors/*"]
  }
}
```

## 字段说明

| 字段 | 说明 |
|---|---|
| `endpoint_count` | 目录内端点总数 |
| `endpoints[].method` | HTTP 方法，同路径多方法合并为 `GET\|POST` |
| `endpoints[].path` | 路由路径 |
| `endpoints[].params[]` | 参数：`name` + `in`（query/path/body） |
| `endpoints[].params[]`（verbose） | 追加 `type` / `required` / `default` |
| `endpoints[].description` | 端点 docstring 首行 |
| `groups` | 按用途预分组的路径清单，便于快速定位 |

## 与其他能力接口的关系

| 接口 | 回答什么 |
|---|---|
| `/api/help` | 有哪些接口、各要什么参数（能力目录） |
| `/api/bulk-lite` | 这个大小该用哪个通道（按体量 + 预算感知推荐） |
| `/api/fleet-status` | 整个池子现在什么状态（冷却/自检/预算余量） |

三者互补：`help` 是目录，`fleet-status` 是全局概览，`bulk-lite` 是定向推荐。
