"""搜索代理（Search Proxy）—— 聚合点 farm 用过的全部搜索通道，统一入口。

设计对齐两个既有模式：
  1. GPS 源模式（gps_search.py）：静态注册表 + adapter + 动态可用性 + 调用统计
  2. LLM key 模式（config.channel_credentials）：只存 env var 名，值由 .env 注入

通道清单来自 tools/probe_search_channels.py 的真实调用评估（2026-11-24）：
  可用   searxng / poi_360 / tencent_poi / bing / sogou / socom / osm
  不可用 baidu_web（反爬）/ sogou_baike（HTTP404）/ tavily（缺付费 key）

统一结果格式：
  {"channel", "kind", "title", "url", "snippet", "latency_ms", "poi"}
  kind ∈ {web, poi, geo, wiki}；poi 非空时表示该结果是地图 POI（含坐标）

用法：
  import flex_llm_router.search_proxy as sp
  sp.search("聚贤楼酒店", city="上饶市", limit=10)
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

UA_MOB = "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15"
UA_DESK = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# ── 通道注册表 ────────────────────────────────────────────────
# base_url_env / key_env 只存 env var 名（对齐 Provider.api_key_env），值由 .env 注入。
# blocked 非空表示真实探测已失败的通道（默认跳过，仍可手动强制调用）。
SEARCH_CHANNELS: dict = {
    "searxng": {
        "name": "SearXNG 本地", "kind": "web", "needs_key": False,
        "base_url_env": "SEARXNG_BASE_URL", "default_base": "http://127.0.0.1:8080",
        "engines": "bing,ddg,google", "probe": "602ms n=10 本地最快但中文拆词质量差（佐证用）",
    },
    "poi_360": {
        "name": "360 地图 POI", "kind": "poi", "needs_key": False,
        "url": "https://m.map.360.cn/m/search?keyword={q}", "probe": "1055ms n=10 主源",
    },
    "tencent_poi": {
        "name": "腾讯 POI", "kind": "poi", "needs_key": True,
        "key_env": "TENCENT_WEB_API_KEY",
        "url": "https://apis.map.qq.com/ws/place/v1/search",
        "referer": "http://localhost:5000", "probe": "301ms n=1 需 Referer 白名单",
    },
    "bing": {
        "name": "Bing 直爬", "kind": "web", "needs_key": False,
        "url": "https://www.bing.com/search?q={q}&setlang=zh-CN&count=10",
        "probe": "4426ms n=10 慢但可用",
    },
    "sogou": {
        "name": "搜狗网页", "kind": "web", "needs_key": False,
        "url": "https://m.sogou.com/web/searchList.jsp?keyword={q}",
        "mobile": True, "referer": "https://www.sogou.com/",
        "probe": "996ms n=8 移动版可用",
    },
    "socom": {
        "name": "360 网页", "kind": "web", "needs_key": False,
        "url": "https://www.so.com/s?q={q}", "probe": "1036ms n=14",
    },
    "osm": {
        "name": "OSM Nominatim", "kind": "geo", "needs_key": False,
        "url": "https://nominatim.openstreetmap.org/search?q={q}&format=json&limit={limit}",
        "ua": "flex-search-proxy/1.0", "probe": "738ms n=1 免 key 国外地理源",
    },
    "tavily": {
        "name": "Tavily 付费", "kind": "web", "needs_key": True,
        "key_env": "TAVILY_API_KEY", "url": "https://api.tavily.com/search",
        "depth": "advanced", "probe": "4772ms n=5 高质量（玉山聚贤大酒店 精确命中）",
    },
    "baidu_web": {
        "name": "百度网页", "kind": "web", "needs_key": False,
        "url": "https://www.baidu.com/s?wd={q}&rn=10", "cookie": "BAIDUID=xxxx",
        "blocked": "反爬：HTTP200 但返回验证页，解析 0 结果",
    },
    "sogou_baike": {
        "name": "搜狗百科", "kind": "wiki", "needs_key": False,
        "url": "https://baike.sogou.com/v2/API/lemmaShowV2?lemmaName={q}",
        "referer": "https://baike.sogou.com/", "blocked": "HTTP 404 接口已失效",
    },
}

# 默认尝试顺序（按探测可用性 + 语义优先级；SearXNG 因中文拆词质量差降为佐证通道）
SEARCH_ORDER: list = [
    "poi_360", "tencent_poi", "searxng", "bing", "sogou", "socom",
    "osm", "tavily", "baidu_web", "sogou_baike",
]

_KIND_ALIAS = {"web": {"web", "wiki"}, "poi": {"poi"}, "geo": {"geo", "poi"}}


def resolve_search_key(channel: str) -> str:
    """按通道取 key（对齐 channel_credentials：只存 env var 名，值由 .env 注入）。"""
    meta = SEARCH_CHANNELS.get(channel) or {}
    env_name = meta.get("key_env") or ""
    return os.getenv(env_name, "").strip() if env_name else ""


def resolve_search_base_url(channel: str) -> str:
    """可覆盖的 base url（如 SearXNG 自部署地址）。"""
    meta = SEARCH_CHANNELS.get(channel) or {}
    if not meta.get("base_url_env"):
        return meta.get("default_base") or ""
    return os.getenv(meta["base_url_env"], "").strip() or meta.get("default_base", "")


def channel_available(channel: str) -> bool:
    """动态可用性：被标记 blocked 的通道需要强制；缺 key 的通道视为不可用。"""
    meta = SEARCH_CHANNELS.get(channel)
    if not meta:
        return False
    if meta.get("blocked"):
        return False
    if meta.get("needs_key") and not resolve_search_key(channel):
        return False
    return True


def channel_reason(channel: str) -> str:
    """不可用原因（人类可读，供 /api/search/channels 展示）。"""
    meta = SEARCH_CHANNELS.get(channel)
    if not meta:
        return "未知通道"
    if meta.get("blocked"):
        return meta["blocked"]
    if meta.get("needs_key") and not resolve_search_key(channel):
        return f"缺 key（env {meta.get('key_env')} 未配置）"
    return f"可用（{meta.get('probe', '')}）"


# ── HTTP 工具 ────────────────────────────────────────────────

def _fetch(url: str, headers: dict | None = None, timeout: float = 15.0):
    """返回 (status, text, latency_ms, err)。"""
    hdr = {"User-Agent": UA_DESK, "Accept-Language": "zh-CN,zh;q=0.9"}
    if headers:
        hdr.update(headers)
    req = urllib.request.Request(url, headers=hdr)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode("utf-8", "replace")
            return r.status, text, int((time.perf_counter() - t0) * 1000), ""
    except urllib.error.HTTPError as e:
        return e.code, "", int((time.perf_counter() - t0) * 1000), f"HTTP {e.code}"
    except Exception as e:
        return 0, "", int((time.perf_counter() - t0) * 1000), f"{type(e).__name__}: {str(e)[:80]}"


def _res(channel: str, kind: str, title: str, url: str = "", snippet: str = "",
         poi: dict | None = None) -> dict:
    return {"channel": channel, "kind": kind, "title": title, "url": url,
            "snippet": snippet, "poi": poi, "latency_ms": None}


def _html_web_results(html: str, title_pat: str, url_attr: str = "href") -> list:
    """通用网页结果提取：返回 (title, url, snippet) 列表。"""
    out = []
    for blk in re.findall(title_pat, html, re.S):
        body = blk if isinstance(blk, str) else blk[0]
        t = re.sub(r"<[^>]+>", "", body).strip()
        m = re.search(rf'\b{url_attr}="(https?://[^"]+)"', body)
        u = m.group(1) if m else ""
        sm = re.search(r'<p[^>]*>(.*?)</p>', body, re.S)
        s = re.sub(r"<[^>]+>", "", sm.group(1)).strip() if sm else ""
        if t:
            out.append((t[:200], u[:500], s[:400]))
    return out


def _360_state_pois(html: str) -> list:
    """360 移动端 HTML：从 window.__STATE__ 栈配平提取 poi 数组。"""
    i = html.find("window.__STATE__")
    if i < 0:
        return []
    s = html[i:]
    start = s.find("{")
    depth = 0
    end = None
    in_str = False
    esc = False
    for k in range(start, len(s)):
        c = s[k]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    end = k + 1
                    break
    if end is None:
        return []
    try:
        data = json.loads(s[start:end])
    except Exception:
        return []
    return ((data.get("searchList") or {}).get("data") or {}).get("poi") or []


# ── 通道 adapter：每个返回 list[_res] ─────────────────────────

def search_searxng(query: str, limit: int = 10) -> list:
    base = resolve_search_base_url("searxng")
    url = (f"{base.rstrip('/')}/search?q=" + urllib.parse.quote(query)
           + "&format=json&engines=" + SEARCH_CHANNELS["searxng"]["engines"])
    st, text, ms, err = _fetch(url, timeout=15.0)
    if st != 200 or err:
        return [{"channel": "searxng", "kind": "web", "title": "", "url": "",
                "snippet": "", "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    try:
        rows = json.loads(text).get("results") or []
    except Exception:
        rows = []
    return [{"channel": "searxng", "kind": "web",
             "title": r.get("title", "")[:200], "url": r.get("url", "")[:500],
             "snippet": (r.get("content") or "")[:400], "poi": None, "latency_ms": ms}
            for r in rows[:limit]]


def search_poi_360(query: str, city: str = "", limit: int = 10) -> list:
    q = f"{query} {city}".strip()
    url = SEARCH_CHANNELS["poi_360"]["url"].replace("{q}", urllib.parse.quote(q))
    st, text, ms, err = _fetch(url, {"User-Agent": UA_MOB}, timeout=15.0)
    if st != 200:
        return [{"channel": "poi_360", "kind": "poi", "title": "", "url": "",
                "snippet": "", "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    rows = _360_state_pois(text)
    out = []
    for p in rows[:limit]:
        out.append(_res("poi_360", "poi", str(p.get("name", "")),
                       snippet=str(p.get("address", "")),
                       poi={"lng": p.get("x"), "lat": p.get("y"),
                            "address": p.get("address"), "adcode": p.get("adcode"),
                            "category": p.get("category"), "coord_sys": "GCJ-02"}))
    for r in out:
        r["latency_ms"] = ms
    return out


def search_tencent_poi(query: str, city: str = "", district: str = "", limit: int = 10) -> list:
    key = resolve_search_key("tencent_poi")
    if not key:
        return [{"channel": "tencent_poi", "kind": "poi", "title": "", "url": "",
                "snippet": "", "poi": None, "latency_ms": 0, "error": "缺 TENCENT_WEB_API_KEY"}]
    region = district or city or ""
    q = urllib.parse.urlencode({
        "key": key, "keyword": query, "boundary": f"region({region},0)" if region else "",
        "page_size": min(limit, 20), "page_index": 1,
    })
    st, text, ms, err = _fetch(
        SEARCH_CHANNELS["tencent_poi"]["url"] + "?" + q,
        {"Referer": SEARCH_CHANNELS["tencent_poi"]["referer"]}, timeout=15.0)
    if st != 200:
        return [{"channel": "tencent_poi", "kind": "poi", "title": "", "url": "",
                "snippet": "", "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    try:
        d = json.loads(text)
    except Exception:
        return [{"channel": "tencent_poi", "kind": "poi", "title": "", "url": "",
                 "snippet": "", "poi": None, "latency_ms": ms, "error": "响应非 JSON"}]
    if int(d.get("status") or 0) != 0:
        return [{"channel": "tencent_poi", "kind": "poi", "title": "", "url": "",
                 "snippet": "", "poi": None, "latency_ms": ms,
                 "error": f"status={d.get('status')} {d.get('message', '')}"}]
    out = []
    for p in (d.get("data") or [])[:limit]:
        loc = p.get("location") or {}
        out.append(_res("tencent_poi", "poi", str(p.get("title", "")),
                       snippet=str(p.get("address", "")),
                       poi={"lng": loc.get("lng"), "lat": loc.get("lat"),
                            "address": p.get("address"),
                            "adcode": (p.get("ad_info") or {}).get("adcode"),
                            "coord_sys": "GCJ-02"}))
    for r in out:
        r["latency_ms"] = ms
    return out


def search_bing(query: str, limit: int = 10) -> list:
    url = SEARCH_CHANNELS["bing"]["url"].replace("{q}", urllib.parse.quote(query))
    st, text, ms, err = _fetch(url)
    if st != 200:
        return [{"channel": "bing", "kind": "web", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    rows = _html_web_results(text, r'<li class="b_algo"')
    return [{**_res("bing", "web", t, u, s), "latency_ms": ms} for t, u, s in rows[:limit]]


def search_sogou(query: str, limit: int = 10) -> list:
    url = SEARCH_CHANNELS["sogou"]["url"].replace("{q}", urllib.parse.quote(query))
    st, text, ms, err = _fetch(url, {"User-Agent": UA_MOB,
                                     "Referer": SEARCH_CHANNELS["sogou"]["referer"]})
    if st != 200:
        return [{"channel": "sogou", "kind": "web", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    rows = _html_web_results(text, r'<h3[^>]*>.*?</h3>.*?(?=<h3|$)', "href")
    return [{**_res("sogou", "web", t, u, s), "latency_ms": ms} for t, u, s in rows[:limit]]


def search_socom(query: str, limit: int = 10) -> list:
    url = SEARCH_CHANNELS["socom"]["url"].replace("{q}", urllib.parse.quote(query))
    st, text, ms, err = _fetch(url)
    if st != 200:
        return [{"channel": "socom", "kind": "web", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    rows = _html_web_results(text, r'<h3[^>]*>.*?</h3>')
    return [{**_res("socom", "web", t, u, s), "latency_ms": ms} for t, u, s in rows[:limit]]


def search_osm(query: str, limit: int = 10) -> list:
    url = SEARCH_CHANNELS["osm"]["url"].replace("{q}", urllib.parse.quote(query)
                                                ).replace("{limit}", str(limit))
    st, text, ms, err = _fetch(url, {"User-Agent": SEARCH_CHANNELS["osm"]["ua"]})
    if st != 200:
        return [{"channel": "osm", "kind": "geo", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    try:
        rows = json.loads(text)
    except Exception:
        return [{"channel": "osm", "kind": "geo", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": ms, "error": "响应非 JSON"}]
    out = []
    for r in rows[:limit]:
        out.append(_res("osm", "geo", str(r.get("display_name", "")),
                        snippet=r.get("type", ""),
                        poi={"lng": r.get("lon"), "lat": r.get("lat"),
                             "coord_sys": "WGS-84"}))
    for r in out:
        r["latency_ms"] = ms
    return out


def search_tavily(query: str, limit: int = 10) -> list:
    key = resolve_search_key("tavily")
    if not key:
        return [{"channel": "tavily", "kind": "web", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": 0,
                 "error": "缺 TAVILY_API_KEY（付费通道，未配置）"}]
    body = json.dumps({
        "api_key": key, "query": query,
        "search_depth": SEARCH_CHANNELS["tavily"].get("depth", "advanced"),
        "max_results": min(limit, 20),
    }).encode()
    req = urllib.request.Request(SEARCH_CHANNELS["tavily"]["url"], data=body,
                                 headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        ms = int((time.perf_counter() - t0) * 1000)
    except Exception as e:
        return [{"channel": "tavily", "kind": "web", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": int((time.perf_counter() - t0) * 1000),
                 "error": f"{type(e).__name__}: {str(e)[:100]}"}]
    out = []
    for r in (d.get("results") or [])[:limit]:
        out.append(_res("tavily", "web", str(r.get("title", "")), r.get("url", ""),
                        (r.get("content") or "")[:400]))
    if d.get("answer"):
        out.insert(0, _res("tavily", "web", "Tavily 综合答案", "", d["answer"][:400]))
    for r in out:
        r["latency_ms"] = ms
    return out


def search_baidu_web(query: str, limit: int = 10) -> list:
    url = SEARCH_CHANNELS["baidu_web"]["url"].replace("{q}", urllib.parse.quote(query))
    st, text, ms, err = _fetch(url, {"Cookie": SEARCH_CHANNELS["baidu_web"]["cookie"]})
    if st != 200:
        return [{"channel": "baidu_web", "kind": "web", "title": "", "url": "", "snippet": "",
                 "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    rows = _html_web_results(text, r'<h3[^>]*class="[^"]*t[^"]*"[^>]*>.*?</h3>')
    return [{**_res("baidu_web", "web", t, u, s), "latency_ms": ms} for t, u, s in rows[:limit]]


def search_sogou_baike(query: str, limit: int = 10) -> list:
    url = SEARCH_CHANNELS["sogou_baike"]["url"].replace("{q}", urllib.parse.quote(query))
    st, text, ms, err = _fetch(url, {"Referer": SEARCH_CHANNELS["sogou_baike"]["referer"]},
                               timeout=10.0)
    if st != 200:
        return [{"channel": "sogou_baike", "kind": "wiki", "title": query, "url": "",
                 "snippet": "", "poi": None, "latency_ms": ms, "error": err or f"HTTP {st}"}]
    vals = re.findall(r'"value":"([^"]{2,60})"', text)[:limit]
    return [{**_res("sogou_baike", "wiki", v), "latency_ms": ms} for v in vals]


_ADAPTERS = {
    "searxng": lambda q, c, d, l, k: search_searxng(q, l),
    "poi_360": lambda q, c, d, l, k: search_poi_360(q, c, l),
    "tencent_poi": lambda q, c, d, l, k: search_tencent_poi(q, c, d, l),
    "bing": lambda q, c, d, l, k: search_bing(q, l),
    "sogou": lambda q, c, d, l, k: search_sogou(q, l),
    "socom": lambda q, c, d, l, k: search_socom(q, l),
    "osm": lambda q, c, d, l, k: search_osm(q, l),
    "tavily": lambda q, c, d, l, k: search_tavily(q, l),
    "baidu_web": lambda q, c, d, l, k: search_baidu_web(q, l),
    "sogou_baike": lambda q, c, d, l, k: search_sogou_baike(q, l),
}


# ── 统一入口 ─────────────────────────────────────────────────

def search(query: str, channels: list | None = None, kind: str | None = None,
           city: str = "", district: str = "", limit: int = 10,
           max_channels: int = 3, force: bool = False, state=None) -> dict:
    """统一搜索入口。

    按 SEARCH_ORDER 顺序尝试通道，跳过不可用的；最多尝试 max_channels 个通道，
    任一通道返回结果即停止（aggregate=True 时聚合多通道）。
    """
    query = (query or "").strip()
    if not query:
        return {"query": "", "results": [], "channels_used": [],
                "skipped": [{"channel": c, "reason": "空 query"} for c in SEARCH_ORDER],
                "error": "query 不能为空"}
    want = channels if channels else SEARCH_ORDER
    used, skipped, results = [], [], []
    tried = 0
    for ch in want:
        if ch not in _ADAPTERS:
            skipped.append({"channel": ch, "reason": "未知通道"})
            continue
        if kind and SEARCH_CHANNELS.get(ch, {}).get("kind") not in _KIND_ALIAS.get(kind, {kind}):
            skipped.append({"channel": ch, "reason": f"kind 不匹配（{SEARCH_CHANNELS.get(ch, {}).get('kind')}）"})
            continue
        if not force and not channel_available(ch):
            skipped.append({"channel": ch, "reason": channel_reason(ch)})
            continue
        tried += 1
        try:
            rows = _ADAPTERS[ch](query, city, district, limit, resolve_search_key(ch))
        except Exception as e:
            skipped.append({"channel": ch, "reason": f"{type(e).__name__}: {str(e)[:100]}"})
            continue
        if rows and "error" not in rows[0]:
            used.append({"channel": ch, "name": SEARCH_CHANNELS[ch]["name"],
                         "n": len(rows),
                         "latency_ms": rows[0].get("latency_ms")})
            results.extend(rows)
            if len(used) >= max_channels:
                break
        else:
            err = (rows[0].get("error") if rows else "") or "无结果"
            skipped.append({"channel": ch, "reason": err})
            if not force and len(used) >= max_channels:
                break
    if state is not None and hasattr(state, "record_search_stat"):
        outcome = "hit" if results else "miss"
        try:
            state.record_search_stat(outcome=outcome, channels_used=",".join(
                u["channel"] for u in used) or "none", query=query)
        except Exception:
            pass
    return {"query": query, "results": results, "channels_used": used, "skipped": skipped}


def search_channels_info() -> dict:
    """通道清单 + 实时可用性（供 /api/search/channels 与导航展示）。"""
    channels = []
    for ch in SEARCH_ORDER:
        meta = SEARCH_CHANNELS.get(ch, {})
        channels.append({
            "id": ch, "name": meta.get("name"), "kind": meta.get("kind"),
            "needs_key": meta.get("needs_key", False),
            "key_env": meta.get("key_env", ""),
            "key_configured": bool(resolve_search_key(ch)) if meta.get("key_env") else None,
            "available": channel_available(ch),
            "reason": channel_reason(ch),
            "probe": meta.get("probe", ""),
            "order": SEARCH_ORDER.index(ch),
        })
    return {"object": "search_channels", "count": len(channels),
            "order": SEARCH_ORDER, "channels": channels}


__all__ = [
    "SEARCH_CHANNELS", "SEARCH_ORDER", "resolve_search_key", "resolve_search_base_url",
    "channel_available", "channel_reason", "search", "search_channels_info",
]