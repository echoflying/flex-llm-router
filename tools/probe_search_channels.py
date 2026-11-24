#!/usr/bin/env python3
"""搜索通道可用性探测（一次性评估脚本）

对点 farm 用过的全部搜索通道做真实调用，输出可用性矩阵：
  channel / ok / latency_ms / n_results / note（不回显任何 key）

用法：.venv/bin/python tools/probe_search_channels.py
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

QUERY = "聚贤楼酒店"
CITY = "上饶市"
DISTRICT = "玉山县"

UA_MOB = "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15"
UA_DESK = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

RESULTS: list[dict] = []


def _finish(row: dict) -> dict:
    RESULTS.append(row)
    flag = "OK " if row["ok"] else "ERR"
    print(f"[{flag}] {row['channel']:<16} {row['latency_ms']:>5}ms  n={row['n']:<3} {row['note']}")
    return row


def fetch(url: str, headers: dict | None = None, timeout: float = 12.0):
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
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            pass
        return e.code, body, int((time.perf_counter() - t0) * 1000), f"HTTP {e.code}"
    except Exception as e:
        return 0, "", int((time.perf_counter() - t0) * 1000), f"{type(e).__name__}: {str(e)[:80]}"


def count_pattern(html: str, pat: str) -> int:
    return len(re.findall(pat, html, re.S))


# ── 免 key 通道 ────────────────────────────────────────────────

def probe_searxng() -> None:
    url = ("http://127.0.0.1:8080/search?q=" + urllib.parse.quote(QUERY)
           + "&format=json&engines=bing,ddg,google")
    st, text, ms, err = fetch(url, timeout=15.0)
    n = 0
    if st == 200 and not err:
        try:
            n = len(json.loads(text).get("results", []))
        except Exception:
            n = 0
    _finish({"channel": "searxng", "ok": st == 200 and n > 0, "latency_ms": ms, "n": n,
             "note": (f"本地 127.0.0.1:8080 三引擎" if n else f"HTTP{st} {err or '无结果'}")[:60]})


def probe_bing() -> None:
    url = "https://www.bing.com/search?q=" + urllib.parse.quote(QUERY) + "&setlang=zh-CN&count=10"
    st, text, ms, err = fetch(url)
    n = count_pattern(text, r'<li class="b_algo"')
    _finish({"channel": "bing", "ok": st == 200 and n > 0, "latency_ms": ms, "n": n,
             "note": ("直爬 b_algo" if n else f"HTTP{st} {err or '反爬/无结果'}")[:60]})


def probe_baidu_web() -> None:
    url = "https://www.baidu.com/s?wd=" + urllib.parse.quote(QUERY) + "&rn=10"
    st, text, ms, err = fetch(url, {"Cookie": "BAIDUID=xxxx"})
    n = count_pattern(text, r'<h3[^>]*class="[^"]*t[^"]*"[^>]*>')
    _finish({"channel": "baidu_web", "ok": st == 200 and n > 0, "latency_ms": ms, "n": n,
             "note": ("直爬百度网页" if n else f"HTTP{st} {err or '反爬/无结果'}")[:60]})


def probe_sogou() -> None:
    url = "https://m.sogou.com/web/searchList.jsp?keyword=" + urllib.parse.quote(QUERY)
    st, text, ms, err = fetch(url, {"User-Agent": UA_MOB, "Referer": "https://www.sogou.com/"})
    n = count_pattern(text, r"<h3[^>]*>")
    _finish({"channel": "sogou", "ok": st == 200 and n > 0, "latency_ms": ms, "n": n,
             "note": ("搜狗移动版" if n else f"HTTP{st} {err or 'antispider/无结果'}")[:60]})


def probe_socom() -> None:
    url = "https://www.so.com/s?q=" + urllib.parse.quote(QUERY)
    st, text, ms, err = fetch(url)
    n = count_pattern(text, r"<h3[^>]*>")
    _finish({"channel": "socom", "ok": st == 200 and n > 0, "latency_ms": ms, "n": n,
             "note": ("360 网页搜索" if n else f"HTTP{st} {err or '限流/无结果'}")[:60]})


def probe_sogou_baike() -> None:
    url = ("https://baike.sogou.com/v2/API/lemmaShowV2?lemmaName="
           + urllib.parse.quote(QUERY))
    st, text, ms, err = fetch(url, {"Referer": "https://baike.sogou.com/"}, timeout=10.0)
    n = len(re.findall(r'"value":"([^"]{2,60})"', text))
    _finish({"channel": "sogou_baike", "ok": st == 200 and n > 0, "latency_ms": ms, "n": n,
             "note": ("搜狗百科 lemmaShowV2" if n else f"HTTP{st} {err or '无词条/无字段'}")[:60]})


def _360_state_pois(html: str) -> list[dict]:
    """从 360 移动端 HTML 的 window.__STATE__ 里栈配平提取 poi 数组。"""
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


def probe_poi_360() -> None:
    url = "https://m.map.360.cn/m/search?keyword=" + urllib.parse.quote(f"{QUERY} {CITY}")
    st, text, ms, err = fetch(url, {"User-Agent": UA_MOB}, timeout=12.0)
    n = len(_360_state_pois(text)) if st == 200 else 0
    _finish({"channel": "poi_360", "ok": n > 0, "latency_ms": ms, "n": n,
             "note": ("360 移动端 window.__STATE__" if n else f"HTTP{st} {err or '无 POI'}")[:60]})


def probe_osm() -> None:
    url = ("https://nominatim.openstreetmap.org/search?q=" + urllib.parse.quote(QUERY)
           + "&format=json&limit=5")
    st, text, ms, err = fetch(url, {"User-Agent": "flex-llm-router-probe/1.0"})
    n = 0
    if st == 200:
        try:
            n = len(json.loads(text))
        except Exception:
            n = 0
    _finish({"channel": "osm", "ok": n > 0, "latency_ms": ms, "n": n,
             "note": ("Nominatim 免 key" if n else f"HTTP{st} {err or '无结果'}")[:60]})


# ── 需 key 通道 ────────────────────────────────────────────────

def _load_env() -> dict:
    out = {}
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if not os.path.exists(path):
        return out
    for raw in open(path, encoding="utf-8"):
        s = raw.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


ENV = _load_env()


def probe_tencent_poi() -> None:
    key = ENV.get("TENCENT_WEB_API_KEY", "")
    if not key:
        _finish({"channel": "tencent_poi", "ok": False, "latency_ms": 0, "n": 0, "note": "缺 TENCENT_WEB_API_KEY"})
        return
    q = urllib.parse.urlencode({
        "key": key, "keyword": QUERY, "boundary": f"region({CITY},0)",
        "page_size": 5, "page_index": 1,
    })
    st, text, ms, err = fetch("https://apis.map.qq.com/ws/place/v1/search?" + q,
                             {"Referer": "http://localhost:5000"}, timeout=12.0)
    n = 0
    ok = False
    if st == 200:
        try:
            d = json.loads(text)
            n = len(d.get("data") or [])
            ok = int(d.get("status") or 0) == 0
        except Exception:
            pass
    _finish({"channel": "tencent_poi", "ok": ok and n > 0, "latency_ms": ms, "n": n,
             "note": ("place/v1/search + Referer 白名单" if ok else f"HTTP{st} {err or '无 POI'}")[:60]})


def probe_tavily() -> None:
    """Tavily 需付费 key；这里探测'是否配置'，key 缺失即标记。"""
    key = ENV.get("TAVILY_API_KEY", "")
    if not key:
        _finish({"channel": "tavily", "ok": False, "latency_ms": 0, "n": 0,
                 "note": "缺 TAVILY_API_KEY（flow 的 search_provider_keys 表 0 行，从未录入）"})
        return
    import urllib.request as ur
    body = json.dumps({"api_key": key, "query": QUERY, "search_depth": "advanced",
                       "max_results": 5}).encode()
    req = ur.Request("https://api.tavily.com/search", data=body,
                     headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        with ur.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
        ms = int((time.perf_counter() - t0) * 1000)
        n = len(d.get("results") or [])
        _finish({"channel": "tavily", "ok": n > 0, "latency_ms": ms, "n": n,
                 "note": "api.tavily.com/search 付费"})
    except Exception as e:
        _finish({"channel": "tavily", "ok": False, "latency_ms": int((time.perf_counter() - t0) * 1000),
                 "n": 0, "note": f"{type(e).__name__}: {str(e)[:60]}"})


PROBES = [
    probe_searxng, probe_bing, probe_baidu_web, probe_sogou, probe_socom,
    probe_sogou_baike, probe_poi_360, probe_osm, probe_tencent_poi, probe_tavily,
]

if __name__ == "__main__":
    print(f"探测 query={QUERY} city={CITY} district={DISTRICT}\n" + "-" * 78)
    for p in PROBES:
        try:
            p()
        except Exception as e:
            _finish({"channel": p.__name__.replace("probe_", ""), "ok": False, "latency_ms": 0,
                     "n": 0, "note": f"{type(e).__name__}: {str(e)[:60]}"})
        time.sleep(0.4)
    ok = [r["channel"] for r in RESULTS if r["ok"]]
    bad = [r["channel"] for r in RESULTS if not r["ok"]]
    print("-" * 78)
    print(f"可用 {len(ok)}/{len(RESULTS)}: {ok}")
    print(f"不可用 {len(bad)}: {bad}")
    with open("tools/probe_report.json", "w", encoding="utf-8") as f:
        json.dump(RESULTS, f, ensure_ascii=False, indent=2)
    print("报告 -> tools/probe_report.json")