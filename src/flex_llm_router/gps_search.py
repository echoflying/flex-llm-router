"""Point Farm GPS 坐标搜索服务（独立模块，供 Flask 路由 /api/gps/search 调用）。

设计目标：
  - 对外统一暴露「店名 → 经纬度」搜索能力，调用方不用读 engine.mjs 源码。
  - 四个源：360 / 百度 / 高德 / 腾讯。360、百度可用；高德、腾讯代码就绪但默认挂起。
  - 优先级档：normal（360→百度→高德→腾讯）、high（质量 high 的可用源）。
  - 显式指定 sources 时完全覆盖默认链。
  - Fallback：前一源无有效数据 → 下一源继续，直到命中或链尽。
  - 统计：每次源尝试记一条，存 SQLite。

与 engine.mjs 的关系：本模块复刻 engine.mjs 的 geocodeShop360/geocodeShopBaidu 逻辑
（Python 版），并新增高德/腾讯适配器。engine.mjs 不动；Flask app 通过本模块对外提供能力。
"""

from __future__ import annotations

import json
import math
import os
import re
import time
import urllib.parse
import urllib.request

# 坐标转换（WGS-84 / GCJ-02 / BD-09）——内联实现，本模块完全独立，不依赖点 farm 的 scripts
def gcj02_to_wgs84(lng, lat):
    a, ee = 6378245.0, 0.00669342162296594323
    if not (72.004 < lng < 137.8347 and 0.8293 < lat < 55.8271):
        return lng, lat
    def _t_lat(x, y):
        ret = -100.0 + 2.0*x + 3.0*y + 0.2*y*y + 0.1*x*y + 0.2*math.sqrt(abs(x))
        ret += (20.0*math.sin(6.0*x*math.pi) + 20.0*math.sin(2.0*x*math.pi))*2.0/3.0
        ret += (20.0*math.sin(y*math.pi) + 40.0*math.sin(y/3.0*math.pi))*2.0/3.0
        ret += (160.0*math.sin(y/12.0*math.pi) + 320*math.sin(y*math.pi/30.0))*2.0/3.0
        return ret
    def _t_lng(x, y):
        ret = 300.0 + x + 2.0*y + 0.1*x*x + 0.1*x*y + 0.1*math.sqrt(abs(x))
        ret += (20.0*math.sin(6.0*x*math.pi) + 20.0*math.sin(2.0*x*math.pi))*2.0/3.0
        ret += (20.0*math.sin(x*math.pi) + 40.0*math.sin(x/3.0*math.pi))*2.0/3.0
        ret += (150.0*math.sin(x/12.0*math.pi) + 300*math.sin(x/30.0*math.pi))*2.0/3.0
        return ret
    dlat = _t_lat(lng-105.0, lat-35.0)
    dlng = _t_lng(lng-105.0, lat-35.0)
    radlat = lat/180.0*math.pi
    magic = math.sin(radlat)
    magic = 1 - ee*magic*magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat*180.0)/((a*(1-ee))/(magic*sqrtmagic)*math.pi)
    dlng = (dlng*180.0)/(a/sqrtmagic*math.cos(radlat)*math.pi)
    return lng-dlng, lat-dlat
def bd09_to_gcj02(bd_lng, bd_lat):
    xpi = math.pi*3000.0/180.0
    x, y = bd_lng-0.0065, bd_lat-0.006
    z = math.sqrt(x*x+y*y) - 0.00002*math.sin(y*xpi)
    theta = math.atan2(y, x) - 0.000003*math.cos(x*xpi)
    return z*math.cos(theta), z*math.sin(theta)


def wgs84_to_gcj02(lng, lat):
    """WGS-84 → GCJ-02（正向偏移，与 gcj02_to_wgs84 逆向互配）."""
    a, ee = 6378245.0, 0.00669342162296594323
    def _transform_lat(x, y):
        ret = -100.0 + 2.0*x + 3.0*y + 0.2*y*y + 0.1*x*y + 0.2*math.sqrt(abs(x))
        ret += (20.0*math.sin(6.0*x*math.pi) + 20.0*math.sin(2.0*x*math.pi))*2.0/3.0
        ret += (20.0*math.sin(y*math.pi) + 40.0*math.sin(y/3.0*math.pi))*2.0/3.0
        ret += (160.0*math.sin(y/12.0*math.pi) + 320*math.sin(y*math.pi/30.0))*2.0/3.0
        return ret
    def _transform_lng(x, y):
        ret = 300.0 + x + 2.0*y + 0.1*x*x + 0.1*x*y + 0.1*math.sqrt(abs(x))
        ret += (20.0*math.sin(6.0*x*math.pi) + 20.0*math.sin(2.0*x*math.pi))*2.0/3.0
        ret += (20.0*math.sin(x*math.pi) + 40.0*math.sin(x/3.0*math.pi))*2.0/3.0
        ret += (150.0*math.sin(x/12.0*math.pi) + 300*math.sin(x/30.0*math.pi))*2.0/3.0
        return ret
    if not (72.004 < lng < 137.8347 and 0.8293 < lat < 55.8271):
        return lng, lat  # 国境外不加偏移
    dlat = _transform_lat(lng-105.0, lat-35.0)
    dlng = _transform_lng(lng-105.0, lat-35.0)
    radlat = lat/180.0*math.pi
    magic = math.sin(radlat)
    magic = 1 - ee*magic*magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat*180.0)/((a*(1-ee))/(magic*sqrtmagic)*math.pi)
    dlng = (dlng*180.0)/(a/sqrtmagic*math.cos(radlat)*math.pi)
    return lng+dlng, lat+dlat


def gcj02_to_bd09(gj_lng, gj_lat):
    """GCJ-02 → BD-09（百度私有坐标系）."""
    xpi = math.pi*3000.0/180.0
    z = math.sqrt(gj_lng*gj_lng + gj_lat*gj_lat) + 0.00002*math.sin(gj_lat*xpi)
    theta = math.atan2(gj_lat, gj_lng) + 0.000003*math.cos(gj_lng*xpi)
    return z*math.cos(theta)+0.0065, z*math.sin(theta)+0.006


# ── 名称相似度（Dice bigram + 品牌前缀惩罚）──────────────────
def norm_shop_name(s):
    s = str(s or '')
    s = re.sub(r'[（(][^）)]*[）)]', '', s)
    s = re.sub(r'^(老字号|正宗|百年|通称|本地|据说)', '', s)
    s = re.sub(r'(老店|老街|老字号)', '', s)
    s = re.sub(r'[店馆庄楼铺坊堂]$', '', s)
    return re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]', '', s)


def bigram_set(s):
    return {s[i:i+2] for i in range(len(s) - 1)}


def brand_prefix(n):
    m = re.match(r'^([\u4e00-\u9fa5])(记|氏|家)', n)
    return m.group(1) + m.group(2) if m else ''


def name_similarity(a, b):
    na, nb = norm_shop_name(a), norm_shop_name(b)
    A, B = bigram_set(na), bigram_set(nb)
    if not A or not B:
        return 0.0
    inter = len(A & B)
    sim = (2.0 * inter) / (len(A) + len(B))
    pa, pb = brand_prefix(na), brand_prefix(nb)
    if pa and pb and pa != pb:
        sim *= 0.2
    return sim


NAME_MATCH_MIN = 0.6
NAME_MATCH_WEAK = 0.45


# ── 源注册表 ──────────────────────────────────────────────
GPS_SOURCES = {
    '360':    {'name': '360地图', 'quality': 'high',   'coord': 'GCJ-02→WGS-84'},
    'baidu':  {'name': '百度地图', 'quality': 'medium', 'coord': '墨卡托→BD-09→GCJ-02→WGS-84'},
    'amap':   {'name': '高德地图', 'quality': 'high',   'coord': 'GCJ-02→WGS-84'},
    'tencent': {'name': '腾讯地图', 'quality': 'medium', 'coord': 'GCJ-02→WGS-84'},
}

GPS_ORDER = {
    'normal': ['360', 'baidu', 'amap', 'tencent'],
    'high':   ['360', 'amap'],
}

# Key 管理：对齐 config.py 的 Provider 模型——只存 env var 名，key 值由 .env 注入。
# 360/百度无需 key。缺失 env 时 adapter 返回 disabled，不报错。
GPS_PROVIDERS = {
    'amap':    {'api_key_env': 'AMAP_WEB_API_KEY', 'fallback_key_env': 'AMAP_KEY'},
    'tencent': {'api_key_env': 'TENCENT_WEB_API_KEY'},
}

# 需要 key 的源：配置上可用 = key 存在。360/百度无需 key，恒可用。
_KEY_REQUIRED_SOURCES = {'amap', 'tencent'}


def _source_available(source_id):
    """动态可用性：需要 key 的源必须有 key；360/百度恒可用。"""
    if source_id in _KEY_REQUIRED_SOURCES:
        return bool(resolve_api_key(source_id))
    return True


def _source_reason(source_id):
    """动态原因：需要 key 且缺失时说明缺哪个 env var。"""
    if source_id in _KEY_REQUIRED_SOURCES and not resolve_api_key(source_id):
        envs = GPS_PROVIDERS.get(source_id, {})
        names = [envs.get('api_key_env'), envs.get('fallback_key_env')]
        names = [n for n in names if n]
        return f'{source_id} 未配置 key（需 env: {" / ".join(names)}）'
    return None


def resolve_api_key(source_id):
    """从 env 解析指定源的 API key。缺失返回 ''。key 值绝不写日志。"""
    p = GPS_PROVIDERS.get(source_id)
    if not p:
        return ''
    for env in (p.get('api_key_env'), p.get('fallback_key_env')):
        if env:
            v = (os.getenv(env) or '').strip()
            if v:
                return v
    return ''


def _http_get_json(url, referer=None, timeout=15):
    # 完整 Chrome UA：百度 map.baidu.com 对简写 "Mozilla/5.0" 返回 403，必须完整 UA
    headers = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/120.0.0.0 Safari/537.36')}
    if referer:
        headers['Referer'] = referer
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _strip_suffix(s, suffixes=('市', '县', '区')):
    s = str(s or '')
    for sfx in suffixes:
        if s.endswith(sfx):
            return s[:-len(sfx)]
    return s


def _hit(ok, **kw):
    base = {'ok': ok}
    base.update(kw)
    return base


def geocode_shop_360(name, city, district):
    kw = urllib.parse.quote(f'{name} {_strip_suffix(district or city or "")}')
    url = f'https://restapi.map.360.cn/newapi?d=pc&brand_cpc=on&keyword={kw}&cityname={urllib.parse.quote(city or "")}&sid=1014&_={int(time.time()*1000)}'
    try:
        d = _http_get_json(url)
    except Exception as e:
        return _hit(False, error=f'{e}')
    lst = (d or {}).get('list') or []
    if not lst:
        return _hit(False, error='no poi')
    scored = sorted(((name_similarity(name, p.get('name', '')), p) for p in lst), key=lambda t: -t[0])
    best_s, poi = scored[0]
    if best_s < NAME_MATCH_WEAK:
        return _hit(False, error=f'weak name match (best={poi.get("name", "-")} sim={best_s:.2f})',
                    weak={'name': poi.get('name', ''), 'sim': round(best_s, 2), 'addr': poi.get('address', '')})
    poi_dist = f'{poi.get("district", "")} {poi.get("address", "")}'
    dist_key = _strip_suffix(district)
    if dist_key and poi_dist.strip() and dist_key not in poi_dist:
        return _hit(False, error=f'district mismatch: poi={poi_dist} (want {dist_key})')
    lng, lat = float(poi.get('x', 0) or 0), float(poi.get('y', 0) or 0)
    if not lng or not lat:
        return _hit(False, error='no coords')
    wgs = gcj02_to_wgs84(lng, lat)
    return _hit(True, name=poi.get('name', name), address=poi.get('address', ''),
                district=poi.get('district', ''), adcode=poi.get('adcode'),
                match_sim=round(best_s, 2), match_level='strong' if best_s >= NAME_MATCH_MIN else 'medium',
                gcj02={'lng': lng, 'lat': lat}, wgs84={'lng': wgs[0], 'lat': wgs[1]},
                raw={'x': lng, 'y': lat})


def _mercator_to_lnglat(mx, my):
    lng = mx / 20037508.34 * 180
    lat = math.atan(math.exp(my / 20037508.34 * math.pi)) * 2 - math.pi / 2
    return lng, lat * 180 / math.pi


def geocode_shop_baidu(name, city, district):
    kw = urllib.parse.quote(f'{name} {_strip_suffix(district or city or "")}')
    url = f'https://map.baidu.com/?newmap=1&qt=s&wd={kw}&from=webmap&biz_forward=SG&tn=B_NORMAL'
    try:
        text = _http_get_json(url)
    except Exception as e:
        return _hit(False, error=f'{e}')
    if isinstance(text, dict):
        d = text
    else:
        text = str(text)
        s = text.find('{')
        if s < 0:
            return _hit(False, error='no json')
        depth, e = 0, len(text)
        for i in range(s, len(text)):
            if text[i] == '{':
                depth += 1
            elif text[i] == '}':
                depth -= 1
                if depth == 0:
                    e = i + 1
                    break
        try:
            d = json.loads(text[s:e])
        except Exception:
            return _hit(False, error='no json')
    content = (d or {}).get('content') or []
    if not content:
        return _hit(False, error='no poi')
    scored = sorted(((name_similarity(name, p.get('name', '')), p) for p in content), key=lambda t: -t[0])
    best_s, poi = scored[0]
    if best_s < NAME_MATCH_WEAK:
        return _hit(False, error=f'weak name match (best={poi.get("name", "-")} sim={best_s:.2f})',
                    weak={'name': poi.get('name', ''), 'sim': round(best_s, 2), 'addr': poi.get('addr', '')})
    poi_dist = poi.get('addr', '') or poi.get('area_name', '')
    dist_key = _strip_suffix(district)
    if dist_key and poi_dist and dist_key not in poi_dist:
        return _hit(False, error=f'district mismatch: poi={poi_dist} (want {dist_key})')
    mx = float(poi.get('diPointX', 0) or 0) / 100 or float(poi.get('x', 0) or 0)
    my = float(poi.get('diPointY', 0) or 0) / 100 or float(poi.get('y', 0) or 0)
    if not mx or not my or not (7e6 < mx < 2e7 and 1e6 < my < 1e7):
        return _hit(False, error='no coords')
    lng0, lat0 = _mercator_to_lnglat(mx, my)
    g_lng, g_lat = bd09_to_gcj02(lng0, lat0)
    wgs = gcj02_to_wgs84(g_lng, g_lat)
    return _hit(True, name=poi.get('name', name), address=poi.get('addr', ''),
                district=poi.get('area_name', ''), adcode=(poi.get('admin_info') or {}).get('area_id'),
                match_sim=round(best_s, 2), match_level='strong' if best_s >= NAME_MATCH_MIN else 'medium',
                bd09={'lng': lng0, 'lat': lat0}, wgs84={'lng': wgs[0], 'lat': wgs[1]},
                raw={'x': mx, 'y': my})


def _poi_name(p):
    return p.get('title') or p.get('name') or ''


def _poi_district_text(p):
    """综合地址文本，用于 district 二次过滤（兼容 amap/baidu/tencent 不同字段）。
    tencent 用 ad_info.district；amap/baidu 用 adname / district。"""
    parts = [p.get('address', ''), p.get('adname', ''), p.get('district', '')]
    ad = p.get('ad_info') or {}
    if isinstance(ad, dict):
        parts.append(ad.get('district', ''))
    return ' '.join(str(x) for x in parts)


def _best_poi(pois, name, district):
    """候选排序：优先选 district 匹配的 POI，再按 sim 降序。

    region(city,0) 边界不严会返回跨区 POI，必须按 district 二次过滤；
    在匹配子集里取 sim 最高，避免"最佳 sim 是跨区、本区略低"被误判。
    返回 (best_sim, poi, district_ok)。
    """
    scored = sorted(((name_similarity(name, _poi_name(p)), p) for p in pois), key=lambda t: -t[0])
    if not scored:
        return 0.0, None, False
    dist_key = _strip_suffix(district) if district else ''
    if dist_key:
        for s, p in scored:
            pd = _poi_district_text(p)
            if pd.strip() and dist_key in pd:
                return s, p, True
    return scored[0][0], scored[0][1], False


def geocode_shop_amap(name, city, district):
    key = resolve_api_key('amap')
    if not key:
        return _hit(False, error='no amap key', disabled=True)
    kw = urllib.parse.quote(f'{name} {_strip_suffix(district or city or "")}')
    url = f'https://restapi.amap.com/v3/place/text?key={urllib.parse.quote(key)}&keywords={kw}&city={urllib.parse.quote(city or "")}&citylimit=1&offset=20&page=1&output=JSON'
    try:
        d = _http_get_json(url)
    except Exception as e:
        return _hit(False, error=f'{e}')
    if str(d.get('status')) != '1':
        info_code = str(d.get('infocode', ''))
        return _hit(False, error=d.get('info', 'amap status != 1'), tstatus=info_code, quota=(info_code == '10044'))
    pois = d.get('pois') or []
    if not pois:
        return _hit(False, error='no poi')
    best_s, poi, district_ok = _best_poi(pois, name, district)
    if poi is None:
        return _hit(False, error='no poi')
    if best_s < NAME_MATCH_WEAK:
        return _hit(False, error=f'weak name match (best={_poi_name(poi)} sim={best_s:.2f})',
                    weak={'name': _poi_name(poi), 'sim': round(best_s, 2), 'addr': poi.get('address', '')})
    loc = (poi.get('location') or '').split(',')
    if len(loc) != 2:
        return _hit(False, error='no coords')
    lng, lat = float(loc[0]), float(loc[1])
    if _strip_suffix(district) and not district_ok:
        return _hit(False, error=f'district mismatch: poi={_poi_district_text(poi)} (want {_strip_suffix(district)})')
    wgs = gcj02_to_wgs84(lng, lat)
    return _hit(True, name=_poi_name(poi) or name, address=poi.get('address', ''),
                district=poi.get('adname', ''), adcode=poi.get('adcode'),
                match_sim=round(best_s, 2), match_level='strong' if best_s >= NAME_MATCH_MIN else 'medium',
                gcj02={'lng': lng, 'lat': lat}, wgs84={'lng': wgs[0], 'lat': wgs[1]},
                raw={'x': lng, 'y': lat})


def geocode_shop_tencent(name, city, district):
    key = resolve_api_key('tencent')
    if not key:
        return _hit(False, error='no tencent key', disabled=True)
    kw = urllib.parse.quote(f'{name} {_strip_suffix(district or city or "")}')
    boundary = f'region({city or district or ""},0)'
    url = f'https://apis.map.qq.com/ws/place/v1/search?key={urllib.parse.quote(key)}&keyword={kw}&boundary={urllib.parse.quote(boundary)}&page_size=20'
    try:
        d = _http_get_json(url, referer='https://xp.zaitu.cn')
    except Exception as e:
        return _hit(False, error=f'{e}')
    if d.get('status') != 0:
        st = d.get('status')
        return _hit(False, error=d.get('message', f'tencent status {st}'), tstatus=st, quota=(st == 120))
    data = d.get('data') or []
    if not data:
        return _hit(False, error='no poi')
    best_s, poi, district_ok = _best_poi(data, name, district)
    if poi is None:
        return _hit(False, error='no poi')
    if best_s < NAME_MATCH_WEAK:
        return _hit(False, error=f'weak name match (best={_poi_name(poi)} sim={best_s:.2f})',
                    weak={'name': _poi_name(poi), 'sim': round(best_s, 2), 'addr': poi.get('address', '')})
    loc = poi.get('location') or {}
    lng, lat = float(loc.get('lng', 0) or 0), float(loc.get('lat', 0) or 0)
    if not lng or not lat:
        return _hit(False, error='no coords')
    if _strip_suffix(district) and not district_ok:
        return _hit(False, error=f'district mismatch: poi={_poi_district_text(poi)} (want {_strip_suffix(district)})')
    wgs = gcj02_to_wgs84(lng, lat)
    return _hit(True, name=_poi_name(poi), address=poi.get('address', ''),
                district=poi.get('district', ''), adcode=(poi.get('ad_info') or {}).get('adcode'),
                match_sim=round(best_s, 2), match_level='strong' if best_s >= NAME_MATCH_MIN else 'medium',
                gcj02={'lng': lng, 'lat': lat}, wgs84={'lng': wgs[0], 'lat': wgs[1]},
                raw={'x': lng, 'y': lat})


_ADAPTERS = {
    '360': geocode_shop_360,
    'baidu': geocode_shop_baidu,
    'amap': geocode_shop_amap,
    'tencent': geocode_shop_tencent,
}


def gps_search(name, city=None, district=None, sources=None, priority='normal', state=None):
    """统一入口。state: StateStore 实例（与 LLM 调用数据同库 flex.db），None 则不落统计。"""
    chain = sources or GPS_ORDER.get(priority) or GPS_ORDER['normal']
    attempts = []
    had_fail = False
    for sid in chain:
        src = GPS_SOURCES.get(sid)
        if not src:
            attempts.append({'source': sid, 'outcome': 'disabled', 'sim': None, 'error': 'unknown source'})
            had_fail = True
            continue
        if not _source_available(sid):
            attempts.append({'source': sid, 'outcome': 'disabled', 'sim': None, 'error': _source_reason(sid) or 'disabled'})
            had_fail = True
            continue
        r = _ADAPTERS[sid](name, city, district)
        outcome = 'hit' if r.get('ok') else 'miss'
        attempts.append({'source': sid, 'outcome': outcome,
                         'sim': r.get('match_sim'), 'error': r.get('error')})
        if state is not None:
            try:
                state.record_gps_stat(sid, priority, outcome, r.get('match_sim'),
                                      had_fail, name, r.get('error'))
            except Exception:
                pass
        if r.get('ok'):
            return {'ok': True, 'source': sid, 'coord': r['wgs84'], 'gcj02': r.get('gcj02'),
                    'matchSim': r.get('match_sim'), 'matchLevel': r.get('match_level'),
                    'poiName': r.get('name'), 'poiAddr': r.get('address'),
                    'adcode': r.get('adcode'), 'attempts': attempts, 'raw': r.get('raw')}
        had_fail = True
    return {'ok': False, 'source': None, 'coord': None, 'gcj02': None, 'attempts': attempts}


def gps_sources_info():
    return {'sources': [{'id': sid, **meta,
                         'available': _source_available(sid),
                         'reason': _source_reason(sid)} for sid, meta in GPS_SOURCES.items()],
            'order': GPS_ORDER}
