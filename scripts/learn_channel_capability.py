#!/usr/bin/env python3
"""Learn a per-channel *capability profile* from historical attempts.

Contrast with `learned_limits`: that table learns scheduler-internal safe
rpm/tpm limits per (pool, channel).  This script learns a *channel-level*
behaviour profile and turns it into **usage advice**:

  - 429 density          : how often rpm/tpm_limit fires per active minute
  - cooldown behaviour   : how long a limit episode typically blocks the channel
  - TTFT / latency       : upstream response-speed distribution
  - quota reset          : when a daily quota is expected to release (from error_detail)
  - success rate         : observed outcome mix
  - hourly activity      : when the channel is busiest / least reliable (24h buckets)

It writes `data/channel_capability.json` (merge-on-run, keeps history) and
prints a human-readable table.  It never modifies the router config or state.

Usage:
    python3 scripts/learn_channel_capability.py [--days 14] [--min-samples 20]
"""

import argparse
import json
import re
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

QUOTA_RE = re.compile(
    r'(?:resets at\s+([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}(?::[0-9]{2})?\s+UTC)'
    r'|将于\s+([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2})\s*\(UTC\+8\)\s*恢复)')

# error_type families we treat as "the upstream pushed back" (vs hard errors)
LIMIT_TYPES = {'rpm_limit', 'tpm_limit', 'rate_limit', 'quota_exhausted'}
HARD_TYPES = {'upstream_total_timeout', 'upstream_response_timeout', 'server_error',
              'request_error', 'timeout', 'content_policy_blocked'}


def parse_quota_reset(detail):
    if not detail:
        return None
    m = QUOTA_RE.search(detail)
    if not m:
        return None
    # group(1): "YYYY-mm-dd HH:MM[:SS] UTC"; group(2): "YYYY-mm-dd HH:MM" (UTC+8)
    if m.group(1):
        try:
            ts = m.group(1).strip().replace(' UTC', '')
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                try:
                    return int(time.mktime(time.strptime(ts, fmt)))
                except ValueError:
                    continue
            return None
        except ValueError:
            return None
    try:
        # UTC+8 -> convert to epoch (subtract 8h)
        import datetime as _dt
        dt = _dt.datetime.strptime(m.group(2), '%Y-%m-%d %H:%M')
        dt = dt.replace(tzinfo=_dt.timezone(_dt.timedelta(hours=8)))
        return int(dt.timestamp())
    except ValueError:
        return None


def pct(a, b):
    return 0.0 if not b else round(100.0 * a / b, 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--days', type=float, default=14.0, help='lookback window in days')
    ap.add_argument('--min-samples', type=int, default=20,
                    help='channels with fewer attempts are reported as low-confidence')
    ap.add_argument('--out', default='data/channel_capability.json')
    args = ap.parse_args()

    db = sqlite3.connect('data/flex.db')
    db.row_factory = sqlite3.Row
    since = int(time.time() - args.days * 86400)

    # load config channel->pool membership so we can name a channel's pools
    pools = {}
    try:
        import yaml
        cfg = yaml.safe_load(open('config/pools.yaml'))
        for rname, rpool in (cfg.get('pools') or cfg.get('runners') or {}).items():
            for ch in (rpool.get('channels') or []):
                pools.setdefault(ch, []).append(rname)
    except Exception:
        pass

    rows = db.execute(
        "SELECT started, channel, outcome, error_type, latency_ms, ttft_ms, "
        "total_tokens, error_detail FROM attempts "
        "WHERE started>=? AND channel IS NOT NULL AND channel!=''", (since,)).fetchall()

    # per-channel accumulators
    stats = defaultdict(lambda: {
        'n': 0, 'ok': 0, 'limit': 0, 'hard': 0, 'other': 0,
        'ttft': [], 'lat': [],
        'limit_ts': [],  # timestamps of limit events, for density/cooldown analysis
        'quota_resets': [],  # observed reset epochs
        'hourly': defaultdict(lambda: {'n': 0, 'ok': 0, 'limit': 0}),
        'last': 0,
    })
    for r in rows:
        st = stats[r['channel']]
        st['n'] += 1
        st['last'] = max(st['last'], r['started'])
        if r['outcome'] == 'success':
            st['ok'] += 1
            if r['ttft_ms']:
                st['ttft'].append(r['ttft_ms'])
            if r['latency_ms']:
                st['lat'].append(r['latency_ms'])
        et = r['error_type']
        if et in LIMIT_TYPES:
            st['limit'] += 1
            st['limit_ts'].append(r['started'])
            if et == 'quota_exhausted':
                reset = parse_quota_reset(r['error_detail'])
                if reset:
                    st['quota_resets'].append(reset)
        elif et in HARD_TYPES:
            st['hard'] += 1
        else:
            st['other'] += 1
        # local hour bucket (Beijing = UTC+8)
        local_h = int((r['started'] + 8 * 3600) % 86400 // 3600)
        h = st['hourly'][local_h]
        h['n'] += 1
        if r['outcome'] == 'success':
            h['ok'] += 1
        if et in LIMIT_TYPES:
            h['limit'] += 1

    def pctile(vals, p):
        if not vals:
            return None
        s = sorted(vals)
        return int(s[min(len(s) - 1, int(len(s) * p))])

    # build profile
    profiles = {}
    for ch, st in stats.items():
        span = max(1, int(time.time() - since) // 3600)  # hours of window
        active_min = len(set(int(t // 60) for t in st['limit_ts']))  # distinct minutes w/ limits
        # cooldown: time between consecutive limit events (a repeated limit ~ means still blocked)
        gaps = sorted(b - a for a, b in zip(st['limit_ts'], st['limit_ts'][1:])
                      if 0 < b - a < 3600 * 12)
        quota_resets = sorted(st['quota_resets'])
        # 预测下一次重置：观察到的重置时刻多为「过去」发生的历史事件，需要推进到未来。
        # 若最近一次观察到的重置在过去的周期内，按 24h 周期推算下一次未来时刻。
        now = time.time()
        next_reset = None
        if quota_resets:
            last_obs = quota_resets[-1]
            next_reset = last_obs
            while next_reset < now:
                next_reset += 86400  # 每日配额周期（北京时区跨日）
            # 若已过去超过一个周期（说明最近没观察到新重置），仍给一个合理预测
        # busiest hour = max hourly volume; worst hour = max limit ratio
        hour_busy = max(st['hourly'].items(), key=lambda kv: kv[1]['n']) if st['hourly'] else None
        hour_worst = max(st['hourly'].items(),
                         key=lambda kv: kv[1]['limit'] / max(1, kv[1]['n'])) if st['hourly'] else None
        profiles[ch] = {
            'samples': st['n'],
            'window_days': args.days,
            'success_rate': pct(st['ok'], st['n']),
            'limit_rate': pct(st['limit'], st['n']),
            'hard_rate': pct(st['hard'], st['n']),
            'limit_count': st['limit'],
            'limit_density_per_active_min': round(st['limit'] / max(1, active_min), 2),
            'limit_gap_s_p50': pctile(gaps, 0.5),
            'limit_gap_s_p90': pctile(gaps, 0.9),
            'ttft_ms_p50': pctile(st['ttft'], 0.5),
            'ttft_ms_p95': pctile(st['ttft'], 0.95),
            'latency_ms_p50': pctile(st['lat'], 0.5),
            'quota_resets_utc': [int(t) for t in quota_resets[-5:]],
            'next_quota_reset': int(next_reset) if next_reset else None,
            'busiest_hour_beijing': hour_busy[0] if hour_busy else None,
            'worst_hour_beijing': hour_worst[0] if hour_worst else None,
            'pools': pools.get(ch, []),
            'last_attempt': int(st['last']),
        }

    # merge into persistent file (keep last-seen per channel)
    out = Path(args.out)
    prev = {}
    if out.exists():
        try:
            prev = json.loads(out.read_text()).get('channels', {})
        except Exception:
            prev = {}
    merged = dict(prev)
    for ch, p in profiles.items():
        merged[ch] = p
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({'updated_at': int(time.time()), 'days': args.days,
                               'channels': merged}, indent=1, ensure_ascii=False))

    # ---- render ----
    print(f"# Channel capability profile (last {args.days:g}d, saved {out})")
    print(f"{'channel':<32} {'n':>5} {'ok%':>5} {'limit%':>6} {'ttft50':>7} "
          f"{'ttft95':>7} {'density':>7} {'worstH':>6} {'nextReset':>11}")
    for ch in sorted(merged, key=lambda c: -(merged[c].get('samples') or 0)):
        p = merged[ch]
        if p.get('samples', 0) < args.min_samples:
            continue
        nr = p.get('next_quota_reset')
        nr_s = time.strftime('%m-%d %H:%M', time.localtime(nr)) if nr else '-'
        print(f"{ch[:32]:<32} {p['samples']:>5} {p['success_rate']:>5.1f} "
              f"{p['limit_rate']:>6.1f} {str(p['ttft_ms_p50'] or 0):>7} "
              f"{str(p['ttft_ms_p95'] or 0):>7} {p['limit_density_per_active_min']:>7} "
              f"{str(p['worst_hour_beijing'] or '-'):>6} {nr_s:>11}")

    # ---- usage advice ----
    print("\n# Usage advice (accumulated; re-print at each run)")
    for ch in sorted(merged, key=lambda c: -(merged[c].get('samples') or 0)):
        p = merged[ch]
        if p.get('samples', 0) < args.min_samples:
            continue
        tips = []
        if p['success_rate'] >= 95:
            tips.append('可靠主力')
        elif p['success_rate'] >= 80:
            tips.append('可用')
        elif p['success_rate'] >= 60:
            tips.append('偶发限流')
        else:
            tips.append('频繁受限')
        if p['limit_density_per_active_min'] >= 1.0:
            tips.append(f"429 密集（{p['limit_density_per_active_min']}/活跃分钟）")
        if p['ttft_ms_p95'] and p['ttft_ms_p95'] > 15000:
            tips.append(f"慢响应（p95={p['ttft_ms_p95']/1000:.1f}s）")
        if p['next_quota_reset'] and p['next_quota_reset'] > time.time():
            tips.append(f"配额恢复≈{time.strftime('%m-%d %H:%M', time.localtime(p['next_quota_reset']))}")
        if p['worst_hour_beijing'] is not None and p['worst_hour_beijing'] is not None:
            tips.append(f"最差时段=北京{p['worst_hour_beijing']:02d}点")
        print(f"  {ch:<32} {'; '.join(tips)}")


if __name__ == '__main__':
    main()
