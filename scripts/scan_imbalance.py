#!/usr/bin/env python3
"""Scan all multi-channel Runners for channel load imbalance.

Detects the failure mode where one Channel absorbs the vast majority of
traffic while sibling Channels stay at (or near) zero calls.  The canonical
cause in this router was the *output-capacity preference* filter in
app.py:2902: when the client requests a large ``max_completion_tokens`` and
only one Channel has a ``max_output_tokens`` high enough to satisfy it, the
others are dropped from ``available`` (marked "需夹取") and the round_robin
scheduler spins over a single-element list.  That is a **config defect**
(unequal ``max_output_tokens``), not an upstream condition.

This script is deliberately observational: it prints a per-Runner table and a
verdict, and never modifies anything.  The caller (hourly patrol / daily
review) decides whether a flagged Runner warrants a fix.

Usage:
    python3 scripts/scan_imbalance.py [--hours 6] [--share 0.80]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

import yaml  # noqa: E402


def load_pools():
    from flex_llm_router.config import load_config  # type: ignore
    cfg = load_config(Path('config/pools.yaml'))
    # config exposes .runners (dict of runner name -> pool config)
    return getattr(cfg, 'runners', None) or {}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--hours', type=float, default=6.0, help='lookback window in hours')
    ap.add_argument('--share', type=float, default=0.80, help='share threshold (0..1)')
    ap.add_argument('--idle-max', type=int, default=2,
                    help='a channel counts as "idle" if it has <= this many calls in the window')
    args = ap.parse_args()

    import sqlite3
    db = sqlite3.connect('data/flex.db')
    db.row_factory = sqlite3.Row
    since = int(__import__('time').time() - args.hours * 3600)

    rows = db.execute(
        "SELECT pool, channel, COUNT(*) n FROM attempts "
        "WHERE started>=? AND pool NOT LIKE 'direct:%' GROUP BY pool, channel",
        (since,)).fetchall()
    counts = {}
    for r in rows:
        counts.setdefault(r['pool'], {})[r['channel']] = r['n']

    pools = load_pools()
    flagged = []
    print(f"# Channel-load imbalance scan (last {args.hours:g}h, "
          f"share threshold {args.share:.0%}, idle <= {args.idle_max} calls)")
    for name, pool in pools.items():
        # pool may be a pydantic Runner (config.load_config) or plain dict.
        getter = (lambda k, d=None: getattr(pool, k, d)) if not isinstance(pool, dict) \
                 else (lambda k, d=None: pool.get(k, d))
        chs = list(getter('channels') or [])
        if len(chs) < 2:
            continue  # single-channel Runner is intentional (see ROUTER_INCIDENTS.md)
        dist = counts.get(name, {})
        total = sum(dist.get(c, 0) for c in chs)
        if total == 0:
            continue  # no traffic in window; nothing to judge
        top = max(chs, key=lambda c: dist.get(c, 0))
        top_n = dist.get(top, 0)
        share = top_n / total
        idle = [c for c in chs if dist.get(c, 0) <= args.idle_max]
        # A Runner with an idle sibling is *expected* when that sibling is a
        # degraded upstream or deliberately secondary; we only flag the pattern
        # "one channel > share AND at least one sibling ~idle".
        is_flag = share >= args.share and bool(idle)
        print(f"{'IMBALANCE' if is_flag else '  ok      '} {name:<30} "
              f"total={total:<5} top={top.split('-')[-1]}@{share:.0%}  "
              f"idle={[c.split('-')[-1] for c in idle]}")
        if is_flag:
            flagged.append((name, top, share, idle, dist, chs))

    if not flagged:
        print("\nVerdict: no imbalance (no multi-channel Runner has a single "
              "channel > {:.0%} with an idle sibling)".format(args.share))
        return 0

    print(f"\nVerdict: {len(flagged)} Runner(s) show load imbalance.")
    print("For each, inspect whether the idle sibling is being excluded by:")
    print("  - output-capacity preference (app.py:2902)  -> config defect, unequal max_output_tokens")
    print("  - capability / context compatibility filter -> upstream or config")
    print("  - active cooldown (states table)            -> upstream limit, expected")
    print("  - degraded upstream (known baseline)        -> expected, see ROUTER_INCIDENTS.md")
    return 1


if __name__ == '__main__':
    sys.exit(main())
