#!/usr/bin/env python3
"""Router 失败/挂住调用监控脚本。

由 launchd 每 30 分钟调用一次（label: com.weifeng.flex-monitor）。
检查内容：
  1. 最近 WINDOW 秒内 failed/cancelled 的调用（失败原因、错误类型）
  2. 长时间 running 未完成（挂住）的调用
  3. debug_logs 中 router_* 关键日志（诊断"信息被吞/挂住"用）
结果写入 data/monitor/report.md（覆盖）与 data/monitor/history.md（追加）。
"""
from __future__ import annotations
import json, os, sqlite3, time
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / 'data' / 'flex.db'
OUT_DIR = ROOT / 'data' / 'monitor'
REPORT = OUT_DIR / 'report.md'
HISTORY = OUT_DIR / 'history.md'
WINDOW = 3600          # 最近 1 小时
HANG_SECONDS = 120     # running 超过该秒数视为疑似挂住
MAX_ROWS = 30

def fmt(ts):
    return datetime.fromtimestamp(ts).strftime('%m-%d %H:%M:%S') if ts else '—'

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    out = []
    out.append(f'# Router 监控报告\n生成时间：{fmt(now)}（每 30 分钟自动刷新）\n')

    # ---- 1. 最近失败/取消的调用 ----
    failed = db.execute(
        """SELECT trace_id, started, updated, status, pool, requested_model, final_channel,
                  http_status, error_type, substr(COALESCE(error_detail,''),1,150) AS ed
           FROM request_traces
           WHERE status IN ('failed','cancelled') AND updated >= ?
           ORDER BY updated DESC LIMIT ?""", (now - WINDOW, MAX_ROWS)).fetchall()
    out.append(f'## 最近 1 小时失败/取消调用（{len(failed)} 条）')
    if failed:
        out.append('| 时间 | trace | pool | 状态 | HTTP | 错误类型 | 错误详情 |')
        out.append('|---|---|---|---|---|---|---|')
        for r in failed:
            out.append(f"| {fmt(r['updated'])} | `{r['trace_id'][:16]}` | {r['pool']} | {r['status']} | {r['http_status'] or ''} | {r['error_type'] or ''} | {r['ed']} |")
    else:
        out.append('（无）')

    # ---- 2. 疑似挂住：running 超过阈值 ----
    running = db.execute(
        """SELECT trace_id, started, updated, pool, requested_model, attempt_count
           FROM request_traces WHERE status='running' ORDER BY started""").fetchall()
    hangs = [r for r in running if now - r['started'] > HANG_SECONDS]
    out.append(f'\n## 疑似挂住（running 超过 {HANG_SECONDS}s，共 {len(hangs)} 条 / running 总计 {len(running)}）')
    if hangs:
        for r in hangs:
            last = db.execute("SELECT datetime(ts,'unixepoch','localtime') t,event,substr(COALESCE(detail,''),1,100) d FROM trace_events WHERE trace_id=? ORDER BY id DESC LIMIT 1",(r['trace_id'],)).fetchone()
            out.append(f"- `{r['trace_id'][:20]}` pool={r['pool']} 挂 {int(now-r['started'])}s 最后事件: {last['t']} {last['event']} {last['d']}")
    else:
        out.append('（无）')
    out.append('')
    for r in running:
        out.append(f"  running-ok: `{r['trace_id'][:20]}` {fmt(r['started'])} {int(now-r['started'])}s pool={r['pool']}")

    # ---- 3. debug 日志：router_* 关键事件（诊断挂住/信息丢失）----
    dbg = db.execute("""SELECT datetime(ts,'unixepoch','localtime') t, request_id, dir, channel, model, status, substr(body,1,140) b
                        FROM debug_logs WHERE dir LIKE 'router_%' ORDER BY id DESC LIMIT 40""").fetchall()
    out.append(f'\n## debug_logs router_* 关键日志（最近 {len(dbg)} 条）')
    if dbg:
        for r in reversed(dbg):
            out.append(f"- {r['t']} `{r['request_id'][:20]}` {r['dir']} ch={r['channel']} {r['b']}")
    else:
        out.append('（核心尚未重启，暂未采集 router_* 日志；重启后生效）')

    # ---- 4. 最近 upstream_in 与 client_out 条数对比（信息是否被吞的线索）----
    up_in = db.execute("SELECT count(*) FROM debug_logs WHERE dir='upstream_in' AND ts>=?",(now-WINDOW,)).fetchone()[0]
    cl_out = db.execute("SELECT count(*) FROM debug_logs WHERE dir='client_out' AND ts>=?",(now-WINDOW,)).fetchone()[0]
    out.append(f'\n## 最近 1 小时 upstream_in={up_in} 条 / client_out={cl_out} 条（差距大可能说明上游内容未转发）')

    body = '\n'.join(out) + '\n'
    # 覆盖最新 + 追加历史
    REPORT.write_text(body, encoding='utf-8')
    with HISTORY.open('a', encoding='utf-8') as fh:
        fh.write(f'\n===== {fmt(now)} =====\n' + body)
    # 只保留最近 24 小时（每 30 分钟一份 = 48 份）
    hist = HISTORY.read_text(encoding='utf-8').split('===== ')
    if len(hist) > 49:
        HISTORY.write_text('===== ' + '===== '.join(hist[-49:]), encoding='utf-8')
    print(f'[monitor] report written: {REPORT} (failed={len(failed)} hangs={len(hangs)})')

if __name__ == '__main__':
    main()