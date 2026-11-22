#!/usr/bin/env bash
# probe_router.sh — OS-level watchdog for the Flex LLM Router (:7800).
# Designed to run via launchd every 60s, independent of the router process
# and independent of any Python session / DSH agent.
#
# Checks (in order of cheapness):
#   1) is there a listener on :7800 at all?
#   2) has flex-router.log been written in the last 5 minutes?
#   3) are there zombie traces (status='running' started >15 min ago)?
#
# Actions on finding a problem:
#   - port gone         → try `launchctl kickstart -k`, then alert
#   - log stale (>5m)   → alert only (process alive but may be hung)
#   - zombies > 5       → alert; if combined with stale log, also kickstart
#
# All decisions are logged to logs/probe.out.log.

set -uo pipefail

PROJ=/Users/weifeng/projects/flex-llm-router
LOG="$PROJ/logs/probe.out.log"
ERR_LOG="$PROJ/logs/probe.err.log"
PORT=7800
LABEL=com.weifeng.flex-llm-router
DB="$PROJ/data/flex.db"
ROUTER_LOG="$PROJ/logs/flex-router.log"
ALERT_THRESHOLD_MINUTES=5
ZOMBIE_AGE_MINUTES=15
ZOMBIE_COUNT_WARN=5
ZOMBIE_COUNT_CRIT=10

ts() { date "+%Y-%m-%d %H:%M:%S"; }
log()  { printf '[%s] %s\n' "$(ts)" "$*" | tee -a "$LOG"; }
warn() { log "WARN : $*"; }
crit() { log "CRIT : $*" >&2 | tee -a "$ERR_LOG"; }

# --- helper: port listening? --------------------------------------------------
port_listening() {
    /usr/sbin/lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1
}

# --- helper: log file modified within N seconds? -----------------------------
log_fresh() {
    local max_age_sec=$(( $1 * 60 ))
    local now mod
    now=$(date +%s)
    mod=$(stat -f "%m" "$ROUTER_LOG" 2>/dev/null || echo 0)
    [ $(( now - mod )) -le "$max_age_sec" ]
}

# --- helper: count zombie traces ---------------------------------------------
zombie_count() {
    # Returns the number of request_traces rows where status='running' AND
    # started < NOW - ZOMBIE_AGE_MINUTES minutes.  Falls back to 0 on error.
    python3 - "$DB" "$ZOMBIE_AGE_MINUTES" <<'PY'
import sqlite3, sys
db_path, age_min = sys.argv[1], int(sys.argv[2])
try:
    con = sqlite3.connect(db_path)
    c = con.execute(
        "SELECT COUNT(*) FROM request_traces "
        "WHERE status='running' AND started < strftime('%s','now') - ? * 60",
        (age_min,)
    )
    print(c.fetchone()[0])
except Exception as e:
    print(0)
PY
}

# --- main -------------------------------------------------------------------
main() {
    # Ensure log dir exists (launchd may start before we've created it)
    mkdir -p "$(dirname "$LOG")" "$(dirname "$ERR_LOG")"
    
    local had_port=1
    local log_fresh_flag=1
    local zombie_cnt=0
    local action_taken=""

    # 1) Port check
    if port_listening; then
        log "OK   port $PORT is listening"
    else
        had_port=0
        crit "FATAL: no listener on port $PORT"
    fi

    # 2) Log freshness (only if port was up, otherwise we already know)
    if [ "$had_port" -eq 1 ]; then
        if log_fresh "$ALERT_THRESHOLD_MINUTES"; then
            log "OK   router log fresh (<${ALERT_THRESHOLD_MINUTES}m old)"
        else
            warn "STALE: router log not written in >${ALERT_THRESHOLD_MINUTES}m (possible hang)"
            log_fresh_flag=0
        fi
    fi

    # 3) Zombie traces
    zombie_cnt=$(zombie_count)
    if [ "$zombie_cnt" -gt "$ZOMBIE_COUNT_CRIT" ]; then
        crit "ZOMBIES: $zombie_cnt traces stuck >${ZOMBIE_AGE_MINUTES}m (threshold=$ZOMBIE_COUNT_CRIT)"
    elif [ "$zombie_cnt" -gt "$ZOMBIE_COUNT_WARN" ]; then
        warn "ZOMBIES: $zombie_cnt traces stuck >${ZOMBIE_AGE_MINUTES}m (threshold=$ZOMBIE_COUNT_WARN)"
    else
        log "OK   zombies=$zombie_cnt (below warn=$ZOMBIE_COUNT_WARN)"
    fi

    # 4) Decide action
    if [ "$had_port" -eq 0 ]; then
        crit "Attempting restart via launchctl kickstart"
        if launchctl kickstart -k "$LABEL" 2>>"$ERR_LOG"; then
            action_taken="kickstarted (port missing)"
        else
            action_taken="KICKSTART FAILED (port missing)"
        fi
    elif [ "$log_fresh_flag" -eq 0 ] && [ "$zombie_cnt" -gt "$ZOMBIE_COUNT_WARN" ]; then
        crit "Both log stale AND zombies present; attempting restart"
        if launchctl kickstart -k "$LABEL" 2>>"$ERR_LOG"; then
            action_taken="kickstarted (stale+hung)"
        else
            action_taken="KICKSTART FAILED (stale+hung)"
        fi
    fi

    if [ -n "$action_taken" ]; then
        log "ACTION: $action_taken at $(ts)"
    fi
}

main "$@"
