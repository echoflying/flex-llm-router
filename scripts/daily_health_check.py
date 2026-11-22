#!/usr/bin/env python3
"""Daily health check for all channels.

Uses existing channel test logic from app.py to probe each channel.
Results are saved to channel_tests table and logs/daily-health.log
"""
import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from flex_llm_router.config import load_config, channel_credentials
from flex_llm_router.state import StateStore
import litellm

def test_channel(ch_id, ch, state):
    """Test a single channel and return result."""
    try:
        # Get credentials using existing helper
        base, key = channel_credentials(ch, config.providers)
        
        if not base or not key:
            return {'status': 'skipped', 'error': 'no credentials'}
        
        # Run test
        started = time.monotonic()
        resp = litellm.completion(
            model=ch.litellm_model,
            api_base=base,
            api_key=key,
            messages=[{"role": "user", "content": "Reply with exactly: OK"}],
            max_tokens=10,
            timeout=30
        )
        
        latency_ms = int((time.monotonic() - started) * 1000)
        
        # Record in DB
        state.record_test('*', ch_id, 'success', latency=latency_ms)
        
        return {'status': 'ok', 'latency_ms': latency_ms}
        
    except Exception as e:
        latency_ms = int((time.monotonic() - started) * 1000) if 'started' in dir() else 0
        error_type = type(e).__name__
        
        # Categorize error
        err_str = str(e).lower()
        if 'rate' in err_str or '429' in err_str:
            error_type = 'rpm_limit'
        elif 'auth' in err_str or '401' in err_str or '403' in err_str:
            error_type = 'authentication'
        elif 'connect' in err_str or 'timeout' in err_str:
            error_type = 'connection_error'
        
        state.record_test('*', ch_id, 'failure', error=error_type, 
                         latency=latency_ms, detail=str(e)[:200])
        
        return {'status': 'fail', 'error': error_type}

def main():
    proj_root = Path(__file__).parent.parent
    config_path = proj_root / 'config' / 'pools.yaml'
    db_path = proj_root / 'data' / 'flex.db'
    log_path = proj_root / 'logs' / 'daily-health.log'
    
    global config
    config = load_config(config_path)
    state = StateStore(db_path)
    
    log_lines = []
    log_lines.append(f"\n{'='*60}")
    log_lines.append(f"Daily Health Check - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_lines.append(f"{'='*60}")
    
    ok_count = 0
    fail_count = 0
    skip_count = 0
    
    for ch_id, ch in config.channels.items():
        result = test_channel(ch_id, ch, state)
        
        if result['status'] == 'ok':
            ok_count += 1
            log_lines.append(f"  OK   {ch_id} ({result.get('latency_ms', 0)}ms)")
        elif result['status'] == 'skipped':
            skip_count += 1
            log_lines.append(f"  SKIP {ch_id}: {result.get('error', 'no creds')}")
        else:
            fail_count += 1
            log_lines.append(f"  FAIL {ch_id}: {result.get('error', 'unknown')}")
    
    log_lines.append(f"\nSummary: {ok_count} ok, {fail_count} failed, {skip_count} skipped")
    log_lines.append(f"{'='*60}\n")
    
    # Write log
    with open(log_path, 'a') as f:
        f.write('\n'.join(log_lines))
    
    print('\n'.join(log_lines))

if __name__ == '__main__':
    main()
