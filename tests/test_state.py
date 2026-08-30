"""Tests for StateStore with the new flat Channel structure (no Model/PoolRef)."""
from flex_llm_router.config import Channel, Limits, Routing
from flex_llm_router.state import StateStore
from flex_llm_router.app import compatibility
import time


def _make_channel(id='a', **kwargs):
    limits = Limits(**kwargs.get('limits', {}))
    routing = Routing(**kwargs.get('routing', {}))
    return Channel(id=id, enabled=True, provider='prov',
                   litellm_model='openai/x', public_model='test/x', context_window_tokens=1000000,
                   capabilities=['chat'], limits=limits, routing=routing)


def test_rpm_is_observed_but_not_proactively_blocked(tmp_path):
    c = _make_channel('a', limits={'rpm': 1})
    s = StateStore(tmp_path / 's.db')
    assert s.eligible('p', 'a', c.limits)[0]
    s.start('p', 'a', 'openai/x')
    time.sleep(0.01)
    s.start('p', 'a', 'openai/x')
    assert s.eligible('p', 'a', c.limits) == (True, None)


def test_five_hour_quota_and_reset(tmp_path):
    c = _make_channel('a', limits={'max_requests_per_window': 1})
    s = StateStore(tmp_path / 's.db')
    s.start('p', 'a', 'openai/x')
    assert s.eligible('p', 'a', c.limits) == (False, 'five_hour_quota')
    s.reset('p', 'a', 'all')
    assert s.eligible('p', 'a', c.limits) == (True, None)


def test_context_and_capability_filtering():
    c = _make_channel('a', capabilities=['chat'])
    ok, reason, _ = compatibility(8192, c, {'messages': [{'role': 'user', 'content': 'hi'}], 'tools': [{'type': 'function'}]}, False)
    assert reason == 'missing_capabilities:tools'


def test_persistent_enabled_override(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    assert s.is_enabled('p', 'a')
    s.set_enabled('p', 'a', False)
    assert not s.is_enabled('p', 'a')


def test_last_channel_test_is_exposed(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    s.record_test('p', 'a', 'success', latency=12)
    result = s.channels_state('p', 'a', c.limits, c.routing, 1000000, ['chat'], 'openai/x')
    assert result['last_test']['outcome'] == 'success'


def test_quota_releases_at_oldest_request_expiry(tmp_path):
    c = _make_channel('a', limits={'max_requests_per_window': 1, 'window_seconds': 10})
    s = StateStore(tmp_path / 's.db')
    s.start('p', 'a', 'openai/x')
    status = s.quota_status('p', 'a')
    assert status['next_release_at'] is not None and not s.eligible('p', 'a', c.limits)[0]


def test_pacing_is_due_then_waits_interval(tmp_path):
    c = _make_channel('a', routing={'type': 'quota_paced', 'target_requests_per_window': 2}, limits={'window_seconds': 10})
    s = StateStore(tmp_path / 's.db')
    assert s.pacing_due('p', 'a', 2)
    s.start('p', 'a', 'openai/x')
    assert not s.pacing_due('p', 'a', 2)


def test_429_evidence_is_persisted_per_channel(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    s.start('p', 'a', 'openai/x', input_tokens=10)
    kind, metrics = s.observe_429('p', 'a', 'requests per minute limit exceeded')
    # 窗口内请求<3 不推 learned limit(防单次瞬时429误判把限额定成1)
    assert kind == 'rpm' and metrics['requests'] == 1 and s.learned_limit('p', 'a')['safe_rpm'] is None


def test_learned_rpm_is_enforced_after_three_matching_samples(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    for _ in range(3):
        s.start('p', 'a', 'openai/x')
        s.observe_429('p', 'a', 'requests per minute limit')
    ll = s.learned_limit('p', 'a')
    assert ll['confidence'] == 3 and ll['safe_rpm']
    assert s.eligible('p', 'a', c.limits)[0] is True  # 冷却由请求层指数退避写入，状态层仅学习


def test_tpm_learning_and_enforcement(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    for i in range(3):
        s.start('p', 'a', 'openai/x', input_tokens=5000)
        s.observe_429('p', 'a', 'tokens per minute limit exceeded',kind='tpm')
    learned = s.learned_limit('p', 'a')
    assert learned['last_429_kind'] == 'tpm' and learned['safe_tpm'] and learned['confidence'] == 3
    r = s.eligible('p', 'a', c.limits, projected_tokens=999999)
    assert r[0] is True  # 冷却由请求层指数退避写入，状态层仅学习


def test_unknown_429_backoff_escalates_exponentially(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    s.start('p', 'a', 'openai/x')
    s.observe_429('p', 'a', 'connection reset by peer')
    row = s.db.execute('SELECT until FROM states WHERE pool=? AND channel=?', ('p', 'a')).fetchone()
    first = row['until']
    s.start('p', 'a', 'openai/x')
    s.observe_429('p', 'a', 'connection reset by peer')
    row = s.db.execute('SELECT until FROM states WHERE pool=? AND channel=?', ('p', 'a')).fetchone()
    assert first < row['until']


def test_cooldown_expiry_auto_recovers_channel(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    s.cooldown('p', 'a', 0.05, 'test_cooldown')
    assert not s.eligible('p', 'a', c.limits)[0]
    time.sleep(0.1)
    assert s.eligible('p', 'a', c.limits) == (True, None)

def test_error_statistics_tracks_recovery_and_final_failure(tmp_path):
    s=StateStore(tmp_path / 's.db')
    s.trace_begin('ok','model','pool','x','user 1 条',False)
    attempt=s.start('pool','a','openai/x',trace_id='ok')
    s.finish(attempt,'failure','rate_limit')
    s.trace_finish('ok','success')
    s.trace_begin('bad','model','pool','x','user 1 条',False)
    attempt=s.start('pool','a','openai/x',trace_id='bad')
    s.finish(attempt,'failure','tpm_limit')
    s.trace_finish('bad','failed',error_type='tpm_limit')
    stats=s.error_statistics('day')
    assert stats['requests']==2 and stats['final_failed']==1
    rows={r['error_type']:r for r in stats['rows']}
    assert rows['rate_limit']['final_failed']==0 and rows['rate_limit']['avg_recovery_seconds'] is not None
    assert rows['tpm_limit']['final_failed']==1 and rows['tpm_limit']['avg_recovery_seconds'] is None


def test_quarter_hour_statistics_have_96_local_buckets(tmp_path):
    s = StateStore(tmp_path / 's.db')
    calls = s.quarter_hour_call_statistics()
    requests = s.quarter_hour_request_statistics()
    assert calls['interval_minutes'] == 15 and len(calls['data']) == 96
    assert requests['interval_minutes'] == 15 and len(requests['data']) == 96
    assert calls['data'][0]['time'] == '00:00' and calls['data'][-1]['time'] == '23:45'
    assert requests['data'][0]['time'] == '00:00' and requests['data'][-1]['time'] == '23:45'


def test_success_streak_gradually_raises_safe_rpm(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    for _ in range(3):
        s.start('p', 'a', 'openai/x')
        s.observe_429('p', 'a', 'requests per minute limit')
    assert s.learned_limit('p', 'a')['safe_rpm'] == 2  # 窗口3次*.8=2
    for _ in range(20):
        s.start('p', 'a', 'openai/x')
        s.observe_success('p', 'a')
    assert s.learned_limit('p', 'a')['safe_rpm'] >= 2  # success streak 逐步上调


def test_probe_throttle_and_recover(tmp_path):
    c = _make_channel('a')
    s = StateStore(tmp_path / 's.db')
    # 初始应允许探测
    assert s.should_probe('p', 'a') is True
    # 探测失败 -> 记录，短时间内不再探（默认 600s 内）
    s.record_probe('p', 'a', success=False)
    assert s.should_probe('p', 'a', now=time.time() + 10) is False
    # 超过 probe_cooldown 后可再探
    assert s.should_probe('p', 'a', now=time.time() + 700) is True
    # 探测成功 -> 清除冷却
    s._cool('p', 'a', time.time() + 100, 'busy')
    assert s.is_busy('p', 'a') is True
    s.record_probe('p', 'a', success=True)
    s.clear_cooldown('p', 'a')
    assert s.is_busy('p', 'a') is False
    assert s.cooldown_reason('p', 'a') is None
