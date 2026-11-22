"""Tests for the daily-budget harvesting pool (bulk-lite) and the fleet APIs.

The two features were designed together because they answer one question from
opposite ends: budget_aware scheduling picks *which* Channel spends today's
allowance next, while /api/fleet-status and /api/bulk-lite tell an external
agent what it can bank on before it asks.  The tests keep them in the same
file so a change that breaks the ranking rule cannot slip through by only
touching the API contract.
"""
from flex_llm_router.config import Channel, DailyBudget, Limits
from flex_llm_router.scheduler import RoundRobinScheduler
from flex_llm_router.state import StateStore
from flex_llm_router.app import budget_view
from pathlib import Path
import shutil
import pytest
import time

_REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def tmp_config(tmp_path):
    """复制真实 config 到临时目录，测试不污染项目文件。"""
    src = _REPO / 'config' / 'pools.yaml'
    dst = tmp_path / 'config' / 'pools.yaml'
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dst)
    tpl_src = _REPO / 'templates'
    tpl_dst = tmp_path / 'templates'
    if tpl_src.exists():
        shutil.copytree(tpl_src, tpl_dst, dirs_exist_ok=True)
    return str(dst)


def _ch(id, budget=None, window=1000000):
    return Channel(id=id, enabled=True, provider='prov', litellm_model='openai/x',
                   public_model='test/x', context_window_tokens=window, limits=Limits(),
                   daily_budget=budget)


def _budget(tokens=None, calls=None):
    return DailyBudget(tokens=tokens, calls=calls, reset='local_midnight')


# ── budget_view: 声明额度 vs 实测消耗 ─────────────────────────────────────

def test_budget_view_undeclared_reports_measured_only(tmp_path):
    s = StateStore(tmp_path / 's.db')
    ch = _ch('a')
    view = budget_view(s, ch)
    assert view['ratio'] is None
    assert view['tokens_remaining'] is None
    assert view['tokens_used'] == 0
    assert view['calls_used'] == 0
    assert view['reset'] == 'local_midnight'
    assert view['seconds_to_reset'] > 0


def test_budget_view_binding_dimension_wins(tmp_path):
    s = StateStore(tmp_path / 's.db')
    ch = _ch('a', _budget(tokens=1000, calls=10))
    today = time.strftime('%Y-%m-%d')
    # 用了 500 token (一半) 但 10 次调用全用光 -> 按 calls 计算 0 剩余.
    s.db.execute("INSERT INTO daily_request_analytics(day,pool,channel,requested_model,input_bucket,"
                 "requests,success,first_success,retry_success,failed,cancelled,attempts,"
                 "input_tokens,output_tokens,total_tokens,ttft_samples,ttft_sum_ms,"
                 "latency_samples,latency_sum_ms,latency_max_ms)"
                 "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (today, 'p', 'a', 'test/x', '0', 10, 10, 10, 0, 0, 0, 10,
                  500, 0, 500, 0, 0, 0, 0, 0))
    view = budget_view(s, ch)
    assert view['tokens_used'] == 500
    assert view['tokens_remaining'] == 500
    assert view['calls_used'] == 10
    assert view['calls_remaining'] == 0
    assert view['ratio'] == 0.0


def test_budget_view_fresh_channel_ratio_one(tmp_path):
    s = StateStore(tmp_path / 's.db')
    ch = _ch('a', _budget(tokens=1000))
    view = budget_view(s, ch)
    assert view['ratio'] == 1.0  # 未消耗 -> 剩余比例 1
    assert view['tokens_remaining'] == 1000


# ── budget_aware 调度排序 ────────────────────────────────────────────────

def test_budget_aware_prefers_most_remaining_budget():
    sched = RoundRobinScheduler()
    sel = {'strategy': 'budget_aware'}
    a, b, c = _ch('a'), _ch('b'), _ch('c')
    # a 剩 20%, b 剩 80%, c 无声明.
    budgets = {'a': {'ratio': 0.2, 'tokens_remaining': 100},
               'b': {'ratio': 0.8, 'tokens_remaining': 500},
               'c': {'ratio': None}}
    # 无请求体量参数: b 剩余最多 -> 先选.
    assert sched.select('p', [a, b, c], None, sel, budgets=budgets).id == 'b'
    assert sched.select('p', [a, c, b], None, sel, budgets=budgets).id == 'b'


def test_budget_aware_deprioritizes_request_that_does_not_fit():
    sched = RoundRobinScheduler()
    sel = {'strategy': 'budget_aware'}
    a, b = _ch('a'), _ch('b')
    # a 剩 100 token, b 剩 5000. 请求 1000 token: a 装不下 -> 即使 a 剩余比例更高也应选 b.
    budgets = {'a': {'ratio': 0.9, 'tokens_remaining': 100},
               'b': {'ratio': 0.1, 'tokens_remaining': 5000}}
    assert sched.select('p', [a, b], None, sel, tokens=1000, budgets=budgets).id == 'b'


def test_budget_aware_ranks_selftest_failure_last():
    sched = RoundRobinScheduler()
    sel = {'strategy': 'budget_aware'}
    a, b = _ch('a'), _ch('b')
    budgets = {'a': {'ratio': 0.5, 'healthy': False},
               'b': {'ratio': 0.4, 'healthy': True}}
    # a 自检失败 -> 排后, 即便其剩余比例更高.
    assert sched.select('p', [a, b], None, sel, budgets=budgets).id == 'b'


def test_budget_aware_undeclared_after_declared():
    sched = RoundRobinScheduler()
    sel = {'strategy': 'budget_aware'}
    declared, unknown = _ch('declared'), _ch('unknown')
    budgets = {'declared': {'ratio': 0.05}, 'unknown': {'ratio': None}}
    assert sched.select('p', [unknown, declared], None, sel, budgets=budgets).id == 'declared'


# ── fleet 端点契约 ───────────────────────────────────────────────────────

def test_bulk_advice_returns_runner_and_windows(tmp_config):
    from fastapi.testclient import TestClient
    from flex_llm_router.app import create_app
    client = TestClient(create_app(tmp_config))
    r = client.get('/api/bulk-lite?size=small')
    assert r.status_code == 200
    body = r.json()
    assert body['object'] == 'bulk_advice'
    assert body['runner'] == 'bulk-lite'
    assert body['model'] == 'bulk-lite'
    assert body['usable_window'] > 0
    assert body['context_window'] >= body['usable_window']
    assert any(m['id'] == body['recommended_channel'] for m in body['members'])


def test_bulk_advice_rejects_unknown_size(tmp_config):
    from fastapi.testclient import TestClient
    from flex_llm_router.app import create_app
    client = TestClient(create_app(tmp_config))
    r = client.get('/api/bulk-lite?size=galactic')
    assert r.status_code == 400


def test_fleet_status_contract(tmp_config):
    from fastapi.testclient import TestClient
    from flex_llm_router.app import create_app
    client = TestClient(create_app(tmp_config))
    r = client.get('/api/fleet-status')
    assert r.status_code == 200
    body = r.json()
    assert body['object'] == 'fleet_status'
    assert body['fleet']['channels_total'] >= 20
    assert body['fleet']['runners_total'] >= 1
    names = {runner['name'] for runner in body['runners']}
    assert 'bulk-lite' in names
    bulk = next(r for r in body['runners'] if r['name'] == 'bulk-lite')
    assert bulk['strategy'] == 'budget_aware'
    assert bulk['advertised_window'] >= bulk['usable_window']
    # 每通道视图必须带日预算块与封禁信息.
    for ch in body['channels']:
        assert 'daily_budget' in ch
        assert 'cooldown' in ch
        assert 'last_test' in ch
        assert 'learned' in ch
