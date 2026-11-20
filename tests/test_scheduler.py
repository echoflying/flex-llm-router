"""Tests for RoundRobinScheduler with the current flat Channel model.

Scheduling now keys off Pool-side ``tiers`` (cost_aware) and round-robin order;
the earlier per-Channel ``routing`` type model (primary/quota_paced/fallback_only)
was removed, so those tests are gone.
"""
from flex_llm_router.config import Channel, Limits
from flex_llm_router.scheduler import RoundRobinScheduler


def _ch(id):
    return Channel(id=id, enabled=True, provider='prov',
                   litellm_model='openai/x', public_model='test/x',
                   context_window_tokens=1000000, limits=Limits())


def test_cost_aware_selects_lowest_tier_first():
    free, paid1, paid2 = _ch('free'), _ch('paid1'), _ch('paid2')
    sched = RoundRobinScheduler()
    sel = {'strategy': 'cost_aware'}
    tiers = {'free': 0, 'paid1': 1, 'paid2': 2}
    assert sched.select('p', [paid2, paid1, free], None, sel, tiers=tiers).id == 'free'
    assert sched.select('p', [paid1, free, paid2], None, sel, tiers=tiers).id == 'free'


def test_cost_aware_same_tier_keeps_pool_order():
    a, b = _ch('a'), _ch('b')
    sched = RoundRobinScheduler()
    sel = {'strategy': 'cost_aware'}
    tiers = {'a': 0, 'b': 0}
    assert sched.select('p', [a, b], None, sel, tiers=tiers).id == 'a'
    assert sched.select('p', [b, a], None, sel, tiers=tiers).id == 'b'


def test_cost_aware_zero_zero_one_two_combo():
    c0a, c0b, c1, c2 = _ch('c0a'), _ch('c0b'), _ch('c1'), _ch('c2')
    sched = RoundRobinScheduler()
    sel = {'strategy': 'cost_aware'}
    tiers = {'c0a': 0, 'c0b': 0, 'c1': 1, 'c2': 2}
    pool_order = [c0a, c0b, c1, c2]
    assert sched.select('p', pool_order, None, sel, tiers=tiers).id == 'c0a'
    assert sched.select('p', [c0b, c1, c2], None, sel, tiers=tiers).id == 'c0b'


def test_cost_aware_missing_tier_falls_back_to_pool_order():
    a, b = _ch('a'), _ch('b')
    sched = RoundRobinScheduler()
    sel = {'strategy': 'cost_aware'}
    # tiers 缺省时按 pool 列表 index 作为 tier，a(0) 优先
    assert sched.select('p', [a, b], None, sel).id == 'a'
    assert sched.select('p', [b, a], None, sel).id == 'b'


def test_round_robin_cycles_in_pool_order():
    a, b, c = _ch('a'), _ch('b'), _ch('c')
    sched = RoundRobinScheduler()
    ids = [sched.select('p', [a, b, c]).id for _ in range(4)]
    assert ids == ['a', 'b', 'c', 'a']