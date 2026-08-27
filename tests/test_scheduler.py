"""Tests for RoundRobinScheduler with the new flat Channel structure."""
from flex_llm_router.config import Channel, Limits, Routing
from flex_llm_router.scheduler import RoundRobinScheduler


def _ch(id, type):
    return Channel(id=id, enabled=True, provider='prov',
                   litellm_model='openai/x', public_model='test/x', context_window_tokens=1000000,
                   routing=Routing(type=type), limits=Limits())


class State:
    def __init__(self, due_ids):
        self.due = due_ids  # set of channel IDs (strings)

    def pacing_due(self, key, ch_id):
        return ch_id in self.due


def test_paced_channel_is_selected_when_due():
    primary = _ch('primary', 'primary')
    paced = _ch('paced', 'quota_paced')
    fallback = _ch('fallback', 'fallback_only')
    scheduler = RoundRobinScheduler()
    result = scheduler.select('p', [primary, paced, fallback], State({'paced'}))
    assert result.id == 'paced'


def test_primary_is_selected_when_paced_channel_is_not_due():
    primary = _ch('primary', 'primary')
    paced = _ch('paced', 'quota_paced')
    fallback = _ch('fallback', 'fallback_only')
    scheduler = RoundRobinScheduler()
    result = scheduler.select('p', [primary, paced, fallback], State(set()))
    assert result.id == 'primary'


def test_fallback_only_is_used_only_when_no_normal_channel_exists():
    fallback = _ch('fallback', 'fallback_only')
    scheduler = RoundRobinScheduler()
    result = scheduler.select('p', [fallback], State(set()))
    assert result.id == 'fallback'


def test_cost_aware_selects_lowest_tier_first():
    free = _ch('free', 'primary')
    paid1 = _ch('paid1', 'fallback_only')
    paid2 = _ch('paid2', 'fallback_only')
    sched = RoundRobinScheduler()
    sel = {'strategy': 'cost_aware'}
    tiers = {'free': 0, 'paid1': 1, 'paid2': 2}
    # 最低 tier 优先（免费层），无论传入顺序
    assert sched.select('p', [paid2, paid1, free], None, sel, tiers=tiers).id == 'free'
    assert sched.select('p', [paid1, free, paid2], None, sel, tiers=tiers).id == 'free'


def test_cost_aware_same_tier_keeps_pool_order():
    # 两个 tier0 通道(如 0 0 组合)，顺序优先（不负载均衡）
    a = _ch('a', 'primary')
    b = _ch('b', 'primary')
    sched = RoundRobinScheduler()
    sel = {'strategy': 'cost_aware'}
    tiers = {'a': 0, 'b': 0}
    assert sched.select('p', [a, b], None, sel, tiers=tiers).id == 'a'
    assert sched.select('p', [b, a], None, sel, tiers=tiers).id == 'b'


def test_cost_aware_zero_zero_one_two_combo():
    # 组合 0 0 1 2：两个免费并列 + 收费1 + 收费2
    c0a = _ch('c0a', 'primary')
    c0b = _ch('c0b', 'primary')
    c1 = _ch('c1', 'fallback_only')
    c2 = _ch('c2', 'fallback_only')
    sched = RoundRobinScheduler()
    sel = {'strategy': 'cost_aware'}
    tiers = {'c0a': 0, 'c0b': 0, 'c1': 1, 'c2': 2}
    # 最低 tier(0) 优先；同 tier 按 pool 列表顺序(c0a 在 c0b 前)
    pool_order = [c0a, c0b, c1, c2]
    assert sched.select('p', pool_order, None, sel, tiers=tiers).id == 'c0a'
    # 若 c0a 不可用，同 tier 的 c0b 接上
    assert sched.select('p', [c0b, c1, c2], None, sel, tiers=tiers).id == 'c0b'
