from collections import defaultdict
from threading import Lock

class RoundRobinScheduler:
    def __init__(self): self.next=defaultdict(int); self.lock=Lock()

    def _round_robin(self, key, channels):
        if not channels: raise LookupError('no eligible channels')
        with self.lock:
            i = self.next[key] % len(channels); self.next[key] = (i + 1) % len(channels)
        return channels[i]

    def select(self, key, channels, state=None, selection=None, tiers=None, tokens=None, budgets=None):
        """channels: list[Channel] (already eligible+enabled, in pool order).
        selection: pool.selection dict (for cost_aware / budget_aware strategy).
        tiers: dict[channel_id, int] (pool-side cost tier; not on Channel).
        tokens: this request's projected input tokens (optional).
        budgets: dict[channel_id, dict] carrying optional ``ratio`` (remaining
            daily-budget fraction 0..1), ``tokens_remaining`` and ``healthy``
            (latest self-test verdict).  Consumed only by budget_aware.
        cost_aware: 按 tiers 升序优先(0=最便宜/最优先), 同 tier 保持原 pool 列表顺序(顺序优先);
                    不为了负载均衡频繁切(缓存稳定). 其他策略回退 round_robin.
        budget_aware: 按「剩余日预算」调度, 用于收纳每日定量额度的源(免费额度、
                    每日积分、按天结算的上游). 排序键:
                      1. 装得下本次请求的优先 —— 不为一次大请求烧掉一个快见底的小额度通道;
                      2. 最近自检失败的排后(不硬排除: 自检可能只是暂时性失败);
                      3. 声明过预算的优先于无声明的;
                      4. 剩余比例高者优先; 5. 同档按 tier, 再按原顺序(保证稳定).
                    硬性封禁仍由 quota_429_policy 负责, 这里只是软偏好.
        """
        enabled = [c for c in channels if c.enabled]
        if not enabled: raise LookupError('no eligible channels')
        strategy = (selection or {}).get('strategy', 'round_robin') if isinstance(selection, dict) else 'round_robin'
        if strategy == 'cost_aware':
            # tier 由 POOL 侧显式声明(支持 0 0 1 2 组合), 不按顺序隐含.
            # tiers 缺失则回退列表 index (兼容), 但推荐显式配.
            order = {c.id: (tiers.get(c.id, i) if isinstance(tiers, dict) else i)
                     for i, c in enumerate(channels)}
            return sorted(enabled, key=lambda c: order[c.id])[0]
        if strategy == 'budget_aware':
            order = {c.id: tiers[c.id] for c in enabled if isinstance(tiers, dict) and c.id in tiers}
            def budget_key(item):
                index, channel = item
                entry = (budgets or {}).get(channel.id) or {}
                ratio = entry.get('ratio'); remaining = entry.get('tokens_remaining')
                # 1. 本次请求装不下的排后: 剩余预算小于请求体量时, 发出去就是白烧.
                absorb = 1 if (tokens is not None and remaining is not None and tokens > remaining) else 0
                healthy = entry.get('healthy')
                health = 1 if healthy is False else 0
                return (absorb, health, 0 if ratio is not None else 1,
                        -(ratio if ratio is not None else 0.0), order.get(channel.id, index), index)
            return sorted(((i, c) for i, c in enumerate(channels)), key=budget_key)[0][1]
        if state is None:
            return self._round_robin(key, enabled)
        return self._round_robin(key, enabled)
