from collections import defaultdict
from threading import Lock

class RoundRobinScheduler:
    def __init__(self): self.next=defaultdict(int); self.lock=Lock()

    def _round_robin(self, key, channels):
        if not channels: raise LookupError('no eligible channels')
        with self.lock:
            i = self.next[key] % len(channels); self.next[key] = (i + 1) % len(channels)
        return channels[i]

    def select(self, key, channels, state=None, selection=None, tiers=None):
        """channels: list[Channel] (already eligible+enabled, in pool order).
        selection: pool.selection dict (for cost_aware strategy).
        tiers: dict[channel_id, int] (pool-side cost tier; not on Channel).
        cost_aware: 按 tiers 升序优先(0=最便宜/最优先), 同 tier 保持原 pool 列表顺序(顺序优先);
                   不为了负载均衡频繁切(缓存稳定). 其他策略回退 round_robin.
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
        if state is None:
            return self._round_robin(key, enabled)
        return self._round_robin(key, enabled)
