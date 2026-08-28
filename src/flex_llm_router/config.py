"""Configuration loading for Flex's policy-owned pool definitions."""
from __future__ import annotations
import os
import re
from pathlib import Path
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator

RUNNER_NAME_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$')


def validate_runner_name(value: str) -> str:
    """Validate the internal Runner key used by API paths and config maps.

    Keep names URL/client safe: ASCII letters/digits plus ``.``, ``_`` and
    ``-``; no whitespace, slashes, or other punctuation; max 64 characters.
    Existing YAML is still loaded for compatibility, while newly created
    Runners are required to follow this rule.
    """
    name = str(value or '').strip()
    if not RUNNER_NAME_PATTERN.fullmatch(name):
        raise ValueError('Runner 名称只能包含字母、数字、点、下划线和连字符，且必须以字母或数字开头（最多 64 个字符）')
    return name

class Limits(BaseModel):
    # ① 自我流控（本地闸门，不依赖上游报错）
    rpm: int | None = Field(default=None, ge=1)
    tpm: int | None = Field(default=None, ge=1)
    local_cooldown_seconds: int = Field(default=300, ge=0)
    # ② 5小时总量控制（A 类硬限制，滑动窗口）
    window_seconds: int = Field(default=18000, ge=1)
    max_requests_per_window: int | None = Field(default=None, ge=1)
    quota_cooldown_seconds: int = Field(default=3600, ge=0)
    # ③ 被流控后退让（B 类瞬时限流）
    busy_threshold: int = Field(default=3, ge=1)
    busy_window_minutes: int = Field(default=5, ge=1)
    busy_cooldown_seconds: int = Field(default=300, ge=0)

class RetryPolicy(BaseModel):
    """Per-channel retry behavior for transient errors (429, connection errors, timeouts)."""
    max_retries: int = Field(default=3, ge=0)
    backoff: dict = Field(default={'base_seconds': 5, 'max_seconds': 60, 'exponential': True})
    retry_on: list[str] = Field(default=['rate_limit', 'connection_error', 'timeout', 'server_error'])

    @field_validator('backoff')
    @classmethod
    def validate_backoff(cls, v):
        if 'base_seconds' not in v: v['base_seconds'] = 5
        if 'max_seconds' not in v: v['max_seconds'] = 60
        if 'exponential' not in v: v['exponential'] = True
        return v

class Channel(BaseModel):
    """A Channel is a specific provider/model instance with capabilities and limits."""
    id: str
    enabled: bool = True
    # Direct exposure controls whether this Channel is advertised as an
    # external model in /v1/models.  A hidden Channel may still be selected
    # internally by a Runner; this is deliberately separate from ``enabled``.
    externally_exposed: bool = True
    # Mark channels that are allowed to serve as a Chinese content-policy
    # fallback.  This is deliberately a capability of the fallback Channel,
    # not a claim about whether its own upstream blocks content.
    chn_content_policy_fallback: bool = False
    provider: str  # references providers.xxx
    litellm_model: str
    public_model: str  # 对外 model 名(外部系统填这个). 必填, 不配报错.
    context_window_tokens: int = Field(ge=1)
    capabilities: list[str] = Field(default_factory=list)
    provider_type: str = 'openai_compatible'
    supported_params: list[str] = Field(default_factory=list)
    # Protocol capability probes are explicit, on-demand observations rather
    # than claims inferred from the model name.  Keys currently include
    # ``responses``; each value stores status/checked_at and a safe summary.
    # Keeping this extensible lets us add future protocol checks without
    # changing the Channel identity (which remains Provider + Model).
    protocol_support: dict[str, dict] = Field(default_factory=dict)
    # 成本排位由所属 POOL 的 tiers 显式定义，不在 Channel 上重复配置。
    limits: Limits = Field(default_factory=Limits)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    @model_validator(mode='before')
    @classmethod
    def normalize_exposure_aliases(cls, value):
        """Accept the short legacy/editor aliases while storing one spelling."""
        if isinstance(value, dict):
            value = dict(value)
            if 'externally_exposed' not in value:
                for alias in ('external_exposed', 'exposed', 'external'):
                    if alias in value:
                        value['externally_exposed'] = value[alias]
                        break
            # Compatibility with the first spelling, whose meaning was
            # ambiguous.  Existing true values now mean "eligible fallback".
            if 'chn_content_policy_fallback' not in value and 'chn_content_policy' in value:
                value['chn_content_policy_fallback'] = value['chn_content_policy']
        return value

    @field_validator('id')
    @classmethod
    def non_blank_id(cls, value):
        if not value.strip(): raise ValueError('channel id must not be blank')
        return value

class Pool(BaseModel):
    """A Pool is a named routing group of channels."""
    public_model: str | None = None
    # selection 策略(dict, 宽松):
    #   strategy: cost_aware | round_robin | quota_paced_priority
    #   fallback: {order: cost_ascending, trigger: [quota_exhausted, busy_persistent, failure],
    #              max_fallback_tiers: N, reattach: {probe_before_switch_back, quiet_window_seconds, quota_recover_seconds, failure_retry_after}}
    #   stickiness: {min_stable_seconds: 3600}  # 单通道至少稳定跑这么久才因"平衡"切换(防频繁切, 保缓存)
    #   retry_next_channel_on: [...]  # 兼容旧字段, 映射到 failure 触发
    selection: dict = Field(default_factory=lambda: {'strategy': 'cost_aware', 'fallback': {'order': 'cost_ascending', 'trigger': ['quota_exhausted', 'busy_persistent', 'failure'], 'max_fallback_tiers': 2, 'reattach': {'probe_before_switch_back': True, 'quiet_window_seconds': 1200, 'quota_recover_seconds': 3600, 'failure_retry_after': 300}}, 'stickiness': {'min_stable_seconds': 3600}, 'retry_next_channel_on': []})
    context_policy: dict = Field(default_factory=lambda: {'reserve_output_tokens': 8192})
    session_affinity: dict = Field(default_factory=lambda: {'enabled': False, 'idle_seconds': 1200, 'minimum_messages': 2})
    channels: list[str]  # channel IDs to include (order = same-tier priority)
    # 成本阶梯 tier: channel_id -> int (0=最优先/最便宜, 1, 2...). 显式配置在 POOL 侧,
    # 不在 Channel 上(排位是池内相对属性). 支持 0 0 1 2 等组合(多免费并列).
    tiers: dict[str, int] = Field(default_factory=dict)

    @field_validator('channels')
    @classmethod
    def unique_channel_refs(cls, channels):
        if not channels:
            raise ValueError('a pool needs at least one channel')
        if len(channels) != len(set(channels)):
            raise ValueError('channel ids must be unique within a pool')
        return channels

    @field_validator('tiers')
    @classmethod
    def tiers_cover_channels(cls, tiers, info):
        channels = (info.data or {}).get('channels', [])
        missing = [c for c in channels if c not in tiers]
        if missing:
            raise ValueError(f'tiers must declare a tier for every channel; missing: {missing}')
        return tiers

class Runner(Pool):
    """The unified external resource.

    A Runner owns one or more Channels and reuses the existing Pool selection
    policy.  Pool remains as a compatibility type for older configuration
    files; new configuration should use ``runners``.
    """
    pass

class Provider(BaseModel):
    """Provider holds base_url and api_key env var names."""
    base_url_env: str
    api_key_env: str

    @field_validator('base_url_env', 'api_key_env')
    @classmethod
    def non_blank_env(cls, v):
        if not v.strip(): raise ValueError('env var name must not be blank')
        return v.strip()

class FlexConfig(BaseModel):
    version: int = 1
    providers: dict[str, Provider]
    channels: dict[str, Channel]  # flat channel registry
    # Runner is the canonical external resource.  ``pools`` and
    # ``connections`` are retained as read/write compatibility aliases for a
    # one-time migration from the previous Link + Pool vocabulary.
    runners: dict[str, Runner] = Field(default_factory=dict)
    pools: dict[str, Pool] = Field(default_factory=dict)
    links: dict[str, str] = Field(default_factory=dict)
    connections: dict[str, str] = Field(default_factory=dict)
    # Ordered global fallback channels for policy-specific recovery.  The
    # list is intentionally empty by default so old configs remain valid;
    # operators can put ``agnes-flash`` first in YAML/UI.
    global_fallback: dict[str, list[str]] = Field(default_factory=dict)

    @model_validator(mode='before')
    @classmethod
    def migrate_legacy_resources(cls, data):
        """Normalize legacy Pool/Link YAML into the unified Runner schema."""
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        if not normalized.get('runners') and normalized.get('pools'):
            normalized['runners'] = normalized['pools']
        if not normalized.get('links') and normalized.get('connections'):
            normalized['links'] = normalized['connections']
        if not normalized.get('connections') and normalized.get('links'):
            normalized['connections'] = normalized['links']
        return normalized

    @model_validator(mode='after')
    def normalize_runner_aliases(self):
        # Keep one canonical in-memory definition while exposing old fields to
        # existing callers.  This also makes a new runners-only YAML usable by
        # the current scheduler without introducing a second policy engine.
        if not self.runners and self.pools:
            self.runners = {name: Runner.model_validate(pool.model_dump()) for name, pool in self.pools.items()}
        self.pools = {name: Pool.model_validate(runner.model_dump()) for name, runner in self.runners.items()}
        merged = dict(self.connections)
        merged.update(self.links)
        self.links = merged
        self.connections = dict(merged)
        return self

    @staticmethod
    def external_channel_model(ch: 'Channel') -> str:
        """对外 model 名: 必须显式配置 public_model."""
        return ch.public_model

    @model_validator(mode='after')
    def unique_external_models(self):
        # Runner 是唯一的正式外部资源。Channel 的 public_model 仅作为
        # 兼容字段/内部标识，不参与外部命名空间；单 Channel Runner
        # 可以先沿用对应 Channel 的名称。
        names = {}  # Runner public_model -> source
        for pname, runner in self.runners.items():
            pm = runner.public_model
            if pm in names:
                raise ValueError(f'external model name conflict: {pm!r} used by {names[pm]} and runner {pname}. Rename explicitly.')
            names[pm] = f'runner {pname}'
        return self

    @model_validator(mode='after')
    def validate_connections(self):
        """Legacy Link names must resolve to a Runner or Channel."""
        runner_keys = set(self.runners.keys())
        runner_public = {r.public_model for r in self.runners.values() if r.public_model}
        ch_ids = set(self.channels.keys())
        ch_public = {c.public_model for c in self.channels.values() if c.public_model}
        reserved = runner_keys | runner_public | ch_ids | ch_public
        for name, target in self.links.items():
            valid = (target in runner_keys or target in runner_public
                     or target in ch_ids or target in ch_public)
            if not valid:
                raise ValueError(
                    f'link {name!r} -> {target!r} is invalid; '
                    f'target must be a runner key, runner public_model, channel id, or channel public_model')
            # 连接名若与某个真实对外名撞车(且不是指向自己), 外部请求将无法判断走哪条路径
            if name in reserved and name != target:
                raise ValueError(
                    f'connection name {name!r} conflicts with an existing pool/channel name; rename the connection')
        return self

    @model_validator(mode='after')
    def validate_global_fallbacks(self):
        """Ensure configured global fallback references are real Channels."""
        # Keep the migration safe for old configs while making the built-in
        # Agnes Official Channel the first fallback when it exists.  An
        # explicit empty list remains an intentional opt-out.
        if 'chn_content_policy' not in self.global_fallback:
            marked=[cid for cid,ch in self.channels.items() if ch.chn_content_policy_fallback]
            # Agnes Official is the default first fallback when present.  An
            # otherwise unmarked Agnes is promoted only when no fallback has
            # been explicitly selected, preserving an explicit empty list.
            if not marked and 'agnes-flash' in self.channels:
                self.channels['agnes-flash'].chn_content_policy_fallback=True
                marked=['agnes-flash']
            elif 'agnes-flash' in marked:
                marked=['agnes-flash']+[cid for cid in marked if cid!='agnes-flash']
            self.global_fallback['chn_content_policy'] = marked
        for policy, channel_ids in self.global_fallback.items():
            if not isinstance(channel_ids, list):
                raise ValueError(f'global fallback {policy!r} must be a list of channel ids')
            if len(channel_ids) != len(set(channel_ids)):
                raise ValueError(f'global fallback {policy!r} contains duplicate channels')
            missing = [cid for cid in channel_ids if cid not in self.channels]
            if missing:
                raise ValueError(f'global fallback {policy!r} references unknown channel(s): {missing}')
        return self

    def resolve_connection(self, name: str) -> str | None:
        """Resolve a legacy alias; new callers should use resolve_runner."""
        return self.links.get(name)

    def resolve_runner(self, name: str) -> str | None:
        """Resolve a Runner key/public name or a legacy alias."""
        if name in self.runners:
            return name
        for key, runner in self.runners.items():
            if runner.public_model == name:
                return key
        return self.links.get(name)

    @field_validator('runners')
    @classmethod
    def resolve_public_models(cls, runners):
        for name, runner in runners.items():
            if runner.public_model is None:
                runner.public_model = name
        names = [runner.public_model for runner in runners.values()]
        if len(names) != len(set(names)):
            raise ValueError('public_model values must be unique')
        return runners

    def get_channel(self, channel_id: str) -> tuple[str, Channel]:
        """Resolve channel_id → (provider_name, Channel)."""
        ch = self.channels.get(channel_id)
        if ch is None:
            raise LookupError(f'channel {channel_id!r} not found')
        return ch.provider, ch

    def get_pool_channels(self, pool_name: str) -> list[tuple[str, Channel]]:
        """Return [(provider, Channel), ...] for a pool."""
        pool = self.runners.get(pool_name)
        if pool is None:
            pool = self.pools.get(pool_name)
        if pool is None:
            return []
        results = []
        for ch_id in pool.channels:
            try:
                prov, ch = self.get_channel(ch_id)
                results.append((prov, ch))
            except LookupError:
                continue
        return results

    def get_runner_channels(self, runner_name: str) -> list[tuple[str, Channel]]:
        """Return channels for a Runner (Pool-compatible alias)."""
        return self.get_pool_channels(runner_name)

    def provider_models(self, provider_name: str) -> list[dict]:
        """Return configured model candidates for provider selection UI/API."""
        seen = set(); result = []
        for channel in self.channels.values():
            if channel.provider != provider_name:
                continue
            key = (channel.litellm_model, channel.public_model)
            if key in seen:
                continue
            seen.add(key)
            result.append({'id': channel.public_model, 'public_model': channel.public_model,
                           'litellm_model': channel.litellm_model,
                           'capabilities': list(channel.capabilities),
                           'context_window_tokens': channel.context_window_tokens})
        return result

    def runner_models(self) -> list[dict]:
        """Return the canonical external Runner model catalogue."""
        return [{'id': runner.public_model, 'object': 'model', 'owned_by': 'flex-runner',
                 'runner': name, 'channel_count': len(runner.channels)}
                for name, runner in self.runners.items()]

def load_config(path: str | Path, override: bool = False) -> FlexConfig:
    p = Path(path).expanduser().resolve()
    load_dotenv(p.parent.parent / '.env', override=override)
    with p.open(encoding='utf-8') as h:
        raw = yaml.safe_load(h) or {}
    return FlexConfig.model_validate(raw)

def channel_credentials(channel: Channel, providers: dict) -> tuple[str, str]:
    """Return (base_url, api_key) from env using the Provider's env var names."""
    prov = providers.get(channel.provider)
    if prov is None:
        raise ValueError(f'provider {channel.provider!r} not found')
    base = os.getenv(prov.base_url_env, '').strip()
    key = os.getenv(prov.api_key_env, '').strip()
    if not base or not key:
        missing = [n for n, v in ((prov.base_url_env, base), (prov.api_key_env, key)) if not v]
        raise ValueError(f'provider {channel.provider!r} missing env var(s) in .env: {", ".join(missing)} (set them in your .env file)')
    return base, key
