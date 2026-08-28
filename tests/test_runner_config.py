import pytest

from flex_llm_router.config import Channel, FlexConfig, Limits, Pool, Runner, validate_runner_name


@pytest.mark.parametrize('name', ['coder', 'mix-deepseek-v4-flash', 'runner.v2', 'runner_2'])
def test_runner_name_accepts_url_safe_identifiers(name):
    assert validate_runner_name(name) == name


@pytest.mark.parametrize('name', ['', ' coder', 'coder name', '/coder', 'coder/slash', '-coder', 'a' * 65])
def test_runner_name_rejects_spaces_and_unsafe_characters(name):
    with pytest.raises(ValueError):
        validate_runner_name(name)


def _channel(channel_id='c1'):
    return Channel(
        id=channel_id,
        provider='demo',
        litellm_model='openai/demo',
        public_model='demo-model',
        context_window_tokens=128000,
        capabilities=['chat', 'streaming'],
    )


def test_runners_are_canonical_and_pool_alias_is_compatible():
    runner = Runner(public_model='coder', channels=['c1'], tiers={'c1': 0})
    cfg = FlexConfig.model_validate({
        'providers': {'demo': {'base_url_env': 'DEMO_BASE', 'api_key_env': 'DEMO_KEY'}},
        'channels': {'c1': _channel().model_dump()},
        'runners': {'coder': runner.model_dump()},
        'links': {'hermes-coder': 'coder'},
    })
    assert set(cfg.runners) == {'coder'}
    assert set(cfg.pools) == {'coder'}
    assert cfg.resolve_runner('coder') == 'coder'
    assert cfg.resolve_connection('hermes-coder') == 'coder'
    assert cfg.get_runner_channels('coder')[0][1].id == 'c1'


def test_single_runner_may_reuse_channel_public_model_name():
    cfg = FlexConfig.model_validate({
        'providers': {'demo': {'base_url_env': 'DEMO_BASE', 'api_key_env': 'DEMO_KEY'}},
        'channels': {'c1': _channel().model_dump()},
        'runners': {'demo-model': {
            'public_model': 'demo-model', 'channels': ['c1'], 'tiers': {'c1': 0},
        }},
    })
    assert cfg.runners['demo-model'].public_model == cfg.channels['c1'].public_model


def test_legacy_pool_connection_migrate_once():
    pool = Pool(public_model='legacy', channels=['c1'], tiers={'c1': 0})
    cfg = FlexConfig.model_validate({
        'providers': {'demo': {'base_url_env': 'DEMO_BASE', 'api_key_env': 'DEMO_KEY'}},
        'channels': {'c1': _channel().model_dump()},
        'pools': {'legacy': pool.model_dump()},
        'connections': {'old-name': 'legacy'},
    })
    assert set(cfg.runners) == {'legacy'}
    assert cfg.links == {'old-name': 'legacy'}
    assert cfg.connections == cfg.links


def test_provider_models_are_deduplicated():
    c = _channel()
    cfg = FlexConfig.model_validate({
        'providers': {'demo': {'base_url_env': 'DEMO_BASE', 'api_key_env': 'DEMO_KEY'}},
        'channels': {'c1': c.model_dump(), 'c2': {**c.model_dump(), 'id': 'c2', 'public_model': 'demo-model-2'}},
        'runners': {'coder': {'channels': ['c1'], 'tiers': {'c1': 0}}},
    })
    assert [m['public_model'] for m in cfg.provider_models('demo')] == ['demo-model', 'demo-model-2']


def test_channel_external_exposure_defaults_true_and_alias_is_supported():
    hidden = _channel('hidden')
    hidden_data = hidden.model_dump()
    hidden_data.pop('externally_exposed')
    hidden_data['exposed'] = False
    cfg = FlexConfig.model_validate({
        'providers': {'demo': {'base_url_env': 'DEMO_BASE', 'api_key_env': 'DEMO_KEY'}},
        'channels': {'hidden': hidden_data,
                     'visible': _channel('visible').model_dump()},
        'runners': {'coder': {'channels': ['hidden'], 'tiers': {'hidden': 0}}},
    })
    assert cfg.channels['hidden'].externally_exposed is False
    assert cfg.channels['visible'].externally_exposed is True


def test_chn_content_policy_fallback_and_ordered_global_fallback_are_preserved():
    data = _channel().model_dump()
    data['chn_content_policy_fallback'] = True
    fallback = _channel('agnes-flash').model_dump()
    cfg = FlexConfig.model_validate({
        'providers': {'demo': {'base_url_env': 'DEMO_BASE', 'api_key_env': 'DEMO_KEY'}},
        'channels': {'c1': data, 'agnes-flash': fallback},
        'runners': {'coder': {'channels': ['c1'], 'tiers': {'c1': 0}}},
        'global_fallback': {'chn_content_policy': ['agnes-flash']},
    })
    assert cfg.channels['c1'].chn_content_policy_fallback is True
    assert cfg.global_fallback['chn_content_policy'] == ['agnes-flash']


def test_unknown_global_policy_fallback_channel_is_rejected():
    with pytest.raises(ValueError, match='unknown channel'):
        FlexConfig.model_validate({
            'providers': {'demo': {'base_url_env': 'DEMO_BASE', 'api_key_env': 'DEMO_KEY'}},
            'channels': {'c1': _channel().model_dump()},
            'runners': {'coder': {'channels': ['c1'], 'tiers': {'c1': 0}}},
            'global_fallback': {'chn_content_policy': ['missing']},
        })
