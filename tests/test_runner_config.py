from flex_llm_router.config import Channel, FlexConfig, Limits, Pool, Runner


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
