from types import SimpleNamespace

from flex_llm_router.app import _capability_endpoint, classify_responses_probe, hedge_plan_for, first_activity_deadline_for


def test_capability_endpoint_normalizes_openai_base_url():
    assert _capability_endpoint('https://example.test/v1') == 'https://example.test/v1/chat/completions'
    assert _capability_endpoint('https://example.test/v1/models') == 'https://example.test/v1/chat/completions'


def test_responses_probe_classifies_explicit_unsupported():
    assert classify_responses_probe(
        400,
        {'error': {'type': 'protocol_conversion_not_support', 'message': 'Protocol conversion is not supported'}},
    ) == 'unsupported'


def test_responses_probe_does_not_turn_generic_400_into_unsupported():
    assert classify_responses_probe(400, {'error': {'message': 'Invalid request parameters'}}) == 'error'


def test_large_context_uses_aggressive_hedge_plan(monkeypatch):
    monkeypatch.setattr('flex_llm_router.app.LARGE_CONTEXT_THRESHOLD_TOKENS', 100)
    channels = [SimpleNamespace(id='a'), SimpleNamespace(id='b'), SimpleNamespace(id='c')]
    assert hedge_plan_for('r', channels, channels[0], None, 100) == ((180, ('b',)), (360, ('c',)))
    assert first_activity_deadline_for(channels, 100, None) == 540
