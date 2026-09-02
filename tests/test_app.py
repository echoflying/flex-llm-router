"""Tests for Flex LLM Router app endpoints."""
from pathlib import Path
import asyncio
import shutil
import pytest
from fastapi.testclient import TestClient
from flex_llm_router.app import BufferedUpstreamStream, create_app, error_type, hedge_plan_for, first_activity_deadline_for, rpm_limit_exhausted_action, await_stream_next, has_stream_activity

import os
_REPO = Path(__file__).resolve().parent.parent


def test_single_channel_hedge_retries_same_channel_at_six_minutes():
    class Channel:
        id = 'solo'

    channel = Channel()
    assert hedge_plan_for('solo-runner', [channel], channel, None) == ((360, ('solo',)),)
    assert first_activity_deadline_for([channel]) == 540


def test_stream_idle_read_is_bounded_without_waiting_for_iterator_cleanup():
    class StalledIterator:
        def __init__(self):
            self.future = asyncio.get_running_loop().create_future()

        def __anext__(self):
            return self.future

    async def exercise():
        iterator = StalledIterator()
        with pytest.raises(asyncio.TimeoutError):
            await await_stream_next(iterator, 0.01)

    asyncio.run(exercise())


def test_core_watchdog_keeps_trace_registered_after_first_sse():
    """Post-first-SSE timeout is owned by the core loop, not only the iterator."""
    source = (_REPO / 'src' / 'flex_llm_router' / 'app.py').read_text(encoding='utf-8')
    assert "watch_record['phase']='stream'" in source
    assert "watch_record['on_stream_deadline']=watchdog_stream_deadline" in source
    assert "if record.get('phase')=='stream':" in source


def test_response_object_buffers_first_sse_before_asgi_body():
    """The handoff gap must not leave a returned upstream stream unread."""
    source = (_REPO / 'src' / 'flex_llm_router' / 'app.py').read_text(encoding='utf-8')
    assert 'class BufferedUpstreamStream:' in source
    assert "watch_record['prefetched_first_sse']=first_sse_task" in source
    assert 'Router began raw SSE buffering before downstream streaming phase' in source


def test_buffered_upstream_stream_preserves_raw_order():
    class FakeResponse:
        async def __aiter__(self):
            yield {'unknown_provider_frame': 1}
            yield {'choices': [{'delta': {'content': 'ok'}}]}

    async def exercise():
        buffered = BufferedUpstreamStream(FakeResponse())
        assert await anext(buffered) == {'unknown_provider_frame': 1}
        assert await anext(buffered) == {'choices': [{'delta': {'content': 'ok'}}]}
        with pytest.raises(StopAsyncIteration):
            await anext(buffered)

    asyncio.run(exercise())


def test_all_stream_paths_use_raw_fifo_and_close_the_active_pump():
    """Fallback/hedge cleanup must not bypass the raw SSE FIFO."""
    source = (_REPO / 'src' / 'flex_llm_router' / 'app.py').read_text(encoding='utf-8')
    assert "iterator=BufferedUpstreamStream(resp)" in source
    assert "current_response=new_iterator; response=new_iterator" in source
    assert "response=new_iterator; iterator=new_iterator; first_item=new_first" in source
    assert "response=iterator\n                    ch=winner_channel" in source


def test_initial_response_arbitration_has_completed_task_fifo():
    """Fast 429s must be harvested rather than waiting for a later Hedge tick."""
    source = (_REPO / 'src' / 'flex_llm_router' / 'app.py').read_text(encoding='utf-8')
    assert 'completion_queue=asyncio.Queue()' in source
    assert 'completion_task=asyncio.create_task(completion_queue.get())' in source
    assert 'if notified in active:' in source


def test_empty_sse_does_not_refresh_stream_idle_timer():
    assert not has_stream_activity({'choices': [{'delta': {'role': 'assistant'}}]})
    assert not has_stream_activity({'choices': [{'delta': {}}]})
    assert has_stream_activity({'choices': [{'delta': {'reasoning_content': 'thinking'}}]})
    assert has_stream_activity({'choices': [{'delta': {'content': 'ok'}}]})
    assert has_stream_activity({'choices': [{'delta': {}, 'finish_reason': 'stop'}]})


def test_data_inspection_failure_is_content_policy_error():
    class UpstreamPolicyError(Exception):
        status_code = 400
        message = 'data_inspection_failed: Input text data may contain inappropriate content.'

    assert error_type(UpstreamPolicyError()) == 'content_policy_blocked'


def test_rpm_limit_exhausted_action_has_strategy_compatible_defaults():
    assert rpm_limit_exhausted_action({'strategy': 'cost_aware'}) == 'failover'
    assert rpm_limit_exhausted_action({'strategy': 'round_robin'}) == 'wait'
    assert rpm_limit_exhausted_action({'strategy': 'round_robin', 'rpm_limit': {'on_exhausted': 'failover'}}) == 'failover'
    assert rpm_limit_exhausted_action({'strategy': 'cost_aware', 'rpm_limit': {'on_exhausted': 'fail'}}) == 'fail'


@pytest.fixture
def tmp_config(tmp_path):
    """复制真实 config 到临时目录，测试不污染项目文件。"""
    src = _REPO / 'config' / 'pools.yaml'
    dst = tmp_path / 'config' / 'pools.yaml'
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dst)
    # 复制 templates 目录(渲染 HTML 需要)
    tpl_src = _REPO / 'templates'
    tpl_dst = tmp_path / 'templates'
    if tpl_src.exists():
        shutil.copytree(tpl_src, tpl_dst, dirs_exist_ok=True)
    return str(dst)


def test_config_page_shows_raw_yaml(tmp_config):
    client = TestClient(create_app(tmp_config))
    r = client.get('/config')
    assert r.status_code == 200
    assert 'version: 1' in r.text
    assert '<pre><code>' in r.text


def test_config_page_has_three_resource_tabs_in_order(tmp_config):
    client = TestClient(create_app(tmp_config))
    text = client.get('/config').text
    assert 'data-tab="runner">Runner' in text
    assert 'data-tab="channel">Channel' in text
    assert 'data-tab="model">Model' in text
    assert text.index('data-tab="runner"') < text.index('data-tab="channel"') < text.index('data-tab="model"')
    assert '局域网' in text
    assert '复制' in text
    assert '上移' in text and '下移' in text
    assert 'runner-strategy-info' in text
    assert 'runner-channel-modal' in text
    assert 'runner-channel-pane' in text and 'runner-summary' in text
    assert 'runner-mode-row' in text and 'runner-copy-row' in text
    assert 'Base URL 选择：' in text
    assert 'name="runner_name"' in text and 'autocomplete="off"' in text
    assert '不支持空格和斜杠' in text
    assert 'remove-channel' in text and 'runner-add' in text
    assert 'runner-save' not in text
    assert '立即保存并参与调度' in text
    assert 'name="strategy"' in text and 'type="radio"' in text
    assert 'Provider 在前' in text
    assert '实际 MODEL：' in text
    assert 'CHN Content Policy' in text
    assert '全局 CHN Content Policy Fallback' in text


def test_create_runner_with_initial_channel(tmp_config, monkeypatch):
    for name in ('SENSENOVA_API_BASE', 'SENSENOVA_API_KEY',
                 'OPENCODE_GO_API_BASE', 'OPENCODE_GO_API_KEY',
                 'DEEPSEEK_OFFICIAL_API_BASE', 'DEEPSEEK_OFFICIAL_API_KEY',
                 'AGNES_API_BASE', 'AGNES_API_KEY'):
        monkeypatch.setenv(name, 'test-value')
    client = TestClient(create_app(tmp_config))
    response = client.post('/api/config/runners', json={
        'name': 'new-runner',
        'channel': 'sensenova-deepseek-v4-flash',
    })
    assert response.status_code == 200, response.text
    runners = client.get('/api/config/editor').json()['runners']
    created = next(r for r in runners if r['name'] == 'new-runner')
    assert [c['id'] for c in created['channels']] == ['sensenova-deepseek-v4-flash']


def test_create_runner_can_reuse_single_channel_name(tmp_config, monkeypatch):
    for name in ('SENSENOVA_API_BASE', 'SENSENOVA_API_KEY'):
        monkeypatch.setenv(name, 'test-value')
    client = TestClient(create_app(tmp_config))
    response = client.post('/api/config/runners', json={
        'name': 'sensenova-deepseek-v4-flash',
        'channel': 'sensenova-deepseek-v4-flash',
    })
    assert response.status_code == 200, response.text


def test_config_editor_exposes_provider_env_names_not_values(tmp_config, monkeypatch):
    monkeypatch.setenv('SENSENOVA_API_KEY', 'do-not-return-this')
    client = TestClient(create_app(tmp_config))
    r = client.get('/api/config/editor')
    assert r.status_code == 200
    data = r.json()
    provider = next(p for p in data['providers'] if p['id'] == 'sensenova')
    assert provider['api_key_env'] == 'SENSENOVA_API_KEY'
    assert all('selection' in runner for runner in data['runners'])
    assert data['global_fallback']['chn_content_policy'] == ['agnes-flash']
    channel = next(c for c in data['channels'] if c['id'] == 'sensenova-deepseek-v4-flash')
    assert channel['chn_content_policy_fallback'] is False
    assert 'do-not-return-this' not in r.text


def test_global_policy_fallback_edit_persists_order(tmp_config, monkeypatch):
    for name in ('SENSENOVA_API_BASE', 'SENSENOVA_API_KEY', 'AGNES_API_BASE', 'AGNES_API_KEY'):
        monkeypatch.setenv(name, 'test-value')
    client = TestClient(create_app(tmp_config))
    mark = client.post('/api/config/channels/sensenova-deepseek-v4-flash', json={
        'chn_content_policy_fallback': True,
    })
    assert mark.status_code == 200, mark.text
    response = client.post('/api/config/global-fallback', json={
        'policy': 'chn_content_policy',
        'channels': ['sensenova-deepseek-v4-flash', 'agnes-flash'],
    })
    assert response.status_code == 200, response.text
    assert client.get('/api/config/editor').json()['global_fallback']['chn_content_policy'] == [
        'sensenova-deepseek-v4-flash', 'agnes-flash']


def test_responses_probe_persists_channel_protocol_result(tmp_config, monkeypatch):
    """The explicit Responses probe records its last upstream observation."""
    for name in ('SENSENOVA_API_BASE', 'SENSENOVA_API_KEY',
                 'OPENCODE_GO_API_BASE', 'OPENCODE_GO_API_KEY',
                 'DEEPSEEK_OFFICIAL_API_BASE', 'DEEPSEEK_OFFICIAL_API_KEY',
                 'AGNES_API_BASE', 'AGNES_API_KEY'):
        monkeypatch.setenv(name, 'test-value')

    class FakeResponse:
        status = 200

        def read(self):
            return b'{"id":"resp_test","object":"response","status":"completed","output":[]}'

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    import flex_llm_router.app as app_module
    monkeypatch.setattr(app_module.urllib.request, 'urlopen', lambda request, timeout=20: FakeResponse())
    client = TestClient(create_app(tmp_config))
    response = client.post('/api/config/channels/sensenova-deepseek-v4-flash/responses-test')
    assert response.status_code == 200, response.text
    assert response.json()['status'] == 'supported'
    channel = next(c for c in client.get('/api/config/editor').json()['channels']
                   if c['id'] == 'sensenova-deepseek-v4-flash')
    assert channel['protocol_support']['responses']['status'] == 'supported'
    assert channel['protocol_support']['responses']['http_status'] == 200


def test_runner_channel_order_edit_persists_for_scheduler(tmp_config, monkeypatch):
    """Runner order is stored as the channel list consumed by scheduling."""
    for name in ('SENSENOVA_API_BASE', 'SENSENOVA_API_KEY',
                 'OPENCODE_GO_API_BASE', 'OPENCODE_GO_API_KEY',
                 'DEEPSEEK_OFFICIAL_API_BASE', 'DEEPSEEK_OFFICIAL_API_KEY',
                 'AGNES_API_BASE', 'AGNES_API_KEY'):
        monkeypatch.setenv(name, 'test-value')
    client = TestClient(create_app(tmp_config))
    data = client.get('/api/config/editor').json()
    runner = next(r for r in data['runners'] if r['name'] == 'mix-deepseek-v4-flash')
    ordered = [c['id'] for c in reversed(runner['channels'])]
    response = client.post('/api/config/runners/mix-deepseek-v4-flash', json={
        'channels': ordered,
    })
    assert response.status_code == 200, response.text
    refreshed = client.get('/api/config/editor').json()
    runner = next(r for r in refreshed['runners'] if r['name'] == 'mix-deepseek-v4-flash')
    assert [c['id'] for c in runner['channels']] == ordered


def test_setup_page_shows_env_vars_and_nav(tmp_config):
    client = TestClient(create_app(tmp_config))
    r = client.get('/setup')
    assert r.status_code == 200
    assert 'SENSENOVA_API_KEY' in r.text
    assert 'OPENCODE_GO_API_KEY' in r.text
    assert 'OpenRouter' not in r.text
    assert 'Dashboard' in r.text
    assert 'Config' in r.text
    assert 'Setup' in r.text
    assert 'Help' in r.text
    assert '普通会话粘性' in r.text
    assert '协议兼容' in r.text


def test_affinity_windows_are_separate_and_hot_configurable(tmp_config):
    client = TestClient(create_app(tmp_config))
    before = client.get('/api/setup/affinity').json()
    assert before['session_idle_seconds'] == 3600
    assert before['protocol_idle_seconds'] == 3600
    updated = client.post('/api/setup/affinity', json={
        'session_idle_seconds': 1800,
        'protocol_idle_seconds': 7200,
    })
    assert updated.status_code == 200
    assert updated.json() == {'session_idle_seconds': 1800, 'protocol_idle_seconds': 7200}


def test_setup_override_toggle_on_then_off_and_back(tmp_config):
    override_path = _REPO / 'config' / 'setup.conf'
    original = override_path.read_text() if override_path.exists() else ''
    try:
        client = TestClient(create_app(tmp_config))
        r = client.post('/api/setup/override')
        first_override = r.json()['override']
        r = client.post('/api/setup/override')
        second_override = r.json()['override']
        assert first_override != second_override
        r = client.post('/api/setup/override')
        third_override = r.json()['override']
        assert third_override == first_override
    finally:
        override_path.write_text(original)


def test_config_save_rejects_invalid_yaml(tmp_config):
    client = TestClient(create_app(tmp_config))
    r = client.post('/api/config', content='not: valid: yaml: :::')
    assert r.status_code == 400


def test_config_save_rejects_missing_env_vars(tmp_config):
    body = '''version: 1
providers:
  missing-provider:
    base_url_env: GHOST_BASE_NOT_SET
    api_key_env: GHOST_KEY_NOT_SET
channels:
  missing-channel:
    id: missing-channel
    provider: missing-provider
    litellm_model: openai/x
    public_model: test/x
    context_window_tokens: 1000
    enabled: true
pools:
  test-pool:
    channels:
      - missing-channel
'''
    r = client_post(tmp_config, body)
    assert r.status_code == 400
    assert 'GHOST_BASE_NOT_SET' in r.json()['detail']


def test_config_save_skips_env_check_for_disabled_channel(tmp_config):
    body = '''version: 1
providers:
  ghost-provider:
    base_url_env: GHOST_BASE_NOT_SET
    api_key_env: GHOST_KEY_NOT_SET
channels:
  ghost-channel:
    id: ghost-channel
    provider: ghost-provider
    litellm_model: openai/x
    public_model: test/x
    context_window_tokens: 1000
    enabled: false
pools:
  dormant-pool:
    channels:
      - ghost-channel
'''
    r = client_post(tmp_config, body)
    assert r.status_code == 200  # disabled channels are allowed to reference unset vars


def test_config_save_writes_and_backs_up(tmp_config):
    import yaml
    cfg = yaml.safe_load(Path(tmp_config).read_text())
    # 移除 agnes(依赖未确认 .env 变量) 用干净配置测成功路径
    if 'agnes-official' in cfg.get('providers', {}):
        del cfg['providers']['agnes-official']
    cfg['channels'] = {k: v for k, v in cfg['channels'].items() if v.get('provider') != 'agnes-official'}
    cfg['pools'] = {pn: p for pn, p in cfg.get('pools', {}).items()
                    if any(c in cfg['channels'] for c in p.get('channels', []))}
    # connections 里若指向被移除的 agnes channel/pool, 一并清掉(否则新校验会拒)
    if cfg.get('connections'):
        cfg['connections'] = {n: t for n, t in cfg['connections'].items()
                              if t in cfg['channels'] or t in cfg['pools']}
    clean = yaml.safe_dump(cfg)
    client = TestClient(create_app(tmp_config))
    r = client.post('/api/config', content=clean)
    assert r.status_code == 200 and 'backup' in r.json()


def test_channels_api_returns_pool_and_channels(tmp_config):
    client = TestClient(create_app(tmp_config))
    r = client.get('/api/pools/mix-deepseek-v4-flash/channels')
    assert r.status_code == 200
    data = r.json()
    assert data['pool'] == 'mix-deepseek-v4-flash'
    assert len(data['channels']) == 3  # deepseek pool 不含 agnes(agnes 孤立未挂载)
    for ch in data['channels']:
        assert 'id' in ch
        assert 'context_window_tokens' in ch
        assert 'capabilities' in ch
        assert 'channel_type' in ch
        assert 'enabled' in ch


def test_request_api_returns_recent_attempts(tmp_config):
    client = TestClient(create_app(tmp_config))
    r = client.get('/api/requests?limit=5')
    assert r.status_code == 200
    data = r.json()
    assert 'data' in data
    assert isinstance(data['data'], list)


def client_post(tmp_config, body):
    return TestClient(create_app(tmp_config)).post('/api/config', content=body)


# --- 连接(connections) 功能测试 ---
# TestClient 下 create_app 启动即把磁盘配置载入内存, /api/config 的 POST 只写盘(真实环境靠
# launchd 重启生效), 故"解析类"测试需先把 connections 写进磁盘再 create_app; "校验类"测试
# 走 POST(触发 model_validate) 验证被拒。

def _write_config_with_connections(tmp_config, connections):
    import yaml
    cfg = yaml.safe_load(Path(tmp_config).read_text())
    # 移除 agnes(依赖未确认 .env) 用干净三通道配置, 避免 save 时的 env 校验干扰
    if 'agnes-official' in cfg.get('providers', {}):
        del cfg['providers']['agnes-official']
    cfg['channels'] = {k: v for k, v in cfg['channels'].items() if v.get('provider') != 'agnes-official'}
    cfg['pools'] = {pn: p for pn, p in cfg.get('pools', {}).items()
                    if any(c in cfg['channels'] for c in p.get('channels', []))}
    cfg['connections'] = connections
    Path(tmp_config).write_text(yaml.safe_dump(cfg))
    return str(tmp_config)


def test_connection_to_pool_resolves(tmp_config):
    """连接指向 pool 键时, 内部解析走该 pool 的通道。"""
    path = _write_config_with_connections(tmp_config, {'my-main': 'mix-deepseek-v4-flash'})
    client = TestClient(create_app(path))
    # 用连接名而非真实 pool 名访问 channels 接口
    r2 = client.get('/api/pools/my-main/channels')
    assert r2.status_code == 200, r2.text
    assert r2.json()['pool'] == 'mix-deepseek-v4-flash'
    assert len(r2.json()['channels']) == 3


def test_connection_to_channel_resolves(tmp_config):
    """连接指向单 channel id 时, 解析为直连通道。"""
    path = _write_config_with_connections(tmp_config, {'solo-sensenova': 'sensenova-deepseek-v4-flash'})
    client = TestClient(create_app(path))
    r2 = client.get('/api/pools/solo-sensenova/channels')
    assert r2.status_code == 200, r2.text
    assert r2.json()['pool'] == 'sensenova-deepseek-v4-flash'
    assert len(r2.json()['channels']) == 1
    assert r2.json()['channels'][0]['id'] == 'sensenova-deepseek-v4-flash'


def test_models_list_includes_connections(tmp_config):
    """/v1/models 应把连接作为独立 model 暴露, 外部可直接引用。"""
    path = _write_config_with_connections(tmp_config, {
        'my-main': 'mix-deepseek-v4-flash',
        'solo-sensenova': 'sensenova-deepseek-v4-flash',
    })
    client = TestClient(create_app(path))
    r2 = client.get('/v1/models')
    assert r2.status_code == 200
    ids = {m['id'] for m in r2.json()['data']}
    assert 'my-main' in ids
    assert 'solo-sensenova' not in ids  # direct Channel links are no longer advertised


def test_invalid_connection_target_rejected(tmp_config):
    """连接目标不存在时, 保存配置应被 400 拒绝。"""
    body = _config_with_connections_for_post(tmp_config, {'ghost': 'no-such-pool-or-channel'})
    client = TestClient(create_app(tmp_config))
    r = client.post('/api/config', content=body)
    assert r.status_code == 400, r.text
    assert 'ghost' in r.json()['detail'] or 'no-such-pool-or-channel' in r.json()['detail']


def test_config_view_includes_connections(tmp_config):
    """/api/config/view 应返回每条连接的名称/目标/类型/真实通道。"""
    path = _write_config_with_connections(tmp_config, {
        'my-main': 'mix-deepseek-v4-flash',
        'solo-sensenova': 'sensenova-deepseek-v4-flash',
    })
    client = TestClient(create_app(path))
    r = client.get('/api/config/view')
    assert r.status_code == 200, r.text
    conns = {c['name']: c for c in r.json()['connections']}
    assert 'my-main' in conns and conns['my-main']['type'] == 'pool'
    assert conns['my-main']['channels'] == ['sensenova-deepseek-v4-flash',
                                            'opencode-go-deepseek-v4-flash',
                                            'deepseek-official-deepseek-v4-flash']
    assert 'solo-sensenova' in conns and conns['solo-sensenova']['type'] == 'channel'
    assert conns['solo-sensenova']['channels'] == ['sensenova-deepseek-v4-flash']


def test_connection_name_conflict_with_pool_rejected(tmp_config):
    """连接名若与现有 pool/channel 对外名撞车(非自指)应被拒绝, 避免歧义。"""
    body = _config_with_connections_for_post(tmp_config, {
        'mix-deepseek-v4-flash': 'sensenova-deepseek-v4-flash'  # 名=pool名但指向别处
    })
    client = TestClient(create_app(tmp_config))
    r = client.post('/api/config', content=body)
    assert r.status_code == 400, r.text


def _config_with_connections_for_post(tmp_config, connections):
    import yaml
    cfg = yaml.safe_load(Path(tmp_config).read_text())
    if 'agnes-official' in cfg.get('providers', {}):
        del cfg['providers']['agnes-official']
    cfg['channels'] = {k: v for k, v in cfg['channels'].items() if v.get('provider') != 'agnes-official'}
    cfg['pools'] = {pn: p for pn, p in cfg.get('pools', {}).items()
                    if any(c in cfg['channels'] for c in p.get('channels', []))}
    cfg['connections'] = connections
    return yaml.safe_dump(cfg)
