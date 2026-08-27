"""Tests for Flex LLM Router app endpoints."""
from pathlib import Path
import shutil
import pytest
from fastapi.testclient import TestClient
from flex_llm_router.app import create_app

import os
_REPO = Path(__file__).resolve().parent.parent


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


def test_config_editor_exposes_provider_env_names_not_values(tmp_config, monkeypatch):
    monkeypatch.setenv('SENSENOVA_API_KEY', 'do-not-return-this')
    client = TestClient(create_app(tmp_config))
    r = client.get('/api/config/editor')
    assert r.status_code == 200
    data = r.json()
    provider = next(p for p in data['providers'] if p['id'] == 'sensenova')
    assert provider['api_key_env'] == 'SENSENOVA_API_KEY'
    assert all('selection' in runner for runner in data['runners'])
    assert 'do-not-return-this' not in r.text


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
    assert 'solo-sensenova' in ids


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
