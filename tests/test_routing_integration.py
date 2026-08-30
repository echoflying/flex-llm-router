"""路由主流程集成测试：mock litellm.acompletion，覆盖 P0-1 / P0-3 的可复现用例。

不依赖真实上游；不污染项目 config（复制 pools.yaml 到 tmp）。
"""
from pathlib import Path
import shutil
import pytest
from fastapi.testclient import TestClient
import flex_llm_router.app as app_mod

_REPO = Path(__file__).resolve().parent.parent


class _Err(Exception):
    status_code = 429

    def __init__(self, msg):
        super().__init__(msg)
        self.message = msg


def _copy_config(tmp_path):
    """Copy the runtime config/templates into an isolated test directory."""
    src = _REPO / 'config' / 'pools.yaml'
    dst = tmp_path / 'config' / 'pools.yaml'
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dst)
    tpl_src = _REPO / 'templates'
    if tpl_src.exists():
        shutil.copytree(tpl_src, tmp_path / 'templates', dirs_exist_ok=True)
    return dst


def _make_client(tmp_path, monkeypatch, responses):
    """responses: list; 每次 acompletion 调用弹出一个（最后一个复用）。"""
    dst = _copy_config(tmp_path)

    idx = {'i': 0}

    async def fake_acompletion(**kwargs):
        i = idx['i']
        idx['i'] += 1
        payload = responses[i] if i < len(responses) else responses[-1]
        if isinstance(payload, Exception):
            raise payload
        return payload

    monkeypatch.setattr(app_mod.litellm, 'acompletion', fake_acompletion)
    return TestClient(app_mod.create_app(str(dst)))


def _ok_response(model='x'):
    class _Resp:
        def __init__(self):
            self.choices = [{'index': 0, 'message': {'role': 'assistant', 'content': 'ok'}, 'finish_reason': 'stop'}]
            self.usage = {'completion_tokens': 1, 'prompt_tokens': 2, 'total_tokens': 3}
            self.model = model
        def model_dump(self, mode='json'):
            return {'choices': self.choices, 'usage': self.usage, 'model': self.model}
    return _Resp()


def test_p0_1_retry_uses_per_request_counter_not_rowid(tmp_path, monkeypatch):
    """P0-1：即使 attempts 表已累计很多行，重试仍按本请求计数执行，不永久跳过。"""
    # 预灌 5 行历史（row id 已 >=3），证明后续请求仍会重试
    db = tmp_path / 'flex.db'
    import sqlite3
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    con.executescript("CREATE TABLE attempts(id INTEGER PRIMARY KEY,started REAL,pool TEXT,channel TEXT,model TEXT,outcome TEXT,error_type TEXT,latency_ms INTEGER,input_tokens INTEGER,output_tokens INTEGER,total_tokens INTEGER);")
    con.executemany("INSERT INTO attempts(started,pool,channel,model,outcome) VALUES(?,?,?,?,?)",
                    [(1.0, 'p', 'c', 'm', 'success')] * 5)
    con.commit(); con.close()
    monkeypatch.setenv('FLEX_STATE_DB', str(db))

    err = _Err('HTTP 429: rate limit')
    # seq: 前两次 429，第三次成功 -> 期望重试 2 次后成功（row id 已是 6，旧逻辑会永久跳过）
    client = _make_client(tmp_path, monkeypatch, [err, err, _ok_response()])
    r = client.post('/v1/chat/completions', json={'model': 'mix-deepseek-v4-flash',
                                                   'messages': [{'role': 'user', 'content': 'hi'}]})
    assert r.status_code == 200, r.text  # 走到第三次成功 = 重试逻辑生效


def test_rpm_tpm_retry_stays_on_original_channel(tmp_path, monkeypatch):
    """RPM/TPM retries must pin this request; only other requests may fail over."""
    monkeypatch.setenv('FLEX_STATE_DB', str(tmp_path / 'flex.db'))
    # Keep the test fast while still exercising the cumulative-cap branch.
    monkeypatch.setattr(app_mod, 'QUEUE_TPM_SECONDS', 3)
    seen = []

    class _TpmErr(_Err):
        def __init__(self):
            super().__init__('HTTP 429: inference tpm exhausted')

    async def fake_acompletion(**kwargs):
        seen.append((kwargs.get('model'), kwargs.get('api_base')))
        if len(seen) == 1:
            raise _TpmErr()
        return _ok_response(kwargs.get('model', 'x'))

    monkeypatch.setattr(app_mod.litellm, 'acompletion', fake_acompletion)
    client = TestClient(app_mod.create_app(str(_copy_config(tmp_path))))
    response = client.post('/v1/chat/completions', json={
        'model': 'sensenova-flash-plus',
        'messages': [{'role': 'user', 'content': 'pin this retry'}],
    })
    assert response.status_code == 200, response.text
    assert len(seen) == 2
    assert seen[0] == seen[1], 'TPM retry silently switched Channel'


def test_cost_aware_switches_after_channel_retry_budget(tmp_path, monkeypatch):
    """cost_aware keeps the Channel for its retry budget, then falls back."""
    monkeypatch.setenv('FLEX_STATE_DB', str(tmp_path / 'flex.db'))
    monkeypatch.setattr(app_mod, 'QUEUE_TPM_SECONDS', 100)
    monkeypatch.setattr(app_mod, 'TPM_BACKOFF_BASE', 0)
    seen = []

    class _TpmErr(_Err):
        def __init__(self):
            super().__init__('HTTP 429: inference tpm exhausted')

    async def fake_acompletion(**kwargs):
        seen.append((kwargs.get('model'), kwargs.get('api_base')))
        if len(seen) <= 4:  # initial attempt + 3 configured retries
            raise _TpmErr()
        return _ok_response(kwargs.get('model', 'x'))

    monkeypatch.setattr(app_mod.litellm, 'acompletion', fake_acompletion)
    client = TestClient(app_mod.create_app(str(_copy_config(tmp_path))))
    response = client.post('/v1/chat/completions', json={
        'model': 'sensenova-flash-plus',
        'messages': [{'role': 'user', 'content': 'cost aware fallback'}],
    })
    assert response.status_code == 200, response.text
    assert len(seen) == 5
    assert len(set(seen[:4])) == 1, 'cost_aware switched before retry budget was exhausted'
    assert seen[4] != seen[0], 'cost_aware did not switch after retry budget'


def test_p0_3_quota_exhausted_triggers_cooldown(tmp_path, monkeypatch):
    """P0-3：A 类 quota_exhausted 429 必须触发长冷却（写 states 表），而非直接 502 反复打。"""
    import sqlite3, time
    db = tmp_path / 'flex.db'
    db.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv('FLEX_STATE_DB', str(db))
    err = _Err('HTTP 429: Allocated quota exceeded, please increase your quota limit.')
    client = _make_client(tmp_path, monkeypatch, [err])  # 永远 429
    r = client.post('/v1/chat/completions', json={'model': 'mix-deepseek-v4-flash',
                                                   'messages': [{'role': 'user', 'content': 'hi'}]})
    assert r.status_code == 429
    # 验证 states 表里有 quota_exhausted 冷却写入（修复前为空）
    con = sqlite3.connect(db)
    row = con.execute("SELECT channel,reason,until FROM states").fetchone()
    con.close()
    assert row is not None, 'P0-3 未修复：quota_exhausted 未写入冷却'
    assert row[1] == 'quota_exhausted', f'冷却 reason 应为 quota_exhausted，实际 {row[1]}'


def test_p0_3_observe_429_recovers_channel_after_cooldown_expiry(tmp_path, monkeypatch):
    """P0-3 配套：冷却到期后通道重新 eligible（不永久坏）。"""
    import sqlite3
    db = tmp_path / 'flex.db'
    db.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv('FLEX_STATE_DB', str(db))
    err = _Err('HTTP 429: Allocated quota exceeded, please increase your quota limit.')
    # 第一次 429（写冷却），第二次成功（冷却已过期 -> 走正常路径）
    client = _make_client(tmp_path, monkeypatch, [err, _ok_response()])
    r1 = client.post('/v1/chat/completions', json={'model': 'mix-deepseek-v4-flash',
                                                    'messages': [{'role': 'user', 'content': 'hi'}]})
    assert r1.status_code == 429
    # 手动把冷却设到过去，模拟到期
    con = sqlite3.connect(db)
    con.execute("UPDATE states SET until=0 WHERE reason='quota_exhausted'")
    con.commit(); con.close()
    r2 = client.post('/v1/chat/completions', json={'model': 'mix-deepseek-v4-flash',
                                                    'messages': [{'role': 'user', 'content': 'hi again'}]})
    assert r2.status_code == 200, '冷却到期后应恢复可用'


def test_p0_2_probe_recovery_clears_cooldown(tmp_path, monkeypatch):
    """P0-2：回切探测接线——冷却中通道经探测成功后 clear_cooldown 提前回切。"""
    import sqlite3, asyncio as _asyncio
    db = tmp_path / 'flex.db'
    db.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv('FLEX_STATE_DB', str(db))
    monkeypatch.setenv('FLEX_PROBE_INTERVAL', '0')  # 探测间隔压到 0，便于驱动一轮

    captured = {}

    def _capture_create_task(coro):
        t = _asyncio.ensure_future(coro)
        captured['task'] = t
        return t
    monkeypatch.setattr(app_mod.asyncio, 'create_task', _capture_create_task)

    client = _make_client(tmp_path, monkeypatch, [_ok_response()])  # 探测用真实成功响应(无错误)
    # 触发 startup 事件（拉起探测循环）；在 with 上下文内 loop 存活，驱动探测跑一轮
    with client:
        assert captured.get('task') is not None, '探测循环未在 startup 拉起（P0-2 接线回退）'
        # 预置某通道为 quota_exhausted 冷却
        pool_name = 'mix-deepseek-v4-flash'
        ch_id = 'sensenova-deepseek-v4-flash'
        con = sqlite3.connect(db)
        con.execute("INSERT INTO states(pool,channel,until,reason) VALUES(?,?,?,?)",
                    (pool_name, ch_id, 9e9, 'quota_exhausted'))
        con.commit(); con.close()
        # 驱动探测循环跑一轮（探测成功 -> clear_cooldown）；循环是 while True，用超时只取一轮
        loop = captured['task'].get_loop()
        try:
            fut = _asyncio.run_coroutine_threadsafe(_asyncio.wait_for(captured['task'], timeout=2), loop)
            fut.result(timeout=3)
        except (_asyncio.TimeoutError, Exception) as exc:
            pass  # 一轮跑完进入 sleep 再次等待 -> 超时即证明至少一轮已执行；或循环仍存活
        # 验证：冷却已被探测成功清除(clear_cooldown 执行 DELETE)
        con = sqlite3.connect(db)
        row = con.execute("SELECT until,reason FROM states WHERE pool=? AND channel=?",
                          (pool_name, ch_id)).fetchone()
        con.close()
        assert row is None, 'P0-2 回退：探测成功未清冷却（clear_cooldown 未被调用）'
