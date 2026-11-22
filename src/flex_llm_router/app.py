from __future__ import annotations
import asyncio,calendar,datetime,fnmatch,html,json,logging,os,random,re,shutil,subprocess,sys,time,uuid
import anyio
from pathlib import Path
from typing import Any
import urllib.error,urllib.request
import litellm,yaml
from fastapi import FastAPI,HTTPException,Request
from fastapi.responses import HTMLResponse,JSONResponse,StreamingResponse
from flex_llm_router.config import FlexConfig,ProtocolErrorRule,channel_credentials,load_config,validate_runner_name
from flex_llm_router.scheduler import RoundRobinScheduler
from flex_llm_router.state import StateStore
from flex_llm_router import settings as settings_registry
logger=logging.getLogger('uvicorn.error')

def backup_config_file(config_path: Path, keep: int = 10) -> Path | None:
    """Create a timestamped local config backup and retain only the newest ones.

    Runtime configuration is deliberately kept outside the code-sync workflow.
    Backups sit beside the source file (for example ``pools.yaml.backup.20260829102323``)
    so a Git rollback cannot remove the last known-good runtime configuration.
    """
    if not config_path.exists():
        return None
    stamp = time.strftime('%Y%m%d%H%M%S', time.localtime())
    backup = config_path.with_name(f'{config_path.name}.backup.{stamp}')
    suffix = 1
    while backup.exists():
        backup = config_path.with_name(f'{config_path.name}.backup.{stamp}_{suffix:02d}')
        suffix += 1
    shutil.copy2(config_path, backup)
    backups = sorted(
        config_path.parent.glob(f'{config_path.name}.backup.*'),
        key=lambda path: path.name,
        reverse=True,
    )
    for stale in backups[max(0, keep):]:
        try:
            stale.unlink()
        except FileNotFoundError:
            pass
    return backup

# Change this for every core behavior release.  It is exposed by /healthz so a
# restart can be verified without inferring it from a changing uptime counter.
ROUTER_BUILD='2026-09-02.graceful-restart-drain-v5'
RESPONSE_REPLAY_SECONDS=int(os.getenv('FLEX_RESPONSE_REPLAY_SECONDS','120'))
class ClientDisconnectedBeforeResponse(Exception):
    """The downstream socket closed while LiteLLM was still awaiting headers/SSE."""
class UpstreamTotalTimeout(Exception):
    """No usable upstream response/SSE arrived before the Router safety deadline."""
def cancel_detached(tasks):
    """Cancel provider tasks without allowing cancellation-resistant SDK reads to
    keep the Router's deadline response open. Drain later task results quietly."""
    def drain(done):
        if done.cancelled(): return
        try: done.exception()
        except (asyncio.CancelledError, Exception): pass
    for task in list(tasks):
        if not task.done(): task.cancel()
        task.add_done_callback(drain)
def is_replayable_response(payload):
    """Strictly exclude tool calls, unfinished results, and malformed provider replies."""
    choices=payload.get('choices') if isinstance(payload,dict) else None
    if not isinstance(choices,list) or not choices:return False
    choice=choices[0] if isinstance(choices[0],dict) else {}
    message=choice.get('message') if isinstance(choice.get('message'),dict) else {}
    return choice.get('finish_reason') not in (None,'length') and not message.get('tool_calls')
async def await_upstream_or_disconnect(request: Request, upstream):
    """Race an upstream call with the ASGI disconnect signal before a response exists.

    StreamingResponse can only observe http.disconnect after LiteLLM has returned a
    response object. This closes that earlier blind spot without inventing a
    protocol-level task id.
    """
    upstream_task=asyncio.create_task(upstream)
    async def watch_disconnect():
        while True:
            if await request.is_disconnected():
                return True
            await asyncio.sleep(0.5)
    disconnect_task=asyncio.create_task(watch_disconnect())
    try:
        done,_=await asyncio.wait({upstream_task,disconnect_task},return_when=asyncio.FIRST_COMPLETED)
        # Prefer a completed upstream result if both become ready in the same tick.
        if upstream_task in done:
            return upstream_task.result()
        if disconnect_task.result():
            upstream_task.cancel()
            await asyncio.gather(upstream_task,return_exceptions=True)
            raise ClientDisconnectedBeforeResponse()
        return upstream_task.result()
    finally:
        if not disconnect_task.done():
            disconnect_task.cancel()
        await asyncio.gather(disconnect_task,return_exceptions=True)
class DisconnectAwareStreamingResponse(StreamingResponse):
    """Listen for http.disconnect after LiteLLM has returned the stream response."""
    def __init__(self,*args,on_disconnect=None,timeout_event=None,timeout_detail=None,
                 restart_event=None,on_finished=None,on_call=None,on_stream=None,**kwargs):
        super().__init__(*args,**kwargs)
        self.on_call=on_call
        self.on_stream=on_stream
        self.on_disconnect=on_disconnect
        self.timeout_event=timeout_event
        self.timeout_detail=timeout_detail or 'Upstream stream timed out before producing usable activity.'
        self.restart_event=restart_event
        self.on_finished=on_finished
    async def __call__(self,scope,receive,send):
        if scope['type']=='websocket':
            return await super().__call__(scope,receive,send)
        if self.on_call is not None:
            try: self.on_call()
            except Exception: pass
        # A watchdog can fire after the response object has been selected but
        # before the StreamingResponse generator is entered.  In that gap the
        # generator cannot send its usual SSE error.  Race the body consumer
        # with a dedicated timeout sender so the caller always receives either
        # an HTTP 504 (headers not sent yet) or a terminal SSE error (stream
        # headers already sent).
        if self.timeout_event is None and self.restart_event is None:
            async with anyio.create_task_group() as group:
                async def stream():
                    await self.stream_response(send)
                    group.cancel_scope.cancel()
                async def watch_disconnect():
                    await self.listen_for_disconnect(receive)
                    if self.on_disconnect:self.on_disconnect()
                    group.cancel_scope.cancel()
                group.start_soon(stream)
                group.start_soon(watch_disconnect)
            if self.background is not None:await self.background()
            if self.on_finished:self.on_finished()
            return
        # Keep response headers buffered until the first body event is ready.
        # This lets a pre-first-SSE watchdog return a real HTTP 504 instead of
        # committing the stream to HTTP 200 before the upstream has produced
        # anything usable.
        response_started=anyio.Event(); stream_done=anyio.Event(); send_lock=anyio.Lock(); holder={'start_message':None}
        async def flush_start():
            if response_started.is_set(): return
            message=holder.get('start_message')
            if message is None: return
            await send(message)
            response_started.set()
        async def guarded_send(message):
            async with send_lock:
                kind=message.get('type')
                if kind=='http.response.start':
                    holder['start_message']=message
                    return
                if kind=='http.response.body' and (message.get('body') or not message.get('more_body',False)):
                    await flush_start()
                await send(message)
        async with anyio.create_task_group() as group:
            async def stream():
                with anyio.CancelScope() as scope:
                    holder['scope']=scope
                    if self.on_stream is not None:
                        try: self.on_stream()
                        except Exception: pass
                    try:
                        await self.stream_response(guarded_send)
                    finally:
                        stream_done.set()
                        # A well-behaved stream normally flushes the start with
                        # its first body chunk.  Still release it on an empty
                        # but normally completed stream so the client receives
                        # a valid response rather than an unclosed task.
                        if (not holder.get('timeout_in_progress')
                                and not (self.timeout_event and self.timeout_event.is_set())
                                and not (self.restart_event and self.restart_event.is_set())):
                            async with send_lock:
                                await flush_start()
                        # When the timeout watcher cancelled this stream, it
                        # still has to send the terminal 504/SSE message. Do
                        # not cancel the whole task group from this finally
                        # block; the timeout watcher owns that cancellation.
                        if not holder.get('timeout_in_progress'):
                            group.cancel_scope.cancel()
            async def watch_disconnect():
                await self.listen_for_disconnect(receive)
                if self.on_disconnect:self.on_disconnect()
                group.cancel_scope.cancel()
            async def watch_timeout():
                if self.timeout_event is None:
                    return
                await self.timeout_event.wait()
                if stream_done.is_set(): return
                holder['timeout_in_progress']=True
                scope=holder.get('scope')
                if scope is not None: scope.cancel()
                async with send_lock:
                    if response_started.is_set():
                        body=json.dumps({'error':{'message':self.timeout_detail,'type':'server_error','param':None,'code':'upstream_total_timeout'}},ensure_ascii=False).encode()
                        await send({'type':'http.response.body','body':b'data: '+body+b'\n\n','more_body':False})
                    else:
                        body=json.dumps({'error':{'message':self.timeout_detail,'type':'server_error','param':None,'code':'upstream_total_timeout'}},ensure_ascii=False).encode()
                        await send({'type':'http.response.start','status':504,'headers':[(b'content-type',b'application/json'),(b'content-length',str(len(body)).encode())]})
                        response_started.set()
                        await send({'type':'http.response.body','body':body,'more_body':False})
                group.cancel_scope.cancel()
            async def watch_restart():
                if self.restart_event is None:
                    return
                await self.restart_event.wait()
                if stream_done.is_set(): return
                holder['timeout_in_progress']=True
                scope=holder.get('scope')
                if scope is not None: scope.cancel()
                detail='Router is restarting; this in-flight request was closed. Please retry.'
                async with send_lock:
                    if response_started.is_set():
                        body=json.dumps({'error':{'type':'router_restarting','detail':detail}},ensure_ascii=False).encode()
                        await send({'type':'http.response.body','body':b'data: '+body+b'\n\n','more_body':False})
                    else:
                        body=json.dumps({'detail':detail},ensure_ascii=False).encode()
                        await send({'type':'http.response.start','status':503,'headers':[(b'content-type',b'application/json'),(b'content-length',str(len(body)).encode())]})
                        response_started.set()
                        await send({'type':'http.response.body','body':body,'more_body':False})
                group.cancel_scope.cancel()
            group.start_soon(stream)
            group.start_soon(watch_disconnect)
            group.start_soon(watch_timeout)
            group.start_soon(watch_restart)
        if self.background is not None:await self.background()
        if self.on_finished:self.on_finished()
def data(r,model):
    d=r.model_dump(mode='json') if hasattr(r,'model_dump') else dict(r)
    # 推理模型(reasoning_content)原样透传: 不把思考内容塞进 content,
    # 否则 OpenCode 等客户端会把思考流当正文渲染(Thought: Xms 碎片).
    # 保留 reasoning_content 字段由客户端自行决定是否展示.
    for choice in d.get('choices',[]):
        payload=choice.get('delta') or choice.get('message')
        if payload and 'reasoning_content' in payload and payload.get('reasoning_content') is None:
            payload.pop('reasoning_content',None)
    d['model']=model; return d
def has_visible_content(chunk):
    return any(isinstance((choice.get('delta') or {}).get('content'),str) and (choice.get('delta') or {})['content'] for choice in chunk.get('choices',[]))
def has_stream_activity(chunk):
    """Whether an SSE carries progress that should refresh the idle timer."""
    if not isinstance(chunk,dict): return False
    for choice in chunk.get('choices',[]) or []:
        if not isinstance(choice,dict): continue
        delta=choice.get('delta') or {}
        if not isinstance(delta,dict): delta={}
        for field in ('content','reasoning_content','refusal'):
            if isinstance(delta.get(field),str) and delta[field]: return True
        if delta.get('tool_calls') or delta.get('function_call'):
            return True
        if choice.get('finish_reason') is not None:
            return True
    return bool(chunk.get('usage'))
def message_text(message):
    content=message.get('content','') if isinstance(message,dict) else ''
    if isinstance(content,str):return content
    if isinstance(content,list):return ''.join(part.get('text','') for part in content if isinstance(part,dict) and part.get('type') in ('text','input_text'))
    return ''
def trace_input_summary(messages):
    roles={}
    for item in messages:roles[item.get('role','unknown')]=roles.get(item.get('role','unknown'),0)+1
    latest=next((message_text(item) for item in reversed(messages) if item.get('role')=='user' and message_text(item)), '')
    preview=latest.replace('\n',' ').strip()[:32] or '（无文本 user 输入）'
    return preview, ' · '.join(f'{role} {count} 条' for role,count in roles.items())+f' · 共 {len(messages)} 条'
def response_preview(payload):
    try:
        choices=payload.get('choices',[])
        message=choices[0].get('message',{}) if choices else {}
        return message_text(message).replace('\n',' ').strip()[:512]
    except Exception:return ''
def chunk_content(chunk):
    return ''.join((choice.get('delta') or {}).get('content') or '' for choice in chunk.get('choices',[]))
POLICY_FINISH_REASONS={'content_filter','content_policy_blocked','safety_refusal','policy_blocked'}
def policy_block_reason(payload):
    """Return a normalized content-policy signal from a Chat Completions payload.

    Providers generally expose ``finish_reason=content_filter``; Hermes may
    normalize that to ``content_policy_blocked``.  Refusal text alone is not
    treated as a signal because it is indistinguishable from a normal answer.
    """
    if not isinstance(payload,dict): return None
    for choice in payload.get('choices',[]) or []:
        if not isinstance(choice,dict): continue
        reason=str(choice.get('finish_reason') or '').strip().lower()
        if reason in POLICY_FINISH_REASONS: return reason
        message=choice.get('message') if isinstance(choice.get('message'),dict) else {}
        refusal=message.get('refusal')
        if isinstance(refusal,str) and refusal.strip(): return 'refusal'
    return None
def clock(ts):
    return time.strftime('%H:%M:%S',time.localtime(ts)) if ts else '—'
def remaining_clock(seconds):
    seconds=max(0,int(seconds))
    minutes,seconds=divmod(seconds,60); hours,minutes=divmod(minutes,60)
    if hours:return f'{hours}小时{minutes}分{seconds}秒'
    if minutes:return f'{minutes}分{seconds}秒'
    return f'{seconds}秒'
def cooldown_label(cooling):
    """Human-readable cause for a stored cooldown; never expose the generic scheduler word alone."""
    if cooling.get('reason')=='quota_suspect':return '上游疑似配额异常（等待原请求验证）'
    if cooling.get('reason')=='quota_confirmed':return '上游配额异常已确认（等待原请求复验）'
    if cooling.get('reason')=='engine_suspect':return '上游引擎暂不可用（等待原请求验证）'
    if cooling.get('reason')=='engine_unavailable':return '上游引擎暂不可用（等待原请求复验）'
    kind=cooling.get('limit_kind')
    if kind=='rpm':return '上游 429（RPM 限制）'
    if kind=='tpm':return '上游 429（TPM 限制）'
    if kind=='quota_exhausted':return '上游 429（配额耗尽）'
    if cooling.get('reason')=='five_hour_quota':return '五小时调用配额已用完'
    if cooling.get('reason')=='quota_exhausted':return '上游配额耗尽'
    if cooling.get('reason')=='quota_daily':return f'上游每日额度耗尽（按通道 429 策略封禁至 {clock(cooling.get("until"))} 重置）'
    if cooling.get('reason')=='policy_cooldown':return f'上游 429（按通道 429 策略冷却至 {clock(cooling.get("until"))}）'
    if cooling.get('reason')=='unknown_429':return '上游 429（原因未识别）'
    return f'通道冷却（{cooling.get("reason") or "未知原因"}）'
def client_label(headers):
    """Classify only safe identity headers; never persist credentials or cookies."""
    agent=(headers.get('user-agent') or '').strip()
    declared=(headers.get('x-client-name') or headers.get('x-client') or '').strip()
    source=declared or agent
    if 'hermes' in source.lower():return 'Hermes'
    if declared:return declared[:120]
    if agent:return agent[:120]
    return '未识别本机客户端'
_OUTPUT_LIMIT_PATTERNS=[
    re.compile(r'max[_ ]?(?:completion_)?tokens?\s+exceeds\s+(?:the\s+)?limit\s+of\s+(\d+)',re.I),
    re.compile(r'maximum\s+(?:output\s+|completion\s+)?tokens?\s+(?:is|of)\s+(\d+)',re.I),
    re.compile(r'(?:max|maximum)[_ ]?(?:completion_)?tokens?[^0-9]{0,100}?'
               r'(?:should\s+be\s+in|valid\s+range(?:\s+of\s+max_tokens)?\s+is|限制数值范围)'
               r'\s*\[\s*1\s*,\s*(\d+)\s*\]',re.I),
]
def output_limit_from_detail(detail):
    """Extract the upstream's max-output-tokens cap from any known phrasing.

    Covered spellings (all observed in real provider errors):
      * max_tokens exceeds the limit of N            (Agnes)
      * maximum output tokens is/of N
      * field MaxTokens invalid, should be in [1, N] (GLM / SenseNova)
      * valid range of max_tokens is [1, N]           (DeepSeek)
      * max_tokens参数非法：限制数值范围[1,N]          (Zhipu)
    Returns N or None when the message is not an output-limit complaint.
    """
    if not detail:return None
    for pat in _OUTPUT_LIMIT_PATTERNS:
        m=pat.search(detail)
        if m:
            try:
                value=int(m.group(1))
            except (TypeError,ValueError):
                continue
            if value>0:return value
    return None

def error_type(e):
    n=type(e).__name__.lower(); s=getattr(e,'status_code',None)
    detail=(getattr(e,'message',None) or str(e) or '').lower()
    # 鉴权/授权失败（密钥无效、过期、未授权）：401/403 或明确的鉴权关键字。
    # 注意：403 也可能是区域限制（opencode 等），但鉴权类关键字优先归类。
    if s in (401, 403) or any(marker in detail for marker in (
        'authentication', 'authorization', 'unauthorized', 'not authorized',
        'invalid api key', 'invalid key', 'api key is invalid', 'api key expired',
        'apikey', 'permission denied', 'access denied', 'forbidden', 'credential',
        'token has expired', 'invalid token', 'unauthorised')):
        return 'authentication'
    if any(marker in detail for marker in (
        'content_policy_blocked', 'content policy blocked',
        'content_filter', 'content filter',
        'data_inspection_failed', 'inappropriate content',
        'input text data may contain inappropriate content',
    )):
        return 'content_policy_blocked'
    # 仅识别已验证的上游临时引擎报错；兼容上游偶发的 avaiable 拼写错误。
    # 其他 400 仍为 request_error，避免掩盖请求格式问题。
    if re.search(r'engine\s+is\s+not\s+avai(?:l)?able\s+temporarily',detail):return 'engine_unavailable'
    # 通道硬性上限（并非请求本身有问题，而是该通道能力不足）：请求的输出预算
    # 或上下文超出上游模型允许的上限。这类错误按通道级 Fallback 处理——换到
    # 有更大预算/窗口的通道重试，而不是把 400 直接丢给客户端。
    if (output_limit_from_detail(detail) is not None
            or 'output token limit' in detail
            or 'max output tokens' in detail and 'exceed' in detail):
        return 'output_limit_exceeded'
    if (re.search(r'context[_ ](?:length|window)[_ ](?:exceeded|is\s+larger|limit)',detail)
            or re.search(r'maximum\s+context\s+(?:length|window)',detail)
            or 'context_length_exceeded' in detail
            or 'prompt is too long' in detail
            or 'input is too long' in detail
            or 'too many tokens' in detail and 'limit' in detail):
        return 'context_exceeded'
    if s==429 or 'ratelimit' in n:
        # 两类 429 细分（依据真实日志 P.AAAA/P.WANGYUYAN session）：
        #   A类(套餐/总量配额耗尽): 'HTTP 429: Allocated quota exceeded, please increase your quota limit.'
        #   B类(瞬时限流/忙):       'HTTP 429: rpm exhausted' / 'HTTP 429: inference tpm exhausted' / rate limit
        # 注意: 'rpm exhausted'/'tpm exhausted' 是瞬时窗口限流(B类)，不是总量；只有 'allocated quota exceeded' 是 A类
        # A类: 套餐/总配额耗尽（提额才可恢复）
        if any(x in detail for x in ('allocated quota exceeded','quota exceeded','insufficient_quota','free allocated','exceeded your quota','额度','配额')):
            return 'quota_exhausted'
        # B类细分: tpm / rpm 分开(退避策略不同, 统计分开)
        if 'tpm' in detail or 'tokens per minute' in detail or 'token limit' in detail:return 'tpm_limit'
        return 'rpm_limit'  # RPM 及其他瞬时限流
    if s and s>=500:return 'server_error'
    if 'timeout' in n:return 'timeout'
    if 'connection' in n or 'apierror' in n:return 'connection_error'
    return 'request_error'

def rpm_limit_exhausted_action(selection):
    """Resolve the second-stage RPM/TPM action without model-specific rules.

    The optional ``selection.rpm_limit.on_exhausted`` setting supports
    ``failover``, ``wait`` and ``fail``.  Older Runner configs retain their
    behavior: cost-aware fails over after the Channel retry budget, while
    other strategies wait until their configured queue cap.
    """
    strategy=selection.get('strategy') if isinstance(selection,dict) else None
    config=selection.get('rpm_limit',{}) if isinstance(selection,dict) else {}
    action=config.get('on_exhausted') if isinstance(config,dict) else None
    if action in ('failover','wait','fail'):
        return action
    return 'failover' if strategy=='cost_aware' else 'wait'
# --- 429 / quota policy helpers -------------------------------------------
# Reset timestamps appear in several provider-specific spellings, e.g.
#   "It resets at 2026-11-16 16:00 UTC (2026-11-17 00:00 UTC+8)"
#   "将于 2026-11-17 00:00（UTC+8）恢复"
# Each match captures a wall-clock time plus an optional UTC offset nearby.
_CLOCK_AFTER_RE=re.compile(r'(?:try\s+again|retry)\s+after\s+(\d{1,2}):(\d{2})(?::(\d{2}))?',re.IGNORECASE)
_RESET_TS_RE=re.compile(
    r'(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?'
    r'\s*(?:（|\(|\s)?\s*(UTC|GMT)?\s*([+-]\d{1,2}(?::?\d{2})?)?\s*(?:）|\))?',
    re.IGNORECASE)
def next_local_midnight(now:float|None=None)->float:
    """Epoch of the next local 00:00 (daily allowances reset there)."""
    now=now or time.time(); lt=time.localtime(now)
    return time.mktime((lt.tm_year,lt.tm_mon,lt.tm_mday+1,0,0,0,0,0,-1))
def _offset_seconds(spec:str)->int:
    """Parse ``+8``/``-05:30``/``+08:00`` into a signed seconds offset."""
    sign=-1 if spec.startswith('-') else 1
    body=spec[1:].replace(':','')
    hours=int(body[:2]); minutes=int(body[2:]) if len(body)>2 else 0
    return sign*(hours*3600+minutes*60)
def parse_reset_epoch(text:str)->float|None:
    """Return the first reset instant stated in an upstream error, else None.

    A stated offset is honored (``UTC`` = +0, ``+08:00``/``UTC+8`` = +8); a
    time without an offset is interpreted as local wall-clock time.  Only
    future instants are accepted so a stale example in help text cannot pin a
    Channel in the past.

    Offset-less clocks deserve care: upstreams that emit UTC clock times
    (Agnes writes ``Try again after 04:03:24`` while ``time.time_gmtime`` is
    04:00) would be misread as local time, roll forward a whole day, and pin a
    Channel for a multiple of the real backoff — an 8-hour block on a
    three-minute throttle.  When the local reading lands far in the future
    while the same wall clock read as UTC lands just ahead, the UTC reading is
    the one that fits the stated throttle, so prefer it.
    """
    if not text:return None
    now=time.time()
    # Some upstreams only state a clock time: "Try again after 20:43:10".
    # Interpret it as today (or tomorrow when that time already passed).
    clock_match=_CLOCK_AFTER_RE.search(text)
    if clock_match:
        lt=time.localtime(now)
        base=datetime.datetime(lt.tm_year,lt.tm_mon,lt.tm_mday,
                               int(clock_match.group(1)),int(clock_match.group(2)),
                               int(clock_match.group(3) or 0))
        epoch=time.mktime(base.timetuple())
        if epoch<=now+60:epoch=time.mktime((base+datetime.timedelta(days=1)).timetuple())
        utc_epoch=calendar.timegm(base.timetuple())
        if epoch-now>6*3600 and now<utc_epoch<=now+6*3600:epoch=utc_epoch
        return epoch
    for m in _RESET_TS_RE.finditer(text):
        try:
            base=datetime.datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)),
                                   int(m.group(4)),int(m.group(5)),int(m.group(6) or 0))
        except ValueError:
            continue
        named=(m.group(7) or '').strip().upper()
        offset=(m.group(8) or '').strip()
        try:
            if named:
                # "UTC" alone means the wall clock is UTC; "UTC+8" means the
                # wall clock is in a +8 zone, so move the instant back by 8h.
                epoch=calendar.timegm(base.timetuple())-(0 if not offset else _offset_seconds(offset))
            elif offset:
                epoch=calendar.timegm(base.timetuple())-_offset_seconds(offset)
            else:
                epoch=time.mktime(base.timetuple())
        except ValueError:
            continue
        if epoch>now+60:return epoch
    return None
def quota_429_block(ch,policy,detail,typ):
    """Resolve (reason, seconds) to sideline ``ch`` under its 429 policy.

    Returns ``None`` when the policy does not apply to this error or asks for
    the historical retry behavior, so the request-loop branch keeps its
    existing verification cycle.
    """
    if policy is None:return None
    # ``match`` (upstream text) takes precedence over the error-class list: a
    # daily allowance may arrive as rpm_limit but never clears before its reset.
    if policy.match:
        if not any(re.search(pattern,detail or '',re.IGNORECASE) for pattern in policy.match):
            return None
    elif typ not in policy.applies_to:
        return None
    if policy.mode=='retry':return None
    if policy.mode=='cooldown':
        seconds=int(policy.cooldown_seconds or (ch.limits.quota_cooldown_seconds if typ=='quota_exhausted' else 300))
        return 'policy_cooldown',max(1,seconds)
    # until_reset: parse the upstream's stated reset, else the next 00:00.
    reset=parse_reset_epoch(detail) if policy.reset_hint=='error_text' else None
    if reset is None:reset=next_local_midnight()
    return 'quota_daily',max(1,int(reset-time.time()))
def error_code(e):
    """Return the provider's HTTP status code if present (e.g. 429, 500, 408), else None."""
    return getattr(e, 'status_code', None)

def protocol_error_rule_for(channel, exc, rules):
    """Return a configured protocol rule matching one upstream exception.

    Model matching accepts either the full LiteLLM name or its provider-free
    tail, while provider/model patterns may use shell-style ``*`` wildcards.
    """
    if not channel or not rules: return None
    status=error_code(exc)
    detail=error_detail(exc)
    model_names=(str(channel.litellm_model), str(channel.litellm_model).rsplit('/',1)[-1], str(channel.public_model))
    for rule in rules:
        if not isinstance(rule, ProtocolErrorRule):
            continue
        if rule.provider and not fnmatch.fnmatchcase(str(channel.provider),rule.provider):
            continue
        if rule.model and not any(fnmatch.fnmatchcase(model,rule.model) for model in model_names):
            continue
        statuses=rule.http_status or []
        if statuses and status not in statuses:
            continue
        try:
            if re.search(rule.message_regex,detail,re.IGNORECASE|re.DOTALL):
                return rule
        except re.error:
            # Config validation should normally catch this; fail closed if a
            # live editor bypasses validation or an old process reloads YAML.
            logger.warning('invalid protocol error regex rule=%s',rule.id)
    return None
def error_detail(e):
    """Return the real provider error message, with the litellm class prefix stripped and secrets redacted."""
    raw = getattr(e, 'message', None) or str(e) or type(e).__name__
    # litellm prefixes the original message with "<ClassName>: " (sometimes nested). Strip all such prefixes.
    while True:
        m = re.match(r'^(?:litellm\.)?[A-Za-z_]+Error:\s*(.*)$', raw, re.DOTALL)
        if not m or m.group(1) == raw:
            break
        raw = m.group(1)
    # Prepend request id / trace header if available
    resp = getattr(e, 'response', None)
    hdrs = getattr(resp, 'headers', None)
    if isinstance(hdrs, dict):
        for key in ('x-request-id', 'trace-id', 'request-id'):
            if key in hdrs:
                raw = '[%s=%s] %s' % (key, hdrs[key], raw)
                break
    raw = re.sub(r'(?i)((?:api[_-]?key|authorization|token)\s*[=:]\s*)[^\s,]+', r'\1[redacted]', raw)
    return raw[:800]
def usage_tokens(response):
    usage=getattr(response,'usage',None)
    if usage is None and isinstance(response,dict):usage=response.get('usage')
    if hasattr(usage,'model_dump'):usage=usage.model_dump()
    if not isinstance(usage,dict):return None,None
    output=usage.get('completion_tokens') or usage.get('output_tokens')
    total=usage.get('total_tokens')
    return output,total
def channel_request_kwargs(channel):
    kwargs={'allowed_openai_params':channel.supported_params} if channel.supported_params else {}
    # Fixed per-Channel body overrides (e.g. {"thinking":{"type":"disabled"}} for
    # thinking models like GLM-4.7).  Merged so the caller stays unaware of them.
    if getattr(channel, 'extra_body', None):
        kwargs['extra_body']=dict(channel.extra_body)
    return kwargs

def responses_probe_url(base_url: str) -> str:
    """Build the Responses endpoint from a provider base URL.

    Providers in the config normally expose either ``.../v1`` (where the
    endpoint is ``/responses``) or an unversioned API root (DeepSeek's
    official endpoint).  Be tolerant of an accidentally copied endpoint URL
    while never including credentials in the resulting value.
    """
    base = str(base_url or '').strip().rstrip('/')
    for suffix in ('/chat/completions', '/responses', '/models'):
        if base.endswith(suffix):
            base = base[:-len(suffix)]
            break
    return base + '/responses'

def _probe_detail(value: Any) -> str:
    """Return a bounded, credential-redacted probe detail for the UI/config."""
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            text = str(value)
    text = re.sub(r'(?i)((?:api[_-]?key|authorization|token)\s*[=:]\s*)[^\s,}]+', r'\1[redacted]', text)
    return text[:500]

def _probe_detail_has(status_code: int | None, payload: Any, detail: str, *markers: str) -> bool:
    """Case-insensitively test whether a probe failure matches any transient marker."""
    if status_code is not None and str(status_code) in markers:
        return True
    try:
        payload_txt = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:
        payload_txt = str(payload)
    hay = f'{detail} {payload_txt}'.lower()
    return any(m.lower() in hay for m in markers)

def classify_responses_probe(http_status: int | None, payload: Any, detail: str = '') -> str:
    """Classify an actual Responses probe without treating auth/upstream faults as unsupported."""
    if http_status is not None and 200 <= http_status < 300:
        if isinstance(payload, dict) and (payload.get('object') == 'response' or isinstance(payload.get('output'), list)):
            return 'supported'
        # A successful Chat Completions-shaped payload at /responses is not
        # the Responses protocol, even though HTTP itself succeeded.
        return 'unsupported'
    haystack = f'{detail} {payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)}'.lower()
    if http_status in (404, 405, 501) or any(marker in haystack for marker in (
        'not found', 'unknown route', 'unsupported', 'not supported',
        'does not exist', 'method not allowed', 'no such endpoint')):
        return 'unsupported'
    return 'error'

def _capability_endpoint(base_url: str) -> str:
    """Build the OpenAI-compatible Chat Completions endpoint for probes."""
    base = str(base_url or '').strip().rstrip('/')
    for suffix in ('/chat/completions', '/responses', '/models'):
        if base.endswith(suffix):
            base = base[:-len(suffix)]
            break
    return base + '/chat/completions'

def fetch_provider_model_metadata(channel, providers: dict) -> dict[str, Any]:
    """Fetch only the selected model's public limits from Provider /models."""
    base, key = channel_credentials(channel, providers)
    url = str(base).rstrip('/')
    if not url.endswith('/models'):
        url += '/models'
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {key}', 'Accept': 'application/json',
        'User-Agent': 'flex-router-capability-probe/1.0',
    })
    with urllib.request.urlopen(req, timeout=12) as response:
        payload = json.loads(response.read(262144).decode('utf-8', 'replace'))
    raw = payload.get('data', payload) if isinstance(payload, dict) else payload
    target = channel.litellm_model.rsplit('/', 1)[-1]
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and str(item.get('id', '')) == target:
                return {key: item.get(key) for key in (
                    'context_length', 'context_window', 'max_input_tokens',
                    'max_output_length', 'max_output_tokens', 'max_tokens',
                    'display_name',
                ) if item.get(key) is not None}
    return {}

_OUTPUT_LIMIT_LADDER=(65536,131072,262144,393216,524288,786432,1048576)
def _probe_output_limit_sync(base_url:str,api_key:str,model:str,context_window:int)->tuple[int|None,str]:
    """Find a Channel's max_tokens cap with a tiny, bounded, one-shot probe.

    Sends at most one request per ladder rung with ``max_tokens`` increasing
    until the upstream rejects the value; the rejection's ``[1, N]`` range (or
    "limit of N") yields the exact cap.  Any auth / rate-limit / not-found /
    unrelated-400 response aborts without inventing a value, so a transient
    failure can never write a bogus limit.  The caller persists a successful
    result once, so the probe is never repeated for the same Channel.
    """
    endpoint=_capability_endpoint(base_url)
    accepted=None
    for value in _OUTPUT_LIMIT_LADDER:
        if context_window and value>context_window: break
        body=json.dumps({'model':model,'messages':[{'role':'user','content':'hi'}],'max_tokens':value},ensure_ascii=False).encode('utf-8')
        request=urllib.request.Request(endpoint,data=body,method='POST',
            headers={'Authorization':'Bearer '+api_key,'Content-Type':'application/json',
                     'Accept':'application/json','User-Agent':'flex-router-limit-probe/1.0'})
        try:
            with urllib.request.urlopen(request,timeout=30) as response:
                response.read(4096)
            accepted=value
        except urllib.error.HTTPError as exc:
            raw=exc.read(2048).decode('utf-8','replace')
            cap=output_limit_from_detail(raw)
            if cap:return cap,raw[:200]
            # 400/422 without a parseable range is an unrelated request problem:
            # stop rather than guess a limit from it.
            return None,f'HTTP {exc.code}: {raw[:160]}'
        except Exception as exc:
            return None,str(exc)[:160]
        time.sleep(0.3)
    if accepted:return accepted,f'probe accepted up to {accepted}'
    return None,'no value accepted'

def _capability_probe_sync(base_url: str, api_key: str, model: str, kind: str) -> tuple[str, int | None, str]:
    """Run one bounded, tiny protocol probe without LiteLLM parameter dropping.

    LiteLLM's global ``drop_params`` setting is useful for production traffic,
    but would hide whether an upstream really accepts a parameter.  Probes go
    directly to the OpenAI-compatible endpoint and only retain a safe bounded
    summary of the response.
    """
    body: dict[str, Any] = {
        'model': model,
        'messages': [{'role': 'user', 'content': 'Reply with exactly OK.'}],
        'max_tokens': 16,
    }
    if kind == 'streaming':
        body['stream'] = True
    elif kind == 'tools':
        body.update({
            # Do NOT force tool_choice='required' here: thinking models (e.g.
            # DeepSeek V4) reject 'required' with a 400 ("Thinking mode does
            # not support this tool_choice").  Letting the model decide still
            # yields tool_calls for a tools-capable model when the prompt asks
            # for it.  max_tokens must be large enough for the reasoning phase
            # to finish and reach the tool-call output.
            'messages': [{'role': 'user', 'content': 'Use the flex_probe_noop tool to confirm tool calling works, then reply OK.'}],
            'tools': [{'type': 'function', 'function': {
                'name': 'flex_probe_noop', 'description': 'No-op capability probe',
                'parameters': {'type': 'object', 'properties': {}},
            }}],
            'max_tokens': 256,
        })
    elif kind == 'json':
        body.update({
            'messages': [{'role': 'system', 'content': 'Return valid json.'},
                         {'role': 'user', 'content': 'Return a json object with ok true.'}],
            'response_format': {'type': 'json_object'},
        })
    elif kind == 'reasoning':
        body['reasoning_effort'] = 'low'
    elif kind == 'vision':
        # Probe with a real 1x1 transparent PNG.  A model that accepts image
        # input answers 2xx; one that does not returns 4xx with an explicit
        # message (e.g. "image not supported" / "invalid content type") that
        # we classify as unsupported.  429 stays 'error' (rate-limited), so a
        # transient throttle never demotes a vision-capable model.
        body['max_tokens'] = 64
        body['messages'] = [{'role': 'user', 'content': [
            {'type': 'text', 'text': 'Reply with exactly OK.'},
            {'type': 'image_url', 'image_url': {'url': (
                'data:image/png;base64,'
                'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ'
                'AAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==')}},
        ]}]
    request = urllib.request.Request(
        _capability_endpoint(base_url),
        data=json.dumps(body, ensure_ascii=False).encode('utf-8'), method='POST',
        headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json',
                 'Accept': 'text/event-stream, application/json',
                 'User-Agent': 'flex-router-capability-probe/1.0'},
    )
    status_code: int | None = None
    raw = ''
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            status_code = response.status
            # Streaming probes only need the first SSE event.  Non-streaming
            # responses are bounded to avoid retaining an unexpectedly large
            # provider error or completion.
            if kind == 'streaming':
                chunks = []
                for _ in range(8):
                    line = response.readline(8192)
                    if not line:
                        break
                    chunks.append(line)
                    if b'data:' in line or b'[DONE]' in line.upper():
                        break
                raw = b''.join(chunks).decode('utf-8', 'replace')
            else:
                raw = response.read(65536).decode('utf-8', 'replace')
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        raw = exc.read(4096).decode('utf-8', 'replace')
    except Exception as exc:
        return 'error', None, _probe_detail(exc)
    try:
        payload = json.loads(raw)
    except Exception:
        payload = raw
    detail = _probe_detail(payload)
    if status_code is None or not 200 <= status_code < 300:
        haystack = detail.lower()
        if status_code in (404, 405, 501) or any(x in haystack for x in ('not supported', 'unsupported', 'not found', 'method not allowed')):
            return 'unsupported', status_code, detail
        # Vision probe: an image-capability refusal usually surfaces as a 4xx
        # naming images (not a generic routing error).  Do not treat 429/5xx or
        # a bare 400 as "no vision" — it may be throttling or a bad payload.
        if kind == 'vision' and status_code in (400, 415, 422):
            if any(x in haystack for x in ('image', 'vision', 'multimodal', 'picture', 'attachment', 'image_url', 'not a vision')):
                return 'unsupported', status_code, detail
        return 'error', status_code, detail
    if kind == 'streaming':
        return ('supported' if 'data:' in raw or '[done]' in raw.lower() else 'error'), status_code, detail
    choice = (payload.get('choices') or [{}])[0] if isinstance(payload, dict) else {}
    message = choice.get('message') if isinstance(choice, dict) else {}
    if not isinstance(message, dict):
        message = {}
    if kind == 'tools':
        return ('supported' if message.get('tool_calls') else 'unknown'), status_code, detail
    if kind == 'reasoning':
        usage = payload.get('usage') if isinstance(payload, dict) else {}
        usage = usage if isinstance(usage, dict) else {}
        supported = bool(message.get('reasoning_content')) or bool((usage.get('completion_tokens_details') or {}).get('reasoning_tokens'))
        return ('supported' if supported else 'unknown'), status_code, detail
    if kind == 'json':
        content = message.get('content')
        try:
            json.loads(content or '')
            return 'supported', status_code, detail
        except Exception:
            return 'unknown', status_code, detail
    return ('supported' if isinstance(payload, dict) and isinstance(payload.get('choices'), list) else 'error'), status_code, detail

def _responses_probe_sync(base_url: str, api_key: str, model: str) -> tuple[str, int | None, str, str]:
    endpoint = responses_probe_url(base_url)
    request_body = json.dumps({'model': model, 'input': 'Reply with exactly OK.',
                               'max_output_tokens': 32}, ensure_ascii=False).encode('utf-8')
    request = urllib.request.Request(endpoint, data=request_body, method='POST', headers={
        'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json',
        'Accept': 'application/json', 'User-Agent': 'flex-router-capability-probe/1.0',
    })
    status_code = None; payload: Any = None; detail = ''
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            status_code = response.status
            raw = response.read(65536).decode('utf-8', 'replace')
            try: payload = json.loads(raw)
            except Exception: payload = raw
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        raw = exc.read(4096).decode('utf-8', 'replace')
        try: payload = json.loads(raw)
        except Exception: payload = raw
    except Exception as exc:
        detail = _probe_detail(exc)
    if not detail:
        detail = _probe_detail(payload)
    return classify_responses_probe(status_code, payload, detail), status_code, detail, endpoint

async def probe_channel_capabilities(channel, providers: dict, kinds=None) -> dict[str, dict]:
    """Probe one Channel and return safe observations keyed by capability.

    ``kinds`` limits the probe to specific capabilities (used by the idle
    retry loop to re-probe only the ones still unconfirmed, saving RPM).
    """
    all_kinds = ('chat', 'streaming', 'tools', 'json', 'reasoning', 'vision', 'responses')
    kinds = [k for k in all_kinds if (kinds is None or k in kinds)]
    checked_at = int(time.time())
    try:
        base, key = channel_credentials(channel, providers)
    except Exception as exc:
        detail = _probe_detail(exc)
        return {kind: {'status': 'error', 'checked_at': checked_at, 'http_status': None,
                       'latency_ms': 0, 'detail': detail}
                for kind in kinds}
    model = channel.litellm_model.rsplit('/', 1)[-1]
    observations: dict[str, dict] = {}
    for kind in [k for k in kinds if k != 'responses']:
        started = time.monotonic()
        status, http_status, detail = await asyncio.to_thread(_capability_probe_sync, base, key, model, kind)
        # A single 429 often reflects a momentary window, not missing support:
        # retry once with a short backoff so a rate-limited probe does not
        # permanently leave the capability unconfirmed.
        if status == 'error' and http_status == 429:
            await asyncio.sleep(3)
            status2, http_status2, detail2 = await asyncio.to_thread(_capability_probe_sync, base, key, model, kind)
            observations[kind] = {'status': status2, 'checked_at': checked_at,
                                  'http_status': http_status2,
                                  'latency_ms': int((time.monotonic() - started) * 1000),
                                  'detail': detail2, 'retried_429': True}
            continue
        observations[kind] = {'status': status, 'checked_at': checked_at,
                              'http_status': http_status,
                              'latency_ms': int((time.monotonic() - started) * 1000),
                              'detail': detail}
    # Responses is a separate API protocol rather than a model feature, but
    # it is included in the same post-creation probe so a Channel is complete
    # immediately after it is added.
    if 'responses' in kinds:
        started = time.monotonic()
        status, http_status, detail, endpoint = await asyncio.to_thread(
            _responses_probe_sync, base, key, model)
        observations['responses'] = {'status': status, 'checked_at': checked_at,
                                     'http_status': http_status, 'endpoint': endpoint,
                                     'latency_ms': int((time.monotonic() - started) * 1000),
                                     'detail': detail}
    return observations
# B类瞬时限流：每次真实 429 后按 2^n 退避，直到本请求的类型上限。
# TPM 是 60 秒 token 窗口，实测恢复 p50≈58s：首轮 4s 的重试几乎必然再次 429
# （历史数据里 77% 的退避等待是无效的）。首轮改为 30s，再用 ±20% 抖动避免并发
# 请求同步重试形成二次冲击。
TPM_BACKOFF_BASE=int(os.getenv('FLEX_TPM_BACKOFF_BASE','30'))
RPM_BACKOFF_BASE=int(os.getenv('FLEX_RPM_BACKOFF_BASE','8'))
# 单请求的累计退避上限（秒），setup.conf 可覆盖。
QUEUE_TPM_SECONDS=int(os.getenv('FLEX_QUEUE_TPM','60'))
QUEUE_RPM_SECONDS=int(os.getenv('FLEX_QUEUE_RPM','300'))
# 运行时从上游报错学到的通道输出上限（"max_tokens exceeds the limit of N"）。
# configuration 的 max_output_tokens 优先；这里存放的是未被配置时按经验补齐的
# 结论，供 compatibility() 预先拒绝超过该通道能力的请求。
learned_max_output_tokens: dict[str,int] = {}
# 仅在流式请求尚未收到任何可转发 SSE 事件时使用；Hermes 保留 30 分钟总超时。
# Runner Hedge defaults are derived from the number of configured Channels.
# Three Channels: 6m -> second Channel, 9m -> third Channel, 12m hard stop.
# Two Channels: 6m -> second Channel, 9m hard stop. One Channel retries the
# same Channel at 6m and hard-stops at 9m.
HEDGE_DELAYS=(int(os.getenv('FLEX_HEDGE_FIRST_SECONDS','360')),int(os.getenv('FLEX_HEDGE_SECOND_SECONDS','720')))
UPSTREAM_FIRST_ACTIVITY_TIMEOUT=int(os.getenv('FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT','900'))
# Per-attempt safety bounds.  These are intentionally shorter than the
# request-level first-activity deadline: a stuck provider attempt should not
# block the next configured Hedge stage.  The inner LiteLLM task is detached
# on expiry because some HTTP clients do not promptly acknowledge cancellation.
UPSTREAM_RESPONSE_TIMEOUT=int(os.getenv('FLEX_UPSTREAM_RESPONSE_TIMEOUT','180'))
UPSTREAM_FIRST_CHUNK_TIMEOUT=int(os.getenv('FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT','180'))
# Session affinity has two independent idle windows.  The ordinary window
# preserves a healthy conversation; the protocol window preserves a
# conversation after it had to leave a Channel because of a compatibility
# error such as reasoning_content/tool-call formatting.
SESSION_AFFINITY_IDLE_SECONDS=int(os.getenv('FLEX_SESSION_AFFINITY_IDLE_SECONDS','3600'))
PROTOCOL_AFFINITY_IDLE_SECONDS=int(os.getenv('FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS','3600'))

async def await_bounded(awaitable, timeout_seconds):
    """Await one provider operation without waiting for cancellation cleanup."""
    task=asyncio.ensure_future(awaitable)
    done,_=await asyncio.wait({task},timeout=max(0,float(timeout_seconds)),return_when=asyncio.FIRST_COMPLETED)
    if task not in done:
        cancel_detached({task})
        raise asyncio.TimeoutError()
    return task.result()

async def await_stream_next(iterator, timeout_seconds):
    """Read one SSE item without letting a cancellation-resistant iterator
    defeat the stream idle Hedge/deadline timers.

    ``asyncio.wait_for(iterator.__anext__())`` cancels the read coroutine and
    then waits for cancellation cleanup.  Some HTTP/SSE clients never finish
    that cleanup while the socket is stalled, leaving the request generator
    suspended indefinitely.  Race a detached task instead; on timeout the
    caller immediately regains control and can launch the next Hedge stage or
    emit the hard-stop response.  ``cancel_detached`` drains a late result so
    the provider exception is not reported as an unhandled task warning.
    """
    task=asyncio.ensure_future(iterator.__anext__())
    done,_=await asyncio.wait({task},timeout=max(0,float(timeout_seconds)),return_when=asyncio.FIRST_COMPLETED)
    if task not in done:
        cancel_detached({task})
        raise asyncio.TimeoutError()
    return task.result()

class BufferedUpstreamStream:
    """Decouple the provider's SSE reader from ASGI's downstream sender.

    LiteLLM can hand the Router a response object before Starlette begins
    consuming ``StreamingResponse``.  Reading into this bounded FIFO at once
    preserves every raw provider event during that handoff.  No event type is
    interpreted or discarded here; the normal stream dispatcher receives the
    original objects in order.
    """
    def __init__(self, response, max_items=64):
        self.response=response
        self.iterator=response.__aiter__()
        self.queue=asyncio.Queue(maxsize=max_items)
        self.closed=False
        self.task=asyncio.create_task(self._pump())

    async def _pump(self):
        try:
            async for item in self.iterator:
                await self.queue.put(('item',item))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await self.queue.put(('error',exc))
        else:
            await self.queue.put(('done',None))

    def __aiter__(self):
        return self

    async def __anext__(self):
        kind,value=await self.queue.get()
        if kind=='item':
            return value
        if kind=='done':
            raise StopAsyncIteration
        raise value

    async def aclose(self):
        if self.closed:
            return
        self.closed=True
        cancel_detached({self.task})
        close=getattr(self.response,'aclose',None) or getattr(self.response,'close',None)
        if callable(close):
            try:
                result=close()
                if hasattr(result,'__await__'):
                    cancel_detached({asyncio.create_task(result)})
            except Exception:
                pass
LARGE_CONTEXT_THRESHOLD_TOKENS=int(os.getenv('FLEX_LARGE_CONTEXT_THRESHOLD_TOKENS','100000'))
LARGE_CONTEXT_HEDGE_DELAYS=(int(os.getenv('FLEX_LARGE_CONTEXT_HEDGE_FIRST_SECONDS','180')),
                            int(os.getenv('FLEX_LARGE_CONTEXT_HEDGE_SECOND_SECONDS','360')))
LARGE_CONTEXT_DEADLINES={'one_channel':int(os.getenv('FLEX_LARGE_CONTEXT_DEADLINE_ONE_SECONDS','360')),
                         'two_channels':int(os.getenv('FLEX_LARGE_CONTEXT_DEADLINE_TWO_SECONDS','360')),
                         'three_or_more_channels':int(os.getenv('FLEX_LARGE_CONTEXT_DEADLINE_THREE_SECONDS','540'))}

def _large_context_policy(pool):
    """Return per-Runner large-context Hedge overrides (or global defaults)."""
    cfg=(pool.selection.get('large_context_hedge',{}) if pool is not None and isinstance(pool.selection,dict) else {})
    if not isinstance(cfg,dict): cfg={}
    enabled=bool(cfg.get('enabled',True))
    try: threshold=max(1,int(cfg.get('threshold_tokens',LARGE_CONTEXT_THRESHOLD_TOKENS)))
    except (TypeError,ValueError): threshold=LARGE_CONTEXT_THRESHOLD_TOKENS
    try: first=max(1,int(cfg.get('first_seconds',LARGE_CONTEXT_HEDGE_DELAYS[0])))
    except (TypeError,ValueError): first=LARGE_CONTEXT_HEDGE_DELAYS[0]
    try: second=max(first+1,int(cfg.get('second_seconds',LARGE_CONTEXT_HEDGE_DELAYS[1])))
    except (TypeError,ValueError): second=LARGE_CONTEXT_HEDGE_DELAYS[1]
    deadlines=dict(LARGE_CONTEXT_DEADLINES)
    for key in deadlines:
        try:
            if key in cfg: deadlines[key]=max(1,int(cfg[key]))
        except (TypeError,ValueError): pass
    return enabled,threshold,(first,second),deadlines

def hedge_plan_for(pool_name,channels,selected,pool=None,input_tokens_count=None):
    """Return configured (due_seconds, channel ids) first-activity Hedge steps.
    Channel IDs are always read from the selected Pool; no model/provider names
    are special-cased here. Without explicit stages, use same-channel defaults.
    """
    large_enabled,large_threshold,large_delays,_= _large_context_policy(pool)
    is_large=large_enabled and input_tokens_count is not None and input_tokens_count >= large_threshold
    configured=(pool.selection.get('hedge',{}) if pool is not None and isinstance(pool.selection,dict) else {})
    stages=configured.get('stages',[]) if isinstance(configured,dict) else []
    # A Hedge target must be able to host the request.  If we know the prompt
    # size, drop Channels whose context window can't fit it — otherwise a
    # large request fails over to a sibling that only rejects it (e.g.
    # sensenova-flash-lite 拒绝超过其 262144 窗口的 386k 请求).
    by_id={channel.id:channel for channel in channels}
    def fits(channel):
        if input_tokens_count is None:return True
        ctx=getattr(channel,'context_window_tokens',None)
        return ctx is None or ctx>=input_tokens_count
    if stages and not is_large:
        known={channel.id for channel in channels}
        plan=[]
        for stage in stages:
            if not isinstance(stage,dict):continue
            try:due=max(0,int(stage.get('after_seconds',0)))
            except (TypeError,ValueError):continue
            targets=tuple(str(target) for target in (stage.get('channels') or ())
                          if str(target) in known and fits(by_id[str(target)]))
            if due and targets:plan.append((due,targets))
        if plan:return tuple(plan)
    # Automatic policy is intentionally based only on Runner membership and
    # order, never on provider/model names. The initially selected Channel is
    # the request's model0; subsequent configured Channels are model1/model2.
    ordered=[selected]+[channel for channel in channels if channel.id!=selected.id and fits(channel)]
    if len(ordered)>=3:
        delays=large_delays if is_large else (360,540)
        return ((delays[0],(ordered[1].id,)),(delays[1],(ordered[2].id,)))
    if len(ordered)==2:
        return ((large_delays[0] if is_large else 360,(ordered[1].id,)),)
    # A single Channel still gets one same-Channel retry. This is not a
    # provider failover: it is a second request for the same model at T+6m,
    # followed by the common T+9m hard stop.
    if len(ordered)==1:
        return ((large_delays[0] if is_large else 360,(ordered[0].id,)),)
    return ()

def first_activity_deadline_for(channels,input_tokens_count=None,pool=None):
    """Return the request hard-stop implied by Runner Channel count."""
    large_enabled,threshold,_,deadlines=_large_context_policy(pool)
    if large_enabled and input_tokens_count is not None and input_tokens_count >= threshold:
        key='three_or_more_channels' if len(channels)>=3 else ('two_channels' if len(channels)==2 else 'one_channel')
        return min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,deadlines[key])
    if len(channels)>=3:
        return min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,720)
    if len(channels)==2:
        return min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,540)
    # Single-Channel requests use the same 6m retry / 9m hard-stop window;
    # the global setting may only shorten this safety deadline.
    return min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,540)
def requirements(body,stream):
    required={'chat'}
    if stream:required.add('streaming')
    if body.get('tools') or body.get('tool_choice'):required.add('tools')
    response_format=body.get('response_format')
    if isinstance(response_format,dict) and response_format.get('type') in ('json_object','json_schema'):required.add('json')
    return required
def input_tokens(body,model):
    try:return litellm.token_counter(model=model,messages=body.get('messages',[]),tools=body.get('tools'))
    except Exception:return max(1,len(json.dumps({'messages':body.get('messages',[]),'tools':body.get('tools')},ensure_ascii=False))//4)
def tpm_pacing_ok(state,pool,ch,prompt_tokens):
    """Prefer Channels that will not immediately collide with their TPM.

    Returns False only when a *credible* learned safe TPM exists and the
    60-second token window plus this request's prompt would exceed it.  With no
    credible measurement the Channel is always allowed, so pacing can never
    fail a request on its own.
    """
    learned=state.learned_limit(pool,ch.id)
    if not learned:return True
    safe_tpm=learned.get('safe_tpm'); confidence=learned.get('confidence') or 0
    if not safe_tpm or safe_tpm<1024 or confidence<5:return True
    recent=state.window_metrics(pool,ch.id,60)['tokens'] or 0
    return recent+prompt_tokens<=safe_tpm*0.9
def requested_output_tokens(body,reserve):
    """The client's requested output ceiling (max_tokens/max_completion_tokens)."""
    value=body.get('max_completion_tokens',body.get('max_tokens',reserve))
    return value if isinstance(value,int) and value>=0 else reserve
def channel_output_cap(ch):
    """Configured cap first, then the persisted runtime-learned cap."""
    return ch.max_output_tokens or learned_max_output_tokens.get(ch.id)
def compatibility(reserve,ch,body,stream,prompt_tokens=None):
    needed=requirements(body,stream); missing=needed-set(ch.capabilities)
    if missing:return False,'missing_capabilities:'+','.join(sorted(missing)),None
    prompt=prompt_tokens if prompt_tokens is not None else input_tokens(body,ch.litellm_model)
    requested=requested_output_tokens(body,reserve)
    # 输出上限不再作为“不兼容”条件：客户端给的是上限而不是硬需求，超过通道
    # 能力时应夹取发送（见 call_upstream），只有上下文窗口装不下才判不兼容。
    # 这里用夹取后的有效输出做窗口校验，避免把本可服务的请求误判为超窗。
    cap=channel_output_cap(ch)
    effective=min(requested,cap) if cap else requested
    total=prompt+effective
    if total>ch.context_window_tokens:return False,f'context_exceeded:{total}>{ch.context_window_tokens}',total
    return True,None,total
def create_app(config_path:str|Path):
    config_path_resolved=Path(config_path).expanduser().resolve(); override_path=config_path_resolved.parent/'setup.conf'
    def read_setup():
        """读 setup.conf 为 dict(支持多行 KEY=VALUE)."""
        conf={}
        if override_path.exists():
            for line in override_path.read_text(encoding='utf-8').splitlines():
                line=line.strip()
                if not line or line.startswith('#') or '=' not in line: continue
                k,v=line.split('=',1); conf[k.strip()]=v.strip()
        return conf
    def write_setup(conf):
        with open(override_path,'w',encoding='utf-8') as f:
            for k,v in conf.items(): f.write(f'{k}={v}\n')
    override_on=True
    if override_path.exists():
        try: override_on=bool(int(read_setup().get('FLEX_OVERRIDE','1')))
        except: override_on=True
    config=load_config(config_path,override=override_on); os.environ['FLEX_OVERRIDE']='1' if override_on else '0'; templates_dir=config_path_resolved.parent.parent/'templates'; litellm.drop_params=True; scheduler=RoundRobinScheduler(); state=StateStore(os.getenv('FLEX_STATE_DB','data/flex.db')); learned_max_output_tokens.update(state.output_caps()); instance_id=uuid.uuid4().hex; recovered_traces=state.cancel_interrupted_traces(); state.backfill_error_statistics(); app=FastAPI(title='Flex LLM Router',version='0.2.1'); app.state.probe_tasks=set(); app.state.capability_probe_tasks=set(); app.state.capability_probe_channels=set(); app.state.config_write_lock=asyncio.Lock(); app.state.first_activity_watch={}; app.state.active_restart_signals={}; app.state.restart_draining=False; app.state.watchdog_tasks=set(); app.state.instance_id=instance_id; app.state.started_at=time.time(); app.state.recovered_traces=recovered_traces; app.state.build=ROUTER_BUILD; app.state.last_external_call=time.time()
    # DEBUG 日志开关: setup.conf 的 FLEX_DEBUG 优先, 缺省看环境变量, 默认关
    state.debug_enabled = read_setup().get('FLEX_DEBUG', os.getenv('FLEX_DEBUG','0'))=='1'
    capture_setup=read_setup()
    state.configure_full_request_capture(
        enabled=capture_setup.get('FLEX_FULL_REQUEST_CAPTURE','0')=='1',
        hours=capture_setup.get('FLEX_FULL_REQUEST_CAPTURE_HOURS','3'),
        max_rows=capture_setup.get('FLEX_FULL_REQUEST_CAPTURE_MAX_ROWS','300'),
        max_bytes=int(capture_setup.get('FLEX_FULL_REQUEST_CAPTURE_MAX_MIB','256'))*1024*1024)
    # 重复在途请求观察：默认关；开启后仅记录完全相同请求的重叠，不影响任何路由或上游调用。
    state.duplicate_observer_enabled = read_setup().get('FLEX_DUPLICATE_OBSERVER','0')=='1'
    # 排队上限: setup.conf 的 FLEX_QUEUE_TPM / FLEX_QUEUE_RPM 覆盖默认(60/300)
    global QUEUE_TPM_SECONDS, QUEUE_RPM_SECONDS, TPM_BACKOFF_BASE, RPM_BACKOFF_BASE, UPSTREAM_RESPONSE_TIMEOUT, UPSTREAM_FIRST_CHUNK_TIMEOUT, SESSION_AFFINITY_IDLE_SECONDS, PROTOCOL_AFFINITY_IDLE_SECONDS
    try: QUEUE_TPM_SECONDS=int(read_setup().get('FLEX_QUEUE_TPM', os.getenv('FLEX_QUEUE_TPM','60')))
    except ValueError: pass
    try: QUEUE_RPM_SECONDS=int(read_setup().get('FLEX_QUEUE_RPM', os.getenv('FLEX_QUEUE_RPM','300')))
    except ValueError: pass
    try: TPM_BACKOFF_BASE=max(1,int(read_setup().get('FLEX_TPM_BACKOFF_BASE', os.getenv('FLEX_TPM_BACKOFF_BASE','30'))))
    except ValueError: pass
    try: RPM_BACKOFF_BASE=max(1,int(read_setup().get('FLEX_RPM_BACKOFF_BASE', os.getenv('FLEX_RPM_BACKOFF_BASE','8'))))
    except ValueError: pass
    try: UPSTREAM_RESPONSE_TIMEOUT=max(1,int(read_setup().get('FLEX_UPSTREAM_RESPONSE_TIMEOUT', os.getenv('FLEX_UPSTREAM_RESPONSE_TIMEOUT','180'))))
    except ValueError: pass
    try: UPSTREAM_FIRST_CHUNK_TIMEOUT=max(1,int(read_setup().get('FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT', os.getenv('FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT','180'))))
    except ValueError: pass
    try: SESSION_AFFINITY_IDLE_SECONDS=max(60,int(read_setup().get('FLEX_SESSION_AFFINITY_IDLE_SECONDS', os.getenv('FLEX_SESSION_AFFINITY_IDLE_SECONDS','3600'))))
    except ValueError: pass
    try: PROTOCOL_AFFINITY_IDLE_SECONDS=max(60,int(read_setup().get('FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS', os.getenv('FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS','3600'))))
    except ValueError: pass
    def effective_affinity(pool):
        """Apply Setup's global idle windows without mutating Runner config."""
        cfg=dict(pool.session_affinity or {})
        cfg['idle_seconds']=SESSION_AFFINITY_IDLE_SECONDS
        cfg['protocol_idle_seconds']=PROTOCOL_AFFINITY_IDLE_SECONDS
        return cfg
    # ------------------------------------------------------------------
    # SETUP 配置中心：统一读取“当前值”与“写入 + 热生效”。
    # 当前值优先级：setup.conf → 系统环境变量 → 代码默认值。写入时总是
    # 持久化到 setup.conf 并同时写 os.environ；支持热生效的项再原地更新
    # 内存变量/StateStore，无需重启即可立刻生效。
    # ------------------------------------------------------------------
    _RUNTIME={}
    for _it in settings_registry.ITEMS:
        _opt=read_setup().get(_it['key'])
        if _opt is not None: _val=_opt
        else:
            _env=os.getenv(_it['key'])
            _val=_env if _env is not None else _it['default']
        _RUNTIME[_it['key']]=_val
    def _apply_hot(key,val):
        global QUEUE_TPM_SECONDS,QUEUE_RPM_SECONDS,SESSION_AFFINITY_IDLE_SECONDS,PROTOCOL_AFFINITY_IDLE_SECONDS
        global UPSTREAM_RESPONSE_TIMEOUT,UPSTREAM_FIRST_CHUNK_TIMEOUT,UPSTREAM_FIRST_ACTIVITY_TIMEOUT
        global HEDGE_DELAYS,LARGE_CONTEXT_THRESHOLD_TOKENS,LARGE_CONTEXT_HEDGE_DELAYS,LARGE_CONTEXT_DEADLINES
        it=settings_registry.item_for(key)
        if not (it and it.get('hot',False)): return
        i=int(val); b=(val=='1')
        if key=='FLEX_DEBUG': state.debug_enabled=b
        elif key=='FLEX_DEBUG_MAX_ROWS': state.debug_max_rows=i
        elif key=='FLEX_DEBUG_RETENTION_DAYS': state.debug_retention_days=i
        elif key=='FLEX_DUPLICATE_OBSERVER': state.duplicate_observer_enabled=b
        elif key=='FLEX_FULL_REQUEST_CAPTURE' or key.startswith('FLEX_FULL_REQUEST_CAPTURE_'):
            state.configure_full_request_capture(
                enabled=_RUNTIME['FLEX_FULL_REQUEST_CAPTURE']=='1',
                hours=int(_RUNTIME.get('FLEX_FULL_REQUEST_CAPTURE_HOURS') or 3),
                max_rows=int(_RUNTIME.get('FLEX_FULL_REQUEST_CAPTURE_MAX_ROWS') or 300),
                max_bytes=int(_RUNTIME.get('FLEX_FULL_REQUEST_CAPTURE_MAX_MIB') or 256)*1024*1024)
        elif key=='FLEX_QUEUE_TPM': QUEUE_TPM_SECONDS=i
        elif key=='FLEX_QUEUE_RPM': QUEUE_RPM_SECONDS=i
        elif key=='FLEX_SESSION_AFFINITY_IDLE_SECONDS': SESSION_AFFINITY_IDLE_SECONDS=i
        elif key=='FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS': PROTOCOL_AFFINITY_IDLE_SECONDS=i
        elif key=='FLEX_UPSTREAM_RESPONSE_TIMEOUT': UPSTREAM_RESPONSE_TIMEOUT=i
        elif key=='FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT': UPSTREAM_FIRST_CHUNK_TIMEOUT=i
        elif key=='FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT': UPSTREAM_FIRST_ACTIVITY_TIMEOUT=i
        elif key=='FLEX_HEDGE_FIRST_SECONDS': HEDGE_DELAYS=(i,HEDGE_DELAYS[1])
        elif key=='FLEX_HEDGE_SECOND_SECONDS': HEDGE_DELAYS=(HEDGE_DELAYS[0],i)
        elif key=='FLEX_LARGE_CONTEXT_THRESHOLD_TOKENS': LARGE_CONTEXT_THRESHOLD_TOKENS=i
        elif key=='FLEX_LARGE_CONTEXT_HEDGE_FIRST_SECONDS': LARGE_CONTEXT_HEDGE_DELAYS=(i,LARGE_CONTEXT_HEDGE_DELAYS[1])
        elif key=='FLEX_LARGE_CONTEXT_HEDGE_SECOND_SECONDS': LARGE_CONTEXT_HEDGE_DELAYS=(LARGE_CONTEXT_HEDGE_DELAYS[0],i)
        elif key=='FLEX_LARGE_CONTEXT_DEADLINE_ONE_SECONDS': LARGE_CONTEXT_DEADLINES['one_channel']=i
        elif key=='FLEX_LARGE_CONTEXT_DEADLINE_TWO_SECONDS': LARGE_CONTEXT_DEADLINES['two_channels']=i
        elif key=='FLEX_LARGE_CONTEXT_DEADLINE_THREE_SECONDS': LARGE_CONTEXT_DEADLINES['three_or_more_channels']=i
        elif key in ('FLEX_PROBE_INTERVAL','FLEX_PROBE_COOLDOWN'):
            pass  # 调用处即时读 os.environ，上面已写入环境变量即热生效
        elif key=='FLEX_OVERRIDE':
            from dotenv import load_dotenv
            env_file=config_path_resolved.parent.parent/'.env'
            load_dotenv(env_file,override=b)
    def set_config_value(key,raw):
        it=settings_registry.item_for(key)
        if it is None: return None,'未知配置项'
        try:
            if it['type']=='bool':
                val='1' if str(raw).lower() in ('1','true','yes','on','t') else '0'
            else:
                v=int(str(raw))
                if it.get('min') is not None: v=max(v,it['min'])
                if it.get('max') is not None: v=min(v,it['max'])
                val=str(v)
        except (ValueError,TypeError):
            return None,'值必须是数字'
        conf=read_setup(); conf[key]=val; write_setup(conf)
        os.environ[key]=val
        _RUNTIME[key]=val
        _apply_hot(key,val)
        return val,None
    def _config_source(key):
        if key in read_setup(): return 'setup.conf'
        if os.getenv(key) is not None: return '环境变量'
        return '默认值'
    @app.get('/api/config')
    async def config_tree():
        cats=[]
        for cat in settings_registry.CATEGORIES:
            items=[]
            for item in settings_registry.ITEMS:
                if item['cat']!=cat['id']: continue
                val=_RUNTIME.get(item['key'],item['default'])
                items.append({'key':item['key'],'label':item['label'],'desc':item['desc'],
                              'type':item['type'],'min':item.get('min'),'max':item.get('max'),
                              'unit':item.get('unit',''),'hot':bool(item.get('hot',False)),
                              'value':val,'source':_config_source(item['key'])})
            if items: cats.append({'id':cat['id'],'label':cat['label'],'desc':cat['desc'],'items':items})
        # 只读：各 Channel 的 API Base / API Key 来源（存于 .env，不在 setup.conf）。
        env_path=config_path_resolved.parent.parent/'.env'
        env_vars=set()
        if env_path.exists():
            for line in env_path.read_text(encoding='utf-8').splitlines():
                line=line.strip()
                if not line or line.startswith('#') or '=' not in line: continue
                env_vars.add(line.split('=',1)[0].strip())
        override_state=os.getenv('FLEX_OVERRIDE','1')=='1'
        seen=set(); channels=[]
        for prov in config.providers.values():
            for key in (prov.base_url_env,prov.api_key_env):
                if key in seen: continue
                seen.add(key)
                in_sys=key in os.environ; in_env=key in env_vars
                if in_sys and in_env: src='.env (override)' if override_state else '系统环境变量 (.env 被忽略)'
                elif in_sys: src='系统环境变量'
                elif in_env: src='.env'
                else: src='缺失'
                channels.append({'key':key,'in_env':in_env,'in_sys':in_sys,'source':src})
        return {'categories':cats,'channels':channels}
    @app.post('/api/config/item/{key}')
    async def config_item_set(key:str, request:Request):
        try: payload=await request.json()
        except Exception: payload={}
        raw=payload.get('value')
        val,err=set_config_value(key,raw)
        if err: raise HTTPException(status_code=400,detail=err)
        it=settings_registry.item_for(key)
        return {'ok':True,'key':key,'value':val,'hot':bool(it.get('hot',False)),
                'source':_config_source(key),
                'note':'已保存并热生效' if it.get('hot') else '已保存，需重启核心 Router(:7800) 后生效'}

    def render(name,title,extra_css='',**vars):
        page=(templates_dir/name).read_text(encoding='utf-8')
        for k,v in vars.items():page=page.replace('{{ '+k+' }}',str(v))
        base=(templates_dir/'base.html').read_text(encoding='utf-8')
        return base.replace('{{ title }}',title).replace('{{ extra_css }}',extra_css).replace('{{ content }}',page)
    lucide_help=b'<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><path d="M12 17h.01"/></svg>'
    @app.middleware('http')
    async def no_cache(request,call_next):
        response=await call_next(request)
        response.headers['Cache-Control']='no-store,no-cache,must-revalidate'
        response.headers['Pragma']='no-cache'
        return response
    @app.middleware('http')
    async def lucide_icon(request,call_next):
        response=await call_next(request)
        if request.url.path!='/':return response
        body=b''.join([part async for part in response.body_iterator])
        return HTMLResponse(body.replace('ⓘ'.encode(),lucide_help),status_code=response.status_code)
    def pool_for(reference):
        """Resolve reference to (internal_key, pool, channel).

        Returns (internal_key, pool, direct_channel):
          - pool entry:   (pool_name, pool, None)
          - direct channel: (channel_id, None, Channel)
          - unknown:      (None, None, None)

        Legacy Link aliases are resolved first, then the canonical Runner or
        direct Channel is selected.  The Pool terminology remains only for
        compatibility with existing callers.
        """
        target = config.resolve_connection(reference)
        if target is not None:
            reference = target
        if reference in config.runners:
            return reference, config.runners[reference], None
        for internal, pool in config.runners.items():
            if pool.public_model == reference:
                return internal, pool, None
        if reference in config.channels:
            channel=config.channels[reference]
            # Hidden Channels remain valid Runner members but cannot be used
            # as a direct external model reference.
            if not channel.enabled or not channel.externally_exposed:
                return None, None, None
            return reference, None, channel
        return None, None, None
    @app.get('/',response_class=HTMLResponse)
    async def dashboard():
        return render('dashboard.html','Flex LLM Router','table{width:100%;border-collapse:collapse;background:#fff;margin:16px 0}th,td{padding:9px;border-bottom:1px solid #e7eaf0;text-align:left}button{margin-right:5px;padding:5px 8px}code{font-size:12px}.bad{color:#b42318}.ok{color:#067647}.tgl{cursor:pointer;color:#2764a8}tr.detail td{background:#fbfcfe;color:#667;font-size:13px}')
    @app.get('/traces',response_class=HTMLResponse)
    async def traces_page():
        return render('traces.html','Flex LLM Router · 调用轨迹')
    @app.get('/statistics',response_class=HTMLResponse)
    async def statistics_page():
        return render('statistics.html','Flex LLM Router · 调用统计')
    @app.get('/quota',response_class=HTMLResponse)
    async def quota_page():
        return render('quota.html','Flex LLM Router · 配额仪表盘')
    @app.get('/help',response_class=HTMLResponse)
    async def help_page():
        return render('help.html','Flex Router · 策略说明')
    def detect_lan_ip():
        """探测本机局域网 IP(用于同局域网其他机器访问)."""
        import socket
        try:
            s_=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s_.connect(('8.8.8.8', 80))
            ip=s_.getsockname()[0]
            s_.close()
            return ip
        except Exception:
            return ''

    @app.get('/api/config/view')
    async def config_view():
        """Runner-oriented configuration view for external clients.

        ``pools``/``connections`` remain in the response as deprecated
        compatibility keys; the UI and new callers use ``runners``.
        """
        port = os.getenv('FLEX_PORT','7800')
        base_url = f"http://127.0.0.1:{port}/v1"
        lan_ip = detect_lan_ip()
        lan_url = f"http://{lan_ip}:{port}/v1" if lan_ip else base_url
        runners=[]
        referenced=set()
        for name,pool in config.runners.items():
            selection=pool.selection if isinstance(pool.selection,dict) else {}
            strategy_key=selection.get('strategy','custom')
            hedge_cfg=selection.get('hedge',{}) if isinstance(selection.get('hedge',{}),dict) else {}
            stages=hedge_cfg.get('stages',[]) if isinstance(hedge_cfg,dict) else []
            stage_text=[]
            for stage in stages:
                if isinstance(stage,dict) and stage.get('channels'):
                    try:minute=int(stage.get('after_seconds',0))//60
                    except (TypeError,ValueError):minute=0
                    targets=' + '.join(str(ch) for ch in stage['channels'])
                    stage_text.append(f'{minute} 分钟：{targets}')
            if stages:
                strategy_name='配置驱动多阶段 Hedge'
                strategy_detail=('；'.join(stage_text)+'；最终截止按 Runner Channel 数量计算') if stage_text else '按 Runner 中 hedge.stages 顺序执行；最终截止按 Channel 数量计算'
            elif strategy_key=='round_robin':
                strategy_name='轮转均衡 + 同通道 Hedge'
                count=len(pool.channels)
                if count>=3:
                    strategy_detail='按配置顺序选择 Channel；6 分钟重试第二 Channel，9 分钟重试第三 Channel，12 分钟无首活动截止'
                elif count==2:
                    strategy_detail='按配置顺序选择 Channel；6 分钟重试第二 Channel，9 分钟无首活动截止'
                else:
                    strategy_detail='单 Channel；6 分钟重试同一 Channel，9 分钟无首活动截止'
            elif strategy_key=='cost_aware':
                strategy_name='成本优先 + 故障回退'
                strategy_detail='按 tier/成本优先；配额、限流或故障时切换备用 Channel，并保持会话亲和性'
            elif strategy_key=='quota_paced_priority':
                strategy_name='配额节奏优先 + 故障回退'
                strategy_detail='按配额消耗节奏安排 Channel；异常时依次回退到下一 tier'
            else:
                strategy_name='自定义策略'
                strategy_detail=f'配置策略键：{strategy_key}'
            channels=[]
            for ch_id in pool.channels:
                referenced.add(ch_id)
                try:
                    prov_name,ch=config.get_channel(ch_id)
                    prov=config.providers.get(prov_name)
                except LookupError:
                    continue
                if prov is None: continue
                ext_model=config.external_channel_model(ch)
                real_base=os.getenv(prov.base_url_env,'').strip() or '(unset)'
                channels.append({'id':ch.id,'provider':ch.provider,'model':ext_model,
                                 'litellm_model':ch.litellm_model,'real_base_url':real_base,
                                 'base_url':base_url,'tier':pool.tiers.get(ch_id),
                                 'enabled':ch.enabled,'externally_exposed':ch.externally_exposed,
                                 'chn_content_policy_fallback':ch.chn_content_policy_fallback,
                                 'max_output_tokens':ch.max_output_tokens,
                                 'learned_max_output_tokens':learned_max_output_tokens.get(ch.id),
                                 'effective_max_output_tokens':channel_output_cap(ch),
                                 'context_window_source':ch.context_window_source,
                                 'max_output_tokens_source':ch.max_output_tokens_source,
                                 'limits_verified_at':ch.limits_verified_at,
                                 'quota_429_policy':ch.quota_429_policy.model_dump() if ch.quota_429_policy else None,
                                 'context_window_tokens':ch.context_window_tokens,
                                 'capabilities':list(ch.capabilities),
                                 'protocol_support':dict(ch.protocol_support),
                                 'last_used_at':state.last_used_at(ch.id)})
            runners.append({'name':name,'public_model':pool.public_model,'base_url':base_url,
                          'lan_url':lan_url,'channels':channels,'strategy_key':strategy_key,
                          'strategy_name':strategy_name,'strategy_detail':strategy_detail,
                          # Preserve the full selection object so a UI edit of
                          # only the strategy does not discard hedge/fallback
                          # settings that are not exposed as form fields.
                          'selection':selection})
        # The editor lists every defined Channel, regardless of Runner
        # membership. ``externally_exposed`` controls standalone exposure;
        # ``mounted_in`` is informational only.
        channels_all=[]
        for ch_id,ch in config.channels.items():
            try:
                prov_name,ch2=config.get_channel(ch_id)
                prov=config.providers.get(prov_name)
            except LookupError:
                continue
            if prov is None: continue
            real_base=os.getenv(prov.base_url_env,'').strip() or '(unset)'
            mounted=[pn for pn,p in config.runners.items() if ch_id in p.channels]
            channels_all.append({'id':ch.id,'provider':ch.provider,'model':config.external_channel_model(ch),
                                 'litellm_model':ch.litellm_model,'real_base_url':real_base,
                                 'base_url':base_url,'tier':None,'mounted_in':mounted,
                                 'enabled':ch.enabled,'externally_exposed':ch.externally_exposed,
                                 'chn_content_policy_fallback':ch.chn_content_policy_fallback,
                                 'max_output_tokens':ch.max_output_tokens,
                                 'learned_max_output_tokens':learned_max_output_tokens.get(ch.id),
                                 'effective_max_output_tokens':channel_output_cap(ch),
                                 'context_window_source':ch.context_window_source,
                                 'max_output_tokens_source':ch.max_output_tokens_source,
                                 'limits_verified_at':ch.limits_verified_at,
                                 'quota_429_policy':ch.quota_429_policy.model_dump() if ch.quota_429_policy else None,
                                 'context_window_tokens':ch.context_window_tokens,
                                 'capabilities':list(ch.capabilities),
                                 'protocol_support':dict(ch.protocol_support),
                                 'last_test':state.last_test(ch.id,ch.id),
                                 'last_used_at':state.last_used_at(ch.id)})
        # 连接: 逐个解析出目标类型与真实通道, 供 CONFIG 页"拷贝给其他系统"区块展示
        connections=[]
        for name, target in config.links.items():
            if target in config.runners:
                pool = config.runners[target]
                ctype = 'pool'
                chs = pool.channels
            elif any(p.public_model == target for p in config.runners.values()):
                pool = next(p for p in config.runners.values() if p.public_model == target)
                ctype = 'pool'
                chs = pool.channels
            elif target in config.channels:
                ctype = 'channel'
                chs = [target]
            else:
                ctype = 'unknown'
                chs = []
            connections.append({'name':name,'target':target,'type':ctype,'channels':chs})
        providers=[{'id':name,'base_url_env':provider.base_url_env,
                    'api_key_env':provider.api_key_env,
                    'model_count':len(config.provider_models(name))}
                   for name,provider in config.providers.items()]
        return {'base_url':base_url,'lan_url':lan_url,'runners':runners,
                'pools':runners,'channels':channels_all,'connections':connections,
                'links':connections,'providers':providers,
                'global_fallback':{k:list(v) for k,v in config.global_fallback.items()},
                'protocol_error_rules':[rule.model_dump() for rule in config.protocol_error_rules]}

    @app.get('/config',response_class=HTMLResponse)
    async def config_page():
        raw=config_path_resolved.read_text(encoding='utf-8')
        return render('config.html','Flex LLM Router · Config',
        '', # 样式全在 templates/config.html 的 <style> 内, 不再硬编码(避免与模板冲突/陈旧)
        config_path=str(config_path_resolved),raw_yaml=html.escape(raw))
    @app.post('/api/config')
    async def save_config(request:Request):
        raw=(await request.body()).decode('utf-8')
        try:
            parsed=yaml.safe_load(raw)
            if not isinstance(parsed,dict):raise ValueError('config must be a mapping')
        except Exception as e:
            raise HTTPException(400,f'invalid config: {e}') from e
        try:
            backup=persist_config_mapping(parsed, raw)
        except Exception as e:
            raise HTTPException(400,f'invalid config: {e}') from e
        return {'status':'saved','config':True,'backup':str(backup)}

    def persist_config_mapping(mapping:dict, raw_text:str|None=None):
        """Validate, persist, and hot-apply a structured Config edit.

        Secrets never enter this path: Provider records contain only .env
        variable names.  ``config`` is replaced after the atomic validation so
        schedulers and read APIs see the new definition without a core restart.
        """
        nonlocal config
        cfg=FlexConfig.model_validate(mapping)
        missing_env=[]; missing_refs=[]
        for ch_id,ch in cfg.channels.items():
            if not ch.enabled: continue
            prov=cfg.providers.get(ch.provider)
            if prov is None:
                missing_refs.append(f'{ch_id}: provider {ch.provider!r} not found'); continue
            for env_name in (prov.base_url_env,prov.api_key_env):
                if not os.getenv(env_name,'').strip():
                    if env_name not in missing_env: missing_env.append(env_name)
                    missing_refs.append(f'{ch_id} -> {env_name}')
        if missing_env:
            raise ValueError('missing environment variable(s) in .env: '+', '.join(missing_env)+
                             '. Please set them in your .env file. Referenced by: '+'; '.join(missing_refs))
        if raw_text is None:
            raw_text=yaml.safe_dump(mapping,sort_keys=False,allow_unicode=True)
        backup=backup_config_file(config_path_resolved, keep=10)
        config_path_resolved.write_text(raw_text,encoding='utf-8')
        config=cfg
        logger.warning('config saved backup=%s (hot-applied)',backup)
        return backup

    async def run_channel_capability_probe(channel_id: str, kinds=None):
        """Probe a newly-created Channel and hot-apply safe observations."""
        channel = config.channels.get(channel_id)
        if channel is None:
            return
        try:
            observations = await probe_channel_capabilities(channel, config.providers, kinds=kinds)
            # A successful /models response is the authoritative source for
            # input/output limits when the Provider exposes them.
            metadata = await asyncio.to_thread(fetch_provider_model_metadata, channel, config.providers)
            async with app.state.config_write_lock:
                mapping = yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
                channels = mapping.setdefault('channels', {})
                current = dict(channels.get(channel_id) or {})
                support = dict(current.get('protocol_support') or {})
                # 暂态失败（429/超时/5xx/网络）不得覆盖已确认的结论：
                # 一项能力若此前已 supported/unsupported，探测撞限流只记录诊断，
                # 绝不把绿/红退化成橙色 error。仅当该项从无结论或本来就是 error
                # 时才写入 error（供 idle 重探循环继续跟踪）。
                for name, obs in observations.items():
                    status = obs.get('status')
                    prev = support.get(name) or {}
                    if status == 'error' and prev.get('status') in ('supported', 'unsupported'):
                        diag = dict(obs)
                        diag['status'] = prev['status']          # 保留已确认结论
                        diag['last_error'] = obs.get('detail', '')
                        diag['last_error_http'] = obs.get('http_status')
                        diag['retried_429'] = bool(obs.get('retried_429', False))
                        support[name] = diag
                    else:
                        support[name] = obs
                current['protocol_support'] = support
                capabilities = list(current.get('capabilities') or [])
                for name, result in observations.items():
                    if result.get('status') == 'supported' and name not in capabilities:
                        capabilities.append(name)
                    elif result.get('status') == 'unsupported' and name in capabilities:
                        capabilities.remove(name)
                current['capabilities'] = capabilities
                # Provider /models 是上游对窗口/输出上限的权威声明（sensenova
                # 等会返回 context_length / max_output_length）。元数据存在时覆盖
                # 本地默认值；否则保留手工/实测值。
                provider_ctx=(metadata.get('context_length') or metadata.get('context_window')
                              or metadata.get('max_input_tokens'))
                provider_out=(metadata.get('max_output_length') or metadata.get('max_output_tokens')
                              or metadata.get('max_tokens'))
                if provider_ctx:
                    # 窗口是硬约束，元数据（context_length）比本地默认值可信，覆盖。
                    current['context_window_tokens'] = int(provider_ctx)
                    current['context_window_source'] = 'metadata'
                    current['limits_verified_at'] = time.time()
                if provider_out and not current.get('max_output_tokens'):
                    # max_output_length 常是“建议默认值”，而 API 实际可接受更大
                    # （实测 sensenova 接受 384000 但元数据只写 65536）。因此只在
                    # 通道还没有实测/配置值时用它兜底。
                    current['max_output_tokens'] = int(provider_out)
                    current['max_output_tokens_source'] = 'metadata'
                    current['limits_verified_at'] = time.time()
                # 输出上限的主动探测：只在“配置为空且没有学习值”时做一次，
                # 成功后立即落盘/入库，之后不再重复探测（额度敏感通道也只花一次）。
                if not current.get('max_output_tokens') and not state.output_cap(channel_id):
                    try:
                        probe_base,probe_key=channel_credentials(channel,config.providers)
                    except Exception:
                        probe_base=probe_key=''
                    if probe_base and probe_key:
                        measured,evidence=await asyncio.to_thread(
                            _probe_output_limit_sync,probe_base,probe_key,
                            channel.litellm_model.rsplit('/',1)[-1],
                            int(current.get('context_window_tokens') or 1000000))
                        if measured:
                            current['max_output_tokens']=int(measured)
                            current['max_output_tokens_source']='measured'
                            current['limits_verified_at']=time.time()
                            state.learn_output_cap(channel_id,int(measured),evidence)
                            logger.info('output limit probed channel=%s cap=%s',channel_id,measured)
                        else:
                            logger.info('output limit probe inconclusive channel=%s reason=%s',channel_id,evidence[:120])
                channels[channel_id] = current
                # Probe persistence is best-effort; a transient failure must
                # not take down the Router or roll back the Channel creation.
                persist_config_mapping(mapping)
            logger.info('channel capability probe complete channel=%s capabilities=%s', channel_id, capabilities)
        except Exception as exc:
            logger.warning('channel capability probe failed channel=%s error=%s', channel_id, _probe_detail(exc))

    def schedule_channel_capability_probe(channel_id: str, kinds=None):
        if channel_id in app.state.capability_probe_channels:
            return None
        app.state.capability_probe_channels.add(channel_id)
        task = asyncio.create_task(run_channel_capability_probe(channel_id, kinds=kinds))
        app.state.capability_probe_tasks.add(task)
        def done(completed):
            app.state.capability_probe_tasks.discard(completed)
            app.state.capability_probe_channels.discard(channel_id)
        task.add_done_callback(done)
        return task

    async def structured_config_error(exc):
        raise HTTPException(400,f'invalid config edit: {exc}') from exc

    @app.post('/api/ui/alert')
    async def ui_alert(request:Request):
        """记录前端 #backend-notice 告警条展示的具体错误（供刷新后回溯）。

        前端在任一 fetch 返回 >=500 或连接失败时调用，把触发的错误编号、
        请求路径、时间点持久化到后端日志；即使页面随后整页刷新，也能从
        logs/flex-router.log 里回溯到那次告警的真实内容。
        """
        try:
            body=await request.json()
        except Exception:
            body={}
        code=body.get('code') or '未知'
        path=body.get('path') or request.headers.get('referer') or '-'
        logger.warning('UI_ALERT 前端告警条: 错误编号=%s 触发请求=%s referer=%s', code, path, request.headers.get('referer') or '-')
        return {'status':'ok'}

    @app.get('/api/config/editor')
    async def config_editor():
        """Return editable Runner/Channel/Model records without secret values."""
        view=await config_view()
        return view

    @app.post('/api/config/runners')
    async def create_runner(request:Request):
        """Create a Runner with one or more existing Channels.

        A new Runner is immediately an external resource.  Channel-level
        ``externally_exposed`` remains a legacy compatibility field; new
        membership is controlled here by the Runner editor.
        """
        body=await request.json()
        raw_name=str(body.get('name') or '')
        try:
            name=validate_runner_name(raw_name)  # 校验原始值（含首尾空格拒绝），返回规范化后的名称
        except ValueError as exc:
            raise HTTPException(400,str(exc)) from exc
        public_model=str(body.get('public_model') or name).strip()
        channels=body.get('channels')
        if channels is None and body.get('channel'):
            channels=[body.get('channel')]
        if not public_model:
            raise HTTPException(400,'name is required')
        if not isinstance(channels,list) or not channels:
            raise HTTPException(400,'select at least one Channel')
        channels=[str(channel).strip() for channel in channels if str(channel).strip()]
        if not channels:
            raise HTTPException(400,'select at least one Channel')
        if len(set(channels)) != len(channels):
            raise HTTPException(400,'Runner Channels must be unique')
        if name in config.runners:
            raise HTTPException(409,'runner name already exists')
        if any(r.public_model == public_model for r in config.runners.values()):
            raise HTTPException(409,'public model already exists')
        missing=[channel for channel in channels if channel not in config.channels]
        if missing:
            raise HTTPException(400,'unknown Channel: '+', '.join(missing))
        selection=body.get('selection') if isinstance(body.get('selection'),dict) else {'strategy':'round_robin'}
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        runners=mapping.setdefault('runners',{})
        runners[name]={
            'public_model':public_model,
            'channels':channels,
            'tiers':{channel:0 for channel in channels},
            'selection':selection,
        }
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','runner':name,'backup':str(backup)}

    @app.post('/api/config/runners/{name}')
    async def edit_runner(name:str, request:Request):
        if name not in config.runners: raise HTTPException(404,'unknown runner')
        body=await request.json()
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        runners=mapping.setdefault('runners',{})
        current=dict(runners.get(name) or {})
        for key in ('public_model','selection','context_policy','session_affinity'):
            if key in body: current[key]=body[key]
        if 'channels' in body:
            if not isinstance(body['channels'],list) or not body['channels']:
                raise HTTPException(400,'channels must be a non-empty list')
            current['channels']=body['channels']
            # Keep the Pool/Runner tier map valid when membership is edited in
            # the UI.  Existing tier values are retained; new Channels join
            # the last configured tier (or tier 0 for an empty map).
            old_tiers=current.get('tiers') if isinstance(current.get('tiers'),dict) else {}
            default_tier=max([int(v) for v in old_tiers.values() if isinstance(v,int)] or [0])
            current['tiers']={cid:old_tiers.get(cid,default_tier) for cid in body['channels']}
        if 'tiers' in body: current['tiers']=body['tiers']
        runners[name]=current
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','runner':name,'backup':str(backup)}

    @app.delete('/api/config/runners/{name}')
    async def delete_runner(name:str):
        """Delete a Runner and its channel membership.

        Only the Runner is removed; its Channels remain defined globally and
        can still be used by other Runners.  A Runner referenced by a ``link``
        (e.g. ``links.main``) cannot be deleted until the link points away,
        otherwise the external entry point would dangle.
        """
        if name not in config.runners:
            raise HTTPException(404,'unknown runner')
        refs=[lnk for lnk,target in config.links.items() if target==name or target==config.runners[name].public_model]
        if refs:
            raise HTTPException(409,'runner is referenced by link(s): '+', '.join(refs)+'; remove the link first')
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        runners=mapping.get('runners') or {}
        if name not in runners:
            raise HTTPException(409,'runner not present in persisted config')
        del runners[name]
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        logger.warning('runner deleted: %s (backup=%s)', name, backup)
        return {'status':'deleted','runner':name,'backup':str(backup)}

    @app.post('/api/config/channels/test-all')
    async def config_channels_test_all():
        """One-click self-test for every defined Channel (serial).

        Each Channel is probed one at a time to avoid hammering a shared
        upstream (a parallel sweep would trip the very RPM/TPM limits the
        test is meant to reveal).  Per-Channel failures are collected, never
        raised: the response carries per-Channel status plus a summary.
        """
        results=[]
        ok=0; failed=0
        for ch_id,ch in config.channels.items():
            attempt=state.start(ch_id,ch.id,ch.litellm_model,input_tokens=5); started=time.monotonic()
            try:
                base,key=channel_credentials(ch,config.providers)
                await litellm.acompletion(model=ch.litellm_model,api_base=base,api_key=key,
                                          messages=[{'role':'user','content':'Reply with exactly OK.'}],
                                          max_tokens=256,**channel_request_kwargs(ch))
                latency=int((time.monotonic()-started)*1000); state.finish(attempt,'success',None,latency)
                state.record_test(ch_id,ch.id,'success',None,latency,'')
                results.append({'id':ch.id,'provider':ch.provider,'status':'ok','latency_ms':latency,
                                'error_type':None,'error_detail':''})
                ok+=1
            except Exception as exc:
                typ=error_type(exc); detail=error_detail(exc); latency=int((time.monotonic()-started)*1000)
                state.finish(attempt,'failure',typ,latency,error_detail=detail,error_code=error_code(exc))
                state.record_test(ch_id,ch.id,'failure',typ,latency,detail)
                results.append({'id':ch.id,'provider':ch.provider,'status':'fail','latency_ms':latency,
                                'error_type':typ,'error_detail':detail})
                failed+=1
        return {'results':results,'summary':{'total':len(results),'ok':ok,'failed':failed}}


    @app.post('/api/config/channels/{channel_id}')
    async def edit_channel(channel_id:str, request:Request):
        if channel_id not in config.channels: raise HTTPException(404,'unknown channel')
        body=await request.json()
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        channels=mapping.setdefault('channels',{}); current=dict(channels[channel_id])
        allowed=('provider','litellm_model','public_model','enabled','externally_exposed',
                 'chn_content_policy_fallback','max_output_tokens','quota_429_policy',
                 'context_window_tokens','capabilities','supported_params','protocol_support',
                 'limits','retry_policy','extra_body')
        for key in allowed:
            if key in body: current[key]=body[key]
        # A disabled Channel can never remain directly exposed, even when an
        # API client bypasses the UI's linked checkboxes.
        if current.get('enabled') is False:
            current['externally_exposed']=False
        channels[channel_id]=current
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','channel':channel_id,'backup':str(backup)}

    @app.post('/api/config/global-fallback')
    async def edit_global_fallback(request:Request):
        """Persist an ordered global policy fallback list.

        Only channel ids are accepted; secrets and provider credentials never
        enter this API.  The list order is the retry order.
        """
        body=await request.json()
        policy=str(body.get('policy') or 'chn_content_policy').strip()
        if policy!='chn_content_policy':
            raise HTTPException(400,'only chn_content_policy fallback is supported')
        channel_ids=body.get('channels',[])
        if not isinstance(channel_ids,list): raise HTTPException(400,'channels must be a list')
        channel_ids=[str(cid).strip() for cid in channel_ids if str(cid).strip()]
        if len(channel_ids)!=len(set(channel_ids)): raise HTTPException(400,'fallback channels must be unique')
        missing=[cid for cid in channel_ids if cid not in config.channels]
        if missing: raise HTTPException(400,'unknown fallback Channel: '+', '.join(missing))
        unmarked=[cid for cid in channel_ids if not config.channels[cid].chn_content_policy_fallback]
        if unmarked: raise HTTPException(400,'fallback Channel 未标记 CHN Content Policy Fallback: '+', '.join(unmarked))
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        fallback=mapping.setdefault('global_fallback',{})
        fallback[policy]=channel_ids
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','policy':policy,'channels':channel_ids,'backup':str(backup)}

    @app.post('/api/config/channels')
    async def create_channel(request:Request):
        """Create one Channel from a Provider + model pair in the UI."""
        body=await request.json()
        channel_id=str(body.get('id') or '').strip()
        provider=str(body.get('provider') or '').strip()
        litellm_model=str(body.get('litellm_model') or body.get('model') or '').strip()
        if not channel_id or not provider or not litellm_model:
            raise HTTPException(400,'id, provider and model are required')
        if channel_id in config.channels: raise HTTPException(409,'channel id already exists')
        if provider not in config.providers: raise HTTPException(400,'unknown provider')
        if any(ch.provider==provider and ch.litellm_model==litellm_model for ch in config.channels.values()):
            raise HTTPException(409,'a Channel for this Provider + model already exists')
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        channels=mapping.setdefault('channels',{})
        item={'id':channel_id,'provider':provider,'litellm_model':litellm_model,
              'public_model':str(body.get('public_model') or channel_id).strip(),
              'context_window_tokens':int(body.get('context_window_tokens') or 1000000),
              'capabilities':body.get('capabilities') or ['chat','streaming'],
              'protocol_support':body.get('protocol_support') or {},
              'chn_content_policy_fallback':bool(body.get('chn_content_policy_fallback',False)),
              'externally_exposed':bool(body.get('externally_exposed',True)),
              'enabled':bool(body.get('enabled',True))}
        max_output=body.get('max_output_tokens')
        if isinstance(max_output,int) and max_output>0: item['max_output_tokens']=max_output
        channels[channel_id]=item
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        schedule_channel_capability_probe(channel_id)
        return {'status':'saved','channel':channel_id,'probe':'scheduled','backup':str(backup)}

    @app.post('/api/config/channels-bulk')
    async def create_channels_bulk(request:Request):
        """Create checked Provider models as Channels in one validated edit."""
        body=await request.json()
        provider=str(body.get('provider') or '').strip()
        items=body.get('models')
        if provider not in config.providers: raise HTTPException(400,'unknown provider')
        if not isinstance(items,list) or not items: raise HTTPException(400,'select at least one model')
        if len(items)>100: raise HTTPException(400,'at most 100 models per edit')
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        channels=mapping.setdefault('channels',{})
        existing_pairs={(ch.provider,ch.litellm_model) for ch in config.channels.values()}
        created=[]
        for item in items:
            if not isinstance(item,dict): raise HTTPException(400,'invalid model item')
            litellm_model=str(item.get('litellm_model') or item.get('model') or '').strip()
            if not litellm_model: raise HTTPException(400,'model is required')
            if '/' not in litellm_model: litellm_model='openai/'+litellm_model
            model_tail=litellm_model.rsplit('/',1)[-1]
            channel_id=provider+'-'+re.sub(r'[^a-zA-Z0-9]+','-',model_tail).strip('-').lower()
            if not channel_id: raise HTTPException(400,'model cannot form a channel id')
            if (provider,litellm_model) in existing_pairs:
                continue
            if channel_id in channels:
                raise HTTPException(409,f'channel id conflict: {channel_id}')
            alias=str(item.get('alias') or item.get('public_model') or channel_id).strip()
            entry_dict={
                'id':channel_id,'provider':provider,'litellm_model':litellm_model,
                'public_model':alias,'context_window_tokens':int(item.get('context_window_tokens') or 1000000),
                'capabilities':item.get('capabilities') or ['chat','streaming'],
                'protocol_support':item.get('protocol_support') or {},
                'chn_content_policy_fallback':bool(item.get('chn_content_policy_fallback',False)),
                'externally_exposed':bool(item.get('externally_exposed',True)),
                'enabled':bool(item.get('enabled',True)),
            }
            max_output=item.get('max_output_tokens')
            if isinstance(max_output,int) and max_output>0: entry_dict['max_output_tokens']=max_output
            channels[channel_id]=entry_dict
            existing_pairs.add((provider,litellm_model)); created.append(channel_id)
        if not created: return {'status':'unchanged','created':[]}
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        for channel_id in created:
            schedule_channel_capability_probe(channel_id)
        return {'status':'saved','created':created,'probe':'scheduled','backup':str(backup)}

    @app.post('/api/config/channels/{channel_id}/capabilities-test')
    async def config_channel_capabilities_test(channel_id: str):
        """Run the complete on-demand capability probe for an existing Channel."""
        if channel_id not in config.channels:
            raise HTTPException(404, 'unknown channel')
        # Keep one probe per Channel at a time; callers can safely refresh the
        # UI without multiplying upstream test traffic.
        if channel_id in app.state.capability_probe_channels:
            return {'channel': channel_id, 'status': 'already_running'}
        task = schedule_channel_capability_probe(channel_id)
        return {'channel': channel_id, 'status': 'scheduled' if task is not None else 'already_running'}

    @app.post('/api/config/channels/{channel_id}/test')
    async def config_channel_test(channel_id:str):
        """Admin self-test for any Channel, including internally hidden ones."""
        ch=config.channels.get(channel_id)
        if ch is None: raise HTTPException(404,'unknown channel')
        attempt=state.start(channel_id,ch.id,ch.litellm_model,input_tokens=5); started=time.monotonic()
        try:
            base,key=channel_credentials(ch,config.providers)
            await litellm.acompletion(model=ch.litellm_model,api_base=base,api_key=key,
                                      messages=[{'role':'user','content':'Reply with exactly OK.'}],
                                      max_tokens=256,**channel_request_kwargs(ch))
        except Exception as exc:
            typ=error_type(exc); detail=error_detail(exc); latency=int((time.monotonic()-started)*1000)
            state.finish(attempt,'failure',typ,latency,error_detail=detail,error_code=error_code(exc))
            state.record_test(channel_id,ch.id,'failure',typ,latency,detail)
            raise HTTPException(502,detail)
        latency=int((time.monotonic()-started)*1000); state.finish(attempt,'success',None,latency)
        state.record_test(channel_id,ch.id,'success',None,latency,'')
        return {'channel':channel_id,'status':'ok','latency_ms':latency}

    @app.post('/api/config/channels/{channel_id}/responses-test')
    async def config_channel_responses_test(channel_id:str):
        """Explicitly probe the upstream Responses protocol for one Channel.

        This is intentionally separate from the normal Chat Completions self
        test.  It performs one tiny real request only when an administrator
        clicks the action, then persists the last observation on the Channel
        as ``protocol_support.responses``.  A 401/5xx/network failure is
        recorded as ``error`` rather than incorrectly claiming the protocol is
        unsupported; 404/405 and an explicit unsupported response are marked
        ``unsupported``.
        """
        ch=config.channels.get(channel_id)
        if ch is None:
            raise HTTPException(404,'unknown channel')
        started=time.monotonic(); checked_at=int(time.time())
        endpoint=''; status_code=None; payload=None; detail=''
        try:
            base,key=channel_credentials(ch,config.providers)
            endpoint=responses_probe_url(base)
            model=ch.litellm_model.rsplit('/',1)[-1]
            request_body=json.dumps({'model':model,'input':'Reply with exactly OK.','max_output_tokens':32},ensure_ascii=False).encode('utf-8')
            req=urllib.request.Request(endpoint,data=request_body,method='POST',headers={
                'Authorization':f'Bearer {key}',
                'Content-Type':'application/json',
                'Accept':'application/json',
                'User-Agent':'flex-router-responses-test/1.0',
            })
            try:
                with urllib.request.urlopen(req,timeout=20) as response:
                    status_code=response.status
                    raw=response.read().decode('utf-8','replace')
                    try: payload=json.loads(raw)
                    except Exception: payload=raw
            except urllib.error.HTTPError as exc:
                status_code=exc.code
                raw=exc.read().decode('utf-8','replace')
                try: payload=json.loads(raw)
                except Exception: payload=raw
            detail=_probe_detail(payload)
        except urllib.error.URLError as exc:
            detail=_probe_detail(getattr(exc,'reason',None) or exc)
        except Exception as exc:
            detail=_probe_detail(exc)
        status=classify_responses_probe(status_code,payload,detail)
        latency=int((time.monotonic()-started)*1000)
        observation={'status':status,'checked_at':checked_at,'http_status':status_code,
                     'endpoint':endpoint,'latency_ms':latency,'detail':detail}
        # 暂态失败：限流(429/RPM/TPM)、上游超时、连接失败、无响应。这类"失败"不代表
        # 协议不支持，只是当下通不过；不应覆盖上一次的探测结论，只提示用户。
        transient = (status == 'error' and (
            status_code == 429 or _probe_detail_has(status_code, payload, detail,
                'rpm', 'tpm', 'rate limit', 'throttling', '429', 'too many', 'limit exceeded',
                'timeout', 'timed out', 'read timed out', 'connection', 'refused', 'unreachable',
                'no response', 'server has no response', '504')))
        if transient:
            return {'channel':channel_id,'protocol':'responses','status':'error',
                    'transient':True,'reason':'暂态失败（限流/超时/连接问题），保留原探测结果',
                    'observation':observation,'backup':None}
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        channels=mapping.setdefault('channels',{})
        current=dict(channels.get(channel_id) or {})
        support=dict(current.get('protocol_support') or {})
        support['responses']=observation
        current['protocol_support']=support
        channels[channel_id]=current
        try:
            backup=persist_config_mapping(mapping)
        except Exception as exc:
            await structured_config_error(exc)
        if status == 'supported':
            state.record_test(channel_id,ch.id,'success',None,latency,'Responses API supported')
            return {'channel':channel_id,'protocol':'responses','status':status,
                    'observation':observation,'backup':str(backup)}
        state.record_test(channel_id,ch.id,'failure',
                          'request_error' if status=='unsupported' else 'connection_error',
                          latency,detail)
        return {'channel':channel_id,'protocol':'responses','status':status,
                'observation':observation,'backup':str(backup)}

    @app.post('/api/config/providers')
    async def edit_provider(request:Request):
        body=await request.json(); name=str(body.get('id') or body.get('name') or '').strip()
        if not name: raise HTTPException(400,'provider id is required')
        base=str(body.get('base_url_env') or '').strip(); key=str(body.get('api_key_env') or '').strip()
        if not base or not key: raise HTTPException(400,'base_url_env and api_key_env are required')
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        mapping.setdefault('providers',{})[name]={'base_url_env':base,'api_key_env':key}
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','provider':name,'backup':str(backup)}

    @app.delete('/api/config/providers/{name}')
    async def delete_provider(name:str):
        if name not in config.providers: raise HTTPException(404,'unknown provider')
        if any(ch.provider==name for ch in config.channels.values()):
            raise HTTPException(409,'provider is still referenced by a Channel')
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8') or '{}')
        mapping.get('providers',{}).pop(name,None)
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'deleted','provider':name,'backup':str(backup)}
    @app.get('/setup',response_class=HTMLResponse)
    async def setup_page():
        env_path=config_path_resolved.parent.parent/'.env'
        env_vars={}
        if env_path.exists():
            for line in env_path.read_text(encoding='utf-8').splitlines():
                line=line.strip()
                if not line or line.startswith('#'): continue
                k,v=line.split('=',1)
                env_vars[k.strip()]=v
        needed=set()
        for prov in config.providers.values():
            needed.add(prov.base_url_env); needed.add(prov.api_key_env)
        override_state=True
        if override_path.exists():
            try: override_state=bool(int(override_path.read_text(encoding='utf-8').splitlines()[0].split('=')[1]))
            except: override_state=True
        cls_env='ok' if override_state else 'bad'
        txt_env='ON' if override_state else 'OFF'
        btn_env='off' if override_state else 'on'
        note_env='Override ON: .env 值覆盖系统 ENV' if override_state else 'Override OFF: 系统 ENV 优先，.env 同名项被忽略'
        # DEBUG 日志开关状态
        dbg=state.debug_enabled
        dbg_cls='ok' if dbg else 'bad'; dbg_txt='ON' if dbg else 'OFF'; dbg_btn='off' if dbg else 'on'
        dbg_note='Debug ON: 记录请求/响应 payload 便于诊断(当前保留最近1000条/7天)' if dbg else 'Debug OFF: 不记录 payload'
        capture=state.full_request_capture_status()
        capture_cls='ok' if capture['enabled'] else 'bad'; capture_txt='ON' if capture['enabled'] else 'OFF'; capture_btn='off' if capture['enabled'] else 'on'
        duplicate=state.duplicate_observer_enabled
        duplicate_cls='ok' if duplicate else 'bad'; duplicate_txt='ON' if duplicate else 'OFF'; duplicate_btn='off' if duplicate else 'on'
        duplicate_note='只观察并记录同一 Runner、同一客户端短时间内的完全相同在途请求；不会取消、合并或延迟调用。' if duplicate else '默认关闭。开启后只记录同一 Runner 内的候选重复请求，绝不改变路由行为。'
        rows=[]
        for k in sorted(needed):
            in_env=k in env_vars
            in_sys=k in os.environ
            if in_sys and in_env:
                if override_state: src='env (override)'
                else: src='system ENV (env ignored)'
            elif in_sys: src='system ENV'
            elif in_env: src='.env'
            else: src='MISSING'
            cls_a='ok' if in_env else 'bad'
            cls_b='ok' if in_sys else 'bad'
            cls_c='bad' if src=='MISSING' else 'ok'
            rows.append(f'<tr><td><code>{html.escape(k)}</code></td><td class="{cls_a}">{in_env}</td><td class="{cls_b}">{in_sys}</td><td class="{cls_c}">{src}</td></tr>')
        env_path_str=str(env_path)
        config_path_str=str(config_path_resolved)
        capture_time=lambda value:time.strftime('%m/%d %H:%M:%S',time.localtime(value)) if value else '—'
        return render('setup.html','Flex LLM Router · Setup','table{width:100%;border-collapse:collapse;background:#fff;margin:16px 0}th,td{padding:9px;border-bottom:1px solid #e7eaf0;text-align:left}th{background:#f7f9fc}code{font-size:13px}.bad{color:#b42318}.ok{color:#067647}button{padding:5px 12px;margin-left:8px}.override-box{background:#fff;border:1px solid #e7eaf0;border-radius:8px;padding:14px 18px;margin:16px 0}',total_vars=str(len(needed)),override_cls=cls_env,override_txt=txt_env,btn_txt=btn_env,override_note=note_env,debug_cls=dbg_cls,debug_txt=dbg_txt,debug_btn_txt=dbg_btn,debug_note=dbg_note,capture_cls=capture_cls,capture_txt=capture_txt,capture_btn_txt=capture_btn,capture_hours=capture['retention_hours'],capture_rows=capture['max_rows'],capture_mib=round(capture['max_bytes']/1024/1024),capture_count=capture['count'],capture_used_mib=f"{capture['bytes']/1024/1024:.1f}",capture_oldest=capture_time(capture['oldest_at']),capture_newest=capture_time(capture['newest_at']),duplicate_cls=duplicate_cls,duplicate_txt=duplicate_txt,duplicate_btn_txt=duplicate_btn,duplicate_note=duplicate_note,rows=''.join(rows),env_path=env_path_str,config_path=config_path_str,queue_tpm=QUEUE_TPM_SECONDS,queue_rpm=QUEUE_RPM_SECONDS,affinity_idle=SESSION_AFFINITY_IDLE_SECONDS,protocol_affinity_idle=PROTOCOL_AFFINITY_IDLE_SECONDS)
    @app.post('/api/setup/override')
    async def set_override():
        from dotenv import load_dotenv
        env_file=config_path_resolved.parent.parent/'.env'
        cur=True
        if override_path.exists():
            try: cur=bool(int(read_setup().get('FLEX_OVERRIDE','1')))
            except: cur=True
        new_on=not cur
        load_dotenv(env_file,override=new_on)
        conf=read_setup(); conf['FLEX_OVERRIDE']='1' if new_on else '0'; write_setup(conf)
        os.environ['FLEX_OVERRIDE']='1' if new_on else '0'
        logger.warning('override=%s',new_on)
        return {'override':new_on,'file':str(override_path)}
    @app.post('/api/setup/debug')
    async def toggle_debug():
        """SETUP 页 DEBUG 日志开关: 写 setup.conf 并热生效(无需重启)."""
        cur=state.debug_enabled; new_on=not cur
        conf=read_setup(); conf['FLEX_DEBUG']='1' if new_on else '0'; write_setup(conf)
        state.debug_enabled=new_on
        logger.warning('debug logs=%s',new_on)
        return {'debug':new_on}
    @app.get('/api/setup/full-request-capture')
    async def full_request_capture_state():
        return state.full_request_capture_status()
    @app.post('/api/setup/full-request-capture')
    async def set_full_request_capture(request:Request):
        payload=await request.json()
        current=state.full_request_capture_status()
        enabled=bool(payload.get('enabled',current['enabled']))
        hours=max(1,min(int(payload.get('hours',current['retention_hours'])),24))
        max_rows=max(1,min(int(payload.get('max_rows',current['max_rows'])),10000))
        max_mib=max(1,min(int(payload.get('max_mib',round(current['max_bytes']/1024/1024))),2048))
        conf=read_setup(); conf.update({'FLEX_FULL_REQUEST_CAPTURE':'1' if enabled else '0','FLEX_FULL_REQUEST_CAPTURE_HOURS':str(hours),'FLEX_FULL_REQUEST_CAPTURE_MAX_ROWS':str(max_rows),'FLEX_FULL_REQUEST_CAPTURE_MAX_MIB':str(max_mib)}); write_setup(conf)
        result=state.configure_full_request_capture(enabled,hours,max_rows,max_mib*1024*1024)
        logger.warning('full request capture=%s hours=%s rows=%s max_mib=%s',enabled,hours,max_rows,max_mib)
        return result
    @app.post('/api/setup/duplicate-observer')
    async def toggle_duplicate_observer():
        """Observation mode only: setting is hot-applied and never alters request routing."""
        new_on=not state.duplicate_observer_enabled
        conf=read_setup(); conf['FLEX_DUPLICATE_OBSERVER']='1' if new_on else '0'; write_setup(conf)
        state.duplicate_observer_enabled=new_on
        logger.warning('duplicate observer=%s (observe only)',new_on)
        return {'duplicate_observer':new_on,'mode':'observe_only'}
    @app.get('/api/setup/duplicate-observer')
    async def duplicate_observer_state():
        return {'enabled':state.duplicate_observer_enabled,'mode':'observe_only'}
    @app.get('/api/debug/recent')
    async def debug_recent():
        """最近 DEBUG 日志(四方向 payload), 仅调试期用."""
        return {'enabled':state.debug_enabled,'rows':state.debug_recent(limit=50)}
    @app.get('/api/quality')
    async def quality(window:int=24):
        """CHANNEL 质量统计: 访问数/回退数/恢复数/比例/请求密度(按 window 小时窗口)."""
        return {'window_hours':min(window,168),'channels':state.channel_quality(min(window,168))}
    @app.post('/api/setup/queue')
    async def set_queue(request:Request):
        """SETUP 页设置 TPM/RPM 排队上限(秒), 写 setup.conf 并热生效."""
        global QUEUE_TPM_SECONDS, QUEUE_RPM_SECONDS
        form=await request.json()
        conf=read_setup()
        out={}
        for k,cur in (('FLEX_QUEUE_TPM',QUEUE_TPM_SECONDS),('FLEX_QUEUE_RPM',QUEUE_RPM_SECONDS)):
            if k in form:
                try:
                    v=max(0,int(form[k])); conf[k]=str(v); out[k]=v
                except (ValueError,TypeError): pass
        write_setup(conf)
        if 'FLEX_QUEUE_TPM' in out: QUEUE_TPM_SECONDS=out['FLEX_QUEUE_TPM']
        if 'FLEX_QUEUE_RPM' in out: QUEUE_RPM_SECONDS=out['FLEX_QUEUE_RPM']
        logger.warning('queue caps: tpm=%s rpm=%s',QUEUE_TPM_SECONDS,QUEUE_RPM_SECONDS)
        return {'tpm':QUEUE_TPM_SECONDS,'rpm':QUEUE_RPM_SECONDS}
    @app.get('/api/setup/affinity')
    async def affinity_setup_state():
        return {'session_idle_seconds':SESSION_AFFINITY_IDLE_SECONDS,'protocol_idle_seconds':PROTOCOL_AFFINITY_IDLE_SECONDS}
    @app.post('/api/setup/affinity')
    async def set_affinity_setup(request:Request):
        """Set the ordinary and protocol-fallback affinity idle windows."""
        global SESSION_AFFINITY_IDLE_SECONDS, PROTOCOL_AFFINITY_IDLE_SECONDS
        form=await request.json(); conf=read_setup(); out={}
        for key,field,current in (
            ('FLEX_SESSION_AFFINITY_IDLE_SECONDS','session_idle_seconds',SESSION_AFFINITY_IDLE_SECONDS),
            ('FLEX_PROTOCOL_AFFINITY_IDLE_SECONDS','protocol_idle_seconds',PROTOCOL_AFFINITY_IDLE_SECONDS)):
            if field not in form: continue
            try:
                value=max(60,min(86400,int(form[field])))
            except (ValueError,TypeError):
                value=current
            conf[key]=str(value); out[field]=value
        write_setup(conf)
        if 'session_idle_seconds' in out: SESSION_AFFINITY_IDLE_SECONDS=out['session_idle_seconds']
        if 'protocol_idle_seconds' in out: PROTOCOL_AFFINITY_IDLE_SECONDS=out['protocol_idle_seconds']
        logger.warning('affinity idle windows: session=%ss protocol=%ss',SESSION_AFFINITY_IDLE_SECONDS,PROTOCOL_AFFINITY_IDLE_SECONDS)
        return {'session_idle_seconds':SESSION_AFFINITY_IDLE_SECONDS,'protocol_idle_seconds':PROTOCOL_AFFINITY_IDLE_SECONDS}
    @app.get('/healthz')
    async def health():
        uptime=int(time.time()-app.state.started_at)
        return {
            'status':'ok',
            'router_version':app.version,
            'build':app.state.build,
            'build_features':['deadline_event_hard_stop','pre_response_deadline_hard_stop','detached_upstream_cancellation','detached_stream_idle_read','visible_sse_idle_guard','independent_stream_idle_guard','core_stream_watchdog','buffered_stream_handoff','absolute_post_sse_deadline','trace_finalize_on_stream_watchdog','pre_response_error_preservation','pre_response_completed_task_harvest','completed_limit_task_harvest','downstream_timeout_signal','stream_idle_absolute_hedges','nonblocking_sse_cleanup','config_driven_hedge_stages','generic_runner_policy','inflight_channel_dedupe','bounded_upstream_wait','upstream_lifecycle_events','stream_consumer_observability','watchdog_phase_handoff','handoff_hedge_adoption','rpm_limit_two_stage_escalation','trace_list_query_index','channel_count_aware_6m_9m_hedges','single_channel_6m_retry_9m_deadline','dynamic_9m_12m_first_activity_deadline','local_full_request_viewer'],
            'process':{
                'instance_id':app.state.instance_id,
                'pid':os.getpid(),
                'started_at_epoch':app.state.started_at,
                'started_at_local':time.strftime('%Y-%m-%d %H:%M:%S %z',time.localtime(app.state.started_at)),
                'uptime_seconds':uptime,
                'uptime_human':remaining_clock(uptime),
            },
            'effective_policy':{
                'automatic_hedge_seconds':{'one_channel':[360],'two_channels':[360],'three_or_more_channels':[360,540]},
                'automatic_deadline_seconds':{'one_channel':min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,540),'two_channels':min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,540),'three_or_more_channels':min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,720)},
                'large_context_policy':{'enabled':True,'threshold_tokens':LARGE_CONTEXT_THRESHOLD_TOKENS,
                                        'hedge_seconds':list(LARGE_CONTEXT_HEDGE_DELAYS),
                                        'deadline_seconds':dict(LARGE_CONTEXT_DEADLINES)},
                'configured_pool_hedges':{
                    pool_name:[{'after_seconds':due,'channels':list(targets)} for due,targets in hedge_plan_for(pool_name,[ch for _,ch in config.get_pool_channels(pool_name)],config.get_pool_channels(pool_name)[0][1],config.pools[pool_name])]
                    for pool_name in config.runners
                    if config.get_pool_channels(pool_name)
                },
                'first_activity_deadline_seconds':min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,540),
                'response_timeout_seconds':UPSTREAM_RESPONSE_TIMEOUT,
                'first_chunk_timeout_seconds':UPSTREAM_FIRST_CHUNK_TIMEOUT,
                'session_affinity_idle_seconds':SESSION_AFFINITY_IDLE_SECONDS,
                'protocol_affinity_idle_seconds':PROTOCOL_AFFINITY_IDLE_SECONDS,
                'summary':f'Per-attempt {UPSTREAM_RESPONSE_TIMEOUT}s response / {UPSTREAM_FIRST_CHUNK_TIMEOUT}s first SSE; 1 Channel: 6m same-Channel retry/9m hard deadline; 2 Channels: 6m/9m; 3+ Channels: 6m/9m/12m',
            },
            'watchdog':{'active_trace_count':len(app.state.first_activity_watch),'interrupted_traces_closed_on_start':app.state.recovered_traces},
        }
    @app.get('/v1/models')
    async def models():
        # Runners are the only formal external resources. Channels remain
        # internal implementation members and are not advertised.
        data_list = config.runner_models()
        # Legacy links to Runner resources remain visible. Links pointing
        # directly to a Channel stay resolvable for compatibility but are not
        # advertised as external models.
        data_list += [{'id':name,'object':'model','owned_by':'flex-connection','target':target}
                      for name, target in config.links.items()
                      if target in config.runners or any(r.public_model == target for r in config.runners.values())]
        return {'object':'list','data':data_list}

    @app.get('/api/providers')
    async def providers():
        """List configured providers for the Runner editor.

        This is a configuration catalogue only; it never performs an active
        health check or sends a probe request.
        """
        return {'data':[{'id':name,'base_url_env':provider.base_url_env,
                         'api_key_env':provider.api_key_env,
                         'model_count':len(config.provider_models(name))}
                        for name,provider in config.providers.items()]}

    @app.get('/api/providers/{provider_name}/models')
    async def provider_models(provider_name:str, refresh:bool=False):
        """Return configured candidates, or query the provider on explicit refresh.

        The UI's test button is the only caller that requests ``refresh=1``;
        there is no periodic active health/model probe.  Keys are used only in
        the Authorization header and are never included in the response.
        """
        if provider_name not in config.providers:
            raise HTTPException(404,'unknown provider')
        configured=config.provider_models(provider_name)
        if not refresh:
            return {'provider':provider_name,'refreshed':False,'source':'config',
                    'available':None,'data':configured}
        provider=config.providers[provider_name]
        base=os.getenv(provider.base_url_env,'').strip().rstrip('/')
        key=os.getenv(provider.api_key_env,'').strip()
        if not base or not key:
            return {'provider':provider_name,'refreshed':True,'source':'upstream',
                    'available':False,'error':'missing provider environment variable',
                    'data':[]}
        url=base if base.endswith('/models') else base+'/models'
        def fetch_models():
            # OpenCode's edge rejects Python's default ``urllib`` user agent
            # (HTTP 403/Cloudflare 1010).  Identify this explicit model-list
            # probe while keeping the API key confined to the Authorization header.
            req=urllib.request.Request(url,headers={'Authorization':f'Bearer {key}',
                                                     'Accept':'application/json',
                                                     'User-Agent':'flex-router-model-test/1.0'})
            with urllib.request.urlopen(req,timeout=12) as response:
                return json.loads(response.read().decode('utf-8'))
        try:
            payload=await asyncio.to_thread(fetch_models)
            raw=payload.get('data',payload) if isinstance(payload,dict) else payload
            result=[]
            if isinstance(raw,list):
                for item in raw:
                    if isinstance(item,str): result.append({'id':item})
                    elif isinstance(item,dict) and item.get('id'):
                        # Preserve public model metadata when the Provider
                        # exposes it; never include credentials or raw payloads.
                        result.append({key:item.get(key) for key in (
                            'id','owned_by','display_name','max_input_tokens',
                            'max_tokens','capabilities') if item.get(key) is not None})
            return {'provider':provider_name,'refreshed':True,'source':'upstream',
                    'available':True,'data':result}
        except Exception as exc:
            detail=str(exc).replace(key,'[redacted]')[:240]
            return {'provider':provider_name,'refreshed':True,'source':'upstream',
                    'available':False,'error':detail,'data':[]}

    @app.get('/api/runners')
    async def runner_list():
        return {'data':config.runner_models()}

    @app.get('/api/dashboard')
    async def dashboard_data(hours:int=24):
        """First-section data: recently called Runners, sorted by 1-hour calls.

        A Runner appears only when one of its member Channels was actually
        called (any outcome) inside the 24-hour window; the grouping key is the
        real ``pool`` of the attempt, so a Runner that merely *shares* a Channel
        with the called one does not show up.  Direct (non-Runner) Channel calls
        are shown under the Channel's own name.
        """
        now=time.time(); since24=now-max(1,min(hours,168))*3600; since1=now-3600
        stats=state.dashboard_stats(since24,since1)
        long_cooldown=int(read_setup().get('FLEX_DASHBOARD_LONG_COOLDOWN','1800'))
        def status_of(enabled,cooling,n1,ok1):
            if not enabled:
                return {'color':'gray','label':'已停用'}
            if cooling and cooling['until']-now>long_cooldown:
                return {'color':'gray','label':'下线（冷却至 '+clock(cooling['until'])+'）'}
            if n1<=0:
                return {'color':'gray','label':'1 小时无调用'}
            ratio=ok1/n1
            if ratio>=0.9:
                return {'color':'green','label':f'可用 {ratio:.0%}'}
            if ratio>=0.3:
                return {'color':'yellow','label':f'受限 {ratio:.0%}（429/错误）'}
            return {'color':'red','label':f'低可用 {ratio:.0%}'}
        groups=[]
        for name,runner in config.runners.items():
            rows=[]
            for ch_id in runner.channels:
                ch=config.channels.get(ch_id)
                if ch is None: continue
                st=stats.get((name,ch_id),{})
                cooling=state.cooldown_state(name,ch_id)
                enabled=state.is_enabled(name,ch_id)
                rows.append({'id':ch_id,'provider':ch.provider,'model':ch.litellm_model,
                             'enabled':enabled,'calls_1h':st.get('n1',0),'calls_24h':st.get('n24',0),
                             'last_used_at':st.get('last'),
                             'context_window_tokens':ch.context_window_tokens,
                             'max_output_tokens':ch.max_output_tokens,
                             'learned_max_output_tokens':learned_max_output_tokens.get(ch_id),
                             'effective_max_output_tokens':channel_output_cap(ch),
                             'status':status_of(enabled,cooling,st.get('n1',0),st.get('ok1',0)),
                             'cooldown_until':cooling['until'] if cooling else None})
            if not rows: continue
            calls24=sum(r['calls_24h'] for r in rows)
            if calls24<=0: continue
            groups.append({'name':name,'public_model':runner.public_model,
                           'calls_1h':sum(r['calls_1h'] for r in rows),'calls_24h':calls24,
                           'last_used_at':max([r['last_used_at'] or 0 for r in rows],default=0) or None,
                           'recent':state.runner_recent_tone(name,20),
                           'channels':rows})
        # ``direct:*`` attempts are internal probe/self-test activity whose
        # trace_id never appears in request_traces; they are not user-visible
        # Runner or Channel calls and must not be shown here.
        # Ordering is the cached 24-hour activity rank (recomputed hourly);
        # live lights/counts inside each row stay fresh.
        order=state.runner_activity_order(since24)
        rank=order['rank']; position={name:i for i,name in enumerate(rank)}
        groups.sort(key=lambda g:(position.get(g['name'],len(rank)),-g['calls_24h'],-(g['last_used_at'] or 0)))
        return {'generated_at':now,'hours':hours,
                'activity_cache':{'source':order['source'],'computed_at':order['computed_at'],
                                  'max_age_seconds':order['max_age_seconds']},
                'runners':groups}

    @app.get('/api/quota')
    async def quota_dashboard():
        """Per-Channel quota & usage dashboard.

        A Channel can be a member of several Runners, so every usage figure here
        is aggregated **across all pools** for that Channel: provider quotas are
        billed per account, not per Runner.

        For each configured Channel (config order):
          running - config-enabled, and whether it currently sits in cooldown;
          limits  - explicitly configured rpm/tpm/5h-window cap (``None`` = unset);
          learned - safe rpm/tpm the Router inferred from observed 429s, plus the
                    confidence sample count and the last limit kind seen;
          used    - calls/tokens actually consumed in the 5h sliding window, today,
                    yesterday, and the live last-24h figure.
        """
        now=time.time(); today=time.strftime('%Y-%m-%d',time.localtime(now))
        yesterday=time.strftime('%Y-%m-%d',time.localtime(now-86400))
        with state.lock:
            today_rows={r['channel']:dict(r) for r in state.db.execute(
                "SELECT channel,SUM(requests) AS requests,SUM(success) AS success,SUM(failed) AS failed,"
                "SUM(cancelled) AS cancelled,SUM(total_tokens) AS tokens "
                "FROM daily_request_analytics WHERE day=? GROUP BY channel",(today,))}
            yest_rows={r['channel']:dict(r) for r in state.db.execute(
                "SELECT channel,SUM(requests) AS requests,SUM(success) AS success,SUM(failed) AS failed,"
                "SUM(cancelled) AS cancelled,SUM(total_tokens) AS tokens "
                "FROM daily_request_analytics WHERE day=? GROUP BY channel",(yesterday,))}
            # Cooldowns span pools; collect them once.
            cooled={}
            for r in state.db.execute("SELECT channel,until,reason FROM states WHERE until IS NOT NULL AND until>?",(now,)):
                prev=cooled.get(r['channel'])
                if prev is None or r['until']>prev['until']:
                    cooled[r['channel']]={'until':r['until'],'reason':r['reason']}
        channels=[]
        for cid,ch in config.channels.items():
            limits=ch.limits
            window=limits.window_seconds if limits else 18000
            window_cap=limits.max_requests_per_window if limits else None
            # Cross-pool usage inside the sliding quota window.
            with state.lock:
                used_window=state.db.execute(
                    "SELECT COUNT(*) FROM attempts WHERE channel=? AND started>=?",(cid,now-window)).fetchone()[0]
                live24=state.db.execute(
                    "SELECT COUNT(*) AS requests,COALESCE(SUM(total_tokens),SUM(input_tokens),0) AS tokens "
                    "FROM attempts WHERE channel=? AND started>=?",(cid,now-86400)).fetchone()
                # Highest-confidence learned limit row for this Channel (any pool).
                learned_row=state.db.execute(
                    "SELECT safe_rpm,safe_tpm,confidence,last_429_kind FROM learned_limits "
                    "WHERE channel=? ORDER BY confidence DESC LIMIT 1",(cid,)).fetchone()
                # Latest health check result
                latest_test=state.db.execute(
                    "SELECT outcome,error_type,tested_at FROM channel_tests WHERE channel=? ORDER BY tested_at DESC LIMIT 1",(cid,)).fetchone()
            cool=cooled.get(cid)
            # Determine running status based on both cooldown and latest health check
            if not ch.enabled:
                running='已停用'
            elif cool:
                running='冷却中'
            elif latest_test and latest_test['outcome']=='failure':
                err=latest_test['error_type'] or ''
                if 'rate' in err or '429' in err:
                    running='限流中'
                elif 'auth' in err or '401' in err or '403' in err or 'NotFound' in err:
                    running='配置错误'
                else:
                    running='异常'
            else:
                running='运行中'
            empty={'requests':0,'success':0,'failed':0,'cancelled':0,'tokens':0}
            t=today_rows.get(cid) or empty; y=yest_rows.get(cid) or empty
            channels.append({
                'id':cid,'provider':ch.provider,'model':ch.litellm_model,'enabled':ch.enabled,
                'running':running,
                'cooldown_until':float(cool['until']) if cool else None,
                'cooldown_reason':cool['reason'] if cool else None,
                'limits':{'rpm':limits.rpm if limits else None,'tpm':limits.tpm if limits else None,
                          'window_seconds':window,'max_requests_per_window':window_cap},
                'learned':{'safe_rpm':learned_row['safe_rpm'] if learned_row else None,
                           'safe_tpm':learned_row['safe_tpm'] if learned_row else None,
                           'confidence':(learned_row['confidence'] or 0) if learned_row else 0,
                           'last_429_kind':learned_row['last_429_kind'] if learned_row else None},
                'used':{'window_used':used_window,'window_cap':window_cap,
                        'window_pct':(100.0*used_window/window_cap) if window_cap else None,
                        'live24_requests':live24['requests'] or 0,'live24_tokens':live24['tokens'] or 0},
                'today':{k:(t[k] or 0) for k in ('requests','success','failed','cancelled','tokens')},
                'yesterday':{k:(y[k] or 0) for k in ('requests','success','failed','cancelled','tokens')},
            })
        total_today={k:sum(c['today'][k] for c in channels) for k in ('requests','success','failed','cancelled','tokens')}
        total_yest={k:sum(c['yesterday'][k] for c in channels) for k in ('requests','success','failed','cancelled','tokens')}
        return {'generated_at':now,'today':today,'yesterday':yesterday,
                'total_today':total_today,'total_yesterday':total_yest,
                'channels':channels}

    @app.get('/api/runners/{name}/channels')
    async def runner_channels(name:str):
        internal,runner,direct=pool_for(name)
        if runner is None and direct is None:
            raise HTTPException(404,'unknown runner')
        if runner is not None:
            results=[state.channels_state(internal,ch.id,ch.limits,ch.context_window_tokens,
                                          ch.capabilities,ch.litellm_model,ch.retry_policy,ch.provider)
                     for _,ch in config.get_runner_channels(internal)]
            public=runner.public_model
        else:
            ch=direct
            results=[state.channels_state(ch.id,ch.id,ch.limits,ch.context_window_tokens,
                                          ch.capabilities,ch.litellm_model,ch.retry_policy,ch.provider)]
            public=ch.id
        return {'runner':public,'channels':results}
    @app.get('/api/pools/{name}/channels')
    async def channels(name:str):
        internal,pool,direct=pool_for(name)
        if pool is None and direct is None:raise HTTPException(404,'unknown pool')
        results=[]
        if pool is not None:
            for prov, ch in config.get_pool_channels(internal):
                results.append(state.channels_state(internal, ch.id, ch.limits, ch.context_window_tokens, ch.capabilities, ch.litellm_model, ch.retry_policy, ch.provider))
        else:
            ch = direct
            results.append(state.channels_state(ch.id, ch.id, ch.limits, ch.context_window_tokens, ch.capabilities, ch.litellm_model, ch.retry_policy, ch.provider))
        public = pool.public_model if pool else ch.id
        return {'pool':public,'channels':results}
    @app.get('/api/requests')
    async def requests(limit:int=50):return {'data':state.recent(max(1,min(limit,200)))}
    @app.get('/api/traces')
    async def trace_list(limit:int=100):return {'data':state.traces(limit=max(1,min(limit,1000)))}
    @app.get('/api/traces/{trace_id}/full-request')
    async def trace_full_request(trace_id:str,request:Request):
        # Full prompts can include tool output and user secrets.  Keep this
        # diagnostic route local even when the OpenAI-compatible API is shared
        # with other LAN clients.
        peer=(request.client.host if request.client else '')
        if peer not in ('127.0.0.1','::1','localhost'):
            raise HTTPException(403,'full request viewer is local-admin only')
        if not state.full_request_capture_enabled:
            raise HTTPException(409,'full request retention is disabled')
        result=state.full_request_capture(trace_id)
        if result is None:raise HTTPException(404,'full request was not retained or has expired')
        return {'trace_id':trace_id,**result}
    @app.get('/api/traces/{trace_id}')
    async def trace_detail(trace_id:str):
        result=state.trace(trace_id)
        if result is None:raise HTTPException(404,'unknown trace')
        return result
    @app.get('/api/errors')
    async def errors(limit:int=50):return {'data':state.errors(max(1,min(limit,200)))}
    @app.get('/api/statistics/errors')
    async def error_statistics(period:str='day'):
        if period not in ('day','week','month'):raise HTTPException(400,'period must be day, week, or month')
        return state.error_statistics(period)
    @app.get('/api/statistics/errors/hourly')
    async def hourly_error_statistics():return state.hourly_error_statistics()
    @app.get('/api/statistics/calls')
    async def call_statistics(period:str='day',group_by:str='channel'):
        if period not in ('day','week','month') or group_by not in ('channel','pool'):raise HTTPException(400,'invalid statistics query')
        return state.call_statistics(period,group_by)
    @app.get('/api/statistics/calls/hourly')
    async def hourly_call_statistics():return state.hourly_call_statistics()
    @app.get('/api/statistics/calls/quarter-hourly')
    async def quarter_hour_call_statistics():return state.quarter_hour_call_statistics()
    @app.get('/api/statistics/calls/trend')
    async def call_trend_statistics(period:str='day'):
        if period not in ('day','week','month'):raise HTTPException(400,'period must be day, week, or month')
        return state.call_trend_statistics(period)
    @app.get('/api/statistics/requests')
    async def request_statistics(period:str='day',group_by:str='channel'):
        if period not in ('day','week','month') or group_by not in ('channel','pool'):raise HTTPException(400,'invalid statistics query')
        return state.request_statistics(period,group_by)
    @app.get('/api/statistics/requests/hourly')
    async def hourly_request_statistics():return state.hourly_request_statistics()
    @app.get('/api/statistics/requests/quarter-hourly')
    async def quarter_hour_request_statistics():return state.quarter_hour_request_statistics()
    @app.get('/api/statistics/requests/trend')
    async def request_trend_statistics(period:str='day'):
        if period not in ('day','week','month'):raise HTTPException(400,'period must be day, week, or month')
        return state.request_trend_statistics(period)
    @app.get('/api/statistics/response-times')
    async def response_time_statistics(period:str='day',group_by:str='channel'):
        if period not in ('day','week','month') or group_by not in ('channel','pool'):
            raise HTTPException(400,'invalid statistics query')
        return state.response_time_statistics(period,group_by)
    @app.get('/api/statistics/duplicates')
    async def duplicate_statistics(period:str='day'):
        if period not in ('day','week','month'):raise HTTPException(400,'invalid statistics query')
        return state.duplicate_statistics(period)
    @app.get('/api/statistics/protocol-errors')
    async def protocol_error_statistics(period:str='day'):
        if period not in ('day','week','month'):raise HTTPException(400,'invalid statistics query')
        return state.protocol_error_statistics(period)
    @app.get('/api/statistics/anomalies')
    async def anomaly_statistics(hours:int=24, hang_seconds:int=120):
        """最近 hours 小时的异常事件数量（失败/取消 + 疑似挂住）。"""
        return state.anomaly_summary(max(1,min(hours,168)),max(30,hang_seconds))
    @app.post('/api/runners/{name}/channels/{channel_id}/reset')
    @app.post('/api/pools/{name}/channels/{channel_id}/reset')
    async def reset_channel(name:str,channel_id:str,scope:str='all'):
        if scope not in ('all','quota','cooldown'):raise HTTPException(400,'scope must be all, quota, or cooldown')
        internal,pool,direct=pool_for(name)
        if pool is None and direct is None:raise HTTPException(404,'unknown pool')
        # 运行态键必须与实际选路使用的 key 一致：Runner 内用 Runner 名，
        # 直连通道用 'direct:<id>'；此前误用 channel_id 当 pool，导致 Reset
        # 和 Disable 静默无效。
        state_key=internal if pool is not None else f'direct:{direct.id}'
        state.reset(state_key,channel_id,scope); logger.warning('pool=%s channel=%s reset scope=%s',state_key,channel_id,scope)
        return {'pool':name,'channel':channel_id,'reset':scope}
    @app.post('/api/runners/{name}/channels/{channel_id}/enabled')
    @app.post('/api/pools/{name}/channels/{channel_id}/enabled')
    async def channel_enabled(name:str,channel_id:str,value:bool):
        internal,pool,direct=pool_for(name)
        if pool is None and direct is None:raise HTTPException(404,'unknown pool')
        state_key=internal if pool is not None else f'direct:{direct.id}'
        state.set_enabled(state_key,channel_id,value); logger.warning('pool=%s channel=%s enabled=%s',state_key,channel_id,value)
        return {'pool':name,'channel':channel_id,'enabled':value}
    @app.post('/api/runners/{name}/channels/{channel_id}/test')
    @app.post('/api/pools/{name}/channels/{channel_id}/test')
    async def test_channel(name:str,channel_id:str):
        internal,pool,direct=pool_for(name)
        if pool is None and direct is None:raise HTTPException(404,'unknown pool')
        ch = next((c for _, c in config.get_pool_channels(internal) if c.id == channel_id), None) if pool else direct
        if ch is None or ch.id != channel_id:raise HTTPException(404,'unknown channel')
        attempt=state.start(channel_id,ch.id,ch.litellm_model,input_tokens=5); started=time.monotonic()
        try:
            base,key=channel_credentials(ch, config.providers)
            await litellm.acompletion(model=ch.litellm_model,api_base=base,api_key=key,messages=[{'role':'user','content':'Reply with exactly OK.'}],max_tokens=256,**channel_request_kwargs(ch))
        except Exception as e:
            typ=error_type(e); detail=error_detail(e); latency=int((time.monotonic()-started)*1000); state.finish(attempt,'failure',typ,latency,error_detail=detail,error_code=error_code(e)); state.record_test(internal,ch.id,'failure',typ,latency,detail)
            if typ in ('rpm_limit','quota_exhausted'):state.observe_429(channel_id,ch.id,detail,limits=ch.limits)
            logger.warning('channel test failed channel=%s error=%s detail=%s',channel_id,typ,detail)
            raise HTTPException(error_code(e) or 502,{'channel':ch.id,'outcome':'failure','error_type':typ,'error_detail':detail,'latency_ms':latency}) from e
        output,total=usage_tokens(response) if 'response' in locals() else (None,None); latency=int((time.monotonic()-started)*1000); state.finish(attempt,'success',latency=latency,output_tokens=output,total_tokens=total); state.observe_success(channel_id,ch.id); state.record_test(internal,ch.id,'success',latency=latency)
        logger.info('channel test succeeded channel=%s latency_ms=%s',channel_id,latency)
        return {'channel':ch.id,'outcome':'success','latency_ms':latency}
    @app.post('/api/admin/restart')
    async def restart():
        if app.state.restart_draining:
            return {'status':'restarting','drain_seconds':3,'active_requests':len(app.state.active_restart_signals)}
        app.state.restart_draining=True
        active=list(app.state.active_restart_signals.items())
        for trace_id,signal in active:
            state.trace_event(trace_id,'router_restart_requested',http_status=503,detail='Core restart requested; sending terminal router_restarting error before restart.')
            signal.set()
        target=f"gui/{os.getuid()}/{os.getenv('FLEX_LAUNCHD_LABEL','com.weifeng.flex-llm-router')}"
        async def later():
            # Give active SSE connections a bounded chance to receive their
            # terminal error.  New requests are rejected while draining.
            await asyncio.sleep(3)
            result=subprocess.run(['/bin/launchctl','kickstart','-k',target],capture_output=True,text=True)
            if result.returncode:logger.error('launchd restart failed: %s',result.stderr.strip())
            else:logger.warning('launchd restart requested: %s',target)
        asyncio.create_task(later())
        return {'status':'restarting','drain_seconds':3,'active_requests':len(active)}
    @app.post('/v1/chat/completions')
    async def chat(request:Request):
        if app.state.restart_draining:
            raise HTTPException(503,{'error':{'type':'router_restarting','detail':'Router is restarting; retry this request shortly.'}})
        body=await request.json(); name=body.pop('model',None); stream=bool(body.pop('stream',False)); internal,pool,direct=pool_for(name)
        rid='r-'+uuid.uuid4().hex
        app.state.last_external_call=time.time()  # 标记最后一次外部调用，空闲探测据此暂停
        req_started=time.monotonic()  # 请求起点(排队等待计时用, 早于通道选择)
        if pool is None and direct is None:raise HTTPException(404,f'unknown model: {name}')
        if not isinstance(body.get('messages'),list):raise HTTPException(400,"request field 'messages' is required")
        preview,context_summary=trace_input_summary(body['messages'])
        caller=client_label(request.headers)
        fingerprint=state.request_fingerprint(name,body)
        state.trace_begin(rid,name,internal or name,preview,context_summary,stream,caller,request_fingerprint=fingerprint)
        restart_event=asyncio.Event()
        # Streaming calls remain open after the handler returns.  They are
        # the in-flight responses that need an explicit terminal SSE error
        # during a drain; ordinary JSON calls have not committed a response
        # and launchd will close them within the same bounded restart window.
        if stream:
            app.state.active_restart_signals[rid]=restart_event
        def unregister_restart_signal():
            app.state.active_restart_signals.pop(rid,None)
        state.capture_full_request(rid,caller,name,internal or name,{'model':name,'stream':stream,**body})
        runner_scope=internal or name
        # Replay keys carry the Runner identity as well as the public model,
        # so identical payloads sent to distinct Runners never share a result.
        replay_scope=f'{runner_scope}\0{name}'
        if state.duplicate_observer_enabled:
            duplicates=state.observe_inflight_duplicates(rid,caller,name,runner_scope,fingerprint)
            for duplicate in duplicates:
                detail=(f"与进行中 Trace {duplicate['trace_id']} 的请求内容完全一致；重叠 "
                        f"{duplicate['overlap_ms']/1000:.1f}s；旧请求" + ('已开始输出。' if duplicate['has_output'] else '尚未输出。'))
                state.trace_event(rid,'duplicate_observed',detail=detail)
                state.trace_event(duplicate['trace_id'],'duplicate_observed',detail=f'新 Trace {rid} 与本请求完全一致；仅记录，不取消任何请求。')
        # 对极短时间窗中的安全幂等请求直接恢复完整结果，避免 Hermes 的批准类请求
        # 在上游已成功后因网络重发而再次占用模型。stream/tool 请求绝不进入该路径。
        replay_allowed=not stream and not body.get('tools') and not body.get('tool_choice')
        if replay_allowed:
            replay=state.replay_lookup(caller,replay_scope,fingerprint)
            if replay:
                state.trace_event(rid,'replay_hit',replay['channel'],detail=f'完全相同的非流式无工具请求命中 {RESPONSE_REPLAY_SECONDS} 秒结果恢复窗口；复用 {replay["age_seconds"]:.1f} 秒前的成功结果，不调用上游。')
                state.trace_finish(rid,'success',replay['channel'],200,output_preview=response_preview(replay['payload']),latency_ms=int((time.monotonic()-req_started)*1000))
                state.debug_log(rid,'client_out',pool=internal or name,channel=replay['channel'] or '',model=name,status=200,body=json.dumps(replay['payload'],ensure_ascii=False)[:20000])
                return JSONResponse(content=replay['payload'],headers={'X-Flex-Response-Replay':'hit'})
        state.debug_log(rid,'client_in',pool=name,model=name,body=json.dumps(body,ensure_ascii=False))
        # Resolve channel list + policy for this request
        if direct is not None:
            # 直连路径: 状态命名空间独立于 POOL, 统一加 direct: 前缀, 与真实 pool 状态互不污染
            key = f'direct:{direct.id}'
            channels = [direct]
            sel = {'strategy':'single','retry_next_channel_on':[]}
            affinity = {'enabled':False,'idle_seconds':1200,'minimum_messages':2}
            reserve = 8192
        else:
            key = internal
            channels = [c for _, c in config.get_pool_channels(internal)]
            sel = pool.selection
            affinity = effective_affinity(pool)
            reserve = pool.context_policy.get('reserve_output_tokens', 8192)
        tried=set(); last='no_eligible_channel'; retries=0; quota_retry_channel=None; engine_retry_channel=None; five_hour_retry_channel=None
        # A request that receives a real RPM/TPM response must remain pinned to
        # the Channel that produced it while it backs off.  Other *new*
        # requests may still use the cooled Channel's siblings, but this
        # request must not silently change provider/model semantics.
        limit_retry_channel=None
        policy_fallback_active=False; policy_fallback_tried=set(); policy_fallback_channels=[]
        # 通道级 429 策略（每日配额/次日重置）封掉的通道不会被写进本轮 rejected
        # （选路循环每轮都重建 rejected），但调用方看到的“无可用通道”真相就是它：
        # 大窗兜底通道被配额封禁后，剩下的通道窗口不够，才是本次失败的根因。
        # 这里单独累计，随最终拒绝详情一起返回，避免把它误报成纯上下文超窗。
        policy_block_reasons={}
        # Becomes ``protocol`` only when this request leaves its original
        # Channel because a configured protocol-compatibility rule matched.
        affinity_kind='normal'
        def configured_policy_fallbacks(exclude_id=None):
            """Prefer marked Channels in this Runner, then global ordered fallbacks."""
            local=[c for c in channels if c.chn_content_policy_fallback and c.enabled and c.id!=exclude_id]
            if local:
                return local
            ids=config.global_fallback.get('chn_content_policy',[]) if isinstance(config.global_fallback,dict) else []
            return [config.channels[cid] for cid in ids if cid in config.channels and config.channels[cid].chn_content_policy_fallback and config.channels[cid].enabled and config.channels[cid].id!=exclude_id]
        retry_steps={}  # 分级退避计数: {'tpm_limit':n,'rpm_limit':m} 两类独立
        limit_escalated=set()  # (error kind, channel) 已进入第二阶段的标记
        while True:
            request_channels = policy_fallback_channels if policy_fallback_active else channels
            if policy_fallback_active and not request_channels:
                detail='原 Channel 触发中国内容政策限制，但未配置可用的全局 CHN Content Policy Fallback Channel'
                state.trace_event(rid,'policy_fallback_unavailable',detail=detail)
                state.trace_finish(rid,'failed',http_status=502,error_type='content_policy_blocked',error_detail=detail,latency_ms=int((time.monotonic()-req_started)*1000))
                raise HTTPException(502,{'error_type':'content_policy_blocked','error_detail':detail})
            # 每次选路只保留本轮拒绝原因；不能把上一轮的 busy 重复拼进调用轨迹。
            available=[]; rejected=[]; prompt_by_channel={}; full_capacity=[]
            requested_output=requested_output_tokens(body,reserve)
            for c in request_channels:
                # 配额、引擎异常和 RPM/TPM 限流的原请求必须回到发生异常
                # 的同一 Channel 做验证；不能被调度器静默换走。
                validation_channel=quota_retry_channel or engine_retry_channel or five_hour_retry_channel or limit_retry_channel
                if validation_channel is not None and c.id!=validation_channel:continue
                if not state.is_enabled(key,c.id) or c.id in tried:continue
                prompt_tokens=input_tokens(body,c.litellm_model); prompt_by_channel[c.id]=prompt_tokens
                ok,reason,total=compatibility(reserve,c,body,stream,prompt_tokens)
                if not ok:
                    rejected.append({'channel':c.id,'reason':reason}); continue
                eligible,reason=state.eligible(key,c.id,c.limits,total or 0,ignore_five_hour_quota=(c.id==five_hour_retry_channel))
                if eligible:
                    available.append(c)
                    cap=channel_output_cap(c)
                    if cap is None or requested_output<=cap: full_capacity.append(c)
                else:rejected.append({'channel':c.id,'reason':reason})
            # 输出预算偏好：优先选“能完整满足请求 max_tokens”的通道；只有没有任何
            # 通道满足时才退而用夹取（call_upstream 会把 max_tokens 夹到通道上限），
            # 这样既不会把大预算请求误判为不可用，也不会牺牲本可完整满足的通道。
            if full_capacity and len(full_capacity)<len(available):
                skipped=[c.id for c in available if c not in full_capacity]
                state.trace_event(rid,'output_capacity_preferred',detail=f'优先可完整满足 max_tokens={requested_output} 的通道；暂不选 {", ".join(skipped)}（需夹取）')
                available=full_capacity
            # TPM 预判（偏好而非硬拦截）：历史数据表明 429 主要来自“最近 60 秒
            # token 量 + 本次大上下文”超过上游 TPM。若学习到了可信的安全 TPM，
            # 就优先避开当前会撞线的通道；仅当所有通道都接近阈值时保持 best-effort
            # 发送，绝不因预判本身把请求判死。
            if len(available)>1:
                paced=[c for c in available if tpm_pacing_ok(state,key,c,prompt_by_channel.get(c.id,0))]
                if paced and len(paced)<len(available):
                    skipped=[c.id for c in available if c not in paced]
                    state.trace_event(rid,'tpm_pacing_skip',detail=f'按学习安全 TPM 暂避接近上限的通道：{", ".join(skipped)}；优先 {", ".join(c.id for c in paced)}')
                    available=paced
            if not available:
                # B类时间窗类拒绝(rpm/tpm/learned_*)且请求开始未超排队上限: 排队等待窗口恢复, 不立即503.
                reasons={r.get('reason') for r in rejected}
                transient = reasons & {'rpm','tpm','learned_rpm','learned_tpm','busy','quota_suspect','quota_confirmed','engine_suspect','engine_unavailable'}
                # 只有持有验证序列的原对话等待疑似配额冷却；其他新请求应立即选择备用或快速失败。
                if quota_retry_channel is None and engine_retry_channel is None and five_hour_retry_channel is None and reasons and reasons <= {'quota_suspect','quota_confirmed','engine_suspect','engine_unavailable'}:transient=set()
                # The local five-hour counter is a guardrail, not proof that the
                # upstream rejects the next request. Retain this request and verify
                # the same channel at 1/5/10/20/40... minutes; never empty-probe.
                if not available and reasons and reasons <= {'five_hour_quota','five_hour_quota_retry'}:
                    target=next((r.get('channel') for r in rejected if r.get('reason') in {'five_hour_quota','five_hour_quota_retry'}),None)
                    if target:
                        step=retry_steps.get('five_hour_quota',0)
                        # 1 / 5 / 10 / 20 minutes, then every 30 minutes.
                        delay=(60,300,600,1200)[step] if step<4 else 1800
                        retry_steps['five_hour_quota']=step+1; five_hour_retry_channel=target
                        state.cooldown(key,target,delay,'five_hour_quota_retry')
                        state.trace_event(rid,'five_hour_quota_retry_wait',target,detail=f'本地五小时调用计数达到上限；原请求验证第 {step+1} 次，等待 {delay//60} 分钟后重试同一 Channel（1/5/10/20 分钟，之后每 30 分钟）。期间不发送后台探测。')
                        await asyncio.sleep(delay)
                        continue
                waited = time.monotonic() - req_started
                if transient:
                    logger.info('req=%s queued %.0fs (rejected=%s)', name, waited, [r.get('reason') for r in rejected])
                    cooldowns=[]
                    for item in rejected:
                        cooling=state.cooldown_state(key,item['channel'])
                        if cooling:
                            cooldowns.append((item['channel'],cooling))
                    cooldown_seen=bool(cooldowns)
                    if cooldowns:
                        channel,cooling=min(cooldowns,key=lambda item:item[1]['until'])
                        cap=QUEUE_TPM_SECONDS if cooling.get('limit_kind')=='tpm' else QUEUE_RPM_SECONDS
                        if waited >= cap:
                            cooldowns=[]
                        else:
                            seconds=max(0,cooling['until']-time.time())
                            # 直接睡到最早恢复的通道（或该类型的总退避上限），不再每 5 秒无效检查。
                            delay=min(seconds,max(0,cap-waited))
                            detail=(f'{channel}：{cooldown_label(cooling)}，已进入指数退避，将于 {clock(cooling["until"])} 恢复'
                                    f'（剩余 {remaining_clock(seconds)}）。当前没有可用备用通道，等待冷却结束。')
                            state.trace_event(rid,'queue_wait',channel,detail=detail)
                            await asyncio.sleep(delay)
                            continue
                    if not cooldown_seen and waited < QUEUE_RPM_SECONDS:
                        blocked=', '.join(f"{item['channel']}: {item['reason']}" for item in rejected)
                        state.trace_event(rid,'queue_wait',detail=f'上游限流尚未形成冷却时间；等待 15 秒后重试。{blocked}')
                        await asyncio.sleep(min(15,max(0,QUEUE_RPM_SECONDS-waited)))
                        continue  # 重走选通道循环
                detail={'error':f'no eligible channel for {name}','last_error':last,'rejected':rejected}
                # 若唯一不可用原因是通道 429 策略封禁，把语义正确的 429 返回
                # 给调用方，并附上解封时刻，而不是伪装成 503 服务不可用。
                policy_blocked=[r for r in rejected if r.get('reason') in ('quota_daily','policy_cooldown')]
                if policy_blocked and all(r.get('reason') in ('quota_daily','policy_cooldown') for r in rejected):
                    cooling=[(r['channel'],state.cooldown_state(key,r['channel'])) for r in policy_blocked]
                    until=max((c[1]['until'] for c in cooling if c[1]),default=None)
                    blocked_detail={'error_type':policy_blocked[0]['reason'],'last_error':last,
                                    'blocked':[{'channel':c,'until':cool['until'] if cool else None,
                                                'remaining_seconds':int(cool['until']-time.time()) if cool else None}
                                               for c,cool in cooling],
                                    'message':f'上游 429 后在通道策略封禁期内；{name} 暂无可接受该请求的通道。'}
                    state.trace_event(rid,'quota_policy_blocked_all',http_status=429,detail=json.dumps(blocked_detail,ensure_ascii=False)[:400])
                    state.trace_finish(rid,'failed',http_status=429,error_type=policy_blocked[0]['reason'],error_detail=json.dumps(blocked_detail,ensure_ascii=False)[:800],latency_ms=int((time.monotonic()-req_started)*1000))
                    raise HTTPException(429,blocked_detail)
                # 所有通道都因“上下文超窗”被拒：给调用方明确的拒绝语义（含超出的
                # token 数与可用的最大窗口），请其 Compact 后重试，而不是硬发给装
                # 不下的通道或伪装成 503 服务不可用。
                ctx_rej=[r for r in rejected if r.get('reason') and r.get('reason').startswith('context_exceeded:')]
                if ctx_rej and len(ctx_rej)==len(rejected):
                    try:
                        total=int(rejected[0]['reason'].split(':')[1].split('>')[0])
                    except Exception:
                        total=0
                    window=max((ch.context_window_tokens for ch in request_channels if ch.context_window_tokens),default=0)
                    blocked=[{'channel':b['channel'],'reason':b['reason'],'until':b.get('until'),
                              'remaining_seconds':max(0,int(b['until']-time.time())) if b.get('until') else None}
                             for b in policy_block_reasons.values()]
                    blocked_list=[{'channel':b['channel'],'reason':'quota_policy_blocked:'+str(b['reason']),
                                  'until':b.get('until')} for b in policy_block_reasons.values()]
                    if blocked:
                        blocked_names=', '.join('{}（{}，至 {}）'.format(b['channel'],b['reason'],clock(b.get('until')))
                                                for b in policy_block_reasons.values())
                        ctx_message=(f'请求 {total} token 超出 {name} 的上下文窗口 {window}；当前能承载该请求的通道全部不可用：'
                                     f'{blocked_names}。先对对话执行 Compact（压缩）后重试，或等待上述通道冷却结束。')
                    else:
                        ctx_message=f'请求 {total} token 超出 {name} 的上下文窗口 {window}；请先对对话执行 Compact（压缩）后重试。'
                    ctx_detail={'error_type':'context_exceeded','request_tokens':total,'max_context_window':window,
                                'message':ctx_message,'policy_blocked':blocked,'rejected':rejected+blocked_list}
                    state.trace_event(rid,'context_exceeded_rejected',http_status=400,detail=json.dumps(ctx_detail,ensure_ascii=False)[:400])
                    state.trace_finish(rid,'failed',http_status=400,error_type='context_exceeded',error_detail=json.dumps(ctx_detail,ensure_ascii=False)[:800],latency_ms=int((time.monotonic()-req_started)*1000))
                    raise HTTPException(400,ctx_detail)
                state.trace_finish(rid,'failed',http_status=503,error_type='no_eligible_channel',error_detail=json.dumps(detail,ensure_ascii=False)[:800],latency_ms=int((time.monotonic()-req_started)*1000))
                raise HTTPException(503,detail)
            sticky=state.affinity_channel(key,body['messages'],affinity['idle_seconds'],affinity['minimum_messages'],affinity.get('protocol_idle_seconds')) if affinity.get('enabled') else None
            ch=next((candidate for candidate in available if candidate.id==sticky),None) or (available[0] if policy_fallback_active else scheduler.select(key,available,state,sel,tiers=pool.tiers if pool is not None else None)); request_input_tokens=input_tokens(body,ch.litellm_model); attempt=state.start(key,ch.id,ch.litellm_model,input_tokens=request_input_tokens,trace_id=rid); started=time.monotonic()
            state.trace_event(rid,'channel_selected',ch.id,detail='session affinity hit' if sticky==ch.id else 'scheduler selected channel')
            state.trace_attempt(rid,ch.id)
            initial_hedges_started=0
            hedge_plan=[] if policy_fallback_active else hedge_plan_for(internal,request_channels,ch,pool,request_input_tokens)
            first_activity_deadline=first_activity_deadline_for(request_channels,request_input_tokens,pool)
            channel_by_id={candidate.id:candidate for candidate in request_channels}
            # The watchdog is owned by the core (7800), not the UI.  It sends
            # deadline signals through this queue, so a hung provider request
            # cannot depend on this coroutine's own timeout calculation.
            watchdog_signals=asyncio.Queue()
            downstream_timeout_event=asyncio.Event()
            app.state.first_activity_watch[rid]={'started':req_started,'signals':watchdog_signals,'sent':set(),'hedge_plan':hedge_plan,'deadline_seconds':first_activity_deadline,
                                                 # A response object can be handed to ASGI before the streaming
                                                 # generator is actually entered.  Keep Hedge tasks created in
                                                 # that gap here so the generator can adopt them later.
                                                 'handoff_hedges':{},'handoff_hedge_stages':set(),'stream_consumer_started':False}
            def stop_first_activity_watch():
                app.state.first_activity_watch.pop(rid,None)
            clamped_targets=set()
            async def call_upstream(target):
                target_base,target_key=channel_credentials(target,config.providers)
                target_body=body
                target_cap=channel_output_cap(target)
                if target_cap:
                    for field in ('max_completion_tokens','max_tokens'):
                        value=body.get(field)
                        if isinstance(value,int) and value>target_cap:
                            target_body=dict(body); target_body[field]=target_cap
                            if target.id not in clamped_targets:
                                clamped_targets.add(target.id)
                                state.trace_event(rid,'output_clamped',target.id,detail=f'{field}={value} 超过通道上限 {target_cap}；按 {target_cap} 发送（客户端给的是输出上限，不是硬需求）')
                            break
                state.trace_event(rid,'upstream_task_started',target.id,detail=f'LiteLLM call started; response timeout {UPSTREAM_RESPONSE_TIMEOUT}s')
                started_at=time.monotonic()
                try:
                    response=await await_bounded(
                        litellm.acompletion(**target_body,model=target.litellm_model,api_base=target_base,api_key=target_key,stream=stream,**channel_request_kwargs(target)),
                        UPSTREAM_RESPONSE_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    state.trace_event(rid,'upstream_response_timeout',target.id,504,detail=f'No LiteLLM response object within {UPSTREAM_RESPONSE_TIMEOUT}s; detached provider task')
                    raise
                except Exception as exc:
                    state.trace_event(rid,'upstream_error_received',target.id,error_code(exc),f'LiteLLM raised {error_type(exc)} before response')
                    raise
                state.trace_event(rid,'upstream_response_received',target.id,detail=f'LiteLLM response object received after {time.monotonic()-started_at:.2f}s')
                return response
            # Shared with both the pre-response watchdog callback and the
            # outer HTTP handler, so a forced deadline is only finalized once.
            deadline_forced={'value':False}
            async def prefetch_first_sse(resp,attempt_id,attempt_started,label,target):
                """Begin reading a provider stream immediately after LiteLLM
                returns its response object, before ASGI enters the body.

                ASGI can schedule that body later (or a downstream handoff can
                become half-open).  Leaving the first read inside ``events``
                meant upstream output was not observed during that gap.
                """
                iterator=BufferedUpstreamStream(resp)
                state.debug_log(rid,'router_hedge',pool=key,model=name,body=f'PREFETCH_FIRST_SSE start ch={target.id} att={str(attempt_id)[-8:]} waiting<={UPSTREAM_FIRST_CHUNK_TIMEOUT}s')
                state.trace_event(rid,'upstream_first_sse_wait_started',target.id,detail=f'Router buffering raw SSE events before downstream streaming; waiting up to {UPSTREAM_FIRST_CHUNK_TIMEOUT}s for first event')
                first=await await_bounded(anext(iterator),UPSTREAM_FIRST_CHUNK_TIMEOUT)
                state.trace_event(rid,'upstream_first_sse',target.id,detail=f'First SSE event received within {UPSTREAM_FIRST_CHUNK_TIMEOUT}s (buffered before downstream dispatch)')
                return iterator,first,attempt_id,attempt_started,label,target
            async def await_initial_response():
                """Hedge while the provider has not even returned a response object yet."""
                nonlocal initial_hedges_started, ch, attempt, started
                # Keep the last concrete failure so an exhausted Hedge plan
                # cannot fall through and return ``None`` to the outer unpack.
                # The caller must see the provider's real error (for example
                # rpm_limit/429), not ``cannot unpack ... NoneType``.
                last_error=None; last_attempt_id=attempt; last_attempt_started=started; last_target=ch
                # Do not rely solely on ``asyncio.wait(active)`` noticing a
                # very fast LiteLLM failure.  A completed-task FIFO makes the
                # RPM/TPM state machine consume every result exactly once,
                # including 429s that arrive between scheduler wake-ups.
                completion_queue=asyncio.Queue()
                def track_upstream_task(task):
                    def notify(done):
                        if not done.cancelled():
                            completion_queue.put_nowait(done)
                    task.add_done_callback(notify)
                    return task
                active={track_upstream_task(asyncio.create_task(call_upstream(ch))):(attempt,started,'original',ch)}
                def launch_hedge_targets(hedge_no):
                    due,target_ids=hedge_plan[hedge_no-1]
                    active_channel_ids={target.id for _,(_,_,_,target) in active.items()}
                    for target_id in target_ids:
                        target=channel_by_id[target_id]
                        same_channel_retry=(len(channels)==1 and target.id==ch.id)
                        if target.id in active_channel_ids and not same_channel_retry:
                            state.trace_event(rid,'pre_response_hedge_skipped',target.id,detail=f'第 {hedge_no} 个 Hedge 阶段跳过：该 Channel 仍有未完成请求')
                            continue
                        hedge_attempt=state.start(key,target.id,target.litellm_model,input_tokens=input_tokens(body,target.litellm_model),trace_id=rid)
                        hedge_started=time.monotonic(); state.trace_attempt(rid,target.id)
                        state.trace_event(rid,'pre_response_hedge_started',target.id,detail=f'无上游活动已达 {due}s；第 {hedge_no} 个 Hedge 阶段（{len(target_ids)} 个 Channel），发往 {target.id}')
                        active[track_upstream_task(asyncio.create_task(call_upstream(target)))]=(hedge_attempt,hedge_started,f'pre-response hedge {hedge_no}',target)
                        active_channel_ids.add(target.id)
                # Register a direct callback only after the real task exists.
                # The core watchdog invokes it itself at the deadline; this is
                # intentionally not dependent on a queue consumer waking up.
                watch_record=app.state.first_activity_watch[rid]
                def watchdog_start_hedge(hedge_no):
                    nonlocal initial_hedges_started
                    if hedge_no<=initial_hedges_started or hedge_no>len(hedge_plan): return
                    initial_hedges_started=hedge_no
                    launch_hedge_targets(hedge_no)
                # A dedicated event is separate from the hedge queue. Some HTTP
                # stacks can swallow Task.cancel() while stuck in connect/read;
                # the request coroutine must nevertheless wake and return 504.
                deadline_due={'value':False}
                deadline_event=asyncio.Event()
                def watchdog_cancel_deadline():
                    # Directly close the pre-response phase. A queue signal or
                    # Task.cancel() alone cannot release a cancellation-
                    # resistant provider await.
                    if deadline_forced['value']: return
                    deadline_forced['value']=True
                    deadline_due['value']=True
                    deadline_event.set()
                    detail=f'No upstream response before Router {first_activity_deadline//60}-minute safety deadline; pre-response Hedge hard-stop closed the request.'
                    state.trace_event(rid,'upstream_cancel_requested',http_status=504,detail='Core watchdog requested cancellation of all pending LiteLLM tasks')
                    state.trace_event(rid,'upstream_total_timeout',ch.id,504,detail)
                    now_ms=int((time.monotonic()-req_started)*1000)
                    seen=set()
                    for task,(attempt_id,attempt_started,label,target) in active.items():
                        seen.add(attempt_id)
                        state.finish(attempt_id,'failure','upstream_total_timeout',int((time.monotonic()-attempt_started)*1000),error_detail=detail,error_code=504)
                    if attempt not in seen:
                        state.finish(attempt,'failure','upstream_total_timeout',now_ms,error_detail=detail,error_code=504)
                    state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=now_ms)
                    state.debug_log(rid,'router_deadline',pool=key,model=name,body=f'PRE-RESPONSE HARD STOP at {now_ms/1000:.1f}s active={len(active)}: {detail}')
                    cancel_detached(active)
                    # 硬停即终局：立即从核心看门狗注销，避免已结束请求继续触发
                    # 后续 Hedge 阶段（如 agnes 在硬停后仍被拉起）、重复产生事件。
                    watch_record['on_hedge']=None
                    watch_record['on_deadline']=None
                    app.state.first_activity_watch.pop(rid,None)
                watch_record['on_hedge']=watchdog_start_hedge
                watch_record['on_deadline']=watchdog_cancel_deadline
                # Starlette wraps ``is_disconnected`` in its own anyio
                # CancelScope, which absorbs an outer Task.cancel(): the task
                # keeps polling and can never be cancelled again.  Exit through
                # a flag instead of relying on cancellation, so the handoff
                # never waits on (or leaks) this watcher.
                disconnect_stop=asyncio.Event()
                async def watch_disconnect():
                    while not disconnect_stop.is_set():
                        if await request.is_disconnected(): return True
                        await asyncio.sleep(0.5)
                    return False
                disconnect_task=asyncio.create_task(watch_disconnect())
                watchdog_task=asyncio.create_task(watchdog_signals.get())
                deadline_task=asyncio.create_task(deadline_event.wait())
                completion_task=asyncio.create_task(completion_queue.get())
                try:
                    while active:
                        elapsed=time.monotonic()-req_started
                        remaining=UPSTREAM_FIRST_ACTIVITY_TIMEOUT-elapsed
                        if remaining<=0:
                            for task,(attempt_id,attempt_started,label,target) in active.items():
                                if attempt_id!=attempt:
                                    state.finish(attempt_id,'cancelled','upstream_total_timeout',int((time.monotonic()-attempt_started)*1000),error_detail='No upstream response before Runner safety deadline')
                            cancel_detached(active)
                            raise UpstreamTotalTimeout()
                        next_delay=hedge_plan[initial_hedges_started][0] if initial_hedges_started<len(hedge_plan) else None
                        hedge_wait=max(0,next_delay-elapsed) if next_delay is not None else remaining
                        # Poll completed provider tasks at a short fixed
                        # cadence.  Some HTTP/SDK tasks can report an error
                        # between selector wake-ups; waiting all the way to
                        # the next 3/6-minute Hedge must never delay a 429
                        # state transition.
                        timeout=min(hedge_wait,remaining,1.0)
                        done,_=await asyncio.wait(set(active)|{disconnect_task,watchdog_task,deadline_task,completion_task},timeout=timeout,return_when=asyncio.FIRST_COMPLETED)
                        # 问题1诊断：记录本轮 pre-response 循环的等待/完成状态，
                        # 用于定位“已完成 task 未被消费”的卡点。
                        state.debug_log(rid,'router_wait',pool=key,model=name,body=f'L el={elapsed:.1f}s tmo={timeout:.2f} act={len(active)} wDone={len(done)} aDone={len([t for t in active if t.done()])} ch=[{",".join(tgt.id for _,_,_,tgt in active.values())}]')
                        # A provider task can complete in the same event-loop
                        # turn as a watchdog/control task. Keep that concrete
                        # result so an RPM/TPM exception is harvested before a
                        # time-based Hedge is advanced (or silently skipped).
                        control_done=done
                        completed={task for task in active if task.done()}
                        if completion_task in control_done:
                            notified=completion_task.result()
                            if notified in active:
                                completed.add(notified)
                            completion_task=asyncio.create_task(completion_queue.get())
                        if deadline_task in control_done or deadline_due['value']:
                            for task,(attempt_id,attempt_started,label,target) in active.items():
                                if attempt_id!=attempt:
                                    state.finish(attempt_id,'cancelled','upstream_total_timeout',int((time.monotonic()-attempt_started)*1000),error_detail='Core watchdog cancelled pending hedge at Runner deadline')
                            cancel_detached(active)
                            raise UpstreamTotalTimeout()
                        if watchdog_task in control_done and not completed:
                            signal=watchdog_task.result()
                            watchdog_task=asyncio.create_task(watchdog_signals.get())
                            state.debug_log(rid,'router_hedge',pool=key,model=name,body=f'SIG {signal} inH={initial_hedges_started} plan={len(hedge_plan)} aDone={len(completed)}')
                            if signal=='deadline':
                                for task,(attempt_id,attempt_started,label,target) in active.items():
                                    if attempt_id!=attempt:
                                        state.finish(attempt_id,'cancelled','upstream_total_timeout',int((time.monotonic()-attempt_started)*1000),error_detail='Core watchdog cancelled pending hedge at Runner deadline')
                                cancel_detached(active)
                                raise UpstreamTotalTimeout()
                            hedge_no=int(signal.rsplit('_',1)[1])
                            if hedge_no<=initial_hedges_started:
                                continue
                            initial_hedges_started=hedge_no
                            launch_hedge_targets(hedge_no)
                            continue
                        if disconnect_task in control_done:
                            for task,(attempt_id,attempt_started,label,target) in active.items():
                                task.cancel()
                                if attempt_id!=attempt:
                                    state.finish(attempt_id,'cancelled','client_disconnected',int((time.monotonic()-attempt_started)*1000),error_detail='Downstream disconnected before upstream response')
                            # Detached cleanup must not delay the 504 path.
                            cancel_detached(active)
                            raise ClientDisconnectedBeforeResponse()
                        if completed:
                            done=completed
                        elif not done:
                            # A provider task can finish between the selector
                            # wake-up and this branch (notably when LiteLLM
                            # raises a fast RPM/TPM error).  Harvest completed
                            # tasks before launching a time-based Hedge, so
                            # the normal limit retry state machine sees the
                            # concrete error instead of leaving the Attempt
                            # marked ``started`` until the next Hedge tick.
                            completed={task for task in active if task.done()}
                            if completed:
                                done=completed
                            else:
                                if time.monotonic()-req_started>=UPSTREAM_FIRST_ACTIVITY_TIMEOUT-0.1:
                                    # No provider task completed at the local
                                    # hard deadline.  Do not spin until the
                                    # background watchdog happens to notice;
                                    # that loop may not be running in a test
                                    # client and must not be required for
                                    # correctness.
                                    raise UpstreamTotalTimeout()
                                # A short poll timeout is not itself a Hedge
                                # deadline.  Launch only when the configured
                                # stage is actually due.
                                if next_delay is not None and elapsed>=next_delay:
                                    watchdog_start_hedge(initial_hedges_started+1)
                                continue
                        for task in list(done):
                            if task is disconnect_task: continue
                            attempt_id,attempt_started,label,target=active.pop(task)
                            state.debug_log(rid,'router_task',pool=key,model=name,body=f'POP {label} ch={target.id} att={str(attempt_id)[-8:]} at={elapsed:.1f}s done_tasks={len(list(done))}')
                            try:
                                value=task.result()
                            except asyncio.CancelledError:
                                if deadline_due['value']:
                                    for other,(other_id,other_started,other_label,other_target) in active.items():
                                        if other_id!=attempt:
                                            state.finish(other_id,'cancelled','upstream_total_timeout',int((time.monotonic()-other_started)*1000),error_detail='Core watchdog cancelled pending hedge at Runner deadline')
                                    cancel_detached(active)
                                    raise UpstreamTotalTimeout()
                                raise
                            except asyncio.TimeoutError as hedge_error:
                                # A single provider must not keep the request
                                # blocked until the Runner's first-activity deadline.
                                # Treat the bounded attempt as failed, then
                                # advance the configured Hedge plan while any
                                # other attempts continue in parallel.
                                elapsed_ms=int((time.monotonic()-attempt_started)*1000)
                                last_error=hedge_error; last_attempt_id=attempt_id; last_attempt_started=attempt_started; last_target=target
                                state.debug_log(rid,'router_task',pool=key,model=name,body=f'TIMEOUT {label} ch={target.id} {elapsed_ms}ms')
                                if affinity.get('enabled'):
                                    state.forget_affinity(key,body['messages'],target.id,affinity.get('minimum_messages',2))
                                state.trace_event(rid,'pre_response_timeout',target.id,504,detail=f'{label} exceeded the {UPSTREAM_RESPONSE_TIMEOUT}s response-object bound')
                                if initial_hedges_started < len(hedge_plan):
                                    initial_hedges_started += 1
                                    launch_hedge_targets(initial_hedges_started)
                                    continue
                                if label!='original':
                                    continue
                                raise hedge_error
                            except Exception as hedge_error:
                                last_error=hedge_error; last_attempt_id=attempt_id; last_attempt_started=attempt_started; last_target=target
                                state.debug_log(rid,'router_task',pool=key,model=name,body=f'ERR {label} ch={target.id} type={type(hedge_error).__name__}')
                                if affinity.get('enabled'):
                                    state.forget_affinity(key,body['messages'],target.id,affinity.get('minimum_messages',2))
                                if label=='original':
                                    for other,(other_id,other_started,other_label,other_target) in active.items():
                                        other.cancel()
                                        state.finish(other_id,'cancelled','hedge_cancelled',int((time.monotonic()-other_started)*1000),error_detail='Original upstream request failed')
                                    cancel_detached(active)
                                    raise hedge_error
                                state.finish(attempt_id,'failure',error_type(hedge_error),int((time.monotonic()-attempt_started)*1000),error_detail=error_detail(hedge_error),error_code=error_code(hedge_error))
                                state.trace_event(rid,'pre_response_hedge_error',target.id,error_code(hedge_error),f'{label} failed before response: {error_type(hedge_error)}')
                                continue
                            for other,(other_id,other_started,other_label,other_target) in active.items():
                                other.cancel()
                                state.finish(other_id,'cancelled','hedge_cancelled',int((time.monotonic()-other_started)*1000),error_detail=f'{label} returned first')
                                state.trace_event(rid,'pre_response_hedge_cancelled',other_target.id,detail=f'{other_label} cancelled because {label} returned first')
                            if active: cancel_detached(active)
                            state.debug_log(rid,'router_hedge',pool=key,model=name,body=f'WON {label} ch={target.id} -> handoff at {elapsed:.1f}s att={str(attempt_id)[-8:]}')
                            if label!='original': state.trace_event(rid,'pre_response_hedge_won',target.id,detail=f'{label} returned the first upstream response object')
                            # Response-object arbitration is over.  There is a
                            # small but real gap before ASGI enters the
                            # StreamingResponse generator.  Keep Hedge alive
                            # during that gap instead of assuming that the
                            # response object has already reached the SSE
                            # consumer.  Any tasks launched here are adopted
                            # by ``events()`` when it starts.
                            selected_response=value
                            def handoff_watchdog_start_hedge(hedge_no):
                                nonlocal initial_hedges_started
                                if watch_record.get('stream_consumer_started') or hedge_no<=initial_hedges_started or hedge_no>len(hedge_plan):
                                    return
                                initial_hedges_started=hedge_no
                                due,target_ids=hedge_plan[hedge_no-1]
                                for target_id in target_ids:
                                    if hedge_no in watch_record['handoff_hedge_stages']:
                                        continue
                                    target=channel_by_id[target_id]
                                    hedge_attempt=state.start(key,target.id,target.litellm_model,input_tokens=input_tokens(body,target.litellm_model),trace_id=rid)
                                    hedge_started=time.monotonic(); state.trace_attempt(rid,target.id)
                                    async def handoff_prefetch(target=target, hedge_attempt=hedge_attempt, hedge_started=hedge_started, hedge_no=hedge_no):
                                        hedge_response=await call_upstream(target)
                                        return await prefetch_first_sse(hedge_response,hedge_attempt,hedge_started,f'hedge {hedge_no}',target)
                                    task=asyncio.create_task(handoff_prefetch())
                                    watch_record['handoff_hedges'][task]={'attempt_id':hedge_attempt,'started':hedge_started,'target':target,'number':hedge_no,'prefetched':True}
                                    state.trace_event(rid,'pre_response_hedge_started',target.id,detail=f'无上游活动已达 {due}s；第 {hedge_no} 个 Hedge 阶段（{len(target_ids)} 个 Channel），发往 {target.id}（流式消费者尚未进入，先行启动）')
                                watch_record['handoff_hedge_stages'].add(hedge_no)
                            watch_record['on_hedge']=handoff_watchdog_start_hedge
                            def stream_deadline_hard_stop():
                                if watch_record.get('deadline_forced'): return
                                watch_record['deadline_forced']=True
                                downstream_timeout_event.set()
                                detail=f'No upstream SSE activity before Router {first_activity_deadline//60}-minute safety deadline; watchdog closed the trace before the streaming consumer entered.'
                                state.trace_event(rid,'upstream_cancel_requested',target.id,504,detail='Watchdog hard-stop for a response object whose streaming consumer did not start')
                                state.trace_event(rid,'upstream_total_timeout',target.id,504,detail)
                                state.finish(attempt_id,'failure','upstream_total_timeout',int((time.monotonic()-attempt_started)*1000),error_detail=detail,error_code=504)
                                for handoff_task,info in list(watch_record.get('handoff_hedges',{}).items()):
                                    if not handoff_task.done(): handoff_task.cancel()
                                    state.finish(info['attempt_id'],'cancelled','upstream_total_timeout',int((time.monotonic()-info['started'])*1000),error_detail='Watchdog deadline before streaming consumer entered',error_code=504)
                                prefetched=watch_record.get('prefetched_first_sse')
                                if prefetched is not None:
                                    cancel_detached({prefetched})
                                state.trace_finish(rid,'failed',target.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                                state.debug_log(rid,'router_deadline',pool=key,model=name,body=f'STREAM HARD STOP ch={target.id} at {int((time.monotonic()-req_started))}s: {detail}')
                                close=getattr(selected_response,'aclose',None) or getattr(selected_response,'close',None)
                                if callable(close):
                                    try:
                                        result=close()
                                        if hasattr(result,'__await__'):
                                            cleanup=asyncio.create_task(result)
                                            cleanup.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
                                    except Exception: pass
                                # 硬停即终局：注销看门狗，避免已结束请求继续触发后续 Hedge。
                                watch_record['on_hedge']=None
                                watch_record['on_deadline']=None
                                app.state.first_activity_watch.pop(rid,None)
                            watch_record['deadline_forced']=False
                            watch_record['on_deadline']=stream_deadline_hard_stop
                            if stream:
                                first_sse_task=asyncio.create_task(prefetch_first_sse(value,attempt_id,attempt_started,label,target))
                                watch_record['prefetched_first_sse']=first_sse_task
                                state.trace_event(rid,'watchdog_handoff',target.id,detail='Response object selected; Router began raw SSE buffering before downstream streaming phase')
                            return value,attempt_id,attempt_started,target
                    # Every pre-response attempt ended in an error.  Preserve
                    # the final attempt's identity for trace/error handling and
                    # raise the actual exception instead of implicitly returning
                    # None (which used to mask the root cause as a 502 unpack
                    # error and leave the original Channel as final_channel).
                    if last_error is not None:
                        ch=last_target; attempt=last_attempt_id; started=last_attempt_started
                        raise last_error
                    raise RuntimeError('all pre-response upstream attempts ended without a response')
                finally:
                    disconnect_stop.set()
                    if not disconnect_task.done(): disconnect_task.cancel()
                    if not watchdog_task.done(): watchdog_task.cancel()
                    if not deadline_task.done(): deadline_task.cancel()
                    if not completion_task.done(): completion_task.cancel()
                    # Cancellation cleanup must never delay the handoff: a
                    # provider task stuck in a thread pool cannot be cancelled
                    # promptly, and the StreamingResponse is ready to go now.
                    cleanup={disconnect_task:'disconnect',watchdog_task:'watchdog',deadline_task:'deadline',completion_task:'completion'}
                    done,pending=await asyncio.wait(set(cleanup),timeout=1.0,return_when=asyncio.ALL_COMPLETED)
                    if pending:
                        for task in pending: task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                        state.trace_event(rid,'handoff_cleanup_pending',detail='Cancelled watch task(s) did not stop within 1s: '+', '.join(cleanup[t] for t in pending))
                        for task in pending:
                            if cleanup[task]=='disconnect': task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            try:
                base,key_=channel_credentials(ch, config.providers)
                state.trace_event(rid,'upstream_request',ch.id,detail=f'{ch.provider} · {ch.litellm_model}')
                state.debug_log(rid,'upstream_out',pool=key,channel=ch.id,model=ch.litellm_model,body=json.dumps({'model':ch.litellm_model,'api_base':base,'stream':stream,**body},ensure_ascii=False))
                response,attempt,started,winner_channel=await await_initial_response()
                ch=winner_channel
                base,key_=channel_credentials(ch,config.providers)
                state.trace_event(rid,'handler_response_ready',ch.id,detail=f'Response selected after {int((time.monotonic()-req_started)*1000)}ms; handler proceeds to build the streaming response')
            except ClientDisconnectedBeforeResponse:
                stop_first_activity_watch()
                elapsed=int((time.monotonic()-started)*1000)
                detail='Downstream client disconnected before the upstream returned a response or first SSE event; upstream task cancelled.'
                state.trace_event(rid,'client_disconnected_before_first_token',ch.id,detail=detail)
                state.finish(attempt,'cancelled','client_disconnected',elapsed,error_detail=detail)
                state.trace_finish(rid,'cancelled',ch.id,499,'client_disconnected',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                logger.info('req=%s channel=%s client disconnected before upstream response',name,ch.id)
                return JSONResponse(status_code=499,content={'error':{'message':'client disconnected before upstream response','type':'client_disconnected','param':None,'code':None}})
            except UpstreamTotalTimeout:
                stop_first_activity_watch()
                elapsed=int((time.monotonic()-started)*1000)
                state.debug_log(rid,'router_deadline',pool=key,model=name,body=f'HANDLER UpstreamTotalTimeout at {elapsed/1000:.1f}s forced={deadline_forced["value"]}')
                detail=f'No upstream response before Router {first_activity_deadline//60}-minute safety deadline; cancelled all pending attempts.'
                if affinity.get('enabled'):
                    state.forget_affinity(key,body['messages'],ch.id,affinity.get('minimum_messages',2))
                # The pre-response watchdog may already have persisted the
                # terminal outcome while this coroutine was unwinding. Avoid
                # duplicating completion events and analytics in that case.
                if not deadline_forced['value']:
                    state.trace_event(rid,'upstream_total_timeout',ch.id,504,detail)
                    state.finish(attempt,'failure','upstream_total_timeout',elapsed,error_detail=detail,error_code=504)
                    state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                return JSONResponse(status_code=504,content={'error':{'message':detail,'type':'server_error','param':None,'code':'upstream_total_timeout'}})
            except Exception as e:
                stop_first_activity_watch()
                typ=error_type(e); detail=error_detail(e); last=typ
                # The limit pin only applies to consecutive RPM/TPM retries.
                # If the retry returns a different class of error, let that
                # class apply its own fallback/validation policy.
                if typ not in ('tpm_limit','rpm_limit'):
                    limit_retry_channel=None
                protocol_rule=protocol_error_rule_for(ch,e,config.protocol_error_rules)
                if affinity.get('enabled') and (protocol_rule is None or protocol_rule.clear_session_affinity):
                    state.forget_affinity(key,body['messages'],ch.id,affinity.get('minimum_messages',2))
                state.trace_event(rid,'upstream_error',ch.id,error_code(e),f'{typ}: {detail}')
                state.debug_log(rid,'upstream_in',pool=key,channel=ch.id,model=ch.litellm_model,status=error_code(e) or 0,body=f'{type(e).__name__}: {detail}')
                state.finish(attempt,'failure',typ,int((time.monotonic()-started)*1000),error_detail=detail,error_code=error_code(e))
                if protocol_rule is not None:
                    state.record_protocol_error(rid,key,ch.id,ch.provider,ch.litellm_model,protocol_rule.id,error_code(e),detail)
                    state.trace_event(rid,'protocol_error_classified',ch.id,error_code(e),detail=f'{protocol_rule.id}: matched configured protocol compatibility rule')
                    # This handler runs before a response/first SSE has been
                    # delivered, so a matched rule may safely start the same
                    # request on the next eligible Channel.
                    if protocol_rule.retry_other_channel:
                        tried.add(ch.id)
                        affinity_kind='protocol'
                        state.trace_fallback(rid,f'{ch.id} matched protocol rule {protocol_rule.id}; retrying on next eligible Channel before first SSE')
                        continue
                if typ in ('rpm_limit','tpm_limit','quota_exhausted'):
                    state.observe_429(key,ch.id,detail,kind={'rpm_limit':'rpm','tpm_limit':'tpm','quota_exhausted':'quota_exhausted'}[typ],limits=ch.limits)
                    cooling=state.cooldown_state(key,ch.id)
                    if typ!='quota_exhausted':state.trace_event(rid,'limit_observed',ch.id,error_code(e),f'{typ} received from upstream; cooldown '+(f"set until {clock(cooling['until'])} ({cooling['reason']})" if cooling else 'not set yet; retry/fallback policy continues'))
                    if typ in ('rpm_limit','tpm_limit'):
                        state.trace_event(rid,'limit_retry_started',ch.id,error_code(e),f'{typ} entered first-stage retry; this request remains pinned to the triggering Channel until retry escalation')
                    # 通道级 429 策略（可配置/可扩展）：不同上游的 429 语义不同。
                    # 例如 AMD 的“每日 1 积分”要封到次日重置，而瞬时限流只需短退避。
                    # 策略命中时：把该通道封禁到策略计算的时刻，本请求立即换兄弟通道，
                    # 而不是每 10 分钟复验同一个已死通道。
                    policy_block=quota_429_block(ch,ch.quota_429_policy,detail,typ)
                    if policy_block is not None:
                        reason,seconds=policy_block
                        state.cooldown(key,ch.id,seconds,reason)
                        if ch.quota_429_policy.scope=='channel':
                            # 每日额度按 API key 计：所有挂载该通道的 Runner 一起封禁。
                            for other_key,other_runner in config.runners.items():
                                if ch.id in other_runner.channels:state.cooldown(other_key,ch.id,seconds,reason)
                        until=time.time()+seconds
                        label=f'（{ch.quota_429_policy.label}）' if ch.quota_429_policy.label else ''
                        blocked_detail=(f'{typ} 命中通道 429 策略 {ch.quota_429_policy.mode}/scope={ch.quota_429_policy.scope}：'
                                        f'{ch.id}{label} 封禁至 {clock(until)}（剩余 {remaining_clock(seconds)}）'
                                        f'，本请求转下一可用通道。上游原文：{detail[:200]}')
                        state.trace_event(rid,'quota_policy_blocked',ch.id,error_code(e) or 429,detail=blocked_detail)
                        retry_steps.pop(typ,None); limit_retry_channel=None
                        tried.add(ch.id); retries=0
                        policy_block_reasons[ch.id]={'channel':ch.id,'reason':reason,'until':until,
                                                    'policy_mode':ch.quota_429_policy.mode,'policy_scope':ch.quota_429_policy.scope,
                                                    'upstream':detail[:200]}
                        remaining=[c for c in request_channels if c.id!=ch.id and c.id not in tried and state.is_enabled(key,c.id)]
                        if not remaining:
                            code=error_code(e) or 429
                            state.trace_finish(rid,'failed',ch.id,code,typ,blocked_detail,latency_ms=int((time.monotonic()-req_started)*1000))
                            raise HTTPException(code,{'channel':ch.id,'error_type':typ,'error_detail':blocked_detail,
                                                      'cooldown_until':until,'policy':ch.quota_429_policy.model_dump()}) from e
                        continue
                if typ=='content_policy_blocked':
                    detail_policy=f'上游错误明确表示内容政策限制；按全局 CHN Content Policy Fallback 顺序处理：{detail}'
                    state.trace_event(rid,'content_policy_blocked',ch.id,error_code(e) or 400,detail=detail_policy)
                    fallback_channels=configured_policy_fallbacks(ch.id)
                    if not policy_fallback_active and fallback_channels:
                        policy_fallback_active=True; policy_fallback_tried={ch.id}; policy_fallback_channels=fallback_channels; tried=set(policy_fallback_tried)
                        scope='Runner 内 CHN Policy Channel' if any(c.id in [x.id for x in channels] for c in fallback_channels) else '全局 CHN Content Policy Fallback'
                        state.trace_fallback(rid,f'{ch.id} returned content policy block; switching to {scope} ({", ".join(c.id for c in fallback_channels)})')
                        continue
                    if policy_fallback_active:
                        policy_fallback_tried.add(ch.id); tried.add(ch.id)
                        if any(c.id not in policy_fallback_tried for c in policy_fallback_channels):
                            state.trace_fallback(rid,f'Fallback {ch.id} also returned content policy block; trying next configured Channel')
                            continue
                    state.trace_finish(rid,'failed',ch.id,502,'content_policy_blocked',detail_policy,latency_ms=int((time.monotonic()-req_started)*1000))
                    raise HTTPException(502,{'channel':ch.id,'error_type':'content_policy_blocked','error_detail':detail_policy}) from e
                # 通道级硬性上限（输出预算 / 上下文超限）：是“该通道能力不足”，
                # 不是“请求格式错误”。先学习/记录上限，再标记本通道已试，让下一
                # 轮选路落到能承载的兄弟通道 —— 与 content_policy Fallback 同一
                # 套机制；没有兄弟通道时保留原始错误返回调用方。
                if typ in ('output_limit_exceeded','context_exceeded'):
                    if typ=='output_limit_exceeded':
                        limit_hint=output_limit_from_detail(detail)
                        if limit_hint:
                            learned_max_output_tokens[ch.id]=limit_hint
                            state.learn_output_cap(ch.id,limit_hint,detail)
                            state.trace_event(rid,'output_limit_learned',ch.id,detail=f'记录 {ch.id} 输出预算上限={limit_hint}；后续超限请求按该上限处理')
                    remaining=[c for c in request_channels if c.id!=ch.id and c.id not in tried]
                    if remaining:
                        tried.add(ch.id); retries=0
                        state.trace_fallback(rid,f'{ch.id} returned {typ}: {detail[:160]}; selecting next eligible Channel ({", ".join(c.id for c in remaining[:4])})')
                        continue
                logger.warning('req=%s channel=%s failed error=%s detail=%s',name,ch.id,typ,detail)
                if typ=='quota_exhausted':
                    # “allocated quota exceeded”在低频下可能只是上游的瞬时/子额度异常。
                    # 保留原对话：同一请求在 1m/2m/4m 验证三次，之后每 10m 复验；绝不后台空探测。
                    step=retry_steps.get(typ,0)
                    delay=(60,120,240)[step] if step<3 else 600
                    retry_steps[typ]=step+1; quota_retry_channel=ch.id
                    phase='疑似配额异常，原请求验证第 %d/3' % (step+1) if step<3 else '配额异常已确认，原请求每 10 分钟复验'
                    state.cooldown(key,ch.id,delay,'quota_suspect' if step<3 else 'quota_confirmed')
                    state.trace_event(rid,'quota_retry_wait',ch.id,detail=f'{phase}；等待 {delay//60} 分钟后重试同一 Channel。期间不执行后台探测。')
                    await asyncio.sleep(delay)
                    continue
                if typ=='engine_unavailable':
                    # 仅针对明确临时引擎异常：保留原对话，在同一 Channel 做短间隔验证；其他 400 不进入此分支。
                    step=retry_steps.get(typ,0)
                    delay=(15,45,120)[step] if step<3 else 300
                    retry_steps[typ]=step+1; engine_retry_channel=ch.id
                    phase='引擎暂不可用，原请求验证第 %d/3' % (step+1) if step<3 else '引擎暂不可用已确认，原请求每 5 分钟复验'
                    state.cooldown(key,ch.id,delay,'engine_suspect' if step<3 else 'engine_unavailable')
                    state.trace_event(rid,'engine_retry_wait',ch.id,detail=f'{phase}；等待 {delay} 秒后重试同一 Channel。期间不执行后台探测。')
                    await asyncio.sleep(delay)
                    continue
                rp = ch.retry_policy
                # RPM / TPM 使用指数退避；cost_aware 达到 Channel 重试次数
                # 后回退，其它策略继续到 setup.conf 的累计等待上限。
                if typ in ('tpm_limit','rpm_limit'):
                    cap = QUEUE_TPM_SECONDS if typ=='tpm_limit' else QUEUE_RPM_SECONDS
                    base = TPM_BACKOFF_BASE if typ=='tpm_limit' else RPM_BACKOFF_BASE
                    step = retry_steps.setdefault(typ,0)
                    # cost_aware 先尊重当前 Channel 的 retry_policy，达到
                    # 配置的重试次数后才向下一个 tier 回退；其它策略继续
                    # 固定原 Channel，直到各自的累计等待上限。
                    cost_aware = isinstance(sel,dict) and sel.get('strategy')=='cost_aware'
                    limit_retry_count = max(0,int(rp.max_retries or 0))
                    exhausted_action=rpm_limit_exhausted_action(sel)
                    # 429 退避期间不再回头打已限流的通道：只要还有其它可立即
                    # 使用的兄弟通道，就直接切换过去。该通道已写入 busy 冷却，
                    # 其它请求同样会绕开它。仅当通道显式配置 quota_429_policy
                    # mode='retry' 时才保留“钉在原通道重试”的旧语义。
                    pin_to_channel = bool(ch.quota_429_policy and ch.quota_429_policy.mode=='retry')
                    if not pin_to_channel:
                        siblings=[]
                        for candidate in request_channels:
                            if candidate.id==ch.id or candidate.id in tried: continue
                            if not state.is_enabled(key,candidate.id): continue
                            ok_c,reason_c,total_c=compatibility(reserve,candidate,body,stream)
                            if not ok_c: continue
                            ok_e,reason_e=state.eligible(key,candidate.id,candidate.limits,total_c or 0)
                            if ok_e: siblings.append(candidate)
                        if siblings:
                            state.cooldown(key,ch.id,max(5,int(base)),'busy')
                            tried.add(ch.id); limit_retry_channel=None; retry_steps.pop(typ,None); retries=0
                            state.trace_fallback(rid,f'{ch.id} returned {typ}; switching to {siblings[0].id} instead of retrying a cooling Channel')
                            continue
                    if step >= limit_retry_count and (typ,ch.id) not in limit_escalated:
                        limit_escalated.add((typ,ch.id))
                        state.trace_event(rid,'limit_escalated',ch.id,detail=f'{typ} retry budget exhausted ({limit_retry_count}); second-stage action={exhausted_action}')
                    if step >= limit_retry_count and exhausted_action in ('failover','fail'):
                        if exhausted_action=='fail':
                            code=error_code(e) or 429
                            state.trace_finish(rid,'failed',ch.id,code,typ,detail,latency_ms=int((time.monotonic()-req_started)*1000))
                            raise HTTPException(code,{'channel':ch.id,'error_type':typ,'error_detail':detail,'retry_attempts':step}) from e
                        tried.add(ch.id)
                        limit_retry_channel=None
                        retry_steps[typ]=0
                        state.trace_fallback(rid,f'{ch.id} reached {limit_retry_count} {typ} retries; Channel cooldown now yields to fallback ({exhausted_action})')
                        continue
                    limit_retry_channel=ch.id
                    waited=time.monotonic()-req_started
                    remaining=cap-waited
                    if remaining > 2:
                        backoff=min(base*(2**step),remaining)
                        # ±20% 抖动：并发请求各自错开，避免同一时刻再次齐发。
                        wait=min(max(1.0,backoff*random.uniform(0.8,1.2)),remaining)
                        retry_steps[typ]=step+1
                        # 将本轮指数等待写成通道冷却；仅其他请求可退让到
                        # 备用 Channel，本请求由 limit_retry_channel 固定原通道。
                        state.cooldown(key,ch.id,wait,'busy')
                        state.trace_event(rid,'retry_wait',ch.id,detail=f'{typ}; 指数退避第 {step+1} 次，等待 {wait:.0f}s（基准 {backoff:.0f}s，累计上限 {cap}s）')
                        continue
                # 连接层瞬态错误（Connection error / server_error / timeout）：反映的是
                # 上游或网络层的短时不可用，不是请求本身。若 Runner 里还有其它可用兄弟
                # 通道，优先切过去，而不是在同一通道上耗完 max_retries 次退避后 500，
                # 白白把 Runner 的多通道冗余降成单通道硬扛。仅当没有兄弟通道时才退回
                # 原来的同通道退避重试。
                if typ in ('connection_error','server_error','timeout','upstream_response_timeout') and retries < rp.max_retries:
                    siblings=[]
                    for candidate in request_channels:
                        if candidate.id==ch.id or candidate.id in tried: continue
                        if not state.is_enabled(key,candidate.id): continue
                        ok_c,reason_c,total_c=compatibility(reserve,candidate,body,stream)
                        if not ok_c: continue
                        ok_e,reason_e=state.eligible(key,candidate.id,candidate.limits,total_c or 0)
                        if ok_e: siblings.append(candidate)
                    if siblings:
                        state.cooldown(key,ch.id,max(5,int(rp.backoff.get('base_seconds',5))),'busy')
                        tried.add(ch.id); retry_steps.pop(typ,None); retries=0
                        state.trace_fallback(rid,f'{ch.id} 连接层错误 {typ}: {detail[:120]}; 切换至兄弟通道 {siblings[0].id}，不再在同一通道重试')
                        continue
                elif typ in rp.retry_on and retries < rp.max_retries:
                    wait = min(rp.backoff['base_seconds'] * (2 ** retries if rp.backoff.get('exponential') else 1), rp.backoff['max_seconds'])
                    logger.info('req=%s channel=%s retry %d/%d in %.1fs', name, ch.id, retries+1, rp.max_retries, wait)
                    state.trace_event(rid,'retry_wait',ch.id,detail=f'{typ}; waiting {wait}s (retry {retries+1}/{rp.max_retries})')
                    await asyncio.sleep(wait); retries += 1
                    continue
                fb=sel.get('fallback',{}) if isinstance(sel,dict) else {}
                failure_trigger = 'failure' in fb.get('trigger',[]) if isinstance(fb,dict) else False
                retry_on = sel.get('retry_next_channel_on',[]) if isinstance(sel,dict) else []
                if not stream and (typ in retry_on or (failure_trigger and typ in ('connection_error','timeout','server_error'))):
                    tried.add(ch.id); state.trace_fallback(rid,f'{ch.id} failed with {typ}; selecting next eligible channel'); retries=0; continue
                # B类限流达到累计上限后才向调用方失败。
                if typ in ('tpm_limit','rpm_limit'):
                    cap = QUEUE_TPM_SECONDS if typ=='tpm_limit' else QUEUE_RPM_SECONDS
                    waited = time.monotonic() - req_started  # 本请求从开始累计的等待(含退避+排队)
                    remaining = cap - waited
                    if remaining > 2:
                        state.trace_event(rid,'queue_wait',ch.id,detail=f'{typ}; 等待 {min(remaining,15):.0f}s 后重新尝试（累计上限 {cap}s）')
                        await asyncio.sleep(min(remaining,15))
                        continue
                code=error_code(e) or 502
                state.trace_finish(rid,'failed',ch.id,code,typ,detail,latency_ms=int((time.monotonic()-req_started)*1000))
                raise HTTPException(code,{'channel':ch.id,'error_type':typ,'error_detail':detail,'retry_attempts':retries}) from e
            logger.info('req=%s channel=%s model=%s',name,ch.id,ch.litellm_model)
            if not stream:
                stop_first_activity_watch()
                if ch.id in (quota_retry_channel,engine_retry_channel,five_hour_retry_channel,limit_retry_channel):
                    state.clear_cooldown(key,ch.id); state.trace_event(rid,'channel_recovered',ch.id,detail='原请求验证成功；清除临时异常状态')
                limit_escalated.discard(('tpm_limit',ch.id)); limit_escalated.discard(('rpm_limit',ch.id))
                payload=data(response,name)
                policy_reason=policy_block_reason(payload)
                if policy_reason:
                    detail=f'上游返回内容政策限制（finish_reason={policy_reason}）；按全局 CHN Content Policy Fallback 顺序处理'
                    state.finish(attempt,'failure','content_policy_blocked',latency=int((time.monotonic()-started)*1000),error_detail=detail,error_code=200)
                    state.trace_event(rid,'content_policy_blocked',ch.id,200,detail=detail)
                    if not policy_fallback_active:
                        fallback_channels=configured_policy_fallbacks(ch.id)
                        if fallback_channels:
                            policy_fallback_active=True; policy_fallback_tried={ch.id}; policy_fallback_channels=fallback_channels; tried=set(policy_fallback_tried)
                            scope='Runner 内 CHN Policy Channel' if any(c.id in [x.id for x in channels] for c in fallback_channels) else '全局 CHN Content Policy Fallback'
                            state.trace_fallback(rid,f'{ch.id} returned content policy block; switching to {scope} ({", ".join(c.id for c in fallback_channels)})')
                            continue
                    else:
                        policy_fallback_tried.add(ch.id); tried.add(ch.id)
                        remaining=[c for c in policy_fallback_channels if c.id not in policy_fallback_tried]
                        if remaining:
                            state.trace_fallback(rid,f'Fallback {ch.id} also returned content policy block; trying next configured Channel')
                            continue
                    state.trace_finish(rid,'failed',ch.id,502,'content_policy_blocked',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                    return JSONResponse(status_code=502,content={'error_type':'content_policy_blocked','error_detail':detail,'channel':ch.id})
                output,total=usage_tokens(response); state.finish(attempt,'success',latency=int((time.monotonic()-started)*1000),output_tokens=output,total_tokens=total); state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity['minimum_messages'],kind=affinity_kind) if affinity.get('enabled') else None; state.observe_success(key,ch.id)
                if replay_allowed and is_replayable_response(payload):
                    state.replay_store(caller,replay_scope,fingerprint,ch.id,payload)
                state.trace_finish(rid,'success',ch.id,200,output_preview=response_preview(payload),latency_ms=int((time.monotonic()-req_started)*1000))
                state.debug_log(rid,'upstream_in',pool=key,channel=ch.id,model=ch.litellm_model,status=200,body=json.dumps(payload,ensure_ascii=False)[:20000])
                state.debug_log(rid,'client_out',pool=key,channel=ch.id,model=name,status=200,body=json.dumps(payload,ensure_ascii=False)[:20000])
                return JSONResponse(content=payload)
            disconnect_notice=asyncio.Event()
            async def events():
                nonlocal ch,base,key_,response
                ttft=None; chunks_seen=0; output_parts=[]; active_attempt=attempt; active_started=started; pending=set(); pending_channels={}; iterator=None; stream_policy_failed=False; first_sse_error=None
                async def close_upstream(value):
                    close=getattr(value,'aclose',None)
                    if not callable(close):return
                    try:
                        result=close()
                        # A provider may hang inside aclose() just like it can
                        # hang while reading SSE.  Cleanup must never delay
                        # trace_finish or the 504 deadline response.
                        if hasattr(result,'__await__'):
                            cleanup=asyncio.create_task(result)
                            cleanup.cancel()
                            cleanup.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
                    except Exception:pass
                async def first_event(resp,attempt_id,attempt_started,label,target):
                    # Every stream, not only the ASGI handoff path, is read
                    # through the same raw FIFO.  This preserves Provider
                    # extension frames and makes downstream backpressure a
                    # bounded pause rather than an implicit event loss.
                    iterator=BufferedUpstreamStream(resp)
                    state.trace_event(rid,'upstream_first_sse_wait_started',target.id,detail=f'Buffering raw SSE events; waiting up to {UPSTREAM_FIRST_CHUNK_TIMEOUT}s for first event')
                    first=await await_bounded(anext(iterator),UPSTREAM_FIRST_CHUNK_TIMEOUT)
                    state.trace_event(rid,'upstream_first_sse',target.id,detail=f'First SSE event received within {UPSTREAM_FIRST_CHUNK_TIMEOUT}s')
                    return iterator,first,attempt_id,attempt_started,label,target
                async def original_first_event(resp):
                    try:
                        return await first_event(resp,attempt,started,'original',ch)
                    except asyncio.TimeoutError:
                        elapsed_ms=int((time.monotonic()-started)*1000)
                        if affinity.get('enabled'):
                            state.forget_affinity(key,body['messages'],ch.id,affinity.get('minimum_messages',2))
                        state.finish(attempt,'failure','upstream_first_sse_timeout',elapsed_ms,error_detail='Provider returned no first SSE event within the bounded wait',error_code=504)
                        state.trace_event(rid,'upstream_first_sse_timeout',ch.id,504,detail=f'No first SSE event within {UPSTREAM_FIRST_CHUNK_TIMEOUT}s; advancing Hedge plan')
                        raise
                async def hedge_event(number,target):
                    due,_=hedge_plan[number-1]
                    hedge_attempt=state.start(key,target.id,target.litellm_model,input_tokens=input_tokens(body,target.litellm_model),trace_id=rid)
                    hedge_started=time.monotonic(); state.trace_attempt(rid,target.id)
                    state.trace_event(rid,'hedge_started',target.id,detail=f'无上游 SSE 活动已达 {due}s；第 {number} 个 Hedge 阶段（{len(hedge_plan[number-1][1])} 个 Channel），发往 {target.id}')
                    try:
                        hedge_response=await call_upstream(target)
                        return await first_event(hedge_response,hedge_attempt,hedge_started,f'hedge {number}',target)
                    except asyncio.CancelledError:
                        state.finish(hedge_attempt,'cancelled',latency=int((time.monotonic()-hedge_started)*1000))
                        state.trace_event(rid,'hedge_cancelled',target.id,detail=f'第 {number} 个 Hedge 阶段已取消：另一副本先返回')
                        raise
                    except Exception as exc:
                        if affinity.get('enabled'):
                            state.forget_affinity(key,body['messages'],target.id,affinity.get('minimum_messages',2))
                        state.finish(hedge_attempt,'failure',error_type(exc),latency=int((time.monotonic()-hedge_started)*1000),error_code=error_code(exc))
                        state.trace_event(rid,'hedge_error',target.id,error_code(exc),f'第 {number} 个 Hedge 阶段异常：{error_type(exc)}')
                        raise
                def schedule_hedge_event(number,target):
                    if target.id in pending_channels.values():
                        state.trace_event(rid,'hedge_skipped',target.id,detail=f'第 {number} 个 Hedge 阶段跳过：该 Channel 仍有未完成 SSE 请求')
                        return
                    task=asyncio.create_task(hedge_event(number,target))
                    pending.add(task); pending_channels[task]=target.id
                async def adopt_handoff_hedge(upstream_task,info):
                    """Adopt a Hedge started while ASGI had not entered events()."""
                    target=info['target']; number=info['number']
                    try:
                        if info.get('prefetched'):
                            return await upstream_task
                        hedge_response=await upstream_task
                        return await first_event(hedge_response,info['attempt_id'],info['started'],f'hedge {number}',target)
                    except asyncio.CancelledError:
                        state.finish(info['attempt_id'],'cancelled','hedge_cancelled',latency=int((time.monotonic()-info['started'])*1000))
                        state.trace_event(rid,'hedge_cancelled',target.id,detail=f'第 {number} 个流式 Hedge 阶段已取消')
                        raise
                    except Exception as exc:
                        if affinity.get('enabled'):
                            state.forget_affinity(key,body['messages'],target.id,affinity.get('minimum_messages',2))
                        state.finish(info['attempt_id'],'failure',error_type(exc),latency=int((time.monotonic()-info['started'])*1000),error_code=error_code(exc))
                        state.trace_event(rid,'hedge_error',target.id,error_code(exc),f'第 {number} 个流式 Hedge 阶段异常：{error_type(exc)}')
                        raise
                try:
                    watch_record=app.state.first_activity_watch.get(rid)
                    if watch_record and watch_record.get('deadline_forced'):
                        detail='Watchdog had already closed this Trace at the first-activity deadline before the streaming consumer entered.'
                        await close_upstream(response)
                        state.trace_event(rid,'stream_deadline_observed',ch.id,504,detail=detail)
                        # Keep the generator open while the response wrapper
                        # emits a real HTTP 504.  Yielding an SSE error here
                        # would flush the delayed 200 headers first.
                        downstream_timeout_event.set()
                        await asyncio.Future()
                    if watch_record:
                        watch_record['stream_consumer_started']=True
                        # From this point the stream loop owns Hedge signals;
                        # the handoff callback must not create duplicates.
                        watch_record['on_hedge']=None
                    state.trace_event(rid,'stream_consumer_started',ch.id,detail='Streaming response body consumer entered')
                    prefetched_first_sse=watch_record.pop('prefetched_first_sse',None) if watch_record else None
                    original_task=(prefetched_first_sse if prefetched_first_sse is not None
                                   else asyncio.create_task(original_first_event(response)))
                    pending={original_task}; pending_channels[original_task]=ch.id
                    if watch_record:
                        for upstream_task,info in list(watch_record.get('handoff_hedges',{}).items()):
                            adopted=asyncio.create_task(adopt_handoff_hedge(upstream_task,info))
                            pending.add(adopted); pending_channels[adopted]=info['target'].id
                    watchdog_task=asyncio.create_task(watchdog_signals.get())
                    hedge_index=initial_hedges_started; winner=None
                    while winner is None:
                        if watch_record and watch_record.get('deadline_forced'):
                            raise UpstreamTotalTimeout()
                        elapsed=time.monotonic()-req_started
                        remaining=UPSTREAM_FIRST_ACTIVITY_TIMEOUT-elapsed
                        if remaining<=0: raise UpstreamTotalTimeout()
                        next_delay=hedge_plan[hedge_index][0] if hedge_index<len(hedge_plan) else None
                        hedge_wait=max(0,next_delay-elapsed) if next_delay is not None else remaining
                        timeout=min(hedge_wait,remaining)
                        done,_=await asyncio.wait(pending|{watchdog_task},timeout=timeout,return_when=asyncio.FIRST_COMPLETED)
                        if watchdog_task in done:
                            signal=watchdog_task.result()
                            watchdog_task=asyncio.create_task(watchdog_signals.get())
                            if signal=='deadline': raise UpstreamTotalTimeout()
                            number=int(signal.rsplit('_',1)[1])
                            if number>hedge_index:
                                hedge_index=number
                                _,target_ids=hedge_plan[number-1]
                                for target_id in target_ids: schedule_hedge_event(number,channel_by_id[target_id])
                            continue
                        if done:
                            for task in done:
                                try:
                                    pending_channels.pop(task,None)
                                    winner=task.result(); break
                                except asyncio.TimeoutError:
                                    pending.discard(task)
                                    # First-SSE timeout is local to that
                                    # attempt.  If a later Hedge stage exists,
                                    # start it immediately instead of waiting
                                    # for the next scheduled tick.
                                    if hedge_index < len(hedge_plan):
                                        hedge_index += 1
                                        _,target_ids=hedge_plan[hedge_index-1]
                                        for target_id in target_ids: schedule_hedge_event(hedge_index,channel_by_id[target_id])
                                except Exception as exc:
                                    pending.discard(task)
                                    if not isinstance(exc,StopAsyncIteration): first_sse_error=exc
                            if winner is None and not pending: raise first_sse_error or RuntimeError('all hedged upstream streams ended before first event')
                            continue
                        if time.monotonic()-req_started>=UPSTREAM_FIRST_ACTIVITY_TIMEOUT-0.1: raise UpstreamTotalTimeout()
                        if next_delay is None: continue
                        hedge_index+=1
                        _,target_ids=hedge_plan[hedge_index-1]
                        for target_id in target_ids: schedule_hedge_event(hedge_index,channel_by_id[target_id])
                    iterator,first_item,active_attempt,active_started,label,winner_channel=winner
                    # ``iterator`` is the BufferedUpstreamStream that owns
                    # the provider reader.  Keep it as the active response so
                    # every cancellation/cleanup path closes the FIFO pump,
                    # not merely the underlying response object.
                    response=iterator
                    ch=winner_channel; base,key_=channel_credentials(ch,config.providers)
                    if affinity.get('enabled'):
                        # A first SSE proves this Channel can serve the current
                        # conversation. Persist it provisionally so a retry
                        # cannot reuse stale affinity left by a failed or
                        # partial Channel; final completion refreshes it.
                        state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity.get('minimum_messages',2))
                        state.trace_event(rid,'session_affinity_provisional',ch.id,detail='First upstream SSE received; provisional affinity updated before stream completion')
                    # The first protocol event ends the *pre-response* phase,
                    # but must not remove this Trace from the core watchdog.
                    # A stalled SSE iterator can stop the request coroutine
                    # itself from making progress, so the core-owned watchdog
                    # takes over the post-first-SSE hard deadline below.
                    if label!='original': state.trace_event(rid,'hedge_won',ch.id,detail=f'{label} produced the first upstream SSE event')
                    for task in pending:
                        if not task.done():
                            state.trace_event(rid,'upstream_cancel_requested',pending_channels.get(task),detail='Cancelled losing Hedge task after first SSE winner')
                            task.cancel()
                    if pending:await asyncio.gather(*pending,return_exceptions=True)
                    # Keep a hard stream-idle guard outside the body generator.
                    # If ASGI send() is blocked by downstream backpressure or
                    # a half-open client, selected_items() cannot advance its
                    # own timer.  This independent task still closes the
                    # response and lets DisconnectAwareStreamingResponse emit
                    # the terminal 504/SSE signal.
                    stream_idle_guard_stop=asyncio.Event()
                    stream_guard_last=[time.monotonic()]
                    stream_absolute_started=[time.monotonic()]
                    stream_idle_timeout_recorded=[False]
                    stream_idle_guard_task=None
                    stream_idle_hard_seconds=(hedge_plan[-1][0]+180) if hedge_plan else 720
                    if watch_record is not None:
                        watch_record['phase']='stream'
                        watch_record['stream_started']=stream_absolute_started[0]
                        watch_record['stream_deadline_seconds']=stream_idle_hard_seconds
                        watch_record['stream_deadline_forced']=False
                        # This callback runs from the independent core loop,
                        # rather than from the provider iterator.  It is the
                        # authoritative escape hatch if an SDK stream ignores
                        # cancellation or never yields another frame.
                        def watchdog_stream_deadline():
                            if watch_record.get('stream_deadline_forced'):
                                return
                            watch_record['stream_deadline_forced']=True
                            detail=(f'No completed upstream response within '
                                    f'{stream_idle_hard_seconds//60} minutes after first SSE; '
                                    'core watchdog closed the stream.')
                            if not stream_idle_timeout_recorded[0]:
                                stream_idle_timeout_recorded[0]=True
                                state.trace_event(rid,'stream_idle_deadline',ch.id,504,detail=detail)
                                state.finish(active_attempt,'failure','upstream_total_timeout',int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=504)
                                state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                            state.trace_event(rid,'upstream_cancel_requested',ch.id,504,detail='Core watchdog forced post-first-SSE stream shutdown')
                            downstream_timeout_event.set()
                        watch_record['on_stream_deadline']=watchdog_stream_deadline
                    async def stream_idle_guard():
                        while not stream_idle_guard_stop.is_set():
                            try:
                                await asyncio.wait_for(stream_idle_guard_stop.wait(), timeout=1.0)
                                return
                            except asyncio.TimeoutError:
                                # This is an absolute post-first-SSE deadline.
                                # Hidden reasoning/heartbeat frames must not
                                # keep a request with no visible completion
                                # alive forever.
                                if time.monotonic()-stream_absolute_started[0] >= stream_idle_hard_seconds:
                                    detail=f'No completed upstream response within {stream_idle_hard_seconds//60} minutes after first SSE; Router closed the stream.'
                                    if not stream_idle_timeout_recorded[0]:
                                        stream_idle_timeout_recorded[0]=True
                                        state.trace_event(rid,'stream_idle_deadline',ch.id,504,detail=detail)
                                        state.finish(active_attempt,'failure','upstream_total_timeout',int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=504)
                                        state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                                    state.trace_event(rid,'upstream_cancel_requested',ch.id,504,detail='Independent stream-idle guard closed a backpressured downstream stream')
                                    downstream_timeout_event.set()
                                    return
                    stream_idle_guard_task=asyncio.create_task(stream_idle_guard())
                    async def selected_items():
                        """Yield SSE items, switching before the first visible
                        refusal when a CHN-policy Channel emits content_filter."""
                        nonlocal iterator,first_item,active_attempt,active_started,ch,base,key_,response,stream_policy_failed
                        current_iterator=iterator; current_item=first_item; current_response=response
                        fallback_candidates=configured_policy_fallbacks(ch.id); fallback_index=0; emitted=False; buffered=[]; buffered_chars=0
                        # Keep the post-first-SSE Hedge clock independent from
                        # the pre-response watchdog.  Each stage is measured
                        # from the last meaningful upstream activity: 6m ->
                        # first Hedge, 9m -> next Hedge, then +3m hard stop.
                        idle_started=time.monotonic(); idle_stage=0
                        idle_final_deadline=(hedge_plan[-1][0]+180) if hedge_plan else 720
                        while True:
                            chunk=data(current_item,name); policy_reason=policy_block_reason(chunk)
                            # A role/metadata/heartbeat SSE is not model
                            # progress. Do not let endless empty frames keep
                            # a stalled stream alive indefinitely.
                            if has_stream_activity(chunk):
                                idle_started=time.monotonic(); idle_stage=0
                                stream_guard_last[0]=time.monotonic()
                            if policy_reason and not emitted:
                                detail=f'上游返回内容政策限制（finish_reason={policy_reason}）；按全局 CHN Content Policy Fallback 顺序处理'
                                state.finish(active_attempt,'failure','content_policy_blocked',latency=int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=200)
                                state.trace_event(rid,'content_policy_blocked',ch.id,200,detail=detail)
                                while fallback_index < len(fallback_candidates) and fallback_candidates[fallback_index].id == ch.id:
                                    fallback_index += 1
                                if fallback_index < len(fallback_candidates):
                                    target=fallback_candidates[fallback_index]; fallback_index += 1
                                    state.trace_fallback(rid,f'{ch.id} returned content policy block; switching stream to global fallback {target.id}')
                                    h_attempt=None; h_started=time.monotonic()
                                    try:
                                        h_attempt=state.start(key,target.id,target.litellm_model,input_tokens=input_tokens(body,target.litellm_model),trace_id=rid)
                                        h_started=time.monotonic(); state.trace_attempt(rid,target.id); state.trace_event(rid,'policy_fallback_request',target.id,detail='Fallback stream request started')
                                        fallback_response=await call_upstream(target)
                                        new_iterator,new_first,_,_,_,_=await first_event(fallback_response,h_attempt,h_started,'policy fallback',target)
                                        await close_upstream(current_response)
                                        current_response=new_iterator; response=new_iterator; current_iterator=new_iterator; current_item=new_first
                                        ch=target; base,key_=channel_credentials(ch,config.providers); active_attempt=h_attempt; active_started=h_started; emitted=False; buffered=[]; buffered_chars=0; idle_started=time.monotonic(); idle_stage=0
                                        stream_guard_last[0]=time.monotonic()
                                        if affinity.get('enabled'):
                                            state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity.get('minimum_messages',2))
                                            state.trace_event(rid,'session_affinity_provisional',ch.id,detail='Fallback Channel produced first SSE; provisional affinity updated')
                                        continue
                                    except Exception as exc:
                                        if h_attempt is not None:
                                            state.finish(h_attempt,'failure',error_type(exc),latency=int((time.monotonic()-h_started)*1000),error_detail=error_detail(exc),error_code=error_code(exc))
                                        state.trace_event(rid,'policy_fallback_error',target.id,error_code(exc),detail=f'{error_type(exc)}: {error_detail(exc)}')
                                        continue
                                stream_policy_failed=True
                            elif policy_reason:
                                stream_policy_failed=True
                                state.trace_event(rid,'content_policy_blocked_after_output',ch.id,200,detail=f'finish_reason={policy_reason}; fallback suppressed after stream output began')
                            buffered.append(current_item); buffered_chars += len(chunk_content(chunk))
                            if not emitted and (buffered_chars >= 512 or len(buffered) >= 8):
                                for buffered_item in buffered: yield buffered_item
                                buffered=[]; buffered_chars=0; emitted=True
                            elif emitted:
                                yield current_item
                            try:
                                elapsed_idle=time.monotonic()-idle_started
                                next_idle_deadline=(hedge_plan[idle_stage][0]
                                                     if idle_stage < len(hedge_plan)
                                                     else idle_final_deadline)
                                wait_for=max(0.1,next_idle_deadline-elapsed_idle)
                                current_item=await await_stream_next(current_iterator,wait_for)
                            except asyncio.TimeoutError:
                                if idle_stage>=len(hedge_plan):
                                    detail=f'上游 SSE 已有活动，但连续 {idle_final_deadline//60} 分钟没有后续事件；按流式空闲截止终止'
                                    stream_idle_timeout_recorded[0]=True
                                    state.trace_event(rid,'stream_idle_deadline',ch.id,504,detail=detail)
                                    # The body generator may be suspended in a
                                    # downstream send (for example after a
                                    # client presses ESC but leaves its socket
                                    # open). Let the response wrapper own the
                                    # terminal HTTP 504/SSE frame instead of
                                    # waiting for this generator to yield.
                                    state.trace_event(rid,'upstream_cancel_requested',ch.id,504,detail='Stream idle hard-stop; closing upstream and downstream response')
                                    cancel_detached(pending)
                                    await close_upstream(current_response)
                                    detail=f'No upstream SSE activity for {idle_final_deadline//60} minutes; Router closed the stalled stream.'
                                    state.finish(active_attempt,'failure','upstream_total_timeout',int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=504)
                                    state.trace_event(rid,'upstream_total_timeout',ch.id,504,detail=detail)
                                    state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                                    downstream_timeout_event.set()
                                    await asyncio.Future()
                                idle_stage += 1; due,target_ids=hedge_plan[idle_stage-1]
                                same_channel_retry=(len(channel_by_id)==1 and ch.id in target_ids)
                                target=next((channel_by_id[target_id] for target_id in target_ids
                                             if target_id in channel_by_id and (target_id!=ch.id or same_channel_retry)),None)
                                if target is None:
                                    state.trace_event(rid,'stream_idle_hedge_skipped',ch.id,detail=f'流式空闲第 {idle_stage} 阶段没有可切换的其他 Channel')
                                    idle_started=time.monotonic(); continue
                                state.trace_event(rid,'stream_idle_hedge_started',target.id,detail=f'上游连续 {due}s 没有后续 SSE；第 {idle_stage} 个流式空闲 Hedge 阶段')
                                try:
                                    remaining=max(1,idle_final_deadline-(time.monotonic()-idle_started))
                                    result=await asyncio.wait_for(hedge_event(idle_stage,target),remaining)
                                except Exception as exc:
                                    state.trace_event(rid,'stream_idle_hedge_error',target.id,error_code(exc),detail=f'{error_type(exc)}: {error_detail(exc)}')
                                    # Keep the original idle baseline after a
                                    # failed Hedge so the next stage remains at
                                    # its configured absolute 9m/12m deadline.
                                    continue
                                new_iterator,new_first,new_attempt,new_started,new_label,new_channel=result
                                # The old stream has already produced activity but
                                # is now superseded by the idle Hedge winner. Mark
                                # that attempt closed before moving the active
                                # pointer, otherwise it remains ``started`` in the
                                # history forever and falsely appears hung.
                                if active_attempt is not None:
                                    state.finish(active_attempt,'failure','stream_idle_hedged',int((time.monotonic()-active_started)*1000),error_detail=f'流式连续 {due}s 无后续 SSE；已切换到 Hedge Channel {new_channel.id}',error_code=504)
                                state.trace_event(rid,'upstream_cancel_requested',ch.id,504,detail=f'流式空闲 Hedge 已获胜；关闭旧 Channel {ch.id} 的上游读取')
                                await close_upstream(current_response)
                                current_response=new_iterator; response=new_iterator; iterator=new_iterator; current_iterator=new_iterator; current_item=new_first
                                ch=new_channel; active_attempt=new_attempt; active_started=new_started; base,key_=channel_credentials(ch,config.providers)
                                if affinity.get('enabled'):
                                    state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity.get('minimum_messages',2))
                                    state.trace_event(rid,'session_affinity_provisional',ch.id,detail='Idle Hedge Channel produced first SSE; provisional affinity updated')
                                idle_started=time.monotonic(); idle_stage=0; stream_guard_last[0]=time.monotonic(); emitted=False; buffered=[]; buffered_chars=0; continue
                            except StopAsyncIteration:
                                if not emitted:
                                    for buffered_item in buffered: yield buffered_item
                                break
                    while True:
                        try:
                            async for item in selected_items():
                                chunk=data(item,name)
                                if ttft is None and has_visible_content(chunk):
                                    ttft=int((time.monotonic()-active_started)*1000); state.trace_event(rid,'first_token',ch.id,detail=f'TTFT {ttft}ms')
                                if sum(len(part) for part in output_parts)<512:
                                    output_parts.append(chunk_content(chunk))
                                chunks_seen+=1
                                if chunks_seen<=3 or (chunk.get('choices') and chunk['choices'][0].get('finish_reason')):
                                    state.debug_log(rid,'upstream_in',pool=key,channel=ch.id,model=ch.litellm_model,status=200,body=json.dumps(chunk,ensure_ascii=False)[:20000])
                                yield f"data: {json.dumps(chunk,ensure_ascii=False)}\n\n"
                            break
                        except Exception as exc:
                            # 连接层瞬态截断（TransferEncodingError / Connection error /
                            # server_error）：上游在首个 SSE 前就掐断响应体（如 Content-Length
                            # 不符）。只要还没有任何 token 发回客户端（chunks_seen==0），请求就
                            # 是“尚未开始输出”，此时换到 Runner 里其它可用兄弟通道重发是安全的。
                            stream_typ=error_type(exc)
                            if stream_typ in ('server_error','connection_error','timeout') and chunks_seen==0:
                                # 只在确实存在可用兄弟通道时才重试，否则直接让原错误抛出。
                                sibling=next((candidate for candidate in available
                                              if candidate.id not in tried and candidate.id!=ch.id
                                              and candidate.enabled and state.is_enabled(key,candidate.id)
                                              and state.eligible(key,candidate.id,candidate.limits)[0]),None)
                                if sibling is not None:
                                    state.finish(active_attempt,'failure',stream_typ,int((time.monotonic()-active_started)*1000),error_detail=error_detail(exc),error_code=error_code(exc))
                                    state.trace_fallback(rid,f'{ch.id} 流式中段 {stream_typ}: {error_detail(exc)[:120]}; 首个 SSE 前截断，改发兄弟通道 {sibling.id}')
                                    tried.add(ch.id)
                                    new_attempt=state.start(key,sibling.id,sibling.litellm_model,input_tokens=input_tokens(body,sibling.litellm_model),trace_id=rid)
                                    new_started=time.monotonic(); state.trace_attempt(rid,sibling.id); state.trace_event(rid,'stream_fallback_request',sibling.id,detail=f'{stream_typ}: 首个 SSE 前截断，在兄弟通道重发同一请求')
                                    try:
                                        new_response=await call_upstream(sibling)
                                        new_iterator,new_first,_,_,_,_=await first_event(new_response,new_attempt,new_started,'stream fallback',sibling)
                                    except Exception as fallback_exc:
                                        state.finish(new_attempt,'failure',error_type(fallback_exc),int((time.monotonic()-new_started)*1000),error_detail=error_detail(fallback_exc),error_code=error_code(fallback_exc))
                                        state.trace_event(rid,'stream_fallback_error',sibling.id,error_code(fallback_exc),detail=f'{error_type(fallback_exc)}: {error_detail(fallback_exc)}')
                                        raise fallback_exc
                                    await close_upstream(response)
                                    response=new_iterator; iterator=new_iterator; first_item=new_first; ch=sibling; active_attempt=new_attempt; active_started=new_started; base,key_=channel_credentials(ch,config.providers)
                                    if affinity.get('enabled'):
                                        state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity.get('minimum_messages',2))
                                        state.trace_event(rid,'session_affinity_provisional',ch.id,detail='Stream fallback Channel produced first SSE; provisional affinity updated')
                                    continue
                            # A provider may return a response object and then
                            # reject the first SSE request with a protocol 400.
                            # It is still safe to retry because no downstream
                            # token has been emitted yet.
                            rule=protocol_error_rule_for(ch,exc,config.protocol_error_rules)
                            if rule is None or not rule.retry_other_channel or (rule.only_before_first_sse and chunks_seen>0):
                                raise
                            state.record_protocol_error(rid,key,ch.id,ch.provider,ch.litellm_model,rule.id,error_code(exc),error_detail(exc))
                            state.trace_event(rid,'protocol_error_classified',ch.id,error_code(exc),detail=f'{rule.id}: matched during first-SSE streaming phase')
                            if rule.clear_session_affinity and affinity.get('enabled'):
                                state.forget_affinity(key,body['messages'],ch.id,affinity.get('minimum_messages',2))
                            tried.add(ch.id)
                            target=next((candidate for candidate in available if candidate.id not in tried and candidate.enabled),None)
                            if target is None:
                                raise
                            state.finish(active_attempt,'failure',error_type(exc),int((time.monotonic()-active_started)*1000),error_detail=error_detail(exc),error_code=error_code(exc))
                            state.trace_fallback(rid,f'{ch.id} matched protocol rule {rule.id}; retrying stream on {target.id} before first SSE')
                            new_attempt=state.start(key,target.id,target.litellm_model,input_tokens=input_tokens(body,target.litellm_model),trace_id=rid)
                            new_started=time.monotonic(); state.trace_attempt(rid,target.id); state.trace_event(rid,'protocol_fallback_request',target.id,detail=f'{rule.id}: retrying same request before first SSE')
                            try:
                                new_response=await call_upstream(target)
                                new_iterator,new_first,_,_,_,_=await first_event(new_response,new_attempt,new_started,'protocol fallback',target)
                            except Exception as fallback_exc:
                                state.finish(new_attempt,'failure',error_type(fallback_exc),int((time.monotonic()-new_started)*1000),error_detail=error_detail(fallback_exc),error_code=error_code(fallback_exc))
                                state.trace_event(rid,'protocol_fallback_error',target.id,error_code(fallback_exc),detail=f'{rule.id}: {error_type(fallback_exc)}: {error_detail(fallback_exc)}')
                                raise fallback_exc
                            await close_upstream(response)
                            response=new_iterator; iterator=new_iterator; first_item=new_first; ch=target; active_attempt=new_attempt; active_started=new_started; base,key_=channel_credentials(ch,config.providers)
                            if affinity.get('enabled'):
                                state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity.get('minimum_messages',2),kind='protocol')
                                state.trace_event(rid,'session_affinity_provisional',ch.id,detail='Protocol fallback Channel produced first SSE; provisional affinity updated')
                            state.trace_fallback(rid,f'protocol rule {rule.id}: stream switched from failed Channel to {target.id}')
                            continue
                    state.debug_log(rid,'client_out',pool=key,channel=ch.id,model=name,status=200,body=f'[stream] {chunks_seen} chunks delivered')
                    if stream_policy_failed:
                        state.trace_finish(rid,'failed',ch.id,502,'content_policy_blocked','All configured CHN Content Policy fallback Channels were exhausted',latency_ms=int((time.monotonic()-req_started)*1000))
                        yield f'data: {json.dumps({"error":{"type":"content_policy_blocked","detail":"All configured CHN Content Policy fallback Channels were exhausted"}},ensure_ascii=False)}\n\n'
                        return
                    state.finish(active_attempt,'success',latency=int((time.monotonic()-active_started)*1000),ttft_ms=ttft); state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity['minimum_messages']) if affinity.get('enabled') else None
                    if ch.id in (quota_retry_channel,engine_retry_channel,five_hour_retry_channel,limit_retry_channel):
                        state.clear_cooldown(key,ch.id); state.trace_event(rid,'channel_recovered',ch.id,detail='原请求验证成功；清除临时异常状态')
                    limit_escalated.discard(('tpm_limit',ch.id)); limit_escalated.discard(('rpm_limit',ch.id))
                    state.trace_finish(rid,'success',ch.id,200,output_preview=''.join(output_parts)[:512],ttft_ms=ttft,latency_ms=int((time.monotonic()-req_started)*1000)); yield 'data: [DONE]\n\n'
                except asyncio.CancelledError:
                    for task in pending:
                        if not task.done():task.cancel()
                    if pending:await asyncio.gather(*pending,return_exceptions=True)
                    await close_upstream(iterator)
                    await close_upstream(response)
                    # A Router watchdog hard-stop already finalized this Trace
                    # as an upstream timeout.  The response wrapper cancels
                    # this generator only to make room for the terminal 504/
                    # SSE error, so do not overwrite the failure as a client
                    # disconnect or generic stream cancellation.
                    forced_deadline=downstream_timeout_event.is_set() or bool(
                        watch_record and watch_record.get('deadline_forced')
                    )
                    if forced_deadline:
                        if not stream_idle_timeout_recorded[0] and downstream_timeout_event.is_set():
                            stream_idle_timeout_recorded[0]=True
                            detail='Stream watchdog forced termination before the generator could finalize the Trace.'
                            state.finish(active_attempt,'failure','upstream_total_timeout',int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=504)
                            state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                        raise
                    disconnected=disconnect_notice.is_set()
                    reason='client_disconnected' if disconnected else 'stream_cancelled'
                    detail='Downstream client disconnected; cancelled upstream stream and pending hedges' if disconnected else 'Streaming task cancelled; cancelled upstream stream and pending hedges'
                    state.trace_event(rid,reason,ch.id,detail=detail)
                    state.finish(active_attempt,'cancelled',reason,latency=int((time.monotonic()-active_started)*1000),error_detail=detail)
                    state.trace_finish(rid,'cancelled',ch.id,error_type=reason,error_detail=detail,latency_ms=int((time.monotonic()-req_started)*1000))
                    raise
                except UpstreamTotalTimeout:
                    # Some upstream HTTP stacks do not unblock promptly after
                    # Task.cancel(); never let that keep the caller/Trace running.
                    if affinity.get('enabled'):
                        state.forget_affinity(key,body['messages'],ch.id,affinity.get('minimum_messages',2))
                    state.trace_event(rid,'upstream_cancel_requested',ch.id,504,detail='First-activity deadline reached; detaching all pending upstream tasks')
                    cancel_detached(pending)
                    await close_upstream(iterator)
                    await close_upstream(response)
                    detail=f'No upstream SSE activity before Router {first_activity_deadline//60}-minute safety deadline; cancelled all pending attempts.'
                    state.trace_event(rid,'upstream_total_timeout',ch.id,504,detail)
                    state.finish(active_attempt,'failure','upstream_total_timeout',int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=504)
                    state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                    # The wrapper owns the terminal response so it can still
                    # send HTTP 504 when no SSE/headers reached the client.
                    downstream_timeout_event.set()
                    await asyncio.Future()
                except Exception as e:
                    typ=error_type(e); detail=error_detail(e)
                    protocol_rule=protocol_error_rule_for(ch,e,config.protocol_error_rules)
                    if affinity.get('enabled') and (protocol_rule is None or protocol_rule.clear_session_affinity):
                        state.forget_affinity(key,body['messages'],ch.id,affinity.get('minimum_messages',2))
                    if protocol_rule is not None:
                        state.record_protocol_error(rid,key,ch.id,ch.provider,ch.litellm_model,protocol_rule.id,error_code(e),detail)
                        state.trace_event(rid,'protocol_error_classified',ch.id,error_code(e),detail=f'{protocol_rule.id}: stream error after first-SSE phase')
                    state.finish(active_attempt,'failure',typ,int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=error_code(e))
                    state.trace_finish(rid,'failed',ch.id,error_code(e) or 502,typ,detail,latency_ms=int((time.monotonic()-req_started)*1000))
                    if typ in ('rpm_limit','tpm_limit','quota_exhausted'):
                        state.observe_429(key,ch.id,detail,kind={'rpm_limit':'rpm','tpm_limit':'tpm','quota_exhausted':'quota_exhausted'}[typ],limits=ch.limits)
                        cooling=state.cooldown_state(key,ch.id)
                        state.trace_event(rid,'limit_observed',ch.id,error_code(e),f'{typ} received during stream; cooldown '+(f"set until {clock(cooling['until'])} ({cooling['reason']})" if cooling else 'not set yet'))
                    logger.warning('req=%s channel=%s stream error=%s detail=%s',name,ch.id,typ,detail)
                    yield f'data: {json.dumps({"error":{"type":typ,"detail":detail}})}\n\n'
                    return
                finally:
                    stop_first_activity_watch()
                    if 'stream_idle_guard_stop' in locals():
                        stream_idle_guard_stop.set()
                    if 'stream_idle_guard_task' in locals() and stream_idle_guard_task is not None and not stream_idle_guard_task.done():
                        stream_idle_guard_task.cancel()
                    if 'watchdog_task' in locals() and not watchdog_task.done():
                        watchdog_task.cancel()
                    # A response body can be torn down without passing through
                    # any ``except`` branch: an async-generator ``aclose()``
                    # arrives as ``GeneratorExit`` (a BaseException) and the ASGI
                    # layer may simply drop the body task.  Because the watchdog
                    # record is popped just above, that used to leave the Trace
                    # ``running`` forever with nothing left to time it out.
                    # Reconstruct the honest terminal state instead.
                    try:
                        if state.trace_status(rid)=='running':
                            detail=('Stream body closed without a terminal outcome '
                                    '(client abort or generator finalisation); '
                                    'Router closed the Trace from its cleanup path.')
                            state.finish(active_attempt,'cancelled','stale_no_terminal',
                                         latency=int((time.monotonic()-active_started)*1000),error_detail=detail)
                            state.trace_finish(rid,'cancelled',ch.id,error_type='stale_no_terminal',
                                               error_detail=detail,latency_ms=int((time.monotonic()-req_started)*1000))
                    except Exception:
                        logger.exception('stream cleanup could not finalise trace=%s',rid)
            state.trace_event(rid,'handler_response_built',ch.id,detail='StreamingResponse constructed; returning to ASGI layer')
            return DisconnectAwareStreamingResponse(events(),on_call=lambda: state.trace_event(rid,'asgi_response_call',ch.id,detail='ASGI entered the streaming response wrapper'),
                on_stream=lambda: state.trace_event(rid,'asgi_stream_task_started',ch.id,detail='ASGI body-producer task started'),on_disconnect=disconnect_notice.set,timeout_event=downstream_timeout_event,restart_event=restart_event,on_finished=unregister_restart_signal,timeout_detail='No upstream SSE activity before Router safety deadline; request closed.',media_type='text/event-stream',headers={'Cache-Control':'no-cache','Connection':'keep-alive','X-Accel-Buffering':'no'})
    async def _first_activity_watchdog():
        """Core-owned watchdog for requests that have not received upstream activity.

        It deliberately lives beside the 7800 request server: the admin UI, a
        browser tab, and the 7801 process are not part of its correctness.
        Request handlers consume the signals and own cancellation of their
        concrete LiteLLM tasks.
        """
        while True:
            await asyncio.sleep(1)
            now=time.monotonic()
            for trace_id,record in list(app.state.first_activity_watch.items()):
                # Once first SSE arrived, the pre-response queue must no
                # longer drive hedges.  Keep the Trace registered solely so
                # this independent loop can enforce an absolute stream
                # deadline even if the upstream async iterator is wedged.
                if record.get('phase')=='stream':
                    stream_started=record.get('stream_started')
                    stream_deadline=int(record.get('stream_deadline_seconds',0) or 0)
                    if stream_started is not None and stream_deadline>0:
                        stream_elapsed=now-stream_started
                        if stream_elapsed>=stream_deadline and 'stream_deadline' not in record['sent']:
                            record['sent'].add('stream_deadline')
                            state.trace_event(trace_id,'watchdog_stream_deadline_due',http_status=504,detail=f'Core stream watchdog reached {stream_deadline}s after first SSE; cancellation due.')
                            callback=record.get('on_stream_deadline')
                            if callback is not None:
                                try:
                                    callback()
                                except Exception as exc:
                                    logger.exception('stream watchdog cancellation failed trace=%s: %s',trace_id,exc)
                    continue
                elapsed=now-record['started']; sent=record['sent']; signals=record['signals']
                deadline_seconds=int(record.get('deadline_seconds',UPSTREAM_FIRST_ACTIVITY_TIMEOUT))
                for index,(delay,target_ids) in enumerate(record.get('hedge_plan',()),1):
                    signal=f'hedge_{index}'
                    if elapsed>=delay and signal not in sent:
                        sent.add(signal); signals.put_nowait(signal)
                        state.trace_event(trace_id,'watchdog_hedge_due',detail=f'无上游活动已达 {delay}s；第 {index} 个 Hedge 阶段（{len(target_ids)} 个 Channel）到期：{", ".join(target_ids)}')
                        callback=record.get('on_hedge')
                        if callback is not None:
                            try:
                                callback(index)
                            except Exception as exc:
                                logger.exception('watchdog hedge launch failed trace=%s: %s',trace_id,exc)
                if elapsed>=deadline_seconds and 'deadline' not in sent:
                    sent.add('deadline'); signals.put_nowait('deadline')
                    state.trace_event(trace_id,'watchdog_deadline_due',http_status=504,detail=f'Core watchdog reached {deadline_seconds}s without upstream activity; cancellation due.')
                    callback=record.get('on_deadline')
                    if callback is not None:
                        try:
                            callback()
                        except Exception as exc:
                            logger.exception('watchdog deadline cancellation failed trace=%s: %s',trace_id,exc)

    # ---- 回切探测（Y 方案）：冷却中通道到期前异步探测，成功则提前清除冷却回切 ----
    async def _probe_loop(interval_seconds: int | None = None):
        """后台周期探测：对处于冷却(busy/quota_exhausted)且满足 should_probe 节流的通道，
        发送一次最小探测请求；成功 clear_cooldown 提前回切，失败 record_probe 记 streak。"""
        while True:
            await asyncio.sleep(interval_seconds or int(os.getenv('FLEX_PROBE_INTERVAL', '120')))
            try:
                now = time.time()
                for pool_name, pool in config.runners.items():
                    sel = pool.selection if isinstance(pool.selection, dict) else {}
                    # Accept the documented nested fallback location and the
                    # earlier top-level spelling while configs migrate.
                    reattach = {}
                    if isinstance(sel, dict):
                        reattach = sel.get('reattach', {}) or {}
                        if not reattach and isinstance(sel.get('fallback'), dict):
                            reattach = sel['fallback'].get('reattach', {}) or {}
                    if not reattach.get('probe_before_switch_back', True):
                        continue  # 配置关闭探测则跳过
                    for ch_id in pool.channels:
                        if not state.is_enabled(pool_name, ch_id):
                            continue
                        reason = state.cooldown_reason(pool_name, ch_id, now=now)
                        # A类配额与本地五小时额度可探测；RPM/TPM 到期自动放行，不探测。
                        if reason not in ('quota_exhausted','five_hour_quota','five_hour_quota_retry'):
                            continue
                        five_hour_probe = reason in ('five_hour_quota','five_hour_quota_retry')
                        # 原请求仍在等待时，它自己负责验证；后台不得并发多发一次。
                        if five_hour_probe and state.has_active_five_hour_validation(pool_name, ch_id):
                            continue
                        if not state.should_probe(pool_name, ch_id, now=now,
                                                  probe_cooldown=1800 if five_hour_probe else reattach.get('probe_cooldown_seconds', 600)):
                            continue
                        prov_name, ch = config.get_channel(ch_id)
                        try:
                            base, key_ = channel_credentials(ch, config.providers)
                            await litellm.acompletion(model=ch.litellm_model, messages=[{'role': 'user', 'content': 'ping'}],
                                                      api_base=base, api_key=key_, max_tokens=1)
                            state.clear_cooldown(pool_name, ch_id)
                            logger.info('probe ok pool=%s channel=%s cooldown cleared', pool_name, ch_id)
                        except Exception:
                            state.record_probe(pool_name, ch_id, now=now, success=False)
                            logger.info('probe failed pool=%s channel=%s', pool_name, ch_id)
                # 直连路径(direct:<ch_id> 命名空间)的冷却同样探测恢复: 与 POOL 状态分开, 但恢复机制一致
                for row in state.cooled_entries(prefix='direct:'):
                    ns, ch_id = row['pool'], row['channel']
                    # 与 pool 路径一致：本地五小时额度在无原请求时每 30 分钟探测一次。
                    if row['reason'] not in ('quota_exhausted','five_hour_quota','five_hour_quota_retry'):
                        continue
                    five_hour_probe = row['reason'] in ('five_hour_quota','five_hour_quota_retry')
                    if five_hour_probe and state.has_active_five_hour_validation(ns, ch_id):
                        continue
                    if not state.should_probe(ns, ch_id, now=now,
                                              probe_cooldown=1800 if five_hour_probe else int(os.getenv('FLEX_PROBE_COOLDOWN', '120'))):
                        continue
                    try:
                        prov_name, ch = config.get_channel(ch_id)
                        base, key_ = channel_credentials(ch, config.providers)
                        await litellm.acompletion(model=ch.litellm_model, messages=[{'role': 'user', 'content': 'ping'}],
                                                  api_base=base, api_key=key_, max_tokens=1)
                        state.clear_cooldown(ns, ch_id)
                        logger.info('probe ok direct=%s channel=%s cooldown cleared', ns, ch_id)
                    except Exception:
                        state.record_probe(ns, ch_id, now=now, success=False)
                        logger.info('probe failed direct=%s channel=%s', ns, ch_id)
            except Exception as exc:  # 探测循环自身不应拖垮进程
                logger.error('probe loop error: %s', exc)

    async def _capability_retry_loop():
        """空闲时自动重探橙色(error)能力，直到它们变绿(supported)或变红(unsupported)。

        规则（整机级别）：
        - 系统连续 30 分钟没有外部调用后，才开始重探；
        - 之后每 10 分钟重探一次；
        - 一旦出现外部调用，立即停止重探（不干扰真实流量），
          直到再次空闲满 30 分钟。
        只重探 protocol_support 中 status=='error' 的项，不触碰已确认的结论。
        """
        idle_start_seconds = int(os.getenv('FLEX_CAP_RETRY_IDLE_SECONDS', '1800'))   # 30 分钟
        retry_interval_seconds = int(os.getenv('FLEX_CAP_RETRY_INTERVAL', '600'))    # 10 分钟
        tick_seconds = int(os.getenv('FLEX_CAP_RETRY_TICK', '60'))
        next_run_at = 0.0
        was_idle = False
        while True:
            await asyncio.sleep(tick_seconds)
            try:
                now = time.time()
                last_call = getattr(app.state, 'last_external_call', 0.0)
                # 有外部调用就停止：重置计时，等再次空闲满阈值。
                if now - last_call < idle_start_seconds:
                    if was_idle:
                        logger.info('cap_retry paused: external call resumed, last_external_call_delta=%.0fs', now - last_call)
                        was_idle = False
                    next_run_at = 0.0
                    continue
                if now < next_run_at:
                    continue
                if not was_idle:
                    logger.info('cap_retry engaged: idle >= %.0fs, retry interval %.0fs', idle_start_seconds, retry_interval_seconds)
                    was_idle = True
                next_run_at = now + retry_interval_seconds
                probed = 0
                for ch_id, ch in list(config.channels.items()):
                    if not ch.enabled:
                        continue
                    errors = [k for k, v in (dict(ch.protocol_support) if isinstance(ch.protocol_support, dict) else {}).items()
                              if isinstance(v, dict) and v.get('status') == 'error']
                    if not errors:
                        continue
                    if ch_id in app.state.capability_probe_channels:
                        continue
                    # 只重探橙色项。能力探测是整体重跑，但结果只会更新这些项
                    # —— supported/unsupported 的项在上一次已定论，重跑无损（同源判定）。
                    task = schedule_channel_capability_probe(ch_id, kinds=errors)
                    if task is not None:
                        probed += 1
                        logger.info('cap_retry channel=%s scheduling probe, orange=%s', ch_id, ','.join(errors))
                        # 探测完成后记录各项新状态，便于确认橙色是否消除或定论。
                        async def log_probe_result(cid=ch_id, task=task):
                            try:
                                await task
                            except Exception:
                                pass
                            ch_now = config.channels.get(cid)
                            ps = dict(ch_now.protocol_support) if ch_now and isinstance(ch_now.protocol_support, dict) else {}
                            states = {k: v.get('status') for k, v in ps.items() if isinstance(v, dict)}
                            still = [k for k, v in states.items() if v == 'error']
                            logger.info('cap_retry channel=%s probe done, states=%s, remaining_orange=%s',
                                        cid, states, ','.join(still) or 'none')
                        cap_task = asyncio.ensure_future(log_probe_result())
                        app.state.probe_tasks.add(cap_task); cap_task.add_done_callback(app.state.probe_tasks.discard)
                if probed:
                    logger.info('cap_retry round done probed=%d', probed)
            except Exception as exc:  # 重探循环自身不应拖垮进程
                logger.error('capability retry loop error: %s', exc)

    async def _stale_trace_sweeper():
        """Last-resort guarantee that no Trace stays ``running`` unsupervised.

        Normal request paths and the stream cleanup fallback both write a
        terminal state, but an ASGI body can still be torn down without running
        any ``except`` branch, and a process crash can orphan a row.  Without
        this sweep the admin UI and the monitor report would keep showing such a
        request as "hung" for hours even though nothing is working on it.
        """
        grace=int(os.getenv('FLEX_STALE_TRACE_GRACE','1800'))
        interval=max(30,int(os.getenv('FLEX_STALE_TRACE_INTERVAL','120')))
        while True:
            await asyncio.sleep(interval)
            try:
                closed=state.sweep_stale_traces(grace)
                if closed:
                    logger.warning('stale trace sweeper closed %d running trace(s) older than %ds',closed,grace)
            except Exception as exc:  # 清扫循环自身不应拖垮进程
                logger.error('stale trace sweeper error: %s', exc)

    @app.on_event('startup')
    async def _start_probe_loop():
        t = asyncio.create_task(_probe_loop())
        app.state.probe_tasks.add(t); t.add_done_callback(app.state.probe_tasks.discard)
        cap_retry = asyncio.create_task(_capability_retry_loop())
        app.state.probe_tasks.add(cap_retry); cap_retry.add_done_callback(app.state.probe_tasks.discard)
        watchdog = asyncio.create_task(_first_activity_watchdog())
        app.state.watchdog_tasks.add(watchdog); watchdog.add_done_callback(app.state.watchdog_tasks.discard)
        sweeper = asyncio.create_task(_stale_trace_sweeper())
        app.state.watchdog_tasks.add(sweeper); sweeper.add_done_callback(app.state.watchdog_tasks.discard)

    return app
