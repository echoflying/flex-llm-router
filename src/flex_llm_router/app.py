from __future__ import annotations
import asyncio,html,json,logging,os,re,subprocess,sys,time,uuid
import anyio
from pathlib import Path
from typing import Any
import urllib.error,urllib.request
import litellm,yaml
from fastapi import FastAPI,HTTPException,Request
from fastapi.responses import HTMLResponse,JSONResponse,StreamingResponse
from flex_llm_router.config import FlexConfig,channel_credentials,load_config
from flex_llm_router.scheduler import RoundRobinScheduler
from flex_llm_router.state import StateStore
logger=logging.getLogger('uvicorn.error')
# Change this for every core behavior release.  It is exposed by /healthz so a
# restart can be verified without inferring it from a changing uptime counter.
ROUTER_BUILD='2026-08-27.pre-response-deadline-v11'
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
    def __init__(self,*args,on_disconnect=None,**kwargs):
        super().__init__(*args,**kwargs); self.on_disconnect=on_disconnect
    async def __call__(self,scope,receive,send):
        if scope['type']=='websocket':
            return await super().__call__(scope,receive,send)
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
def error_type(e):
    n=type(e).__name__.lower(); s=getattr(e,'status_code',None)
    detail=(getattr(e,'message',None) or str(e) or '').lower()
    # 仅识别已验证的上游临时引擎报错；兼容上游偶发的 avaiable 拼写错误。
    # 其他 400 仍为 request_error，避免掩盖请求格式问题。
    if re.search(r'engine\s+is\s+not\s+avai(?:l)?able\s+temporarily',detail):return 'engine_unavailable'
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
        return 'rate_limit'  # rpm 及其他瞬时限流
    if s and s>=500:return 'server_error'
    if 'timeout' in n:return 'timeout'
    if 'connection' in n or 'apierror' in n:return 'connection_error'
    return 'request_error'
def error_code(e):
    """Return the provider's HTTP status code if present (e.g. 429, 500, 408), else None."""
    return getattr(e, 'status_code', None)
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
    return {'allowed_openai_params':channel.supported_params} if channel.supported_params else {}
# B类瞬时限流：每次真实 429 后按 2^n 退避，直到本请求的类型上限。
TPM_BACKOFF_BASE=4
RPM_BACKOFF_BASE=8
# 单请求的累计退避上限（秒），setup.conf 可覆盖。
QUEUE_TPM_SECONDS=int(os.getenv('FLEX_QUEUE_TPM','60'))
QUEUE_RPM_SECONDS=int(os.getenv('FLEX_QUEUE_RPM','300'))
# 仅在流式请求尚未收到任何可转发 SSE 事件时使用；Hermes 保留 30 分钟总超时。
# Runner Hedge defaults are derived from the number of configured Channels.
# Three Channels: 6m -> second Channel, 9m -> third Channel, 12m hard stop.
# Two Channels: 6m -> second Channel, 9m hard stop. One Channel keeps the
# configured global safety deadline because there is no useful fallback.
HEDGE_DELAYS=(int(os.getenv('FLEX_HEDGE_FIRST_SECONDS','360')),int(os.getenv('FLEX_HEDGE_SECOND_SECONDS','720')))
UPSTREAM_FIRST_ACTIVITY_TIMEOUT=int(os.getenv('FLEX_UPSTREAM_FIRST_ACTIVITY_TIMEOUT','900'))
# Per-attempt safety bounds.  These are intentionally shorter than the
# request-level first-activity deadline: a stuck provider attempt should not
# block the next configured Hedge stage.  The inner LiteLLM task is detached
# on expiry because some HTTP clients do not promptly acknowledge cancellation.
UPSTREAM_RESPONSE_TIMEOUT=int(os.getenv('FLEX_UPSTREAM_RESPONSE_TIMEOUT','180'))
UPSTREAM_FIRST_CHUNK_TIMEOUT=int(os.getenv('FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT','180'))

async def await_bounded(awaitable, timeout_seconds):
    """Await one provider operation without waiting for cancellation cleanup."""
    task=asyncio.ensure_future(awaitable)
    done,_=await asyncio.wait({task},timeout=max(0,float(timeout_seconds)),return_when=asyncio.FIRST_COMPLETED)
    if task not in done:
        cancel_detached({task})
        raise asyncio.TimeoutError()
    return task.result()
def hedge_plan_for(pool_name,channels,selected,pool=None):
    """Return configured (due_seconds, channel ids) first-activity Hedge steps.
    Channel IDs are always read from the selected Pool; no model/provider names
    are special-cased here. Without explicit stages, use same-channel defaults.
    """
    configured=(pool.selection.get('hedge',{}) if pool is not None and isinstance(pool.selection,dict) else {})
    stages=configured.get('stages',[]) if isinstance(configured,dict) else []
    if stages:
        known={channel.id for channel in channels}
        plan=[]
        for stage in stages:
            if not isinstance(stage,dict):continue
            try:due=max(0,int(stage.get('after_seconds',0)))
            except (TypeError,ValueError):continue
            targets=tuple(str(target) for target in (stage.get('channels') or ()) if str(target) in known)
            if due and targets:plan.append((due,targets))
        if plan:return tuple(plan)
    # Automatic policy is intentionally based only on Runner membership and
    # order, never on provider/model names. The initially selected Channel is
    # the request's model0; subsequent configured Channels are model1/model2.
    ordered=[selected]+[channel for channel in channels if channel.id!=selected.id]
    if len(ordered)>=3:
        return ((360,(ordered[1].id,)),(540,(ordered[2].id,)))
    if len(ordered)==2:
        return ((360,(ordered[1].id,)),)
    return ()

def first_activity_deadline_for(channels):
    """Return the request hard-stop implied by Runner Channel count."""
    if len(channels)>=3:
        return min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,720)
    if len(channels)==2:
        return min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,540)
    return UPSTREAM_FIRST_ACTIVITY_TIMEOUT
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
def compatibility(reserve,ch,body,stream):
    needed=requirements(body,stream); missing=needed-set(ch.capabilities)
    if missing:return False,'missing_capabilities:'+','.join(sorted(missing)),None
    prompt=input_tokens(body,ch.litellm_model); output=body.get('max_completion_tokens',body.get('max_tokens',reserve))
    if not isinstance(output,int) or output<0:output=reserve
    total=prompt+output
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
    config=load_config(config_path,override=override_on); os.environ['FLEX_OVERRIDE']='1' if override_on else '0'; templates_dir=config_path_resolved.parent.parent/'templates'; litellm.drop_params=True; scheduler=RoundRobinScheduler(); state=StateStore(os.getenv('FLEX_STATE_DB','data/flex.db')); instance_id=uuid.uuid4().hex; recovered_traces=state.cancel_interrupted_traces(); state.backfill_error_statistics(); app=FastAPI(title='Flex LLM Router',version='0.2.1'); app.state.probe_tasks=set(); app.state.first_activity_watch={}; app.state.watchdog_tasks=set(); app.state.instance_id=instance_id; app.state.started_at=time.time(); app.state.recovered_traces=recovered_traces; app.state.build=ROUTER_BUILD
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
    global QUEUE_TPM_SECONDS, QUEUE_RPM_SECONDS, UPSTREAM_RESPONSE_TIMEOUT, UPSTREAM_FIRST_CHUNK_TIMEOUT
    try: QUEUE_TPM_SECONDS=int(read_setup().get('FLEX_QUEUE_TPM', os.getenv('FLEX_QUEUE_TPM','60')))
    except ValueError: pass
    try: QUEUE_RPM_SECONDS=int(read_setup().get('FLEX_QUEUE_RPM', os.getenv('FLEX_QUEUE_RPM','300')))
    except ValueError: pass
    try: UPSTREAM_RESPONSE_TIMEOUT=max(1,int(read_setup().get('FLEX_UPSTREAM_RESPONSE_TIMEOUT', os.getenv('FLEX_UPSTREAM_RESPONSE_TIMEOUT','180'))))
    except ValueError: pass
    try: UPSTREAM_FIRST_CHUNK_TIMEOUT=max(1,int(read_setup().get('FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT', os.getenv('FLEX_UPSTREAM_FIRST_CHUNK_TIMEOUT','180'))))
    except ValueError: pass
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
                    strategy_detail=f'单 Channel；沿用全局 {UPSTREAM_FIRST_ACTIVITY_TIMEOUT//60} 分钟无首活动截止'
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
                                 'context_window_tokens':ch.context_window_tokens,
                                 'capabilities':list(ch.capabilities),
                                 'last_used_at':state.last_used_at(ch.id)})
            runners.append({'name':name,'public_model':pool.public_model,'base_url':base_url,
                          'lan_url':lan_url,'channels':channels,'strategy_key':strategy_key,
                          'strategy_name':strategy_name,'strategy_detail':strategy_detail})
        # CHANNEL 与 POOL 平等: 所有定义的 channel 都列出(外部可直接引用), 不论是否挂 pool.
        # mounted_in 标它属于哪些 pool (仅参考, 不降级).
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
                                 'context_window_tokens':ch.context_window_tokens,
                                 'capabilities':list(ch.capabilities),
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
                'links':connections,'providers':providers}

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
        backup=config_path_resolved.parent/(config_path_resolved.name+'.bak')
        backup.write_text(config_path_resolved.read_text(encoding='utf-8'),encoding='utf-8')
        config_path_resolved.write_text(raw_text,encoding='utf-8')
        config=cfg
        logger.warning('config saved backup=%s (hot-applied)',backup)
        return backup

    async def structured_config_error(exc):
        raise HTTPException(400,f'invalid config edit: {exc}') from exc

    @app.get('/api/config/editor')
    async def config_editor():
        """Return editable Runner/Channel/Model records without secret values."""
        view=await config_view()
        return view

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

    @app.post('/api/config/channels/{channel_id}')
    async def edit_channel(channel_id:str, request:Request):
        if channel_id not in config.channels: raise HTTPException(404,'unknown channel')
        body=await request.json()
        mapping=yaml.safe_load(config_path_resolved.read_text(encoding='utf-8')) or {}
        channels=mapping.setdefault('channels',{}); current=dict(channels[channel_id])
        allowed=('provider','litellm_model','public_model','enabled','externally_exposed',
                 'context_window_tokens','capabilities','supported_params','limits','retry_policy')
        for key in allowed:
            if key in body: current[key]=body[key]
        channels[channel_id]=current
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','channel':channel_id,'backup':str(backup)}

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
              'externally_exposed':bool(body.get('externally_exposed',True)),
              'enabled':bool(body.get('enabled',True))}
        channels[channel_id]=item
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','channel':channel_id,'backup':str(backup)}

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
            channels[channel_id]={
                'id':channel_id,'provider':provider,'litellm_model':litellm_model,
                'public_model':alias,'context_window_tokens':int(item.get('context_window_tokens') or 1000000),
                'capabilities':item.get('capabilities') or ['chat','streaming'],
                'externally_exposed':bool(item.get('externally_exposed',True)),
                'enabled':bool(item.get('enabled',True)),
            }
            existing_pairs.add((provider,litellm_model)); created.append(channel_id)
        if not created: return {'status':'unchanged','created':[]}
        try: backup=persist_config_mapping(mapping)
        except Exception as exc: await structured_config_error(exc)
        return {'status':'saved','created':created,'backup':str(backup)}

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
                                      max_tokens=8,**channel_request_kwargs(ch))
        except Exception as exc:
            typ=error_type(exc); detail=error_detail(exc); latency=int((time.monotonic()-started)*1000)
            state.finish(attempt,'failure',typ,latency,error_detail=detail,error_code=error_code(exc))
            state.record_test(channel_id,ch.id,'failure',typ,latency,detail)
            raise HTTPException(502,detail)
        latency=int((time.monotonic()-started)*1000); state.finish(attempt,'success',None,latency)
        state.record_test(channel_id,ch.id,'success',None,latency,'')
        return {'channel':channel_id,'status':'ok','latency_ms':latency}

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
        duplicate_note='只观察并记录同一客户端短时间内的完全相同在途请求；不会取消、合并或延迟调用。' if duplicate else '默认关闭。开启后只记录候选重复请求，绝不改变路由行为。'
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
        return render('setup.html','Flex LLM Router · Setup','table{width:100%;border-collapse:collapse;background:#fff;margin:16px 0}th,td{padding:9px;border-bottom:1px solid #e7eaf0;text-align:left}th{background:#f7f9fc}code{font-size:13px}.bad{color:#b42318}.ok{color:#067647}button{padding:5px 12px;margin-left:8px}.override-box{background:#fff;border:1px solid #e7eaf0;border-radius:8px;padding:14px 18px;margin:16px 0}',total_vars=str(len(needed)),override_cls=cls_env,override_txt=txt_env,btn_txt=btn_env,override_note=note_env,debug_cls=dbg_cls,debug_txt=dbg_txt,debug_btn_txt=dbg_btn,debug_note=dbg_note,capture_cls=capture_cls,capture_txt=capture_txt,capture_btn_txt=capture_btn,capture_hours=capture['retention_hours'],capture_rows=capture['max_rows'],capture_mib=round(capture['max_bytes']/1024/1024),capture_count=capture['count'],capture_used_mib=f"{capture['bytes']/1024/1024:.1f}",capture_oldest=capture_time(capture['oldest_at']),capture_newest=capture_time(capture['newest_at']),duplicate_cls=duplicate_cls,duplicate_txt=duplicate_txt,duplicate_btn_txt=duplicate_btn,duplicate_note=duplicate_note,rows=''.join(rows),env_path=env_path_str,config_path=config_path_str,queue_tpm=QUEUE_TPM_SECONDS,queue_rpm=QUEUE_RPM_SECONDS)
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
    @app.get('/healthz')
    async def health():
        uptime=int(time.time()-app.state.started_at)
        return {
            'status':'ok',
            'router_version':app.version,
            'build':app.state.build,
            'build_features':['deadline_event_hard_stop','pre_response_deadline_hard_stop','detached_upstream_cancellation','nonblocking_sse_cleanup','config_driven_hedge_stages','generic_runner_policy','inflight_channel_dedupe','bounded_upstream_wait','upstream_lifecycle_events','stream_consumer_observability','watchdog_phase_handoff','trace_list_query_index','channel_count_aware_6m_9m_hedges','dynamic_9m_12m_first_activity_deadline','local_full_request_viewer'],
            'process':{
                'instance_id':app.state.instance_id,
                'pid':os.getpid(),
                'started_at_epoch':app.state.started_at,
                'started_at_local':time.strftime('%Y-%m-%d %H:%M:%S %z',time.localtime(app.state.started_at)),
                'uptime_seconds':uptime,
                'uptime_human':remaining_clock(uptime),
            },
            'effective_policy':{
                'automatic_hedge_seconds':{'two_channels':[360],'three_or_more_channels':[360,540]},
                'automatic_deadline_seconds':{'one_channel':UPSTREAM_FIRST_ACTIVITY_TIMEOUT,'two_channels':min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,540),'three_or_more_channels':min(UPSTREAM_FIRST_ACTIVITY_TIMEOUT,720)},
                'configured_pool_hedges':{
                    pool_name:[{'after_seconds':due,'channels':list(targets)} for due,targets in hedge_plan_for(pool_name,[ch for _,ch in config.get_pool_channels(pool_name)],config.get_pool_channels(pool_name)[0][1],config.pools[pool_name])]
                    for pool_name in config.runners
                    if config.get_pool_channels(pool_name)
                },
                'first_activity_deadline_seconds':UPSTREAM_FIRST_ACTIVITY_TIMEOUT,
                'response_timeout_seconds':UPSTREAM_RESPONSE_TIMEOUT,
                'first_chunk_timeout_seconds':UPSTREAM_FIRST_CHUNK_TIMEOUT,
                'summary':f'Per-channel {UPSTREAM_RESPONSE_TIMEOUT}s response / {UPSTREAM_FIRST_CHUNK_TIMEOUT}s first SSE; 2 Channel: 6m/9m, 3+ Channels: 6m/9m/12m hard deadline',
            },
            'watchdog':{'active_trace_count':len(app.state.first_activity_watch),'interrupted_traces_closed_on_start':app.state.recovered_traces},
        }
    @app.get('/v1/models')
    async def models():
        # Runners are the canonical external resources.  Direct Channel and
        # legacy Link names remain listed so existing clients keep working.
        data_list = config.runner_models()
        # Also include direct channels
        data_list += [{'id':ch.id,'object':'model','owned_by':ch.provider}
                      for ch in config.channels.values()
                      if ch.enabled and ch.externally_exposed]
        # 连接: 暴露为独立 model 名(指向已解析的 pool/channel), 外部系统可直接引用;
        # 附带 target 便于前端标注"连接 -> 目标"
        data_list += [{'id':name,'object':'model','owned_by':'flex-connection','target':target}
                      for name, target in config.links.items()
                      if target not in config.channels or
                         (config.channels[target].enabled and config.channels[target].externally_exposed)]
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
                        result.append({'id':str(item['id']),'owned_by':item.get('owned_by')})
            return {'provider':provider_name,'refreshed':True,'source':'upstream',
                    'available':True,'data':result}
        except Exception as exc:
            detail=str(exc).replace(key,'[redacted]')[:240]
            return {'provider':provider_name,'refreshed':True,'source':'upstream',
                    'available':False,'error':detail,'data':[]}

    @app.get('/api/runners')
    async def runner_list():
        return {'data':config.runner_models()}

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
    @app.get('/api/statistics/requests')
    async def request_statistics(period:str='day',group_by:str='channel'):
        if period not in ('day','week','month') or group_by not in ('channel','pool'):raise HTTPException(400,'invalid statistics query')
        return state.request_statistics(period,group_by)
    @app.get('/api/statistics/requests/hourly')
    async def hourly_request_statistics():return state.hourly_request_statistics()
    @app.get('/api/statistics/duplicates')
    async def duplicate_statistics(period:str='day'):
        if period not in ('day','week','month'):raise HTTPException(400,'invalid statistics query')
        return state.duplicate_statistics(period)
    @app.post('/api/runners/{name}/channels/{channel_id}/reset')
    @app.post('/api/pools/{name}/channels/{channel_id}/reset')
    async def reset_channel(name:str,channel_id:str,scope:str='all'):
        if scope not in ('all','quota','cooldown'):raise HTTPException(400,'scope must be all, quota, or cooldown')
        internal,pool,direct=pool_for(name)
        if pool is None and direct is None:raise HTTPException(404,'unknown pool')
        state.reset(channel_id,channel_id,scope); logger.warning('pool=%s channel=%s reset scope=%s',internal,channel_id,scope)
        return {'pool':name,'channel':channel_id,'reset':scope}
    @app.post('/api/runners/{name}/channels/{channel_id}/enabled')
    @app.post('/api/pools/{name}/channels/{channel_id}/enabled')
    async def channel_enabled(name:str,channel_id:str,value:bool):
        internal,pool,direct=pool_for(name)
        if pool is None and direct is None:raise HTTPException(404,'unknown pool')
        state.set_enabled(channel_id,channel_id,value); logger.warning('pool=%s channel=%s enabled=%s',internal,channel_id,value)
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
            await litellm.acompletion(model=ch.litellm_model,api_base=base,api_key=key,messages=[{'role':'user','content':'Reply with exactly OK.'}],max_tokens=8,**channel_request_kwargs(ch))
        except Exception as e:
            typ=error_type(e); detail=error_detail(e); latency=int((time.monotonic()-started)*1000); state.finish(attempt,'failure',typ,latency,error_detail=detail,error_code=error_code(e)); state.record_test(internal,ch.id,'failure',typ,latency,detail)
            if typ in ('rate_limit','quota_exhausted'):state.observe_429(channel_id,ch.id,detail,limits=ch.limits)
            logger.warning('channel test failed channel=%s error=%s detail=%s',channel_id,typ,detail)
            raise HTTPException(error_code(e) or 502,{'channel':ch.id,'outcome':'failure','error_type':typ,'error_detail':detail,'latency_ms':latency}) from e
        output,total=usage_tokens(response) if 'response' in locals() else (None,None); latency=int((time.monotonic()-started)*1000); state.finish(attempt,'success',latency=latency,output_tokens=output,total_tokens=total); state.observe_success(channel_id,ch.id); state.record_test(internal,ch.id,'success',latency=latency)
        logger.info('channel test succeeded channel=%s latency_ms=%s',channel_id,latency)
        return {'channel':ch.id,'outcome':'success','latency_ms':latency}
    @app.post('/api/admin/restart')
    async def restart():
        target=f"gui/{os.getuid()}/{os.getenv('FLEX_LAUNCHD_LABEL','com.weifeng.flex-llm-router')}"
        async def later():
            await asyncio.sleep(.25)
            result=subprocess.run(['/bin/launchctl','kickstart','-k',target],capture_output=True,text=True)
            if result.returncode:logger.error('launchd restart failed: %s',result.stderr.strip())
            else:logger.warning('launchd restart requested: %s',target)
        asyncio.create_task(later())
        return {'status':'restarting'}
    @app.post('/v1/chat/completions')
    async def chat(request:Request):
        body=await request.json(); name=body.pop('model',None); stream=bool(body.pop('stream',False)); internal,pool,direct=pool_for(name)
        rid='r-'+uuid.uuid4().hex
        req_started=time.monotonic()  # 请求起点(排队等待计时用, 早于通道选择)
        if pool is None and direct is None:raise HTTPException(404,f'unknown model: {name}')
        if not isinstance(body.get('messages'),list):raise HTTPException(400,"request field 'messages' is required")
        preview,context_summary=trace_input_summary(body['messages'])
        caller=client_label(request.headers)
        fingerprint=state.request_fingerprint(name,body)
        state.trace_begin(rid,name,internal or name,preview,context_summary,stream,caller,request_fingerprint=fingerprint)
        state.capture_full_request(rid,caller,name,internal or name,{'model':name,'stream':stream,**body})
        if state.duplicate_observer_enabled:
            duplicates=state.observe_inflight_duplicates(rid,caller,name,internal or name,fingerprint)
            for duplicate in duplicates:
                detail=(f"与进行中 Trace {duplicate['trace_id']} 的请求内容完全一致；重叠 "
                        f"{duplicate['overlap_ms']/1000:.1f}s；旧请求" + ('已开始输出。' if duplicate['has_output'] else '尚未输出。'))
                state.trace_event(rid,'duplicate_observed',detail=detail)
                state.trace_event(duplicate['trace_id'],'duplicate_observed',detail=f'新 Trace {rid} 与本请求完全一致；仅记录，不取消任何请求。')
        # 对极短时间窗中的安全幂等请求直接恢复完整结果，避免 Hermes 的批准类请求
        # 在上游已成功后因网络重发而再次占用模型。stream/tool 请求绝不进入该路径。
        replay_allowed=not stream and not body.get('tools') and not body.get('tool_choice')
        if replay_allowed:
            replay=state.replay_lookup(caller,name,fingerprint)
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
            affinity = pool.session_affinity
            reserve = pool.context_policy.get('reserve_output_tokens', 8192)
        tried=set(); last='no_eligible_channel'; retries=0; quota_retry_channel=None; engine_retry_channel=None; five_hour_retry_channel=None
        retry_steps={}  # 分级退避计数: {'tpm_limit':n,'rate_limit':m} 两类独立
        while True:
            # 每次选路只保留本轮拒绝原因；不能把上一轮的 busy 重复拼进调用轨迹。
            available=[]; rejected=[]
            for c in channels:
                # 配额异常的原请求必须回到发生异常的同一 Channel 做验证；不能被调度器静默换走。
                validation_channel=quota_retry_channel or engine_retry_channel or five_hour_retry_channel
                if validation_channel is not None and c.id!=validation_channel:continue
                if not state.is_enabled(key,c.id) or c.id in tried:continue
                ok,reason,total=compatibility(reserve,c,body,stream)
                if not ok:
                    rejected.append({'channel':c.id,'reason':reason}); continue
                eligible,reason=state.eligible(key,c.id,c.limits,total or 0,ignore_five_hour_quota=(c.id==five_hour_retry_channel))
                if eligible:available.append(c)
                else:rejected.append({'channel':c.id,'reason':reason})
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
                state.trace_finish(rid,'failed',http_status=503,error_type='no_eligible_channel',error_detail=json.dumps(detail,ensure_ascii=False)[:800],latency_ms=int((time.monotonic()-req_started)*1000))
                raise HTTPException(503,detail)
            sticky=state.affinity_channel(key,body['messages'],affinity['idle_seconds'],affinity['minimum_messages']) if affinity.get('enabled') else None
            ch=next((candidate for candidate in available if candidate.id==sticky),None) or scheduler.select(key,available,state,sel,tiers=pool.tiers if pool is not None else None); attempt=state.start(key,ch.id,ch.litellm_model,input_tokens=input_tokens(body,ch.litellm_model),trace_id=rid); started=time.monotonic()
            state.trace_event(rid,'channel_selected',ch.id,detail='session affinity hit' if sticky==ch.id else 'scheduler selected channel')
            state.trace_attempt(rid,ch.id)
            initial_hedges_started=0
            hedge_plan=hedge_plan_for(internal,channels,ch,pool)
            first_activity_deadline=first_activity_deadline_for(channels)
            channel_by_id={candidate.id:candidate for candidate in channels}
            # The watchdog is owned by the core (7800), not the UI.  It sends
            # deadline signals through this queue, so a hung provider request
            # cannot depend on this coroutine's own timeout calculation.
            watchdog_signals=asyncio.Queue()
            app.state.first_activity_watch[rid]={'started':req_started,'signals':watchdog_signals,'sent':set(),'hedge_plan':hedge_plan,'deadline_seconds':first_activity_deadline}
            def stop_first_activity_watch():
                app.state.first_activity_watch.pop(rid,None)
            async def call_upstream(target):
                target_base,target_key=channel_credentials(target,config.providers)
                state.trace_event(rid,'upstream_task_started',target.id,detail=f'LiteLLM call started; response timeout {UPSTREAM_RESPONSE_TIMEOUT}s')
                started_at=time.monotonic()
                try:
                    response=await await_bounded(
                        litellm.acompletion(**body,model=target.litellm_model,api_base=target_base,api_key=target_key,stream=stream,**channel_request_kwargs(target)),
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
            async def await_initial_response():
                """Hedge while the provider has not even returned a response object yet."""
                nonlocal initial_hedges_started
                active={asyncio.create_task(call_upstream(ch)):(attempt,started,'original',ch)}
                def launch_hedge_targets(hedge_no):
                    due,target_ids=hedge_plan[hedge_no-1]
                    active_channel_ids={target.id for _,(_,_,_,target) in active.items()}
                    for target_id in target_ids:
                        target=channel_by_id[target_id]
                        if target.id in active_channel_ids:
                            state.trace_event(rid,'pre_response_hedge_skipped',target.id,detail=f'第 {hedge_no} 个 Hedge 阶段跳过：该 Channel 仍有未完成请求')
                            continue
                        hedge_attempt=state.start(key,target.id,target.litellm_model,input_tokens=input_tokens(body,target.litellm_model),trace_id=rid)
                        hedge_started=time.monotonic(); state.trace_attempt(rid,target.id)
                        state.trace_event(rid,'pre_response_hedge_started',target.id,detail=f'无上游活动已达 {due}s；第 {hedge_no} 个 Hedge 阶段（{len(target_ids)} 个 Channel），发往 {target.id}')
                        active[asyncio.create_task(call_upstream(target))]=(hedge_attempt,hedge_started,f'pre-response hedge {hedge_no}',target)
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
                    cancel_detached(active)
                watch_record['on_hedge']=watchdog_start_hedge
                watch_record['on_deadline']=watchdog_cancel_deadline
                async def watch_disconnect():
                    while True:
                        if await request.is_disconnected(): return True
                        await asyncio.sleep(0.5)
                disconnect_task=asyncio.create_task(watch_disconnect())
                watchdog_task=asyncio.create_task(watchdog_signals.get())
                deadline_task=asyncio.create_task(deadline_event.wait())
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
                        timeout=min(hedge_wait,remaining)
                        done,_=await asyncio.wait(set(active)|{disconnect_task,watchdog_task,deadline_task},timeout=timeout,return_when=asyncio.FIRST_COMPLETED)
                        if deadline_task in done or deadline_due['value']:
                            for task,(attempt_id,attempt_started,label,target) in active.items():
                                if attempt_id!=attempt:
                                    state.finish(attempt_id,'cancelled','upstream_total_timeout',int((time.monotonic()-attempt_started)*1000),error_detail='Core watchdog cancelled pending hedge at Runner deadline')
                            cancel_detached(active)
                            raise UpstreamTotalTimeout()
                        if watchdog_task in done:
                            signal=watchdog_task.result()
                            watchdog_task=asyncio.create_task(watchdog_signals.get())
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
                        if disconnect_task in done:
                            for task,(attempt_id,attempt_started,label,target) in active.items():
                                task.cancel()
                                if attempt_id!=attempt:
                                    state.finish(attempt_id,'cancelled','client_disconnected',int((time.monotonic()-attempt_started)*1000),error_detail='Downstream disconnected before upstream response')
                            # Detached cleanup must not delay the 504 path.
                            cancel_detached(active)
                            raise ClientDisconnectedBeforeResponse()
                        if not done:
                            if time.monotonic()-req_started>=UPSTREAM_FIRST_ACTIVITY_TIMEOUT-0.1:
                                continue
                            watchdog_start_hedge(initial_hedges_started+1)
                            continue
                        for task in list(done):
                            if task is disconnect_task: continue
                            attempt_id,attempt_started,label,target=active.pop(task)
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
                                state.finish(attempt_id,'failure','upstream_response_timeout',elapsed_ms,error_detail='Provider response-object timeout; detached LiteLLM task',error_code=504)
                                state.trace_event(rid,'pre_response_timeout',target.id,504,detail=f'{label} exceeded the {UPSTREAM_RESPONSE_TIMEOUT}s response-object bound')
                                if initial_hedges_started < len(hedge_plan):
                                    initial_hedges_started += 1
                                    launch_hedge_targets(initial_hedges_started)
                                    continue
                                if label!='original':
                                    continue
                                raise hedge_error
                            except Exception as hedge_error:
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
                            if label!='original': state.trace_event(rid,'pre_response_hedge_won',target.id,detail=f'{label} returned the first upstream response object')
                            # Response-object arbitration is over.  Streaming
                            # owns the same watchdog record from this point;
                            # leaving these callbacks installed would let the
                            # pre-response callback launch a duplicate Hedge
                            # at 6/9 minutes even though a response object has
                            # already been handed to the SSE consumer.
                            watch_record['on_hedge']=None
                            selected_response=value
                            def stream_deadline_hard_stop():
                                if watch_record.get('deadline_forced'): return
                                watch_record['deadline_forced']=True
                                detail=f'No upstream SSE activity before Router {first_activity_deadline//60}-minute safety deadline; watchdog closed the trace before the streaming consumer entered.'
                                state.trace_event(rid,'upstream_cancel_requested',target.id,504,detail='Watchdog hard-stop for a response object whose streaming consumer did not start')
                                state.trace_event(rid,'upstream_total_timeout',target.id,504,detail)
                                state.trace_finish(rid,'failed',target.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                                close=getattr(selected_response,'aclose',None) or getattr(selected_response,'close',None)
                                if callable(close):
                                    try:
                                        result=close()
                                        if hasattr(result,'__await__'):
                                            cleanup=asyncio.create_task(result)
                                            cleanup.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
                                    except Exception: pass
                            watch_record['deadline_forced']=False
                            watch_record['on_deadline']=stream_deadline_hard_stop
                            state.trace_event(rid,'watchdog_handoff',target.id,detail='Response object selected; first-SSE watchdog queue now owned by streaming phase')
                            return value,attempt_id,attempt_started,target
                finally:
                    if not disconnect_task.done(): disconnect_task.cancel()
                    if not watchdog_task.done(): watchdog_task.cancel()
                    if not deadline_task.done(): deadline_task.cancel()
                    await asyncio.gather(disconnect_task,watchdog_task,deadline_task,return_exceptions=True)
            try:
                base,key_=channel_credentials(ch, config.providers)
                state.trace_event(rid,'upstream_request',ch.id,detail=f'{ch.provider} · {ch.litellm_model}')
                state.debug_log(rid,'upstream_out',pool=key,channel=ch.id,model=ch.litellm_model,body=json.dumps({'model':ch.litellm_model,'api_base':base,'stream':stream,**body},ensure_ascii=False))
                response,attempt,started,winner_channel=await await_initial_response()
                ch=winner_channel
                base,key_=channel_credentials(ch,config.providers)
            except ClientDisconnectedBeforeResponse:
                stop_first_activity_watch()
                elapsed=int((time.monotonic()-started)*1000)
                detail='Downstream client disconnected before the upstream returned a response or first SSE event; upstream task cancelled.'
                state.trace_event(rid,'client_disconnected_before_first_token',ch.id,detail=detail)
                state.finish(attempt,'cancelled','client_disconnected',elapsed,error_detail=detail)
                state.trace_finish(rid,'cancelled',ch.id,499,'client_disconnected',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                logger.info('req=%s channel=%s client disconnected before upstream response',name,ch.id)
                return JSONResponse(status_code=499,content={'detail':'client disconnected'})
            except UpstreamTotalTimeout:
                stop_first_activity_watch()
                elapsed=int((time.monotonic()-started)*1000)
                detail=f'No upstream response before Router {first_activity_deadline//60}-minute safety deadline; cancelled all pending attempts.'
                # The pre-response watchdog may already have persisted the
                # terminal outcome while this coroutine was unwinding. Avoid
                # duplicating completion events and analytics in that case.
                if not deadline_forced['value']:
                    state.trace_event(rid,'upstream_total_timeout',ch.id,504,detail)
                    state.finish(attempt,'failure','upstream_total_timeout',elapsed,error_detail=detail,error_code=504)
                    state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                return JSONResponse(status_code=504,content={'detail':detail})
            except Exception as e:
                stop_first_activity_watch()
                typ=error_type(e); detail=error_detail(e); last=typ
                state.trace_event(rid,'upstream_error',ch.id,error_code(e),f'{typ}: {detail}')
                state.debug_log(rid,'upstream_in',pool=key,channel=ch.id,model=ch.litellm_model,status=error_code(e) or 0,body=f'{type(e).__name__}: {detail}')
                state.finish(attempt,'failure',typ,int((time.monotonic()-started)*1000),error_detail=detail,error_code=error_code(e))
                if typ in ('rate_limit','tpm_limit','quota_exhausted'):
                    state.observe_429(key,ch.id,detail,kind={'rate_limit':'rpm','tpm_limit':'tpm','quota_exhausted':'quota_exhausted'}[typ],limits=ch.limits)
                    cooling=state.cooldown_state(key,ch.id)
                    if typ!='quota_exhausted':state.trace_event(rid,'limit_observed',ch.id,error_code(e),f'{typ} received from upstream; cooldown '+(f"set until {clock(cooling['until'])} ({cooling['reason']})" if cooling else 'not set yet; retry/fallback policy continues'))
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
                # RPM / TPM 都采用无限指数退避；总等待由 setup.conf 中对应上限截断。
                if typ in ('tpm_limit','rate_limit'):
                    cap = QUEUE_TPM_SECONDS if typ=='tpm_limit' else QUEUE_RPM_SECONDS
                    base = TPM_BACKOFF_BASE if typ=='tpm_limit' else RPM_BACKOFF_BASE
                    step = retry_steps.setdefault(typ,0)
                    waited=time.monotonic()-req_started
                    remaining=cap-waited
                    if remaining > 2:
                        wait=min(base*(2**step),remaining); retry_steps[typ]=step+1
                        # 将本轮指数等待写成通道冷却，其他请求可立即退让到备用通道。
                        state.cooldown(key,ch.id,wait,'busy')
                        state.trace_event(rid,'retry_wait',ch.id,detail=f'{typ}; 指数退避第 {step+1} 次，等待 {wait:.0f}s（累计上限 {cap}s）')
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
                if typ in ('tpm_limit','rate_limit'):
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
                output,total=usage_tokens(response); state.finish(attempt,'success',latency=int((time.monotonic()-started)*1000),output_tokens=output,total_tokens=total); state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity['minimum_messages']) if affinity.get('enabled') else None; state.observe_success(key,ch.id)
                if ch.id in (quota_retry_channel,engine_retry_channel,five_hour_retry_channel):state.clear_cooldown(key,ch.id); state.trace_event(rid,'channel_recovered',ch.id,detail='原请求验证成功；清除临时异常状态')
                payload=data(response,name)
                if replay_allowed and is_replayable_response(payload):
                    state.replay_store(caller,name,fingerprint,ch.id,payload)
                state.trace_finish(rid,'success',ch.id,200,output_preview=response_preview(payload),latency_ms=int((time.monotonic()-req_started)*1000))
                state.debug_log(rid,'upstream_in',pool=key,channel=ch.id,model=ch.litellm_model,status=200,body=json.dumps(payload,ensure_ascii=False)[:20000])
                state.debug_log(rid,'client_out',pool=key,channel=ch.id,model=name,status=200,body=json.dumps(payload,ensure_ascii=False)[:20000])
                return JSONResponse(content=payload)
            disconnect_notice=asyncio.Event()
            async def events():
                nonlocal ch,base,key_
                ttft=None; chunks_seen=0; output_parts=[]; active_attempt=attempt; active_started=started; pending=set(); pending_channels={}; iterator=None
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
                    iterator=resp.__aiter__()
                    state.trace_event(rid,'upstream_first_sse_wait_started',target.id,detail=f'Waiting up to {UPSTREAM_FIRST_CHUNK_TIMEOUT}s for first SSE event')
                    first=await await_bounded(anext(iterator),UPSTREAM_FIRST_CHUNK_TIMEOUT)
                    state.trace_event(rid,'upstream_first_sse',target.id,detail=f'First SSE event received within {UPSTREAM_FIRST_CHUNK_TIMEOUT}s')
                    return iterator,first,attempt_id,attempt_started,label,target
                async def original_first_event(resp):
                    try:
                        return await first_event(resp,attempt,started,'original',ch)
                    except asyncio.TimeoutError:
                        elapsed_ms=int((time.monotonic()-started)*1000)
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
                        state.finish(hedge_attempt,'failure',error_type(exc),latency=int((time.monotonic()-hedge_started)*1000),error_code=error_code(exc))
                        state.trace_event(rid,'hedge_error',target.id,error_code(exc),f'第 {number} 个 Hedge 阶段异常：{error_type(exc)}')
                        raise
                def schedule_hedge_event(number,target):
                    if target.id in pending_channels.values():
                        state.trace_event(rid,'hedge_skipped',target.id,detail=f'第 {number} 个 Hedge 阶段跳过：该 Channel 仍有未完成 SSE 请求')
                        return
                    task=asyncio.create_task(hedge_event(number,target))
                    pending.add(task); pending_channels[task]=target.id
                try:
                    watch_record=app.state.first_activity_watch.get(rid)
                    if watch_record and watch_record.get('deadline_forced'):
                        detail='Watchdog had already closed this Trace at the first-activity deadline before the streaming consumer entered.'
                        await close_upstream(response)
                        state.trace_event(rid,'stream_deadline_observed',ch.id,504,detail=detail)
                        yield f'data: {json.dumps({"error":{"type":"upstream_total_timeout","detail":detail}})}\n\n'
                        return
                    state.trace_event(rid,'stream_consumer_started',ch.id,detail='Streaming response body consumer entered')
                    original_task=asyncio.create_task(original_first_event(response))
                    pending={original_task}; pending_channels[original_task]=ch.id
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
                                except Exception:
                                    pending.discard(task)
                            if winner is None and not pending: raise RuntimeError('all hedged upstream streams ended before first event')
                            continue
                        if time.monotonic()-req_started>=UPSTREAM_FIRST_ACTIVITY_TIMEOUT-0.1: raise UpstreamTotalTimeout()
                        if next_delay is None: continue
                        hedge_index+=1
                        _,target_ids=hedge_plan[hedge_index-1]
                        for target_id in target_ids: schedule_hedge_event(hedge_index,channel_by_id[target_id])
                    iterator,first_item,active_attempt,active_started,label,winner_channel=winner
                    ch=winner_channel; base,key_=channel_credentials(ch,config.providers)
                    # A protocol event has arrived.  No more first-activity
                    # hedges or deadline checks are appropriate for this trace.
                    stop_first_activity_watch()
                    if label!='original': state.trace_event(rid,'hedge_won',ch.id,detail=f'{label} produced the first upstream SSE event')
                    for task in pending:
                        if not task.done():
                            state.trace_event(rid,'upstream_cancel_requested',pending_channels.get(task),detail='Cancelled losing Hedge task after first SSE winner')
                            task.cancel()
                    if pending:await asyncio.gather(*pending,return_exceptions=True)
                    async def selected_items():
                        yield first_item
                        async for next_item in iterator:yield next_item
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
                    state.debug_log(rid,'client_out',pool=key,channel=ch.id,model=name,status=200,body=f'[stream] {chunks_seen} chunks delivered')
                    state.finish(active_attempt,'success',latency=int((time.monotonic()-active_started)*1000),ttft_ms=ttft); state.remember_affinity(key,body['messages'],ch.id,affinity['idle_seconds'],affinity['minimum_messages']) if affinity.get('enabled') else None
                    if ch.id in (quota_retry_channel,engine_retry_channel,five_hour_retry_channel):state.clear_cooldown(key,ch.id); state.trace_event(rid,'channel_recovered',ch.id,detail='原请求验证成功；清除临时异常状态')
                    state.trace_finish(rid,'success',ch.id,200,output_preview=''.join(output_parts)[:512],ttft_ms=ttft,latency_ms=int((time.monotonic()-req_started)*1000)); yield 'data: [DONE]\n\n'
                except asyncio.CancelledError:
                    for task in pending:
                        if not task.done():task.cancel()
                    if pending:await asyncio.gather(*pending,return_exceptions=True)
                    await close_upstream(iterator)
                    await close_upstream(response)
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
                    state.trace_event(rid,'upstream_cancel_requested',ch.id,504,detail='First-activity deadline reached; detaching all pending upstream tasks')
                    cancel_detached(pending)
                    await close_upstream(iterator)
                    await close_upstream(response)
                    detail=f'No upstream SSE activity before Router {first_activity_deadline//60}-minute safety deadline; cancelled all pending attempts.'
                    state.trace_event(rid,'upstream_total_timeout',ch.id,504,detail)
                    state.finish(active_attempt,'failure','upstream_total_timeout',int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=504)
                    state.trace_finish(rid,'failed',ch.id,504,'upstream_total_timeout',detail,latency_ms=int((time.monotonic()-req_started)*1000))
                    yield f'data: {json.dumps({"error":{"type":"upstream_total_timeout","detail":detail}})}\n\n'
                    return
                except Exception as e:
                    typ=error_type(e); detail=error_detail(e)
                    state.finish(active_attempt,'failure',typ,int((time.monotonic()-active_started)*1000),error_detail=detail,error_code=error_code(e))
                    state.trace_finish(rid,'failed',ch.id,error_code(e) or 502,typ,detail,latency_ms=int((time.monotonic()-req_started)*1000))
                    if typ in ('rate_limit','tpm_limit','quota_exhausted'):
                        state.observe_429(key,ch.id,detail,kind={'rate_limit':'rpm','tpm_limit':'tpm','quota_exhausted':'quota_exhausted'}[typ],limits=ch.limits)
                        cooling=state.cooldown_state(key,ch.id)
                        state.trace_event(rid,'limit_observed',ch.id,error_code(e),f'{typ} received during stream; cooldown '+(f"set until {clock(cooling['until'])} ({cooling['reason']})" if cooling else 'not set yet'))
                    logger.warning('req=%s channel=%s stream error=%s detail=%s',name,ch.id,typ,detail)
                    yield f'data: {json.dumps({"error":{"type":typ,"detail":detail}})}\n\n'
                    return
                finally:
                    stop_first_activity_watch()
                    if 'watchdog_task' in locals() and not watchdog_task.done():
                        watchdog_task.cancel()
            return DisconnectAwareStreamingResponse(events(),on_disconnect=disconnect_notice.set,media_type='text/event-stream',headers={'Cache-Control':'no-cache','Connection':'keep-alive','X-Accel-Buffering':'no'})
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
                    reattach = sel.get('reattach', {}) if isinstance(sel, dict) else {}
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

    @app.on_event('startup')
    async def _start_probe_loop():
        t = asyncio.create_task(_probe_loop())
        app.state.probe_tasks.add(t); t.add_done_callback(app.state.probe_tasks.discard)
        watchdog = asyncio.create_task(_first_activity_watchdog())
        app.state.watchdog_tasks.add(watchdog); watchdog.add_done_callback(app.state.watchdog_tasks.discard)

    return app
