from __future__ import annotations
import hashlib, hmac, json, os, secrets, sqlite3, time
from pathlib import Path
from threading import RLock

# 学习值采信门槛: 429 样本 confidence 达到此值才用 learned limit 做限流决策.
# 几十上百个样本才有统计规律; 三五个样本学出的规则不可信(会锁死正常通道).
LEARNED_MIN_CONFIDENCE=int(os.getenv('FLEX_LEARNED_MIN_CONFIDENCE','100'))
RESPONSE_REPLAY_SECONDS=int(os.getenv('FLEX_RESPONSE_REPLAY_SECONDS','120'))
RESPONSE_REPLAY_MAX_BYTES=int(os.getenv('FLEX_RESPONSE_REPLAY_MAX_BYTES','1048576'))

class StateStore:
    def __init__(self, path: str):
        p=Path(path); p.parent.mkdir(parents=True, exist_ok=True)
        self.db=sqlite3.connect(p, check_same_thread=False); self.db.row_factory=sqlite3.Row; self.lock=RLock(); self.session_key=self._load_session_key(p.parent/'session-hmac.key')
        with self.db:
            self.db.executescript('''CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY,started REAL,pool TEXT,channel TEXT,model TEXT,outcome TEXT,error_type TEXT,latency_ms INTEGER,input_tokens INTEGER,output_tokens INTEGER,total_tokens INTEGER); CREATE INDEX IF NOT EXISTS attempts_channel_time ON attempts(pool,channel,started); CREATE TABLE IF NOT EXISTS states(pool TEXT,channel TEXT,until REAL,reason TEXT,PRIMARY KEY(pool,channel)); CREATE TABLE IF NOT EXISTS busy_counts(pool TEXT,channel TEXT,count INTEGER,window_start REAL,PRIMARY KEY(pool,channel)); CREATE TABLE IF NOT EXISTS probe_log(pool TEXT,channel TEXT,last_probe_at REAL,probe_fail_streak INTEGER,PRIMARY KEY(pool,channel)); CREATE TABLE IF NOT EXISTS quota_resets(pool TEXT,channel TEXT,reset_at REAL,PRIMARY KEY(pool,channel)); CREATE TABLE IF NOT EXISTS channel_overrides(pool TEXT,channel TEXT,enabled INTEGER,PRIMARY KEY(pool,channel)); CREATE TABLE IF NOT EXISTS channel_tests(pool TEXT,channel TEXT,tested_at REAL,outcome TEXT,error_type TEXT,latency_ms INTEGER,error_detail TEXT,PRIMARY KEY(pool,channel)); CREATE TABLE IF NOT EXISTS learned_limits(pool TEXT,channel TEXT,safe_rpm INTEGER,safe_tpm INTEGER,last_429_at REAL,last_429_kind TEXT,last_429_evidence TEXT,confidence INTEGER NOT NULL DEFAULT 0,success_since_429 INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(pool,channel)); CREATE TABLE IF NOT EXISTS session_affinity(pool TEXT,prefix_hmac TEXT,channel TEXT,updated REAL,PRIMARY KEY(pool,prefix_hmac)); CREATE INDEX IF NOT EXISTS session_affinity_updated ON session_affinity(updated); CREATE TABLE IF NOT EXISTS request_traces(trace_id TEXT PRIMARY KEY,started REAL,updated REAL,status TEXT,requested_model TEXT,pool TEXT,client_label TEXT,input_preview TEXT,context_summary TEXT,stream INTEGER,attempt_count INTEGER NOT NULL DEFAULT 0,fallback_count INTEGER NOT NULL DEFAULT 0,final_channel TEXT,http_status INTEGER,error_type TEXT,error_detail TEXT,output_preview TEXT,ttft_ms INTEGER,latency_ms INTEGER); CREATE INDEX IF NOT EXISTS request_traces_status_updated ON request_traces(status,updated); CREATE TABLE IF NOT EXISTS trace_events(id INTEGER PRIMARY KEY,trace_id TEXT NOT NULL,ts REAL,event TEXT NOT NULL,channel TEXT,http_status INTEGER,detail TEXT); CREATE INDEX IF NOT EXISTS trace_events_trace_id_id ON trace_events(trace_id,id);''')
            # Trace list rendering joins attempts by trace_id.  Without this
            # index the correlated channel-count subquery scans the entire
            # attempts table once per retained trace and can starve the
            # asyncio watchdog while the UI polls every few seconds.
            self.db.execute('CREATE INDEX IF NOT EXISTS attempts_trace_id ON attempts(trace_id)')
            if 'error_detail' not in [row[1] for row in self.db.execute('PRAGMA table_info(channel_tests)')]:self.db.execute('ALTER TABLE channel_tests ADD COLUMN error_detail TEXT')
            for column in ('input_tokens','output_tokens','total_tokens','ttft_ms'):
                if column not in [row[1] for row in self.db.execute('PRAGMA table_info(attempts)')]:self.db.execute(f'ALTER TABLE attempts ADD COLUMN {column} INTEGER')
            if 'success_since_429' not in [row[1] for row in self.db.execute('PRAGMA table_info(learned_limits)')]:self.db.execute('ALTER TABLE learned_limits ADD COLUMN success_since_429 INTEGER NOT NULL DEFAULT 0')
            if 'error_detail' not in [row[1] for row in self.db.execute('PRAGMA table_info(attempts)')]:self.db.execute('ALTER TABLE attempts ADD COLUMN error_detail TEXT')
            if 'error_code' not in [row[1] for row in self.db.execute('PRAGMA table_info(attempts)')]:self.db.execute('ALTER TABLE attempts ADD COLUMN error_code INTEGER')
            if 'trace_id' not in [row[1] for row in self.db.execute('PRAGMA table_info(attempts)')]:self.db.execute('ALTER TABLE attempts ADD COLUMN trace_id TEXT')
            if 'client_label' not in [row[1] for row in self.db.execute('PRAGMA table_info(request_traces)')]:self.db.execute('ALTER TABLE request_traces ADD COLUMN client_label TEXT')
            if 'request_fingerprint' not in [row[1] for row in self.db.execute('PRAGMA table_info(request_traces)')]:self.db.execute('ALTER TABLE request_traces ADD COLUMN request_fingerprint TEXT')
            self.db.execute('CREATE INDEX IF NOT EXISTS request_traces_duplicate_lookup ON request_traces(status,client_label,requested_model,request_fingerprint,started)')
            # 短时、精确匹配的非流式结果回放。这里保存的是完整响应（不是 prompt），
            # 因此 TTL 很短且有大小上限；分析表和调用轨迹不会保存这份内容。
            self.db.execute('''CREATE TABLE IF NOT EXISTS response_replays(
                client_label TEXT NOT NULL,requested_model TEXT NOT NULL,request_fingerprint TEXT NOT NULL,
                final_channel TEXT,payload TEXT NOT NULL,created_at REAL NOT NULL,expires_at REAL NOT NULL,
                PRIMARY KEY(client_label,requested_model,request_fingerprint))''')
            self.db.execute('CREATE INDEX IF NOT EXISTS response_replays_expires ON response_replays(expires_at)')
            self.db.execute('''CREATE TABLE IF NOT EXISTS duplicate_observations(
                id INTEGER PRIMARY KEY, observed_at REAL NOT NULL, new_trace_id TEXT NOT NULL,
                existing_trace_id TEXT NOT NULL, client_label TEXT, requested_model TEXT, pool TEXT,
                overlap_ms INTEGER NOT NULL, existing_attempt_count INTEGER NOT NULL DEFAULT 0,
                existing_has_output INTEGER NOT NULL DEFAULT 0, decision TEXT NOT NULL)''')
            self.db.execute('CREATE INDEX IF NOT EXISTS duplicate_observations_time ON duplicate_observations(observed_at)')
            # Long-lived, prompt-free aggregate source. Trace detail itself remains short-retention.
            self.db.execute('CREATE TABLE IF NOT EXISTS request_outcomes(trace_id TEXT PRIMARY KEY,started REAL,completed REAL,status TEXT)')
            self.db.execute('CREATE TABLE IF NOT EXISTS request_error_outcomes(trace_id TEXT,error_type TEXT,first_at REAL,recovery_seconds REAL,final_failed INTEGER,PRIMARY KEY(trace_id,error_type))')
            self.db.execute('CREATE INDEX IF NOT EXISTS request_outcomes_started ON request_outcomes(started)')
            # Prompt-free request facts are retained much longer than Trace detail.
            # They are the source for future analysis without retaining messages/output.
            for column,kind in (('pool','TEXT'),('first_channel','TEXT'),('attempt_count','INTEGER NOT NULL DEFAULT 0'),
                                ('final_channel','TEXT'),('requested_model','TEXT'),('client_label','TEXT'),
                                ('input_bucket','TEXT'),('input_tokens','INTEGER'),('output_tokens','INTEGER'),
                                ('total_tokens','INTEGER'),('ttft_ms','INTEGER'),('latency_ms','INTEGER'),
                                ('fallback_count','INTEGER NOT NULL DEFAULT 0'),('error_type','TEXT'),
                                ('channel_count','INTEGER NOT NULL DEFAULT 0'),('cross_channel','INTEGER NOT NULL DEFAULT 0')):
                if column not in [row[1] for row in self.db.execute('PRAGMA table_info(request_outcomes)')]:self.db.execute(f'ALTER TABLE request_outcomes ADD COLUMN {column} {kind}')
            self.db.execute('CREATE INDEX IF NOT EXISTS request_error_outcomes_type ON request_error_outcomes(error_type)')
            self.db.execute('''CREATE TABLE IF NOT EXISTS daily_request_analytics(
                day TEXT NOT NULL,pool TEXT NOT NULL,channel TEXT NOT NULL,requested_model TEXT NOT NULL,input_bucket TEXT NOT NULL,
                requests INTEGER NOT NULL DEFAULT 0,success INTEGER NOT NULL DEFAULT 0,first_success INTEGER NOT NULL DEFAULT 0,retry_success INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,cancelled INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
                input_tokens INTEGER NOT NULL DEFAULT 0,output_tokens INTEGER NOT NULL DEFAULT 0,total_tokens INTEGER NOT NULL DEFAULT 0,
                ttft_samples INTEGER NOT NULL DEFAULT 0,ttft_sum_ms INTEGER NOT NULL DEFAULT 0,
                latency_samples INTEGER NOT NULL DEFAULT 0,latency_sum_ms INTEGER NOT NULL DEFAULT 0,latency_max_ms INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(day,pool,channel,requested_model,input_bucket))''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS daily_error_analytics(
                day TEXT NOT NULL,pool TEXT NOT NULL,channel TEXT NOT NULL,error_type TEXT NOT NULL,
                occurrences INTEGER NOT NULL DEFAULT 0,requests INTEGER NOT NULL DEFAULT 0,final_failed INTEGER NOT NULL DEFAULT 0,
                recovered INTEGER NOT NULL DEFAULT 0,recovery_samples INTEGER NOT NULL DEFAULT 0,recovery_sum_seconds REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(day,pool,channel,error_type))''')
            self.db.execute('CREATE TABLE IF NOT EXISTS analytics_rollup_marks(trace_id TEXT PRIMARY KEY,rolled_at REAL NOT NULL)')
            # DEBUG 请求/响应日志: FLEX_DEBUG=1 时记录四方向 payload, 上限 max_rows 条 / 保留 retention_days 天
            self.debug_enabled=os.getenv('FLEX_DEBUG','0')=='1'
            self.debug_max_rows=int(os.getenv('FLEX_DEBUG_MAX_ROWS','1000'))
            self.debug_retention_days=int(os.getenv('FLEX_DEBUG_RETENTION_DAYS','7'))
            self.db.execute('''CREATE TABLE IF NOT EXISTS debug_logs(id INTEGER PRIMARY KEY,ts REAL,request_id TEXT,dir TEXT,pool TEXT,channel TEXT,model TEXT,status INTEGER,body TEXT)''')
            self.db.execute('CREATE INDEX IF NOT EXISTS debug_logs_ts ON debug_logs(ts)')
            # 独立于通用 DEBUG 的短期全文上行请求留存。仅在用户显式开启时写入，
            # 便于复现特定 payload；默认关闭，且由时间/条数/空间三重上限清理。
            self.full_request_capture_enabled=os.getenv('FLEX_FULL_REQUEST_CAPTURE','0')=='1'
            self.full_request_capture_hours=int(os.getenv('FLEX_FULL_REQUEST_CAPTURE_HOURS','3'))
            self.full_request_capture_max_rows=int(os.getenv('FLEX_FULL_REQUEST_CAPTURE_MAX_ROWS','300'))
            self.full_request_capture_max_bytes=int(os.getenv('FLEX_FULL_REQUEST_CAPTURE_MAX_BYTES',str(256*1024*1024)))
            self.db.execute('''CREATE TABLE IF NOT EXISTS full_request_captures(
                id INTEGER PRIMARY KEY,ts REAL NOT NULL,expires_at REAL NOT NULL,trace_id TEXT NOT NULL,
                client_label TEXT,requested_model TEXT,pool TEXT,size_bytes INTEGER NOT NULL,body TEXT NOT NULL)''')
            self.db.execute('CREATE INDEX IF NOT EXISTS full_request_captures_expiry ON full_request_captures(expires_at)')
            self.db.execute('CREATE INDEX IF NOT EXISTS full_request_captures_trace ON full_request_captures(trace_id)')
    # ---- DEBUG 日志 ----
    def debug_log(self,request_id,dir_,pool='',channel='',model='',status=None,body=''):
        """记录一次方向的 payload. dir: client_in/client_out/upstream_out/upstream_in.
        仅 FLEX_DEBUG=1 生效; 超过 max_rows 删最旧; 超 retention_days 清理."""
        if not getattr(self,'debug_enabled',False):return
        now=time.time()
        with self.lock,self.db:
            self.db.execute('INSERT INTO debug_logs(ts,request_id,dir,pool,channel,model,status,body) VALUES(?,?,?,?,?,?,?,?)',
                            (now,request_id,dir_,pool,channel,model,status,str(body)[:20000]))
            self.db.execute(f'DELETE FROM debug_logs WHERE id NOT IN (SELECT id FROM debug_logs ORDER BY id DESC LIMIT {self.debug_max_rows})')
            self.db.execute('DELETE FROM debug_logs WHERE ts<?',(now-self.debug_retention_days*86400,))
    def debug_recent(self,limit=50):
        with self.lock:
            return [dict(r) for r in self.db.execute('SELECT * FROM debug_logs ORDER BY id DESC LIMIT ?',(limit,))]
    # ---- 独立的短期全文上行请求留存（默认关闭） ----
    def configure_full_request_capture(self,enabled=None,hours=None,max_rows=None,max_bytes=None):
        with self.lock,self.db:
            if enabled is not None:self.full_request_capture_enabled=bool(enabled)
            if hours is not None:self.full_request_capture_hours=max(1,min(int(hours),24))
            if max_rows is not None:self.full_request_capture_max_rows=max(1,min(int(max_rows),10000))
            if max_bytes is not None:self.full_request_capture_max_bytes=max(1024*1024,min(int(max_bytes),2048*1024*1024))
            self._clean_full_request_captures()
            return self.full_request_capture_status()
    def _clean_full_request_captures(self):
        now=time.time(); self.db.execute('DELETE FROM full_request_captures WHERE expires_at<=?',(now,))
        excess=self.db.execute('SELECT id FROM full_request_captures ORDER BY id DESC LIMIT -1 OFFSET ?',(self.full_request_capture_max_rows,)).fetchall()
        if excess:self.db.executemany('DELETE FROM full_request_captures WHERE id=?',[(row['id'],) for row in excess])
        while True:
            row=self.db.execute('SELECT COALESCE(SUM(size_bytes),0) AS total FROM full_request_captures').fetchone()
            if int(row['total'] or 0)<=self.full_request_capture_max_bytes:break
            oldest=self.db.execute('SELECT id FROM full_request_captures ORDER BY id LIMIT 1').fetchone()
            if not oldest:break
            self.db.execute('DELETE FROM full_request_captures WHERE id=?',(oldest['id'],))
    def full_request_capture_status(self):
        with self.lock:
            row=self.db.execute('SELECT COUNT(*) AS count,COALESCE(SUM(size_bytes),0) AS bytes,MIN(ts) AS oldest,MAX(ts) AS newest FROM full_request_captures').fetchone()
            return {'enabled':self.full_request_capture_enabled,'retention_hours':self.full_request_capture_hours,'max_rows':self.full_request_capture_max_rows,'max_bytes':self.full_request_capture_max_bytes,'count':int(row['count'] or 0),'bytes':int(row['bytes'] or 0),'oldest_at':row['oldest'],'newest_at':row['newest']}
    def capture_full_request(self,trace_id,client_label,requested_model,pool,payload):
        if not self.full_request_capture_enabled:return False
        encoded=json.dumps(payload,ensure_ascii=False,separators=(',',':'))
        size=len(encoded.encode('utf-8')); now=time.time()
        with self.lock,self.db:
            self.db.execute('INSERT INTO full_request_captures(ts,expires_at,trace_id,client_label,requested_model,pool,size_bytes,body) VALUES(?,?,?,?,?,?,?,?)',(now,now+self.full_request_capture_hours*3600,trace_id,client_label,requested_model,pool,size,encoded))
            self._clean_full_request_captures()
        return True
    def full_request_capture(self,trace_id):
        """Return one retained request body for the local admin viewer only.

        The body deliberately stays out of normal trace/list responses.  A
        caller must explicitly request it, and only while the short retention
        record remains available.
        """
        with self.lock,self.db:
            self._clean_full_request_captures()
            row=self.db.execute('SELECT ts,expires_at,size_bytes,body FROM full_request_captures WHERE trace_id=? ORDER BY id DESC LIMIT 1',(trace_id,)).fetchone()
            if not row:return None
            try: payload=json.loads(row['body'])
            except (TypeError,json.JSONDecodeError):return None
            return {'captured_at':row['ts'],'expires_at':row['expires_at'],'size_bytes':row['size_bytes'],'payload':payload}
    # ---- 调用轨迹（默认不保存完整 prompt / response） ----
    def _clean_traces(self):
        cutoff=time.time()-3*86400
        self.db.execute("DELETE FROM trace_events WHERE trace_id IN (SELECT trace_id FROM request_traces WHERE updated<?)",(cutoff,))
        self.db.execute('DELETE FROM request_traces WHERE updated<?',(cutoff,))
        excess=self.db.execute("SELECT trace_id FROM request_traces WHERE status!='running' ORDER BY updated DESC LIMIT -1 OFFSET 1000").fetchall()
        if excess:
            ids=[row['trace_id'] for row in excess]; marks=','.join('?'*len(ids))
            self.db.execute(f'DELETE FROM trace_events WHERE trace_id IN ({marks})',ids)
            self.db.execute(f'DELETE FROM request_traces WHERE trace_id IN ({marks})',ids)
    def request_fingerprint(self,model,body):
        """Stable, keyed digest only; never persist raw request content for duplicate observation."""
        encoded=json.dumps({'model':model,'request':body},sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str).encode()
        return hmac.new(self.session_key,b'flex-duplicate-v1\0'+encoded,hashlib.sha256).hexdigest()
    def replay_lookup(self,client_label,model,fingerprint):
        """Return one still-valid exact result; raw request text is never stored."""
        now=time.time()
        with self.lock,self.db:
            self.db.execute('DELETE FROM response_replays WHERE expires_at<=?',(now,))
            row=self.db.execute('''SELECT final_channel,payload,created_at FROM response_replays
                WHERE client_label=? AND requested_model=? AND request_fingerprint=? AND expires_at>?''',
                (client_label,model,fingerprint,now)).fetchone()
            if not row:return None
            try: payload=json.loads(row['payload'])
            except (TypeError,json.JSONDecodeError):
                self.db.execute('DELETE FROM response_replays WHERE client_label=? AND requested_model=? AND request_fingerprint=?',(client_label,model,fingerprint)); return None
            return {'channel':row['final_channel'],'payload':payload,'age_seconds':max(0,now-row['created_at'])}
    def replay_store(self,client_label,model,fingerprint,channel,payload):
        """Save a bounded, successful non-stream response for the very short replay window."""
        try: encoded=json.dumps(payload,ensure_ascii=False,separators=(',',':'))
        except (TypeError,ValueError):return False
        if len(encoded.encode('utf-8'))>RESPONSE_REPLAY_MAX_BYTES:return False
        now=time.time()
        with self.lock,self.db:
            self.db.execute('DELETE FROM response_replays WHERE expires_at<=?',(now,))
            self.db.execute('''INSERT INTO response_replays(client_label,requested_model,request_fingerprint,final_channel,payload,created_at,expires_at)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(client_label,requested_model,request_fingerprint) DO UPDATE SET
                final_channel=excluded.final_channel,payload=excluded.payload,created_at=excluded.created_at,expires_at=excluded.expires_at''',
                (client_label,model,fingerprint,channel,encoded,now,now+RESPONSE_REPLAY_SECONDS))
        return True
    def trace_begin(self,trace_id,model,pool,input_preview,context_summary,stream,client_label='未识别本机客户端',request_fingerprint=None):
        now=time.time()
        with self.lock,self.db:
            self._clean_traces()
            self.db.execute('INSERT INTO request_traces(trace_id,started,updated,status,requested_model,pool,client_label,input_preview,context_summary,stream,request_fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(trace_id,now,now,'running',model,pool,client_label,input_preview,context_summary,int(stream),request_fingerprint))
            self.trace_event(trace_id,'received',detail='Request received')
    def observe_inflight_duplicates(self,trace_id,client_label,model,pool,fingerprint,window_seconds=60):
        """Observation only. No request is delayed, shared, cancelled, or otherwise altered."""
        if not fingerprint:return []
        now=time.time(); cutoff=now-window_seconds
        with self.lock,self.db:
            matches=self.db.execute('''SELECT trace_id,started,attempt_count FROM request_traces
                WHERE trace_id!=? AND status='running' AND client_label=? AND requested_model=?
                  AND request_fingerprint=? AND started>=? ORDER BY started''',(trace_id,client_label,model,fingerprint,cutoff)).fetchall()
            findings=[]
            for old in matches:
                has_output=bool(self.db.execute("SELECT 1 FROM trace_events WHERE trace_id=? AND event='first_token' LIMIT 1",(old['trace_id'],)).fetchone())
                overlap=max(0,int((now-old['started'])*1000))
                self.db.execute('INSERT INTO duplicate_observations(observed_at,new_trace_id,existing_trace_id,client_label,requested_model,pool,overlap_ms,existing_attempt_count,existing_has_output,decision) VALUES(?,?,?,?,?,?,?,?,?,?)',(now,trace_id,old['trace_id'],client_label,model,pool,overlap,old['attempt_count'],int(has_output),'observed_only'))
                findings.append({'trace_id':old['trace_id'],'overlap_ms':overlap,'has_output':has_output})
            return findings
    def duplicate_statistics(self,period='day'):
        now=time.time()
        if period=='day':
            local=time.localtime(now); since=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1))
        else: since=now-{'week':7*86400,'month':30*86400}.get(period,86400)
        with self.lock:
            total=self.db.execute('SELECT COUNT(*) AS count,AVG(overlap_ms) AS avg_overlap_ms,SUM(existing_has_output) AS with_output FROM duplicate_observations WHERE observed_at>=?',(since,)).fetchone()
            rows=self.db.execute('''SELECT requested_model,client_label,COUNT(*) AS count,AVG(overlap_ms) AS avg_overlap_ms,
                SUM(existing_has_output) AS with_output FROM duplicate_observations WHERE observed_at>=?
                GROUP BY requested_model,client_label ORDER BY count DESC''',(since,)).fetchall()
            return {'period':period,'count':total['count'] or 0,'avg_overlap_ms':total['avg_overlap_ms'],'with_output':total['with_output'] or 0,'rows':[dict(row) for row in rows]}
    def trace_event(self,trace_id,event,channel=None,http_status=None,detail=None):
        now=time.time()
        with self.lock,self.db:
            self.db.execute('INSERT INTO trace_events(trace_id,ts,event,channel,http_status,detail) VALUES(?,?,?,?,?,?)',(trace_id,now,event,channel,http_status,(detail or '')[:800]))
            self.db.execute('UPDATE request_traces SET updated=? WHERE trace_id=?',(now,trace_id))
    def trace_attempt(self,trace_id,channel):
        with self.lock,self.db:self.db.execute('UPDATE request_traces SET attempt_count=attempt_count+1,updated=? WHERE trace_id=?',(time.time(),trace_id))
    def trace_fallback(self,trace_id,detail):
        with self.lock,self.db:
            self.db.execute('UPDATE request_traces SET fallback_count=fallback_count+1,updated=? WHERE trace_id=?',(time.time(),trace_id))
            self.trace_event(trace_id,'fallback',detail=detail)
    def trace_finish(self,trace_id,status,channel=None,http_status=None,error_type=None,error_detail=None,output_preview=None,ttft_ms=None,latency_ms=None):
        now=time.time()
        with self.lock,self.db:
            self.db.execute('UPDATE request_traces SET updated=?,status=?,final_channel=?,http_status=?,error_type=?,error_detail=?,output_preview=?,ttft_ms=?,latency_ms=? WHERE trace_id=?',(now,status,channel,http_status,error_type,error_detail,output_preview,ttft_ms,latency_ms,trace_id))
            trace=self.db.execute('SELECT * FROM request_traces WHERE trace_id=?',(trace_id,)).fetchone()
            if trace:
                first=self.db.execute('SELECT channel FROM attempts WHERE trace_id=? ORDER BY id LIMIT 1',(trace_id,)).fetchone()
                channel_count=self.db.execute('SELECT COUNT(DISTINCT channel) AS count FROM attempts WHERE trace_id=?',(trace_id,)).fetchone()['count'] or 0
                metrics=self._trace_metrics(trace_id)
                self.db.execute('''INSERT INTO request_outcomes(trace_id,started,completed,status,pool,first_channel,attempt_count,final_channel,requested_model,client_label,input_bucket,input_tokens,output_tokens,total_tokens,ttft_ms,latency_ms,fallback_count,error_type,channel_count,cross_channel)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(trace_id) DO UPDATE SET
                    completed=excluded.completed,status=excluded.status,pool=excluded.pool,first_channel=excluded.first_channel,attempt_count=excluded.attempt_count,final_channel=excluded.final_channel,requested_model=excluded.requested_model,client_label=excluded.client_label,input_bucket=excluded.input_bucket,input_tokens=excluded.input_tokens,output_tokens=excluded.output_tokens,total_tokens=excluded.total_tokens,ttft_ms=excluded.ttft_ms,latency_ms=excluded.latency_ms,fallback_count=excluded.fallback_count,error_type=excluded.error_type,channel_count=excluded.channel_count,cross_channel=excluded.cross_channel''',
                    (trace_id,trace['started'],now,status,trace['pool'],first['channel'] if first else None,trace['attempt_count'],trace['final_channel'],trace['requested_model'],trace['client_label'],metrics['input_bucket'],metrics['input_tokens'],metrics['output_tokens'],metrics['total_tokens'],trace['ttft_ms'],trace['latency_ms'],trace['fallback_count'],trace['error_type'],channel_count,int(channel_count>1)))
                self.db.execute('DELETE FROM request_error_outcomes WHERE trace_id=?',(trace_id,))
                errors=self.db.execute("SELECT error_type,MIN(started) AS first_at FROM attempts WHERE trace_id=? AND outcome='failure' AND error_type IS NOT NULL GROUP BY error_type",(trace_id,)).fetchall()
                if not errors and error_type: errors=[{'error_type':error_type,'first_at':trace['started']}]
                for item in errors:
                    recovered=(now-item['first_at']) if status=='success' else None
                    self.db.execute('INSERT INTO request_error_outcomes(trace_id,error_type,first_at,recovery_seconds,final_failed) VALUES(?,?,?,?,?)',(trace_id,item['error_type'],item['first_at'],recovered,int(status!='success')))
                self._rollup_analytics(trace_id,trace,metrics,errors,now)
                self.db.execute('DELETE FROM request_error_outcomes WHERE trace_id IN (SELECT trace_id FROM request_outcomes WHERE started<?)',(now-400*86400,))
                self.db.execute('DELETE FROM request_outcomes WHERE started<?',(now-400*86400,))
            self.trace_event(trace_id,'completed' if status=='success' else status,channel,http_status,error_detail or status)
            self._clean_traces()
    @staticmethod
    def _input_bucket(tokens):
        if tokens is None:return 'unknown'
        if tokens<50000:return '<50k'
        if tokens<100000:return '50-100k'
        if tokens<150000:return '100-150k'
        if tokens<200000:return '150-200k'
        return '200k+'

    def _trace_metrics(self,trace_id):
        row=self.db.execute('''SELECT MAX(COALESCE(input_tokens,0)) AS input_tokens,
            MAX(COALESCE(output_tokens,0)) AS output_tokens,MAX(COALESCE(total_tokens,0)) AS total_tokens
            FROM attempts WHERE trace_id=?''',(trace_id,)).fetchone()
        input_tokens=int(row['input_tokens'] or 0)
        return {'input_tokens':input_tokens,'output_tokens':int(row['output_tokens'] or 0),
                'total_tokens':int(row['total_tokens'] or 0),'input_bucket':self._input_bucket(input_tokens)}

    def _rollup_analytics(self,trace_id,trace,metrics,errors,completed):
        """Idempotently add one prompt-free request fact into durable daily aggregates."""
        if self.db.execute('SELECT 1 FROM analytics_rollup_marks WHERE trace_id=?',(trace_id,)).fetchone():return
        day=time.strftime('%Y-%m-%d',time.localtime(trace['started']))
        channel=trace['final_channel'] or self.db.execute('SELECT channel FROM attempts WHERE trace_id=? ORDER BY id LIMIT 1',(trace_id,)).fetchone()
        channel=channel['channel'] if hasattr(channel,'keys') else (channel or 'unselected')
        status=trace['status']; attempts=int(trace['attempt_count'] or 0); success=int(status=='success')
        # 0 次上游 attempt 仅可能是短时结果恢复；从调用方角度仍是一次成功。
        first_success=int(success and attempts<=1); retry_success=int(success and attempts>1)
        failed=int(status=='failed'); cancelled=int(status=='cancelled')
        ttft=trace['ttft_ms']; latency=trace['latency_ms']
        self.db.execute('''INSERT INTO daily_request_analytics(day,pool,channel,requested_model,input_bucket,requests,success,first_success,retry_success,failed,cancelled,attempts,input_tokens,output_tokens,total_tokens,ttft_samples,ttft_sum_ms,latency_samples,latency_sum_ms,latency_max_ms)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(day,pool,channel,requested_model,input_bucket) DO UPDATE SET
            requests=requests+1,success=success+excluded.success,first_success=first_success+excluded.first_success,retry_success=retry_success+excluded.retry_success,failed=failed+excluded.failed,cancelled=cancelled+excluded.cancelled,attempts=attempts+excluded.attempts,input_tokens=input_tokens+excluded.input_tokens,output_tokens=output_tokens+excluded.output_tokens,total_tokens=total_tokens+excluded.total_tokens,ttft_samples=ttft_samples+excluded.ttft_samples,ttft_sum_ms=ttft_sum_ms+excluded.ttft_sum_ms,latency_samples=latency_samples+excluded.latency_samples,latency_sum_ms=latency_sum_ms+excluded.latency_sum_ms,latency_max_ms=MAX(latency_max_ms,excluded.latency_max_ms)''',
            (day,trace['pool'] or 'unclassified',channel,trace['requested_model'] or 'unknown',metrics['input_bucket'],1,success,first_success,retry_success,failed,cancelled,attempts,metrics['input_tokens'],metrics['output_tokens'],metrics['total_tokens'],int(ttft is not None),int(ttft or 0),int(latency is not None),int(latency or 0),int(latency or 0)))
        for item in errors:
            error=item['error_type']; recovery=(completed-item['first_at']) if success else None
            self.db.execute('''INSERT INTO daily_error_analytics(day,pool,channel,error_type,occurrences,requests,final_failed,recovered,recovery_samples,recovery_sum_seconds)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(day,pool,channel,error_type) DO UPDATE SET occurrences=occurrences+1,requests=requests+1,final_failed=final_failed+excluded.final_failed,recovered=recovered+excluded.recovered,recovery_samples=recovery_samples+excluded.recovery_samples,recovery_sum_seconds=recovery_sum_seconds+excluded.recovery_sum_seconds''',
                (day,trace['pool'] or 'unclassified',channel,error,1,1,int(not success),int(success),int(recovery is not None),float(recovery or 0)))
        self.db.execute('INSERT INTO analytics_rollup_marks(trace_id,rolled_at) VALUES(?,?)',(trace_id,completed))

    def cancel_interrupted_traces(self):
        """A process restart cannot resume an in-memory request; close stale running traces honestly."""
        with self.lock,self.db:
            rows=self.db.execute("SELECT trace_id FROM request_traces WHERE status='running'").fetchall()
            for row in rows:
                trace_id=row['trace_id']; now=time.time()
                self.db.execute("UPDATE request_traces SET updated=?,status='cancelled',error_type='router_restarted',error_detail='Router restarted while this request was in progress' WHERE trace_id=?",(now,trace_id))
                self.db.execute("INSERT INTO trace_events(trace_id,ts,event,detail) VALUES(?,?,?,?)",(trace_id,now,'cancelled','Router restarted while request was in progress'))
            return len(rows)
    def traces(self,status=None,limit=100):
        with self.lock:
            # The list view needs only a prompt-free Channel count to distinguish
            # a same-channel retry from a successful cross-Channel Hedge.
            sql='''SELECT t.*,COALESCE(a.channel_count,0) AS channel_count
                FROM request_traces t
                LEFT JOIN (SELECT trace_id,COUNT(DISTINCT channel) AS channel_count
                    FROM attempts GROUP BY trace_id) a ON a.trace_id=t.trace_id'''; args=[]
            if status: sql+=' WHERE t.status=?'; args.append(status)
            sql+=' ORDER BY CASE t.status WHEN \'running\' THEN 0 ELSE 1 END, t.updated DESC LIMIT ?'; args.append(limit)
            return [dict(row) for row in self.db.execute(sql,args)]
    def error_statistics(self,period='day'):
        now=time.time()
        if period=='day':
            local=time.localtime(now); since=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1))
        else: since=now-{'week':7*86400,'month':30*86400}.get(period,86400)
        with self.lock:
            totals=self.db.execute("SELECT COUNT(*) AS total,SUM(CASE WHEN status!='success' THEN 1 ELSE 0 END) AS failed FROM request_outcomes WHERE started>=?",(since,)).fetchone()
            rows=self.db.execute('''SELECT e.error_type,COUNT(*) AS occurrences,COUNT(DISTINCT e.trace_id) AS requests,
                SUM(e.final_failed) AS final_failed,AVG(e.recovery_seconds) AS avg_recovery_seconds
                FROM request_error_outcomes e JOIN request_outcomes o ON o.trace_id=e.trace_id
                WHERE o.started>=? GROUP BY e.error_type ORDER BY occurrences DESC''',(since,)).fetchall()
            return {'period':period,'since':since,'requests':totals['total'] or 0,'final_failed':totals['failed'] or 0,
                    'rows':[dict(r) for r in rows]}
    def hourly_error_statistics(self):
        now=time.time(); local=time.localtime(now); start=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1))
        buckets=[{'hour':f'{hour:02d}:00','errors':0,'rpm':0,'tpm':0,'final_failed':0} for hour in range(24)]
        with self.lock:
            rows=self.db.execute('SELECT error_type,first_at,final_failed FROM request_error_outcomes WHERE first_at>=? AND first_at<?',(start,start+86400)).fetchall()
        for row in rows:
            item=buckets[time.localtime(row['first_at']).tm_hour]; item['errors']+=1
            if row['error_type']=='rate_limit':item['rpm']+=1
            if row['error_type']=='tpm_limit':item['tpm']+=1
            item['final_failed']+=row['final_failed']
        return {'start':start,'data':buckets}
    def call_statistics(self,period='day',group_by='channel'):
        now=time.time()
        if period=='day':
            local=time.localtime(now); since=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1))
        else: since=now-{'week':7*86400,'month':30*86400}.get(period,86400)
        column='channel' if group_by=='channel' else 'pool'
        with self.lock:
            totals=self.db.execute("SELECT COUNT(*) AS calls,SUM(CASE WHEN outcome='success' THEN 1 ELSE 0 END) AS success FROM attempts WHERE started>=? AND outcome!='started'",(since,)).fetchone()
            rows=self.db.execute(f'''SELECT {column} AS name,COUNT(*) AS calls,SUM(CASE WHEN outcome='success' THEN 1 ELSE 0 END) AS success,
              SUM(CASE WHEN outcome!='success' THEN 1 ELSE 0 END) AS failed,AVG(latency_ms) AS avg_latency_ms,AVG(ttft_ms) AS avg_ttft_ms,
              SUM(COALESCE(total_tokens,0)) AS total_tokens FROM attempts WHERE started>=? AND outcome!='started' GROUP BY {column} ORDER BY calls DESC''',(since,)).fetchall()
            return {'period':period,'group_by':group_by,'calls':totals['calls'] or 0,'success':totals['success'] or 0,'rows':[dict(r) for r in rows]}
    def hourly_call_statistics(self):
        now=time.time(); local=time.localtime(now); start=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1))
        buckets=[{'hour':f'{hour:02d}:00','calls':0,'success':0,'failed':0} for hour in range(24)]
        with self.lock: rows=self.db.execute("SELECT started,outcome FROM attempts WHERE started>=? AND started<? AND outcome!='started'",(start,start+86400)).fetchall()
        for row in rows:
            item=buckets[time.localtime(row['started']).tm_hour]; item['calls']+=1; item['success']+=int(row['outcome']=='success'); item['failed']+=int(row['outcome']!='success')
        return {'start':start,'data':buckets}
    def request_statistics(self,period='day',group_by='channel'):
        now=time.time(); local=time.localtime(now); since=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1)) if period=='day' else now-{'week':604800,'month':2592000}.get(period,86400)
        column="COALESCE(first_channel,'未选择通道')" if group_by=='channel' else "COALESCE(pool,'未分类池')"
        with self.lock:
            fields="COUNT(*) AS total,SUM(CASE WHEN status='success' AND attempt_count<=1 THEN 1 ELSE 0 END) AS first_success,SUM(CASE WHEN status='success' AND attempt_count>1 THEN 1 ELSE 0 END) AS retry_success,SUM(CASE WHEN status!='success' THEN 1 ELSE 0 END) AS failed,SUM(CASE WHEN cross_channel=1 THEN 1 ELSE 0 END) AS cross_total,SUM(CASE WHEN cross_channel=1 AND status='success' THEN 1 ELSE 0 END) AS cross_success,SUM(CASE WHEN cross_channel=1 AND status!='success' THEN 1 ELSE 0 END) AS cross_failed"
            rows=self.db.execute(f'''SELECT {column} AS name,{fields} FROM request_outcomes WHERE started>=? GROUP BY {column} ORDER BY total DESC''',(since,)).fetchall()
            total=self.db.execute(f"SELECT {fields} FROM request_outcomes WHERE started>=?",(since,)).fetchone()
            return {'rows':[dict(r) for r in rows],**{key:(total[key] or 0) for key in ('total','first_success','retry_success','failed','cross_total','cross_success','cross_failed')}}
    def hourly_request_statistics(self):
        now=time.time(); local=time.localtime(now); start=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1))
        buckets=[{'hour':f'{hour:02d}:00','total':0,'first_success':0,'retry_success':0,'failed':0} for hour in range(24)]
        with self.lock: rows=self.db.execute('SELECT started,status,attempt_count FROM request_outcomes WHERE started>=? AND started<?',(start,start+86400)).fetchall()
        for row in rows:
            item=buckets[time.localtime(row['started']).tm_hour]; item['total']+=1
            if row['status']!='success':item['failed']+=1
            elif row['attempt_count']==1:item['first_success']+=1
            else:item['retry_success']+=1
        return {'start':start,'data':buckets}
    def backfill_error_statistics(self):
        """One-time/idempotent import from still-retained traces so today's pre-release calls appear."""
        with self.lock,self.db:
            rows=self.db.execute("SELECT * FROM request_traces WHERE status!='running'").fetchall()
            for trace in rows:
                first=self.db.execute('SELECT channel FROM attempts WHERE trace_id=? ORDER BY id LIMIT 1',(trace['trace_id'],)).fetchone()
                channel_count=self.db.execute('SELECT COUNT(DISTINCT channel) AS count FROM attempts WHERE trace_id=?',(trace['trace_id'],)).fetchone()['count'] or 0
                self.db.execute('INSERT INTO request_outcomes(trace_id,started,completed,status,pool,first_channel,attempt_count) VALUES(?,?,?,?,?,?,?) ON CONFLICT(trace_id) DO UPDATE SET pool=excluded.pool,first_channel=excluded.first_channel,attempt_count=excluded.attempt_count,status=excluded.status,completed=excluded.completed',(trace['trace_id'],trace['started'],trace['updated'],trace['status'],trace['pool'],first['channel'] if first else None,trace['attempt_count']))
                metrics=self._trace_metrics(trace['trace_id'])
                self.db.execute('''UPDATE request_outcomes SET final_channel=?,requested_model=?,client_label=?,input_bucket=?,input_tokens=?,output_tokens=?,total_tokens=?,ttft_ms=?,latency_ms=?,fallback_count=?,error_type=?,channel_count=?,cross_channel=? WHERE trace_id=?''',
                    (trace['final_channel'],trace['requested_model'],trace['client_label'],metrics['input_bucket'],metrics['input_tokens'],metrics['output_tokens'],metrics['total_tokens'],trace['ttft_ms'],trace['latency_ms'],trace['fallback_count'],trace['error_type'],channel_count,int(channel_count>1),trace['trace_id']))
                exists=self.db.execute('SELECT 1 FROM request_error_outcomes WHERE trace_id=? LIMIT 1',(trace['trace_id'],)).fetchone()
                errors=self.db.execute("SELECT error_type,MIN(started) AS first_at FROM attempts WHERE trace_id=? AND outcome='failure' AND error_type IS NOT NULL GROUP BY error_type",(trace['trace_id'],)).fetchall()
                if not errors and trace['error_type']: errors=[{'error_type':trace['error_type'],'first_at':trace['started']}]
                if not exists:
                    for item in errors:
                        recovery=(trace['updated']-item['first_at']) if trace['status']=='success' else None
                        self.db.execute('INSERT OR IGNORE INTO request_error_outcomes(trace_id,error_type,first_at,recovery_seconds,final_failed) VALUES(?,?,?,?,?)',(trace['trace_id'],item['error_type'],item['first_at'],recovery,int(trace['status']!='success')))
                self._rollup_analytics(trace['trace_id'],trace,metrics,errors,trace['updated'])
    def trace(self,trace_id):
        with self.lock:
            row=self.db.execute('SELECT * FROM request_traces WHERE trace_id=?',(trace_id,)).fetchone()
            if not row:return None
            events=[dict(item) for item in self.db.execute('SELECT * FROM trace_events WHERE trace_id=? ORDER BY id',(trace_id,))]
            attempts=[dict(item) for item in self.db.execute('SELECT * FROM attempts WHERE trace_id=? ORDER BY id',(trace_id,))]
            return {'trace':dict(row),'events':events,'attempts':attempts}
    # ---- CHANNEL 质量统计 ----
    def channel_quality(self,window_hours=24):
        """按 channel 聚合质量指标(基于 attempts + states):
        total访问 / success / fallback回退数(被限流/冷却拒后切走) / recovered恢复数(冷却到期或probe清后成功)
        限流率、成功率、请求密度(req/min, 按活跃分钟摊).
        回退判定: 该 channel 在窗口内出现 rate_limit/tpm_limit/quota_exhausted 失败即计一次回退.
        恢复判定: 冷却清除(states 清空)次数近似= probe ok 次数 + 到期自动放行, 这里用
        '失败后再次 success' 的转移次数计恢复."""
        now=time.time(); since=now-window_hours*3600
        rows=self.db.execute('''SELECT pool,channel,outcome,error_type,started FROM attempts WHERE started>=? ORDER BY started''',(since,)).fetchall()
        per={}  # (pool,channel) -> dict
        for r in rows:
            k=(r['pool'],r['channel']); d=per.setdefault(k,{'total':0,'success':0,'failure':0,'fallback':0,'recovered':0,'tpm':0,'rpm':0,'quota':0,'first':r['started'],'last':r['started'],'latencies':[]})
            d['total']+=1; d['last']=r['started']
            if r['outcome']=='success':
                d['success']+=1
                if d['failure']>d['recovered']: d['recovered']+=1  # 失败后再次成功=恢复一次
            else:
                d['failure']+=1; t=r['error_type'] or ''
                if t in ('rate_limit','tpm_limit','quota_exhausted'):
                    d['fallback']+=1
                    if t=='tpm_limit':d['tpm']+=1
                    elif t=='quota_exhausted':d['quota']+=1
                    else:d['rpm']+=1
        out=[]
        for (pool,ch),d in per.items():
            span_min=max((d['last']-d['first'])/60.0,1.0)  # 至少1分钟避免除零
            out.append({'pool':pool,'channel':ch,'total':d['total'],'success':d['success'],'failure':d['failure'],
                        'fallback':d['fallback'],'recovered':d['recovered'],'tpm_429':d['tpm'],'rpm_429':d['rpm'],'quota_429':d['quota'],
                        'success_rate':round(d['success']/d['total']*100,1),'fallback_rate':round(d['fallback']/d['total']*100,1),
                        'density_per_min':round(d['total']/span_min,2),'window_hours':window_hours})
        out.sort(key=lambda x:-x['total'])
        return out
    @staticmethod
    def _load_session_key(path):
        if path.exists():return path.read_bytes()
        key=secrets.token_bytes(32); path.write_bytes(key); path.chmod(0o600); return key
    def _prefixes(self,messages):
        chain=b'flex-session-affinity-v1'; result=[]
        for message in messages:
            encoded=json.dumps(message,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
            chain=hmac.new(self.session_key,chain+b'\0'+encoded,hashlib.sha256).digest(); result.append(chain.hex())
        return result
    def affinity_channel(self,pool,messages,idle_seconds,minimum_messages=2):
        prefixes=self._prefixes(messages)
        if len(prefixes)<minimum_messages:return None
        now=time.time(); cutoff=now-idle_seconds
        with self.lock,self.db:
            self.db.execute('DELETE FROM session_affinity WHERE updated<?',(cutoff,))
            for prefix in reversed(prefixes[minimum_messages-1:]):
                row=self.db.execute('SELECT channel FROM session_affinity WHERE pool=? AND prefix_hmac=?',(pool,prefix)).fetchone()
                if row:return row['channel']
        return None
    def remember_affinity(self,pool,messages,channel,idle_seconds,minimum_messages=2):
        prefixes=self._prefixes(messages)
        if len(prefixes)<minimum_messages:return
        now=time.time(); cutoff=now-idle_seconds
        with self.lock,self.db:
            self.db.execute('DELETE FROM session_affinity WHERE updated<?',(cutoff,))
            for prefix in prefixes[minimum_messages-1:]:
                self.db.execute('INSERT INTO session_affinity(pool,prefix_hmac,channel,updated) VALUES(?,?,?,?) ON CONFLICT(pool,prefix_hmac) DO UPDATE SET updated=excluded.updated',(pool,prefix,channel,now))
    def is_enabled(self,pool,ch_id):
        with self.lock:
            row=self.db.execute('SELECT enabled FROM channel_overrides WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
            return True if row is None else bool(row['enabled'])
    def set_enabled(self,pool,ch_id,enabled):
        with self.lock,self.db:self.db.execute('INSERT INTO channel_overrides(pool,channel,enabled) VALUES(?,?,?) ON CONFLICT(pool,channel) DO UPDATE SET enabled=excluded.enabled',(pool,ch_id,int(enabled)))
    def record_test(self,pool,ch_id,outcome,error=None,latency=None,detail=None):
        with self.lock,self.db:self.db.execute('INSERT INTO channel_tests(pool,channel,tested_at,outcome,error_type,latency_ms,error_detail) VALUES(?,?,?,?,?,?,?) ON CONFLICT(pool,channel) DO UPDATE SET tested_at=excluded.tested_at,outcome=excluded.outcome,error_type=excluded.error_type,latency_ms=excluded.latency_ms,error_detail=excluded.error_detail',(pool,ch_id,time.time(),outcome,error,latency,detail))
    def _quota_start(self,pool,ch_id,window,now):
        row=self.db.execute('SELECT reset_at FROM quota_resets WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        return max(now-window,row['reset_at'] if row else 0)
    def _quota_calls(self,pool,ch_id,window,now):
        return self.db.execute('SELECT COUNT(*) FROM attempts WHERE pool=? AND channel=? AND started>=?',(pool,ch_id,self._quota_start(pool,ch_id,window,now))).fetchone()[0]
    def quota_status(self,pool,ch_id,now=None,window=18000):
        now=now or time.time(); start=self._quota_start(pool,ch_id,window,now); used=self._quota_calls(pool,ch_id,window,now)
        row=self.db.execute('SELECT MIN(started) AS oldest,MAX(started) AS newest FROM attempts WHERE pool=? AND channel=? AND started>=?',(pool,ch_id,start)).fetchone()
        return {'used':used,'window_seconds':window,'next_release_at':row['oldest']+window if row and row['oldest'] else None,'last_call_at':row['newest'] if row else None}
    def pacing_due(self,pool,ch_id,target,now=None):
        if not target:return False
        now=now or time.time(); status=self.quota_status(pool,ch_id,now)
        if status['used']>=target:return False
        interval=status['window_seconds']/target
        return status['last_call_at'] is None or now>=status['last_call_at']+interval
    def window_metrics(self,pool,ch_id,seconds=60,now=None):
        now=now or time.time(); start=now-seconds
        row=self.db.execute('SELECT COUNT(*) AS requests,COALESCE(SUM(total_tokens),SUM(input_tokens),0) AS tokens,MIN(started) AS oldest FROM attempts WHERE pool=? AND channel=? AND started>=?',(pool,ch_id,start)).fetchone()
        return {'requests':row['requests'],'tokens':row['tokens'],'next_release_at':row['oldest']+seconds if row['oldest'] else None}
    def learned_limit(self,pool,ch_id):
        row=self.db.execute('SELECT * FROM learned_limits WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        return dict(row) if row else None
    def observe_429(self,pool,ch_id,detail,kind=None,limits=None):
        """kind: 'quota_exhausted' (A类 总量配额) | 'rate_limit' (B类 瞬时限流/忙) | None(由detail推断).
        A类 -> 长冷却(quota_exhausted)，不切回直到冷却到期；B类 -> busy 窗口计数，达阈值才标记 busy 向下切。"""
        now=time.time(); metrics=self.window_metrics(pool,ch_id,now=now); text=(detail or '').lower()
        if kind is None:
            # 与 app.error_type 一致: A类=allocated quota exceeded; B类=rpm/tpm exhausted/rate limit
            if any(x in text for x in ('allocated quota exceeded','quota exceeded','insufficient_quota','free allocated','exceeded your quota','额度','配额')):
                kind='quota_exhausted'
            elif any(x in text for x in ('requests per minute','request limit',' rpm','rate limit','exhausted',' tpm','tokens per minute','token limit')):
                kind='rpm'  # B类瞬时限流(含 rpm/tpm exhausted) 统一归 rpm 走 busy 计数
            else:
                kind='unknown_429'
        # learned limit 估算（rpm/tpm）仍做
        # 防误判: 窗口内请求太少(<3)时推不出可靠 safe_rpm/safe_tpm —— 一次瞬时429不该把限额定成1
        safe_rpm=safe_tpm=None
        if metrics['requests']>=3:
            if any(x in text for x in ('requests per minute','request limit',' rpm','rate limit')):
                safe_rpm=max(1,int(metrics['requests']*.8))
            elif any(x in text for x in ('tokens per minute','token limit',' tpm')):
                safe_tpm=max(1,int(metrics['tokens']*.8))
        with self.lock,self.db:
            old=self.db.execute('SELECT safe_rpm,safe_tpm,confidence FROM learned_limits WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
            if old:
                safe_rpm=min(old['safe_rpm'],safe_rpm) if safe_rpm and old['safe_rpm'] else safe_rpm or old['safe_rpm']
                safe_tpm=min(old['safe_tpm'],safe_tpm) if safe_tpm and old['safe_tpm'] else safe_tpm or old['safe_tpm']
                confidence=old['confidence']+1
            else:confidence=1
            self.db.execute('INSERT INTO learned_limits(pool,channel,safe_rpm,safe_tpm,last_429_at,last_429_kind,last_429_evidence,confidence,success_since_429) VALUES(?,?,?,?,?,?,?,?,0) ON CONFLICT(pool,channel) DO UPDATE SET safe_rpm=excluded.safe_rpm,safe_tpm=excluded.safe_tpm,last_429_at=excluded.last_429_at,last_429_kind=excluded.last_429_kind,last_429_evidence=excluded.last_429_evidence,confidence=excluded.confidence,success_since_429=0',(pool,ch_id,safe_rpm,safe_tpm,now,kind,json.dumps(metrics),confidence))
            if kind=='quota_exhausted':
                # A类：长冷却，冷却到期由 eligible 自动放行（期间不选中）
                self._cool(pool,ch_id,now+self._quota_cooldown_seconds(limits),'quota_exhausted')
            elif kind in ('rpm','tpm'):
                # B类瞬时限流的冷却长度由请求层指数退避决定；这里仅记录学习样本。
                # 不能在第 N 次 429 后额外写入固定 busy_cooldown，否则会打断指数序列。
                pass
            else:
                # unknown/connection 类：立即短冷却兜底（退避）
                backoff=min(300,30*(2**min(self._learn_confidence(pool,ch_id)-1,3))); self._cool(pool,ch_id,now+backoff,'unknown_429')
        return kind,metrics

    def _learn_confidence(self,pool,ch_id):
        row=self.db.execute('SELECT confidence FROM learned_limits WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        return row['confidence'] if row else 1
    def _quota_cooldown_seconds(self,limits):
        return limits.quota_cooldown_seconds if limits else 3600

    def _observe_busy(self,pool,ch_id,now,limits=None):
        window_minutes=limits.busy_window_minutes if limits else 5
        threshold=limits.busy_threshold if limits else 3
        row=self.db.execute('SELECT count,window_start FROM busy_counts WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        if row and now-row['window_start']<=window_minutes*60:
            count=row['count']+1; ws=row['window_start']
        else:
            count=1; ws=now
        self.db.execute('INSERT INTO busy_counts(pool,channel,count,window_start) VALUES(?,?,?,?) ON CONFLICT(pool,channel) DO UPDATE SET count=excluded.count,window_start=excluded.window_start',(pool,ch_id,count,ws))
        if count>=threshold:
            self._cool(pool,ch_id,now+(limits.busy_cooldown_seconds if limits else 1200),'busy')
            self.db.execute('DELETE FROM busy_counts WHERE pool=? AND channel=?',(pool,ch_id))

    def is_busy(self,pool,ch_id):
        row=self.db.execute('SELECT until,reason FROM states WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        return bool(row and row['until'] and row['until']>time.time() and row['reason']=='busy')

    # ---- 回切探测节流（Y 方案）----
    def should_probe(self,pool,ch_id,now=None,probe_cooldown=600):
        """冷却中的通道是否该发探测：距上次失败探测超过 probe_cooldown 才再探。"""
        now=now or time.time()
        row=self.db.execute('SELECT last_probe_at,probe_fail_streak FROM probe_log WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        if not row or row['last_probe_at'] is None:return True
        return now - row['last_probe_at'] >= probe_cooldown
    def has_active_five_hour_validation(self,pool,ch_id):
        """True while a held downstream request is already validating this quota state."""
        row=self.db.execute('''SELECT 1 FROM request_traces t JOIN trace_events e ON e.trace_id=t.trace_id
                               WHERE t.pool=? AND t.status='running' AND e.channel=?
                                 AND e.event='five_hour_quota_retry_wait' LIMIT 1''',(pool,ch_id)).fetchone()
        return bool(row)
    def record_probe(self,pool,ch_id,now=None,success=False):
        now=now or time.time()
        with self.lock,self.db:
            row=self.db.execute('SELECT probe_fail_streak FROM probe_log WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
            streak=(row['probe_fail_streak'] if row else 0) + (0 if success else 1)
            self.db.execute('INSERT INTO probe_log(pool,channel,last_probe_at,probe_fail_streak) VALUES(?,?,?,?) ON CONFLICT(pool,channel) DO UPDATE SET last_probe_at=excluded.last_probe_at,probe_fail_streak=excluded.probe_fail_streak',(pool,ch_id,now,streak))
    def clear_cooldown(self,pool,ch_id):
        """探测成功 -> 清除该通道的 busy/quota_exhausted 冷却，允许回切。"""
        with self.lock,self.db:
            self.db.execute('DELETE FROM states WHERE pool=? AND channel=?',(pool,ch_id))
            self.db.execute('DELETE FROM busy_counts WHERE pool=? AND channel=?',(pool,ch_id))
    def cooldown_reason(self,pool,ch_id,now=None):
        now=now or time.time()
        row=self.db.execute('SELECT until,reason FROM states WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        if row and row['until'] and row['until']>now:return row['reason']
        return None
    def cooldown_state(self,pool,ch_id,now=None):
        now=now or time.time()
        row=self.db.execute('SELECT until,reason FROM states WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        if row and row['until'] and row['until']>now:
            learned=self.db.execute('SELECT last_429_kind FROM learned_limits WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
            return {'reason':row['reason'],'until':row['until'],'limit_kind':learned['last_429_kind'] if learned else None}
        return None
    def cooled_entries(self,prefix=''):
        """列出所有冷却中的条目 [{pool,channel,until,reason}], 可按 pool 前缀过滤(如 'direct:').
        供探测循环发现需要恢复的通道(含直连命名空间)."""
        now=time.time()
        rows=self.db.execute('SELECT pool,channel,until,reason FROM states WHERE until IS NOT NULL AND until>?',(now,)).fetchall()
        return [dict(r) for r in rows if r['pool'].startswith(prefix)]
    def observe_success(self,pool,ch_id):
        with self.lock,self.db:
            row=self.db.execute('SELECT safe_rpm,safe_tpm,confidence,success_since_429 FROM learned_limits WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
            if not row or row['confidence']<2:return
            streak=row['success_since_429']+1; rpm=row['safe_rpm']; tpm=row['safe_tpm']
            if streak>=20:
                rpm=rpm+1 if rpm else None; tpm=int(tpm*1.05) if tpm else None; streak=0
            self.db.execute('UPDATE learned_limits SET safe_rpm=?,safe_tpm=?,success_since_429=? WHERE pool=? AND channel=?',(rpm,tpm,streak,pool,ch_id))
    def eligible(self,pool,ch_id,limits,projected_tokens=0,ignore_five_hour_quota=False):
        """Check if a channel is eligible. limits=Limits object, ch_id=str."""
        now=time.time()
        with self.lock,self.db:
            row=self.db.execute('SELECT until,reason FROM states WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
            if row and row['until'] and row['until']>now:
                # Only the held original request may perform its explicit quota
                # validation. New requests continue to be protected by this state.
                if not (ignore_five_hour_quota and row['reason'] in ('five_hour_quota','five_hour_quota_retry')):
                    return False,row['reason']
            if row and row['until'] and row['until']<=now:self.db.execute('UPDATE states SET until=NULL,reason=NULL WHERE pool=? AND channel=?',(pool,ch_id))
            # RPM/TPM 不做本地预判拦截：只有上游实际返回 429 后才退让或等待。
            # limits.rpm 与 learned_limits 保留作观测/展示，不能在此创建主动冷却。
            if limits.max_requests_per_window and not ignore_five_hour_quota:
                n=self._quota_calls(pool,ch_id,limits.window_seconds,now)
                if n>=limits.max_requests_per_window:
                    # We need quota_status which takes ch_id, not ch
                    qs=self.quota_status(pool,ch_id,now,limits.window_seconds)
                    self._cool(pool,ch_id,qs['next_release_at'] or now+limits.quota_cooldown_seconds,'five_hour_quota'); return False,'five_hour_quota'
        return True,None
    def start(self,pool,ch_id,model_name,input_tokens=None,trace_id=None):
        with self.lock,self.db:return self.db.execute('INSERT INTO attempts(started,pool,channel,model,outcome,input_tokens,trace_id) VALUES(?,?,?,?,?,?,?)',(time.time(),pool,ch_id,model_name,'started',input_tokens,trace_id)).lastrowid
    def finish(self,id,outcome,error=None,latency=None,output_tokens=None,total_tokens=None,ttft_ms=None,error_detail=None,error_code=None):
        with self.lock,self.db:self.db.execute('UPDATE attempts SET outcome=?,error_type=?,latency_ms=?,output_tokens=?,total_tokens=?,ttft_ms=?,error_detail=?,error_code=? WHERE id=?',(outcome,error,latency,output_tokens,total_tokens,ttft_ms,error_detail,error_code,id))
    def cooldown(self,pool,ch_id,seconds,reason):
        with self.lock,self.db:self._cool(pool,ch_id,time.time()+seconds,reason)
    def _cool(self,pool,ch_id,until,reason):
        self.db.execute('INSERT INTO states(pool,channel,until,reason) VALUES(?,?,?,?) ON CONFLICT(pool,channel) DO UPDATE SET until=excluded.until,reason=excluded.reason',(pool,ch_id,until,reason))
    def reset(self,pool,ch_id,scope):
        now=time.time()
        with self.lock,self.db:
            if scope in ('quota','all'):
                self.db.execute('INSERT INTO quota_resets(pool,channel,reset_at) VALUES(?,?,?) ON CONFLICT(pool,channel) DO UPDATE SET reset_at=excluded.reset_at',(pool,ch_id,now))
            if scope in ('cooldown','all'):
                self.db.execute('DELETE FROM states WHERE pool=? AND channel=?',(pool,ch_id))
    def calls_today(self,ch_id):
        """Count attempts for this channel since local midnight today."""
        now=time.time(); local=time.localtime(now); midnight=time.mktime((local.tm_year,local.tm_mon,local.tm_mday,0,0,0,0,0,-1))
        with self.lock:return self.db.execute('SELECT COUNT(*) FROM attempts WHERE channel=? AND started>=?',(ch_id,midnight)).fetchone()[0]
    def channels_state(self,pool,ch_id,limits,context_window_tokens,capabilities,provided_model_name,retry_policy=None,provider=None):
        """Build channel state dict for dashboard. Use ch_id directly."""
        now=time.time(); n=self.db.execute('SELECT COUNT(*) FROM attempts WHERE channel=? AND started>=?',(ch_id,now-60)).fetchone()[0]
        metrics=self.window_metrics(pool,ch_id,now=now)
        quota=self.quota_status(pool,ch_id,now); q=quota['used']
        learned=self.learned_limit(pool,ch_id)
        row=self.db.execute('SELECT until,reason FROM states WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        t=self.db.execute('SELECT tested_at,outcome,error_type,latency_ms,error_detail FROM channel_tests WHERE pool=? AND channel=?',(pool,ch_id)).fetchone()
        return {
            'id': ch_id,
            'provider': provider,
            'model': provided_model_name,
            'context_window_tokens': context_window_tokens,
            'capabilities': capabilities,
            'enabled': self.is_enabled(pool, ch_id),
            'rpm': limits.rpm,
            'calls_last_minute': n,
            'tokens_last_minute': metrics['tokens'],
            'requests_per_5_hours': limits.max_requests_per_window,
            'calls_last_5_hours': q,
            'calls_today': self.calls_today(ch_id),
            'next_quota_release_at': quota['next_release_at'],
            'cooldown_until': row['until'] if row else None,
            'cooldown_reason': row['reason'] if row else None,
            'learned_limits': learned,
            'last_test': dict(t) if t else None,
            'retry_policy': dict(retry_policy) if retry_policy else None,
        }
    def recent(self,limit):
        with self.lock:return [dict(r) for r in self.db.execute('SELECT * FROM attempts ORDER BY id DESC LIMIT ?',(limit,)).fetchall()]
    def errors(self,limit=50):
        with self.lock:return [dict(r) for r in self.db.execute('SELECT * FROM attempts WHERE outcome!=? ORDER BY id DESC LIMIT ?',('success',limit)).fetchall()]
