#!/usr/bin/env python3
"""Independent local UI server for Flex Router.

The backend remains the sole owner of /v1 and /api.  This server owns the
browser UI on port 7801 and proxies only browser API/config requests to 7800.
"""
from __future__ import annotations
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT=Path(os.getenv('FLEX_UI_TEMPLATE_ROOT',Path(__file__).resolve().parents[1]/'templates'))
BACKEND=os.getenv('FLEX_BACKEND_URL','http://127.0.0.1:7800').rstrip('/')
PAGES={'/':('dashboard.html','Flex LLM Router'),'/traces':('traces.html','Flex LLM Router · 调用轨迹'),'/statistics':('statistics.html','Flex LLM Router · 调用统计'),'/help':('help.html','Flex Router · 策略说明')}

class UI(BaseHTTPRequestHandler):
    def log_message(self,fmt,*args): return
    def _send(self,code,body,content_type='text/html; charset=utf-8'):
        self.send_response(code); self.send_header('Content-Type',content_type); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
    def _proxy(self):
        data=self.rfile.read(int(self.headers.get('Content-Length','0'))) if self.command in ('POST','PUT','PATCH') else None
        headers={'Content-Type':self.headers.get('Content-Type','application/json'),'User-Agent':'Flex-UI/1.0'}
        timeout=4 if self.path.split('?',1)[0] in ('/config','/setup','/healthz') else 35
        try:
            with urlopen(Request(BACKEND+self.path,data=data,headers=headers,method=self.command),timeout=timeout) as response:
                self._send(response.status,response.read(),response.headers.get_content_type()+'; charset=utf-8')
        except HTTPError as error:self._send(error.code,error.read(),error.headers.get_content_type()+'; charset=utf-8')
        except Exception as error:
            if self.command=='GET' and self.path.split('?',1)[0] in ('/config','/setup'):
                return self._offline_page(self.path.split('?',1)[0],error)
            self._send(502,('{"detail":"backend unavailable: %s"}'%error).encode(),'application/json; charset=utf-8')
    def _offline_page(self,path,error):
        label='Config' if path=='/config' else 'Setup'
        body=f'''<!doctype html><meta charset="utf-8"><title>Flex Router · {label}</title>
        <style>body{{font:15px system-ui;margin:32px;color:#18212f;background:#f7f9fc}}nav{{margin-bottom:24px}}nav a{{color:#2764a8;text-decoration:none;margin-right:18px}}main{{max-width:680px;background:#fff;border:1px solid #e4e9f1;border-radius:11px;padding:22px}}h1{{margin-top:0}}p{{color:#586779;line-height:1.55}}.state{{color:#8c651d;background:#fff8e6;border:1px solid #ead49b;border-radius:7px;padding:9px 11px;font-size:13px}}</style>
        <nav><a href="/">Dashboard</a><a href="/traces">调用轨迹</a><a href="/statistics">调用统计</a><a href="/config">Config</a><a href="/setup">Setup</a><a href="/help">Help</a></nav>
        <main><h1>{label}</h1><p class="state">核心 Router 正在重启或暂不可用。独立 UI 仍在运行；服务恢复后此页面会自动重新打开。</p><p>不会执行任何配置写入。若这是刚触发的核心重启，请等待几秒。</p></main><script>setTimeout(()=>location.reload(),2000)</script>'''
        self._send(503,body.encode())
    def _page(self,file,title):
        try:
            base=(ROOT/'base.html').read_text(encoding='utf-8'); content=(ROOT/file).read_text(encoding='utf-8')
            html=base.replace('{{ title }}',title).replace('{{ extra_css }}','').replace('{{ content }}',content)
            self._send(200,html.encode())
        except FileNotFoundError:self._send(404,b'Not found','text/plain; charset=utf-8')
    def do_GET(self):
        path=self.path.split('?',1)[0]
        if path in PAGES:return self._page(*PAGES[path])
        if path.startswith('/api/') or path.startswith('/v1/') or path in ('/healthz','/config','/setup'):return self._proxy()
        self._send(404,b'Not found','text/plain; charset=utf-8')
    def do_POST(self): self._proxy()
    do_PUT=do_POST
    do_PATCH=do_POST

if __name__=='__main__':
    ThreadingHTTPServer((os.getenv('FLEX_UI_HOST','127.0.0.1'),int(os.getenv('FLEX_UI_PORT','7801'))),UI).serve_forever()
