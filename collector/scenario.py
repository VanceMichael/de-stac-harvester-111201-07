"""按场景描述提供合成 STAC 目录的本地服务，用于演示与测试。

支持两种翻页协议：POST 搜索的 next 链接携带需合并进原始请求体的 token，
GET 搜索的 next 链接是带游标参数的完整地址。`phase` 模拟目录随时间演进：
阶段 1 的瞬时故障在阶段 2 消失，late_assets 中的条目在阶段 2 才发布。
"""
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class ScenarioCatalog:
    """把 sources 描述变成可分页的 STAC 响应，并记录收到的请求。"""

    def __init__(self, scenario, phase=1):
        self.scenario = scenario
        self.phase = phase
        self.calls = []
        self.base_url = None
        self._lock = threading.Lock()

    def pool(self, source):
        items = list(source.get('items') or [])
        if self.phase >= 2:
            for late in self.scenario.get('late_assets') or []:
                if late.get('source') == source['name'] and isinstance(late.get('item'), dict):
                    items.append(late['item'])
        return items

    def pages(self, source):
        pool = self.pool(source)
        size = max(1, int(source.get('page_size') or 2))
        return [pool[index:index + size] for index in range(0, len(pool), size)] or [[]]

    def failure(self, source, marker):
        for failure in source.get('failures') or []:
            if failure.get('token') == marker and self.phase in (failure.get('phases') or [1]):
                return int(failure.get('status') or 500)
        return None

    def next_link(self, source, name, page_number):
        endpoint = source['endpoint']
        method = str(endpoint.get('method', 'GET')).upper()
        if method == 'POST':
            template = (source.get('links') or [{}])[0]
            body = dict(template.get('body') or {})
            body['token'] = f'p{page_number}'
            link = {'rel': 'next', 'method': 'POST',
                    'href': template.get('href', endpoint['href']), 'body': body}
            if template.get('merge'):
                link['merge'] = True
            return link
        return {'rel': 'next', 'method': 'GET',
                'href': f"{self.base_url}/{name}{endpoint['href']}?cursor=m{page_number}"}

    def handle(self, method, path, body):
        try:
            recorded = json.loads(body) if body else None
        except ValueError:
            recorded = body
        with self._lock:
            self.calls.append({'method': method, 'path': path, 'body': recorded})
        parts = [part for part in urlparse(path).path.split('/') if part]
        if len(parts) != 2 or parts[1] != 'search':
            return 404, {'error': '未找到资源'}
        source = next((entry for entry in self.scenario.get('sources') or []
                       if entry.get('name') == parts[0]), None)
        if source is None:
            return 404, {'error': '未找到资源'}
        endpoint = source['endpoint']
        pages = self.pages(source)
        if str(endpoint.get('method', 'GET')).upper() == 'POST':
            marker = recorded.get('token') if isinstance(recorded, dict) else None
        else:
            marker = (parse_qs(urlparse(path).query).get('cursor') or [None])[0]
        if marker is None:
            index = 0
        else:
            try:
                index = int(str(marker)[1:]) - 1
            except ValueError:
                return 400, {'error': '无效游标'}
            if not 0 <= index < len(pages):
                return 400, {'error': '无效游标'}
        failed = self.failure(source, marker)
        if failed is not None:
            return failed, {'error': '目录暂时不可用'}
        links = [self.next_link(source, parts[0], index + 2)] if index + 1 < len(pages) else []
        return 200, {'type': 'FeatureCollection', 'features': pages[index], 'links': links}


@contextmanager
def serve_scenario(scenario, phase=1):
    """启动合成目录服务，返回 (目录对象, 供 FederatedSearch 使用的来源清单)。"""
    catalog = ScenarioCatalog(scenario, phase=phase)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string, *arguments):
            return None

        def do_GET(self):
            self._respond('GET')

        def do_POST(self):
            self._respond('POST')

        def _respond(self, method):
            length = int(self.headers.get('Content-Length') or 0)
            body = self.rfile.read(length).decode('utf-8', errors='replace') if length else ''
            status, payload = catalog.handle(method, self.path, body)
            data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    catalog.base_url = f'http://127.0.0.1:{server.server_port}'
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        sources = [
            {'name': source['name'],
             'base': f"{catalog.base_url}/{source['name']}",
             'endpoint': source['endpoint']}
            for source in scenario.get('sources') or []
        ]
        yield catalog, sources
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
