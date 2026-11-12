"""跨目录联合检索：跨日界线窗口拆分、翻页续传、观测级去重与缺口报告。

观测身份只由集合、平台、观测时刻与足迹构成，处理版本（processing:version）
作为修订保留；条目 ID 与签名资产地址只是来源内部标识和临时定位，绝不进入
去重键。每个来源、每个空间窗口的翻页进度独立落库，坏条目隔离后该来源保持
不完整状态，不波及其他来源已取得的成果。
"""
import json
from urllib.parse import urlencode

from .domain import instant, intersects
from .transport import Client

REVISION_KEYS = ('processing:version', 'version')


def normalize_windows(bbox):
    """把可能跨越 180° 经线的矩形拆成普通经度窗口。"""
    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        raise ValueError('范围需要四个数值')
    try:
        minlon, minlat, maxlon, maxlat = (float(value) for value in bbox)
    except (TypeError, ValueError):
        raise ValueError('范围需要数值') from None
    if not all(-180.0 <= lon <= 180.0 for lon in (minlon, maxlon)):
        raise ValueError('经度超出 [-180, 180]')
    if not -90.0 <= minlat <= maxlat <= 90.0:
        raise ValueError('纬度范围无效')
    if minlon <= maxlon:
        return [[minlon, minlat, maxlon, maxlat]]
    return [[minlon, minlat, 180.0, maxlat], [-180.0, minlat, maxlon, maxlat]]


def windows_intersect(bbox, windows):
    """条目范围（自身也可能跨日界线）是否与任一查询窗口相交。"""
    return any(intersects(item, window) for item in normalize_windows(bbox) for window in windows)


def validate_item(item):
    """校验并规范化单条 STAC 条目；结构缺陷抛出 ValueError 供调用方隔离。"""
    if not isinstance(item, dict):
        raise ValueError('条目不是对象')
    item_id = item.get('id')
    if not isinstance(item_id, str) or not item_id:
        raise ValueError('条目缺少有效标识')
    collection = item.get('collection')
    if not isinstance(collection, str) or not collection:
        raise ValueError('条目缺少集合')
    properties = item.get('properties')
    if not isinstance(properties, dict):
        raise ValueError('条目缺少属性')
    observed_raw = properties.get('datetime')
    if not isinstance(observed_raw, str) or not observed_raw:
        raise ValueError('条目缺少观测时间')
    observed = instant(observed_raw)
    bbox = item.get('bbox')
    if not isinstance(bbox, list) or len(bbox) != 4:
        raise ValueError('条目范围无效')
    normalize_windows(bbox)
    assets = item.get('assets') or {}
    if not isinstance(assets, dict):
        raise ValueError('资产清单无效')
    clean_assets = {}
    for name, asset in assets.items():
        if not isinstance(asset, dict):
            continue
        href = asset.get('href')
        if not isinstance(href, str) or not href:
            continue
        roles = asset.get('roles') or []
        clean_assets[str(name)] = {
            'href': href,
            'roles': sorted({role for role in roles if isinstance(role, str)}),
        }
    revision = None
    for key in REVISION_KEYS:
        if properties.get(key) is not None:
            revision = str(properties[key])
            break
    platform = properties.get('platform') or properties.get('constellation')
    observation = {
        'collection': collection,
        'platform': str(platform) if platform else None,
        'datetime': observed.isoformat(),
        'bbox': [float(value) for value in bbox],
    }
    observation_key = json.dumps(observation, ensure_ascii=False, sort_keys=True)
    record_key = json.dumps({'observation': observation, 'revision': revision},
                            ensure_ascii=False, sort_keys=True)
    return {
        'item_id': item_id,
        'observed': observed,
        'collection': collection,
        'bbox': [float(value) for value in bbox],
        'observation': observation,
        'observation_key': observation_key,
        'revision': revision,
        'record_key': record_key,
        'assets': clean_assets,
    }


def _zulu(moment):
    return moment.strftime('%Y-%m-%dT%H:%M:%SZ')


def slice_key(window, start, end):
    return f'{json.dumps(window)}|{start.isoformat()}|{end.isoformat()}'


def initial_cursor(source, window, start, end, collections):
    """构造切片首个请求；游标即"下一个待发的请求"，便于中断后续传。"""
    endpoint = source['endpoint']
    method = str(endpoint.get('method', 'GET')).upper()
    href = endpoint['href']
    if method == 'GET':
        separator = '&' if '?' in href else '?'
        href = href + separator + urlencode({
            'bbox': ','.join(str(value) for value in window),
            'datetime': f'{_zulu(start)}/{_zulu(end)}',
            'collections': ','.join(collections),
        })
        return {'method': 'GET', 'href': href}
    body = {
        'bbox': list(window),
        'datetime': f'{_zulu(start)}/{_zulu(end)}',
        'collections': list(collections),
    }
    return {'method': 'POST', 'href': href, 'body': body}


def cursor_from_link(link, original_body):
    """把目录返回的 next 链接转成下一个请求；POST 链接按需沿用原始请求体。"""
    method = str(link.get('method', 'GET')).upper()
    href = link.get('href')
    if not isinstance(href, str) or not href:
        raise ValueError('next 链接缺少地址')
    if method == 'GET':
        return {'method': 'GET', 'href': href}
    body = link.get('body')
    if link.get('merge') and isinstance(body, dict) and isinstance(original_body, dict):
        body = {**original_body, **body}
    if body is None:
        body = original_body
    return {'method': 'POST', 'href': href, 'body': body}


def resolve_url(source, href):
    if href.startswith(('http://', 'https://')):
        return href
    return source['base'].rstrip('/') + '/' + href.lstrip('/')


class FederatedSearch:
    """对多个 STAC 目录执行可恢复的联合检索。"""

    def __init__(self, store, query, client=None):
        self.store = store
        self.client = client or Client()
        self.query = query
        self.windows = normalize_windows(query['bbox'])
        self.start = instant(query['interval'][0])
        self.end = instant(query['interval'][1])
        if not self.start < self.end:
            raise ValueError('时间范围为空')
        self.collections = [str(name) for name in (query.get('collections') or [])]
        self.roles = [str(role) for role in (query.get('roles') or [])]

    def run(self, sources):
        self.store.save_query(self.query)
        for source in sources:
            for window in self.windows:
                key = slice_key(window, self.start, self.end)
                try:
                    self._run_slice(source, window, key)
                except Exception as error:  # 单个切片失败不波及其他来源
                    self.store.taint_slice(source['name'], key, window, self.start,
                                           self.end, f'检索中断: {error}')

    def _run_slice(self, source, window, key):
        name = source['name']
        state = self.store.get_slice(name, key)
        if state and state['cursor']:
            cursor = json.loads(state['cursor'])
        else:
            cursor = initial_cursor(source, window, self.start, self.end, self.collections)
            self.store.begin_pass(name, key, window, self.start, self.end,
                                  json.dumps(cursor, ensure_ascii=False))
        original_body = initial_cursor(source, window, self.start, self.end,
                                       self.collections).get('body')
        while True:
            url = resolve_url(source, cursor['href'])
            if cursor['method'] == 'GET':
                status, _headers, raw = self.client.request(url)
            else:
                body = json.dumps(cursor.get('body') or {}, ensure_ascii=False).encode('utf-8')
                status, _headers, raw = self.client.request(
                    url, method='POST', headers={'Content-Type': 'application/json'}, body=body)
            if status != 200:
                self.store.fail_slice(name, key, f'来源响应状态 {status}')
                return
            try:
                page = json.loads(raw)
            except ValueError:
                self.store.fail_slice(name, key, '响应不是有效 JSON')
                return
            items = page.get('features') or page.get('items') or []
            views = []
            quarantined = []
            for item in items:
                try:
                    view = validate_item(item)
                except ValueError as error:
                    quarantined.append((item, str(error)))
                    continue
                if self.collections and view['collection'] not in self.collections:
                    continue
                if not self.start <= view['observed'] < self.end:
                    continue
                if not windows_intersect(view['bbox'], self.windows):
                    continue
                views.append(view)
            link = next((entry for entry in page.get('links') or []
                         if entry.get('rel') == 'next'), None)
            try:
                following = cursor_from_link(link, original_body) if link else None
            except ValueError as error:
                self.store.fail_slice(name, key, str(error))
                return
            self.store.commit_page(
                name, key, views, quarantined,
                json.dumps(following, ensure_ascii=False) if following else None)
            if following is None:
                self.store.finish_pass(name, key)
                return
            cursor = following


def build_report(store):
    """汇总检索结果：已发现观测、缺失的必需资产、未完成的检索区间。"""
    query = store.load_query() or {}
    required_roles = [str(role) for role in (query.get('roles') or [])]
    sources = store.record_sources()
    observations = {}
    for payload in store.all_records():
        obs_key = json.dumps(payload['observation'], ensure_ascii=False, sort_keys=True)
        entry = observations.setdefault(
            obs_key, {'observation': payload['observation'], 'revisions': []})
        missing = [role for role in required_roles
                   if not any(role in asset['roles'] for asset in payload['assets'].values())]
        entry['revisions'].append({
            'revision': payload['revision'],
            'record_key': payload['record_key'],
            'sources': sources.get(payload['record_key'], []),
            'assets': sorted(payload['assets']),
            'missing_roles': missing,
        })
    for entry in observations.values():
        entry['revisions'].sort(key=lambda item: (item['revision'] is None, item['revision']))
    ordered = sorted(observations.values(),
                     key=lambda item: (item['observation']['datetime'],
                                       item['observation']['collection']))
    missing_assets = [
        {'observation': entry['observation'], 'revision': revision['revision'],
         'missing': revision['missing_roles'],
         'sources': [source['source'] for source in revision['sources']]}
        for entry in ordered for revision in entry['revisions'] if revision['missing_roles']
    ]
    slices = store.all_slices()
    incomplete = [
        {'source': row['source'], 'window': json.loads(row['window']),
         'interval': [row['interval_start'], row['interval_end']],
         'reasons': json.loads(row['taint']), 'pages': row['pages'], 'updated': row['updated']}
        for row in slices if row['status'] != 'complete'
    ]
    coverage = [
        {'source': row['source'], 'window': json.loads(row['window']),
         'interval': [row['interval_start'], row['interval_end']],
         'status': row['status'], 'pages': row['pages'],
         'resumable': bool(row['cursor']), 'updated': row['updated']}
        for row in slices
    ]
    quarantine = [
        {'source': row['source'], 'slice': row['slice_key'], 'item': row['item_ref'],
         'reason': row['reason'], 'seen': row['seen']}
        for row in store.all_quarantine()
    ]
    return {
        'query': query,
        'observations': ordered,
        'missing_assets': missing_assets,
        'incomplete_slices': incomplete,
        'quarantine': quarantine,
        'coverage': coverage,
    }
