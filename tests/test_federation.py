import json
from pathlib import Path
import tempfile
import unittest

from collector.federation import (FederatedSearch, build_report, normalize_windows,
                                  validate_item, windows_intersect)
from collector.scenario import serve_scenario
from collector.storage import SearchStore

SCENARIO = json.loads(Path('fixtures/catalogs.json').read_text(encoding='utf-8'))
QUERY = SCENARIO['query']


class WindowTests(unittest.TestCase):
    def test_crossing_bbox_splits(self):
        self.assertEqual(normalize_windows([170, -10, -170, 10]),
                         [[170.0, -10.0, 180.0, 10.0], [-180.0, -10.0, -170.0, 10.0]])

    def test_plain_bbox_single_window(self):
        self.assertEqual(normalize_windows([120, 0, 130, 10]), [[120.0, 0.0, 130.0, 10.0]])

    def test_invalid_ranges_rejected(self):
        for bbox in ([170, 20, -170, 10], [190, 0, 200, 5], [0, -95, 10, 0], [1, 2, 3]):
            with self.assertRaises(ValueError):
                normalize_windows(bbox)

    def test_crossing_item_matches_windows(self):
        windows = normalize_windows([170, -10, -170, 10])
        self.assertTrue(windows_intersect([178, -6, -178, 2], windows))
        self.assertTrue(windows_intersect([-179, -4, -171, 4], windows))
        self.assertTrue(windows_intersect([172, -8, 178, -2], windows))
        self.assertFalse(windows_intersect([0, 0, 10, 5], windows))


def base_item():
    return {
        'id': 'orbit-7-frame-1',
        'collection': 'ocean-color',
        'bbox': [172, -8, 178, -2],
        'properties': {
            'datetime': '2026-01-01T06:00:00Z',
            'platform': 'ocean-sat-1',
            'processing:version': '1',
        },
        'assets': {
            'data': {'href': 'https://primary.example/a?sig=P1A', 'roles': ['data']},
        },
    }


class IdentityTests(unittest.TestCase):
    def test_mirror_id_and_signed_url_stay_out_of_key(self):
        first = validate_item(base_item())
        twin = base_item()
        twin['id'] = 'other-88'
        twin['assets']['data']['href'] = 'https://mirror.example/88?sig=zzz'
        self.assertEqual(first['record_key'], validate_item(twin)['record_key'])

    def test_timezone_representation_normalized(self):
        first = validate_item(base_item())
        twin = base_item()
        twin['properties']['datetime'] = '2026-01-01T14:00:00+08:00'
        self.assertEqual(first['record_key'], validate_item(twin)['record_key'])

    def test_new_revision_is_new_record_of_same_observation(self):
        first = validate_item(base_item())
        twin = base_item()
        twin['properties']['processing:version'] = '2'
        second = validate_item(twin)
        self.assertNotEqual(first['record_key'], second['record_key'])
        self.assertEqual(first['observation_key'], second['observation_key'])

    def test_structural_defects_rejected(self):
        def drop_datetime(item):
            item['properties'].pop('datetime')

        def bad_bbox(item):
            item['bbox'] = [171, 0, 173]

        def drop_collection(item):
            item['collection'] = None

        def naive_datetime(item):
            item['properties']['datetime'] = '2026-01-01 06:00:00'

        for mutate in (drop_datetime, bad_bbox, drop_collection, naive_datetime):
            item = base_item()
            mutate(item)
            with self.assertRaises(ValueError):
                validate_item(item)


class FederationRunTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = SearchStore(Path(self.temporary.name) / 'federation.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def run_phase(self, phase):
        with serve_scenario(SCENARIO, phase=phase) as (catalog, sources):
            FederatedSearch(self.store, QUERY).run(sources)
            return catalog.calls

    def test_first_run_dedupes_and_isolates(self):
        calls = self.run_phase(1)
        report = build_report(self.store)
        observations = {entry['observation']['datetime']: entry
                        for entry in report['observations']}
        self.assertEqual(set(observations), {
            '2026-01-01T06:00:00+00:00',
            '2026-01-01T09:00:00+00:00',
            '2026-01-01T15:00:00+00:00',
            '2026-01-01T18:00:00+00:00',
        })
        # 镜像用不同条目 ID 重复发布的观测只保留一条记录，两个来源都留凭据
        first = observations['2026-01-01T06:00:00+00:00']
        self.assertEqual(len(first['revisions']), 1)
        sources = {source['source']: source['item_id']
                   for source in first['revisions'][0]['sources']}
        self.assertEqual(sources, {'primary': 'orbit-7-frame-1', 'mirror': 'other-88'})
        # 跨日界线的景像在两个窗口各出现一次，去重后只有一条记录
        crossing = observations['2026-01-01T09:00:00+00:00']
        self.assertEqual(len(crossing['revisions']), 1)
        self.assertEqual([source['source'] for source in crossing['revisions'][0]['sources']],
                         ['primary'])
        # 必需资产缺失被点名到具体观测与修订
        self.assertEqual(len(report['missing_assets']), 1)
        self.assertEqual(report['missing_assets'][0]['missing'], ['data'])
        self.assertEqual(report['missing_assets'][0]['revision'], '1')
        # 坏条目被隔离，来源两个切片都保持不完整
        self.assertEqual({row['item'] for row in report['quarantine']}, {'broken-1'})
        incomplete = report['incomplete_slices']
        self.assertEqual(len(incomplete), 2)
        self.assertEqual({row['source'] for row in incomplete}, {'primary'})
        self.assertTrue(all(row['reasons'] for row in incomplete))
        # 另一来源不受牵连，切片完整
        mirror_coverage = [row for row in report['coverage'] if row['source'] == 'mirror']
        self.assertTrue(mirror_coverage)
        self.assertTrue(all(row['status'] == 'complete' for row in mirror_coverage))
        # POST 翻页沿用了原始请求体（bbox/collections 与 token 合并）
        posts = [call for call in calls
                 if call['method'] == 'POST' and isinstance(call['body'], dict)
                 and call['body'].get('token') == 'p2']
        self.assertTrue(posts)
        self.assertIn('bbox', posts[0]['body'])
        self.assertIn('collections', posts[0]['body'])
        # 区间外与集合不符的条目没有进入记录
        self.assertEqual(len(self.store.all_records()), 4)

    def test_second_run_resumes_and_catches_late_assets(self):
        self.run_phase(1)
        calls = self.run_phase(2)
        report = build_report(self.store)
        # 晚发布的修订被追补，旧版本仍保留
        first = next(entry for entry in report['observations']
                     if entry['observation']['datetime'] == '2026-01-01T06:00:00+00:00')
        self.assertEqual([revision['revision'] for revision in first['revisions']], ['1', '2'])
        revisions = {revision['revision']: revision for revision in first['revisions']}
        self.assertEqual(revisions['1']['missing_roles'], ['data'])
        self.assertEqual(revisions['2']['missing_roles'], [])
        # 中断的切片从已存游标续传，没有重新请求首页
        primary_posts = [call for call in calls if call['method'] == 'POST']
        self.assertTrue(primary_posts)
        self.assertTrue(all(call['body'].get('token') == 'p3' for call in primary_posts))
        # 记录总数 = 首阶段 4 条 + 晚到修订 1 条
        self.assertEqual(len(self.store.all_records()), 5)
        # 坏条目仍在目录中，来源依旧不完整，遗漏持续可见
        self.assertEqual({row['source'] for row in report['incomplete_slices']}, {'primary'})
        self.assertEqual({row['item'] for row in report['quarantine']}, {'broken-1'})

    def test_state_survives_reopen(self):
        self.run_phase(1)
        path = Path(self.temporary.name) / 'federation.sqlite3'
        self.store.close()
        self.store = SearchStore(path)
        report = build_report(self.store)
        self.assertEqual(len(report['observations']), 4)
        self.assertEqual({row['source'] for row in report['incomplete_slices']}, {'primary'})
        resumable = [row for row in self.store.all_slices()
                     if row['source'] == 'primary' and row['cursor']]
        self.assertEqual(len(resumable), 2)


class IntervalBoundaryTests(unittest.TestCase):
    def test_half_open_interval(self):
        def item(item_id, moment, bbox):
            return {
                'id': item_id,
                'collection': 'ocean-color',
                'bbox': bbox,
                'properties': {'datetime': moment, 'processing:version': '1'},
                'assets': {'data': {'href': f'https://x.example/{item_id}', 'roles': ['data']}},
            }

        scenario = {
            'sources': [{
                'name': 'solo',
                'endpoint': {'method': 'GET', 'href': '/search'},
                'page_size': 10,
                'items': [
                    item('at-start', '2026-01-01T00:00:00Z', [175, -5, 176, -4]),
                    item('at-end', '2026-01-02T00:00:00Z', [-175, -5, -174, -4]),
                ],
            }],
        }
        query = {
            'bbox': [170, -10, -170, 10],
            'interval': ['2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z'],
            'collections': [],
            'roles': ['data'],
        }
        with tempfile.TemporaryDirectory() as directory:
            store = SearchStore(Path(directory) / 'federation.sqlite3')
            try:
                with serve_scenario(scenario) as (catalog, sources):
                    FederatedSearch(store, query).run(sources)
                records = store.all_records()
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]['observation']['datetime'],
                                 '2026-01-01T00:00:00+00:00')
            finally:
                store.close()
