import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS raw_objects(
                digest TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS imports(
                id INTEGER PRIMARY KEY, source TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES raw_objects(digest),
                UNIQUE(source,digest));
            CREATE TABLE IF NOT EXISTS records(
                record_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                import_id INTEGER NOT NULL REFERENCES imports(id));
        """)

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def ingest(self, source, raw, records):
        digest = hashlib.sha256(raw).hexdigest()
        with self.transaction():
            self.connection.execute("INSERT OR IGNORE INTO raw_objects VALUES (?,?)", (digest, raw))
            self.connection.execute("INSERT OR IGNORE INTO imports(source,digest) VALUES (?,?)", (source, digest))
            import_id = self.connection.execute("SELECT id FROM imports WHERE source=? AND digest=?", (source, digest)).fetchone()[0]
            for record in records:
                key = record.get('key')
                if not isinstance(key, str) or not key:
                    raise ValueError('记录缺少有效标识')
                encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False)
                self.connection.execute("INSERT INTO records VALUES (?,?,?) ON CONFLICT(record_key) DO UPDATE SET payload=excluded.payload, import_id=excluded.import_id", (key, encoded, import_id))
        return digest

    def records(self):
        return [json.loads(row[0]) for row in self.connection.execute("SELECT payload FROM records ORDER BY record_key")]

    def provenance(self, key):
        row = self.connection.execute("SELECT imports.source, imports.digest FROM records JOIN imports ON imports.id=records.import_id WHERE record_key=?", (key,)).fetchone()
        return dict(row) if row else None

    def close(self):
        self.connection.close()


def _now():
    return datetime.now(timezone.utc).isoformat()


class SearchStore:
    """联合检索状态：各来源翻页进度、覆盖范围、观测记录、来源关系与隔离区。

    观测记录以稳定去重键（观测身份 + 处理版本）为主键，签名资产地址只作为
    临时定位保存在 payload 的 locators 中，不参与键的构成。
    """

    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS search_meta(
                key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS slices(
                source TEXT NOT NULL, slice_key TEXT NOT NULL,
                window TEXT NOT NULL, interval_start TEXT NOT NULL,
                interval_end TEXT NOT NULL,
                cursor TEXT, taint TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'incomplete',
                pages INTEGER NOT NULL DEFAULT 0, updated TEXT NOT NULL,
                PRIMARY KEY(source, slice_key));
            CREATE TABLE IF NOT EXISTS search_records(
                record_key TEXT PRIMARY KEY, observation_key TEXT NOT NULL,
                payload TEXT NOT NULL, updated TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS record_sources(
                record_key TEXT NOT NULL, source TEXT NOT NULL,
                item_id TEXT NOT NULL, slice_key TEXT NOT NULL, seen TEXT NOT NULL,
                PRIMARY KEY(record_key, source));
            CREATE TABLE IF NOT EXISTS quarantine(
                source TEXT NOT NULL, slice_key TEXT NOT NULL,
                item_ref TEXT NOT NULL, reason TEXT NOT NULL,
                payload TEXT NOT NULL, seen TEXT NOT NULL,
                PRIMARY KEY(source, slice_key, item_ref, reason));
        """)

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def save_query(self, query):
        with self.transaction():
            self.connection.execute(
                "INSERT INTO search_meta VALUES ('query', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (json.dumps(query, ensure_ascii=False, sort_keys=True),))

    def load_query(self):
        row = self.connection.execute("SELECT value FROM search_meta WHERE key='query'").fetchone()
        return json.loads(row[0]) if row else None

    def get_slice(self, source, slice_key):
        row = self.connection.execute(
            "SELECT * FROM slices WHERE source=? AND slice_key=?",
            (source, slice_key)).fetchone()
        return dict(row) if row else None

    def begin_pass(self, source, slice_key, window, start, end, cursor_json):
        """开始一轮全新翻页：清空污点与页数，游标指向首个请求。"""
        with self.transaction():
            self.connection.execute("""
                INSERT INTO slices(source, slice_key, window, interval_start,
                                   interval_end, cursor, taint, status, pages, updated)
                VALUES (?,?,?,?,?,?,'[]','incomplete',0,?)
                ON CONFLICT(source, slice_key) DO UPDATE SET
                    window=excluded.window, interval_start=excluded.interval_start,
                    interval_end=excluded.interval_end, cursor=excluded.cursor,
                    taint='[]', status='incomplete', pages=0, updated=excluded.updated
            """, (source, slice_key, json.dumps(window), start.isoformat(),
                  end.isoformat(), cursor_json, _now()))

    def _append_taint(self, source, slice_key, reasons):
        row = self.connection.execute(
            "SELECT taint FROM slices WHERE source=? AND slice_key=?",
            (source, slice_key)).fetchone()
        taint = json.loads(row[0]) if row else []
        for reason in reasons:
            if reason not in taint:
                taint.append(reason)
        self.connection.execute(
            "UPDATE slices SET taint=?, status='incomplete', updated=? "
            "WHERE source=? AND slice_key=?",
            (json.dumps(taint, ensure_ascii=False), _now(), source, slice_key))

    def commit_page(self, source, slice_key, views, quarantined, next_cursor_json):
        """单页成果与下一游标同事务提交；坏条目入隔离区并污点标记该切片。"""
        now = _now()
        with self.transaction():
            for view in views:
                record_key = view['record_key']
                row = self.connection.execute(
                    "SELECT payload FROM search_records WHERE record_key=?",
                    (record_key,)).fetchone()
                if row:
                    payload = json.loads(row[0])
                else:
                    payload = {'record_key': record_key,
                               'observation': view['observation'],
                               'revision': view['revision'], 'assets': {}}
                for asset_name, asset in view['assets'].items():
                    target = payload['assets'].setdefault(
                        asset_name, {'roles': [], 'locators': {}})
                    target['roles'] = sorted(set(target['roles']) | set(asset['roles']))
                    target['locators'][source] = asset['href']
                encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                self.connection.execute("""
                    INSERT INTO search_records VALUES (?,?,?,?)
                    ON CONFLICT(record_key) DO UPDATE SET
                        payload=excluded.payload, updated=excluded.updated
                """, (record_key, view['observation_key'], encoded, now))
                self.connection.execute("""
                    INSERT INTO record_sources VALUES (?,?,?,?,?)
                    ON CONFLICT(record_key, source) DO UPDATE SET
                        item_id=excluded.item_id, slice_key=excluded.slice_key,
                        seen=excluded.seen
                """, (record_key, source, view['item_id'], slice_key, now))
            tainted = []
            for item, reason in quarantined:
                if isinstance(item, dict) and isinstance(item.get('id'), str):
                    item_ref = item['id']
                else:
                    item_ref = '<无标识>'
                try:
                    raw = json.dumps(item, ensure_ascii=False, sort_keys=True)
                except (TypeError, ValueError):
                    raw = repr(item)
                self.connection.execute(
                    "INSERT OR IGNORE INTO quarantine VALUES (?,?,?,?,?,?)",
                    (source, slice_key, item_ref, reason, raw, now))
                tainted.append(f'隔离坏条目 {item_ref}: {reason}')
            if tainted:
                self._append_taint(source, slice_key, tainted)
            self.connection.execute(
                "UPDATE slices SET cursor=?, pages=pages+1, updated=? "
                "WHERE source=? AND slice_key=?",
                (next_cursor_json, now, source, slice_key))

    def fail_slice(self, source, slice_key, reason):
        """翻页中断：保留当前游标，标记不完整，等待下次续传。"""
        with self.transaction():
            self._append_taint(source, slice_key, [reason])

    def taint_slice(self, source, slice_key, window, start, end, reason):
        """检索异常时落污点；切片行可能尚未建立，先补建行。"""
        with self.transaction():
            self.connection.execute("""
                INSERT OR IGNORE INTO slices(source, slice_key, window, interval_start,
                                             interval_end, cursor, taint, status, pages, updated)
                VALUES (?,?,?,?,?,NULL,'[]','incomplete',0,?)
            """, (source, slice_key, json.dumps(window), start.isoformat(),
                  end.isoformat(), _now()))
            self._append_taint(source, slice_key, [reason])

    def finish_pass(self, source, slice_key):
        """一轮翻页结束：无污点才算完整，否则保持不完整状态。"""
        with self.transaction():
            row = self.connection.execute(
                "SELECT taint FROM slices WHERE source=? AND slice_key=?",
                (source, slice_key)).fetchone()
            taint = json.loads(row[0]) if row else []
            status = 'complete' if not taint else 'incomplete'
            self.connection.execute(
                "UPDATE slices SET cursor=NULL, status=?, updated=? "
                "WHERE source=? AND slice_key=?",
                (status, _now(), source, slice_key))

    def all_records(self):
        return [json.loads(row[0]) for row in self.connection.execute(
            "SELECT payload FROM search_records ORDER BY record_key")]

    def record_sources(self):
        result = {}
        for row in self.connection.execute(
                "SELECT record_key, source, item_id, seen FROM record_sources ORDER BY source"):
            result.setdefault(row['record_key'], []).append(
                {'source': row['source'], 'item_id': row['item_id'], 'seen': row['seen']})
        return result

    def all_slices(self):
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM slices ORDER BY source, slice_key")]

    def all_quarantine(self):
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM quarantine ORDER BY source, slice_key, item_ref")]

    def close(self):
        self.connection.close()
