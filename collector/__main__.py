import argparse
import json
from pathlib import Path
import tempfile

from .domain import collect
from .federation import FederatedSearch, build_report
from .scenario import serve_scenario
from .storage import SearchStore, Store
from .transport import Client, replay_server


def main():
    parser = argparse.ArgumentParser(description='本地采集与凭据查看')
    parser.add_argument('command', choices=['demo', 'import', 'show', 'serve',
                                            'federate', 'federation-demo'])
    parser.add_argument('--input', default='fixtures/sample.json')
    parser.add_argument('--scenario', default='fixtures/catalogs.json')
    parser.add_argument('--db', default=None)
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--phase', type=int, default=1)
    parser.add_argument('--report-only', action='store_true')
    arguments = parser.parse_args()
    if arguments.command == 'serve':
        from .api import serve
        serve(arguments.db or 'var/records.sqlite3', arguments.port)
        return
    if arguments.command == 'demo':
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(arguments.input).read_bytes()
            with replay_server({'/source': [dict(body=payload.decode('utf-8'))]}) as (base, calls):
                status, headers, raw = Client().request(base + '/source')
                if status != 200:
                    raise RuntimeError('本地来源未返回成功响应')
                store = Store(Path(directory) / 'demo.sqlite3')
                rows = collect(json.loads(raw))
                digest = store.ingest('本地演示来源', raw, rows)
                print(json.dumps(dict(digest=digest, records=store.records(), requests=len(calls)), ensure_ascii=False, indent=2))
                store.close()
        return
    if arguments.command == 'federation-demo':
        scenario = json.loads(Path(arguments.scenario).read_text(encoding='utf-8'))
        with tempfile.TemporaryDirectory() as directory:
            store = SearchStore(Path(directory) / 'federation.sqlite3')
            try:
                phases = {}
                for phase in (1, 2):
                    with serve_scenario(scenario, phase=phase) as (catalog, sources):
                        FederatedSearch(store, scenario['query']).run(sources)
                        phases[f'第{phase}阶段'] = {
                            '请求记录': catalog.calls,
                            '检索报告': build_report(store),
                        }
                print(json.dumps(phases, ensure_ascii=False, indent=2))
            finally:
                store.close()
        return
    if arguments.command == 'federate':
        scenario = json.loads(Path(arguments.scenario).read_text(encoding='utf-8'))
        database = arguments.db or 'var/federation.sqlite3'
        Path(database).parent.mkdir(parents=True, exist_ok=True)
        store = SearchStore(database)
        try:
            if not arguments.report_only:
                with serve_scenario(scenario, phase=arguments.phase) as (catalog, sources):
                    FederatedSearch(store, scenario['query']).run(sources)
            print(json.dumps(build_report(store), ensure_ascii=False, indent=2))
        finally:
            store.close()
        return
    database = arguments.db or 'var/records.sqlite3'
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    store = Store(database)
    try:
        if arguments.command == 'import':
            raw = Path(arguments.input).read_bytes()
            store.ingest(str(Path(arguments.input)), raw, collect(json.loads(raw)))
        print(json.dumps(store.records(), ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == '__main__':
    main()
