from json import loads
from pathlib import Path
from subprocess import run

from ctbk.stage_dag import metadata_dag, print_dag


def test_metadata_graph_includes_new_month_and_excludes_ignored_worktrees(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    run(['git', 'init', '-q'], check=True)
    Path('.git/info/exclude').write_text('wt/\n')
    for path, size in [('s3/ctbk/normalized/202607.parquet.dvc', 7), ('s3/ctbk/normalized/202609.parquet.dvc', 9), ('wt/scratch/s3/ctbk/normalized/209901.parquet.dvc', 99)]:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f'outs:\n- path: {target.stem}\n  md5: abc\n  size: {size}\n')
    latest = Path('s3/ctbk/normalized/202609.parquet.dvc')
    latest.write_text(latest.read_text() + 'meta:\n  computation:\n    cmd: ctbk cons create 202609\n    deps:\n      /s3/ctbk/normalized/202607.parquet: abc\n')
    run(['git', 'add', 's3/ctbk/normalized/202607.parquet.dvc'], check=True)
    assert metadata_dag() == {
        'nodes': {
            's3/ctbk/normalized/202607.parquet': {'md5': 'abc', 'size': 7, 'cmd': None, 'deps': {}},
            's3/ctbk/normalized/202609.parquet': {'md5': 'abc', 'size': 9, 'cmd': 'ctbk cons create 202609', 'deps': {'s3/ctbk/normalized/202607.parquet': 'abc'}},
        },
        'edges': [{'from': 's3/ctbk/normalized/202607.parquet', 'to': 's3/ctbk/normalized/202609.parquet'}],
    }
    print_dag('json', 'dag.json')
    stage = loads(Path('dag.json').read_text())['stages'][0]
    assert {key: stage[key] for key in ['key', 'fileCount', 'totalSize', 'monthRange']} == {
        'key': 'cons', 'fileCount': 2, 'totalSize': 16, 'monthRange': '202607-202609',
    }
    assert Path('dag.json').read_bytes()[-1:] == b'\n'


def test_monthly_www_step_regenerates_dag_without_fetching_data(monkeypatch):
    from ctbk.update import update

    calls = []
    monkeypatch.setattr('ctbk.update.run', lambda *args, **kwargs: calls.append((args, kwargs)))
    update.callback.__wrapped__(ym='202609', dry_run=True, no_station_harmonize=True, no_www=False)
    assert calls[-2:] == [
        (('node', 'www/scripts/gen-station-urls.js'), {'dry_run': True}),
        (('ctbk', 'dag', '-fjson', '-o', 'www/public/assets/dag.json'), {'dry_run': True}),
    ]
    calls.clear()
    update.callback.__wrapped__(ym='202609', dry_run=True, no_station_harmonize=True, no_www=True)
    assert calls[-1] == (('ctbk', 'spj', 'create', '202609'), {'dry_run': True})
