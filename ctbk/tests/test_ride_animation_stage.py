from json import dumps
from pathlib import Path

from ctbk.ride_animation_stage import RideAnimationMonth


def test_month_stage_records_deterministic_output_and_current_next_source_provenance(tmp_path, monkeypatch):
    from ctbk import consolidated, ride_animation_cli
    from ctbk.pyramid_cascade import rides_assets
    class Source:
        def __init__(self, ym):
            self.url = str(tmp_path / f'{ym}.parquet')
    monkeypatch.setattr(consolidated, 'ConsolidatedMonth', Source)
    for ym in ['202512', '202601']:
        (tmp_path / f'{ym}.parquet').write_bytes(ym.encode())
    for name in ['ID_MAP_PATH', 'STATION_LUC_PATH', 'GEO_JSON_PATH']:
        path = tmp_path / name
        path.write_text('{}\n')
        monkeypatch.setattr(rides_assets, name, path)
    class Stage(RideAnimationMonth):
        @property
        def url(self):
            return str(tmp_path / 'output')
    stage = Stage('202512')
    prepared = stage.to_artifact()
    assert [Path(dep.path) for dep in prepared.computation.deps[:2]] == [tmp_path / '202512.parquet', tmp_path / '202601.parquet']
    def build(ym, input_dir, prefix, output, upload):
        assert (ym, input_dir, prefix, output, upload) == ('202512', tmp_path, 'rides-anim-candidate/v1', tmp_path / 'output', False)
        output.mkdir()
        report = {'final': True, 'sourceRides': 5, 'days': {'2025-12-01': {'rides': 5}}, 'objects': {}, 'inputs': [{'local': 'machine-specific-path'}]}
        (output / 'month.json').write_text(dumps(report))
        return {'final': True, 'sourceRides': 5, 'inputs': report['inputs']}
    monkeypatch.setattr(ride_animation_cli.create, 'callback', build)
    expected = {'final': True, 'sourceRides': 5, 'days': {'2025-12-01': {'rides': 5}}, 'objects': {}}
    assert stage.create() == expected
    assert stage.read() == expected
    artifact = stage.to_artifact()
    assert artifact.computation.cmd == 'ctbk ride-animation create -w0 202512'
    code = Path(ride_animation_cli.__file__).parent
    assert [Path(dep.path) for dep in artifact.computation.deps] == [
        tmp_path / '202512.parquet', tmp_path / '202601.parquet',
        tmp_path / 'ID_MAP_PATH', tmp_path / 'STATION_LUC_PATH', tmp_path / 'GEO_JSON_PATH',
        code / 'ride_animation.py', code / 'ride_animation_cli.py', code / 'ride_animation_stage.py',
        code / 'pyramid_cascade' / 'rides_assets.py', code / 'pyramid_cascade' / 'rides_source.py',
    ]
