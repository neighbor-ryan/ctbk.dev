from importlib.util import module_from_spec, spec_from_file_location
from json import dumps
from pathlib import Path


def test_raw_compaction_retains_same_minute_generations_and_dedupes_alias(tmp_path: Path):
    spec = spec_from_file_location('compact_r2', Path(__file__).parents[2] / 'gbfs' / 'compact-r2.py')
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    generations = tmp_path / 'generations'
    generations.mkdir()
    first = {'ts': 120, 'polled_at': 125, 'stations': [{'station_id': 'A', 'num_bikes_available': 1}]}
    second = {'ts': 179, 'polled_at': 185, 'stations': [{'station_id': 'A', 'num_bikes_available': 2}]}
    (generations / '120.json').write_text(dumps(first))
    (generations / '179.json').write_text(dumps(second))
    (tmp_path / '00-02.json').write_text(dumps({**second, 'polled_at': 200}))
    (generations / '120-content-variant.json').write_text(dumps({**first, 'stations': []}))

    assert module.read_wal_records(tmp_path) == [first, second]
