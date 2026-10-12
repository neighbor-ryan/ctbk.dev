from io import BytesIO
from json import dumps

import polars as pl
import pytest

from ctbk.pyramid_cascade.timelapse_check import daily_reference

DAY = 86400000


def test_open_day_shard_uses_closed_hour_shards_for_exact_daily_counts():
    manifest = '\n'.join(dumps(row) for row in [
        {'tier': '1d', 'shard_dur': '1d', 'period_start': 0, 'period_end': DAY, 'written_at': 0, 'key': 'day'},
        {'tier': '1h', 'shard_dur': '2d', 'period_start': 0, 'period_end': 2 * DAY, 'written_at': 0, 'key': 'hour'},
    ])
    data = pl.DataFrame({'cell': ['A'] * 4, 'dt': [0, DAY // 2, DAY, DAY + DAY // 2], 'count_n': [2, 3, 4, 5], 'count_sum': [2, 3, 4, 5], 'count_sumsq': [2, 3, 4, 5]})
    buffer = BytesIO()
    data.write_parquet(buffer)
    def get(key: str) -> bytes:
        assert key == 'hour'
        return buffer.getvalue()
    tier, totals = daily_reference(manifest, 0, 2 * DAY, get)
    assert tier == '1h'
    assert totals.to_dicts() == [{'dt': 0, 'count_n': 5.0}, {'dt': DAY, 'count_n': 9.0}]


def test_malformed_manifest_is_not_treated_as_missing_coverage():
    manifest = dumps({'tier': '1d', 'shard_dur': '1d', 'period_start': 0, 'period_end': DAY, 'written_at': 'invalid', 'key': 'day'})
    with pytest.raises(ValueError, match="^Invalid isoformat string: 'invalid'$"):
        daily_reference(manifest, 0, DAY, lambda key: b'')
