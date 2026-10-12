from json import dumps

import polars as pl
import pytest

from ctbk.pyramid_cascade.timelapse_check import Read, hourly, tier_cover


def test_cover_deduplicates_rewrites_and_clips_overlapping_rungs() -> None:
    manifest = '\n'.join(dumps(row) for row in [
        dict(tier='1m', shard_dur='4d', period_start=0, period_end=4, key='old', bytes=5, written_at=1),
        dict(tier='1m', shard_dur='4d', period_start=0, period_end=4, key='new', bytes=6, written_at=2),
        dict(tier='1m', shard_dur='2d', period_start=2, period_end=6, key='tail', bytes=7, written_at=3),
        dict(tier='1h', shard_dur='4d', period_start=0, period_end=6, key='other', bytes=8, written_at=4),
    ])
    assert tier_cover(manifest, '1m', 1, 6) == [Read('new', 1, 4, 6), Read('tail', 4, 6, 7)]
    with pytest.raises(ValueError, match=r'^1m: uncovered intervals \[\(6, 7\)\]$'):
        tier_cover(manifest, '1m', 1, 7)


def test_hourly_sums_every_metric_and_preserves_station_identity() -> None:
    frame = pl.DataFrame({
        'cell': ['s:a', 's:a', 's:b', 's:a'],
        'dt': [0, 60_000, 120_000, 3_600_000],
        'count_n': [1., 2., 3., 4.],
        'count_sum': [1., 2., 3., 4.],
        'count_sumsq': [1., 2., 3., 4.],
    })
    assert hourly(frame).to_dicts() == [
        {'cell': 's:a', 'dt': 0, 'count_n': 3., 'count_sum': 3., 'count_sumsq': 3.},
        {'cell': 's:a', 'dt': 3_600_000, 'count_n': 4., 'count_sum': 4., 'count_sumsq': 4.},
        {'cell': 's:b', 'dt': 0, 'count_n': 3., 'count_sum': 3., 'count_sumsq': 3.},
    ]
