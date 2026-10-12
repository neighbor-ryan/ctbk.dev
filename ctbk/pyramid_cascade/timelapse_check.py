"""Exact fine-tier gates over manifest-selected, non-overlapping shards."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from json import loads
from typing import Callable

import polars as pl
from pyrmts.axis import nominal_delta_ms

METRICS = ['count_n', 'count_sum', 'count_sumsq']


class UncoveredTier(ValueError):
    """The manifest has no complete closed-shard cover for this tier/range."""


def epoch_ms(value: int | str) -> int:
    if isinstance(value, int):
        return value
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


@dataclass(frozen=True)
class Read:
    key: str
    start: int
    end: int
    size: int


def tier_cover(
    manifest: str,
    tier: str,
    start: int,
    end: int,
) -> list[Read]:
    latest = {}
    for line in manifest.splitlines():
        if not line.strip():
            continue
        row = loads(line)
        if row['tier'] != tier:
            continue
        row = dict(row, period_start=epoch_ms(row['period_start']), period_end=epoch_ms(row['period_end']), written_at=epoch_ms(row['written_at']))
        slot = (row['shard_dur'], row['period_start'])
        if slot not in latest or row['written_at'] > latest[slot]['written_at']:
            latest[slot] = row
    need = [(start, end)]
    reads = []
    for row in sorted(latest.values(), key=lambda r: (-nominal_delta_ms(r['shard_dur']), r['period_start'])):
        next_need = []
        for a, b in need:
            lo, hi = max(a, row['period_start']), min(b, row['period_end'])
            if lo >= hi:
                next_need.append((a, b))
                continue
            reads.append(Read(row['key'], lo, hi, row.get('bytes', 0)))
            if a < lo:
                next_need.append((a, lo))
            if hi < b:
                next_need.append((hi, b))
        need = next_need
    if need:
        raise UncoveredTier(f'{tier}: uncovered intervals {need}')
    return sorted(reads, key=lambda r: (r.start, r.end))


def read_tier(reads: list[Read], get: Callable[[str], bytes]) -> pl.DataFrame:
    frames = []
    for read in reads:
        frame = pl.read_parquet(BytesIO(get(read.key)), columns=['cell', 'dt', *METRICS])
        dt = pl.col('dt').dt.epoch('ms') if isinstance(frame.schema['dt'], pl.Datetime) else pl.col('dt').cast(pl.Int64)
        frame = frame.with_columns(dt.alias('dt')).filter((pl.col('dt') >= read.start) & (pl.col('dt') < read.end))
        frames.append(frame.with_columns(pl.col(METRICS).cast(pl.Float64)))
    return pl.concat(frames).sort(['cell', 'dt'])


def hourly(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns((pl.col('dt') // 3_600_000 * 3_600_000).alias('dt')).group_by(['cell', 'dt']).agg(
        pl.col(METRICS).sum(),
    ).sort(['cell', 'dt'])


def daily_reference(
    manifest: str,
    start: int,
    end: int,
    get: Callable[[str], bytes],
) -> tuple[str, pl.DataFrame]:
    # Coarse day shards can still be open at a closed monthly tip. Finer
    # dividing tiers provide the exact same day totals, like API routing.
    for tier in ['1d', '1h', '15m', '1m']:
        try:
            reads = tier_cover(manifest, tier, start, end)
        except UncoveredTier:
            continue
        frame = read_tier(reads, get)
        totals = frame.with_columns((pl.col('dt') // 86400000 * 86400000).alias('dt')).group_by('dt').agg(pl.col('count_n').sum()).sort('dt')
        return tier, totals
    raise UncoveredTier(f'no exact daily reference cover for [{start}, {end})')


def compare_anchor(
    candidate: str,
    baseline: str,
    anchor: str,
    start: int,
    end: int,
    get: Callable[[str], bytes],
) -> dict:
    manifests = {p: get(f'{p}/{anchor}/manifest.jsonl').decode() for p in [candidate, baseline]}
    reads = {tier: tier_cover(manifests[candidate], tier, start, end) for tier in ['1m', '15m', '1h']}
    base_reads = tier_cover(manifests[baseline], '1h', start, end)
    frames = {tier: read_tier(plan, get) for tier, plan in reads.items()}
    base = hourly(read_tier(base_reads, get))
    reference = hourly(frames['1h'])
    checks = {
        'minute_to_hour': hourly(frames['1m']).equals(reference),
        'quarter_hour_to_hour': hourly(frames['15m']).equals(reference),
        'hourly_baseline': reference.equals(base),
    }
    report = {
        'anchor': anchor,
        'checks': checks,
        'tiers': {tier: {
            'rows': frame.height,
            'bytes': sum({r.key: r.size for r in reads[tier]}.values()),
            'shards': len({r.key for r in reads[tier]}),
            'count_n': frame['count_n'].sum(),
        } for tier, frame in frames.items()},
    }
    if not all(checks.values()):
        raise ValueError(f'timelapse parity failed: {report}')
    return report


def month_ranges(start: int, end: int) -> list[tuple[int, int]]:
    ranges = []
    while start < end:
        at = datetime.fromtimestamp(start / 1000, timezone.utc)
        following = datetime(at.year + (at.month == 12), at.month % 12 + 1, 1, tzinfo=timezone.utc)
        stop = min(end, int(following.timestamp() * 1000))
        ranges.append((start, stop))
        start = stop
    return ranges
