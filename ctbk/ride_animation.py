"""Version-one columnar ride files; timestamps are local-clock seconds.

The first start is signed zigzag LEB128; remaining starts are unsigned
deltas. Header: magic, version, reserved, station count, rides, carry-in.
Then start varints and duration/source/destination u16 columns, flags u8.
"""
from __future__ import annotations

from dataclasses import dataclass
from gzip import compress
from hashlib import md5
from json import dumps
from pathlib import Path
from struct import pack, unpack_from
from typing import Iterable

import polars as pl

CAP = 10800
HEADER = '<4sBBHII'


@dataclass(frozen=True)
class Ride:
    start: int
    duration: int
    source: int
    destination: int
    flags: int


def ride_order(row: Ride) -> tuple[int, int, int, int, int]:
    return row.start, row.source, row.destination, row.duration, row.flags


def station_bytes(positions: dict[str, tuple[float, float]]) -> bytes:
    return dumps([{'id': sid, 'lat': positions[sid][0], 'lng': positions[sid][1]} for sid in sorted(positions)], separators=(',', ':'), sort_keys=True).encode() + b'\n'


def varint(value: int) -> bytes:
    if value < 0:
        raise ValueError('negative unsigned varint')
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def encode_day(
    rides: Iterable[Ride],
    station_count: int,
    carry: int,
) -> bytes:
    rows = list(rides)
    if not 0 <= station_count <= 65535 or not 0 <= carry <= len(rows):
        raise ValueError('invalid day header')
    if rows != sorted(rows, key=ride_order):
        raise ValueError('rides must be sorted')
    if sum(r.start < 0 for r in rows) != carry:
        raise ValueError('carry-in count disagrees with starts')
    starts = bytearray()
    previous = 0
    for i, row in enumerate(rows):
        if not (-CAP <= row.start < 86400 and 0 < row.duration <= CAP and 0 <= row.flags <= 31):
            raise ValueError('invalid ride time or flags')
        if not (0 <= row.source < station_count and 0 <= row.destination < station_count):
            raise ValueError('station index out of range')
        if row.start < 0 and row.start + row.duration <= 0:
            raise ValueError('carry-in ride must still be active')
        delta = row.start - previous
        starts += varint((row.start << 1) ^ (row.start >> 31) if i == 0 else delta)
        previous = row.start
    header = pack(HEADER, b'CTRA', 1, 0, station_count, len(rows), carry)
    columns = b''.join(pack('<' + 'H' * len(rows), *(getattr(r, field) for r in rows)) for field in ('duration', 'source', 'destination'))
    return header + starts + columns + bytes(r.flags for r in rows)


def decode_day(data: bytes) -> tuple[int, int, list[Ride]]:
    if len(data) < 16:
        raise ValueError('truncated day header')
    magic, version, reserved, stations, count, carry = unpack_from(HEADER, data)
    if (magic, version, reserved) != (b'CTRA', 1, 0) or carry > count or count > (len(data) - 16) // 8:
        raise ValueError('invalid day header')
    offset = 16
    starts = []
    previous = 0
    for i in range(count):
        value = 0
        for shift in range(0, 35, 7):
            if offset >= len(data):
                raise ValueError('truncated start varint')
            byte = data[offset]
            offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                break
        else:
            raise ValueError('start varint too large')
        start = (value >> 1) ^ -(value & 1) if i == 0 else previous + value
        starts.append(start)
        previous = start
    if len(data) != offset + count * 7:
        raise ValueError('invalid day column lengths')
    durations = unpack_from('<' + 'H' * count, data, offset)
    sources = unpack_from('<' + 'H' * count, data, offset + count * 2)
    destinations = unpack_from('<' + 'H' * count, data, offset + count * 4)
    flags = data[offset + count * 6:]
    rows = [Ride(*args) for args in zip(starts, durations, sources, destinations, flags)]
    if encode_day(rows, stations, carry) != data:
        raise ValueError('noncanonical day encoding')
    return stations, carry, rows


def build_month(
    df: pl.DataFrame,
    start: int,
    end: int,
    canonical: dict[str, str],
    positions: dict[str, tuple[float, float]],
    output: Path,
) -> dict:
    ids = sorted(positions)
    indices = {sid: i for i, sid in enumerate(ids)}
    raw_to_index = {raw: indices[canon] for raw, canon in canonical.items() if canon in indices}
    raw_to_index = indices | raw_to_index
    frame = df.with_columns(
        pl.col('Start Time').dt.epoch('s').alias('start'),
        (pl.col('Stop Time') - pl.col('Start Time')).dt.total_seconds().alias('duration'),
        pl.col('Start Station ID').replace_strict(raw_to_index, default=None).alias('source'),
        pl.col('End Station ID').replace_strict(raw_to_index, default=None).alias('destination'),
    ).filter(pl.col('start').is_between(start - CAP, end, closed='left'))
    in_month = pl.col('start') >= start
    invalid = pl.col('duration').is_null() | (pl.col('duration') <= 0)
    unmapped = pl.col('source').is_null() | pl.col('destination').is_null()
    source_count = frame.filter(in_month).height
    invalid_count = frame.filter(in_month & invalid).height
    unmapped_count = frame.filter(in_month & ~invalid & unmapped).height
    valid = frame.filter(~invalid & ~unmapped).with_columns(
        (
            (pl.col('Rideable Type').cast(pl.String) == 'electric_bike').fill_null(False).cast(pl.UInt8)
            + 2 * (pl.col('User Type').cast(pl.String) == 'Customer').fill_null(False).cast(pl.UInt8)
            + 4 * (pl.col('duration') > CAP).cast(pl.UInt8)
            + 8 * (~pl.col('Rideable Type').cast(pl.String).is_in(['electric_bike', 'classic_bike'])).fill_null(True).cast(pl.UInt8)
            + 16 * (~pl.col('User Type').cast(pl.String).is_in(['Subscriber', 'Customer'])).fill_null(True).cast(pl.UInt8)
        ).alias('flags'),
        pl.col('duration').clip(1, CAP),
    ).select('start', 'duration', 'source', 'destination', 'flags')
    output.mkdir(parents=True, exist_ok=True)
    objects = {}

    def save(blob: bytes, suffix: str) -> dict:
        digest = md5(blob).hexdigest()
        key = f'objects/{digest}{suffix}'
        path = output / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(blob)
        objects[key] = {'md5': digest, 'bytes': len(blob)}
        return {'key': key, **objects[key]}

    stations = save(station_bytes(positions), '.json')
    days = {}
    total = 0
    for midnight in range(start, end, 86400):
        day = valid.filter(
            (pl.col('start') < midnight + 86400)
            & (pl.col('start') + pl.col('duration') > midnight)
            & (pl.col('start') >= midnight - CAP),
        ).with_columns(pl.col('start') - midnight).sort('start', 'source', 'destination', 'duration', 'flags')
        rows = [Ride(*row) for row in day.iter_rows()]
        carry = sum(r.start < 0 for r in rows)
        data = encode_day(rows, len(ids), carry)
        if decode_day(data) != (len(ids), carry, rows):
            raise ValueError('day round-trip failed')
        meta = save(compress(data, compresslevel=9, mtime=0), '.bin.gz')
        from datetime import datetime, timezone
        date = datetime.fromtimestamp(midnight, timezone.utc).strftime('%Y-%m-%d')
        count = len(rows) - carry
        days[date] = {**meta, 'rides': count, 'carry': carry, 'stations': stations['key']}
        total += count
    if source_count != total + invalid_count + unmapped_count:
        raise ValueError('month count parity failed')
    report = {
        'version': 1, 'durationCap': CAP, 'canonicalHash': md5(dumps(canonical, sort_keys=True).encode()).hexdigest(),
        'sourceRides': source_count, 'encodedRides': total,
        'invalidDuration': invalid_count, 'unmapped': unmapped_count,
        'longRides': valid.filter(in_month & (pl.col('flags') & 4 != 0)).height,
        'stations': stations, 'days': days, 'objects': objects,
    }
    (output / 'month.json').write_text(dumps(report, indent=2, sort_keys=True) + '\n')
    return report
