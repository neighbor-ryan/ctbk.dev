"""Exact animation count gates, with explicit exclusions from source records."""
from __future__ import annotations


def count_gate(
    month: dict,
    days: dict[str, dict],
    expected: dict[str, int],
) -> dict:
    actual = {day: meta['rides'] for day, meta in days.items()}
    if set(actual) != set(expected):
        raise ValueError('animation and pyramid day sets differ')
    encoded = sum(actual.values())
    excluded = month['invalidDuration'] + month['unmapped']
    source = month['sourceRides']
    baseline = sum(expected.values())
    if encoded != month['encodedRides'] or source != encoded + excluded:
        raise ValueError('animation source accounting disagrees with daily metadata')
    if not encoded <= baseline <= source:
        raise ValueError(f'pyramid total outside accounted animation bounds: {encoded} <= {baseline} <= {source}')
    if any(actual[day] > expected[day] for day in actual):
        raise ValueError('animation day exceeds pyramid day count')
    if excluded == 0 and actual != expected:
        raise ValueError('animation daily counts differ from pyramid')
    return {
        'days': len(actual), 'sourceRides': source, 'encodedRides': encoded, 'pyramidRides': baseline,
        'invalidDuration': month['invalidDuration'], 'unmapped': month['unmapped'],
        'pyramidExcluded': source - baseline, 'exactDailyParity': actual == expected,
    }
