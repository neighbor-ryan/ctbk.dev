import pytest

from ctbk.ride_animation_audit import count_gate


def test_exact_daily_gate_and_explicit_excluded_source_records():
    days = {'2025-06-01': {'rides': 3}, '2025-06-02': {'rides': 5}}
    month = {'sourceRides': 8, 'encodedRides': 8, 'invalidDuration': 0, 'unmapped': 0}
    assert count_gate(month, days, {'2025-06-01': 3, '2025-06-02': 5}) == {
        'days': 2, 'sourceRides': 8, 'encodedRides': 8, 'pyramidRides': 8,
        'invalidDuration': 0, 'unmapped': 0, 'pyramidExcluded': 0, 'exactDailyParity': True,
    }
    month.update(sourceRides=11, invalidDuration=1, unmapped=2)
    assert count_gate(month, days, {'2025-06-01': 4, '2025-06-02': 6}) == {
        'days': 2, 'sourceRides': 11, 'encodedRides': 8, 'pyramidRides': 10,
        'invalidDuration': 1, 'unmapped': 2, 'pyramidExcluded': 1, 'exactDailyParity': False,
    }


@pytest.mark.parametrize('expected', [
    {'2025-06-01': 2, '2025-06-02': 6},
    {'2025-06-01': 3, '2025-06-02': 4},
    {'2025-06-01': 4, '2025-06-02': 5},
    {'2025-06-01': 8},
])
def test_unexplained_day_or_month_disagreement_raises(expected):
    month = {'sourceRides': 8, 'encodedRides': 8, 'invalidDuration': 0, 'unmapped': 0}
    with pytest.raises(ValueError):
        count_gate(month, {'2025-06-01': {'rides': 3}, '2025-06-02': {'rides': 5}}, expected)
