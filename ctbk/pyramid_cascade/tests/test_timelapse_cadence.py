from datetime import datetime, timezone

from ctbk.pyramid_cascade.timelapse_cadence import finality


def test_newest_start_month_stays_mutable_for_multiday_spillback() -> None:
    assert finality(
        datetime(2026, 9, 1, tzinfo=timezone.utc),
        datetime(2026, 10, 1, tzinfo=timezone.utc),
    ) == {
        'start': '2026-09-01T00:00:00Z',
        'end': '2026-10-01T00:00:00Z',
    }
