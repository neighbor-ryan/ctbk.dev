"""Cache finality for monthly, end-attributed normalized sources."""
from datetime import datetime


def finality(month_start: datetime, month_end: datetime) -> dict[str, str]:
    # Starts throughout the newest month can gain late-ending rides from
    # the next source tile. Measured durations exceed one day, so that
    # month's entire interval remains mutable until the next ingest.
    return {
        'start': month_start.isoformat().replace('+00:00', 'Z'),
        'end': month_end.isoformat().replace('+00:00', 'Z'),
    }
