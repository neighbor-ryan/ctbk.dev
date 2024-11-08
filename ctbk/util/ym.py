import re
from typing import Callable

from utz.ym import YM

GENESIS = YM(2013, 6)
NJ_GENESIS = YM(2015, 9)
END = None  # We frequently infer the last available month of data using ``zips.default_end``


def parse_yymm(yymm: str) -> str:
    # Citi Bike NYC began in 201306 ("'13"), and months only go up to 12, so we support shorthand like
    # "1306" for "201306". "2012" could mean "202012" or "201201", but the latter is not a valid Citi Bike
    # month
    dd0 = int(yymm[:2])
    dd1 = int(yymm[2:])
    if dd1 <= 12:
        if dd0 < 13:
            raise ValueError(f"Invalid YM str: {yymm}")
        return f'20{yymm}'
    elif dd0 != 20:
        raise ValueError(f"Invalid YM str: {yymm}")
    else:
        return yymm


def parse_ym_ranges_str(
    ym_ranges_str: str | None,
    default_start: YM | Callable[[], YM] | None = None,
    default_end: YM | Callable[[], YM] | None = None,
) -> list[YM]:
    """Parse a comma-delimited list of "YM" (year-month) strings, of the basic form "YYYYMM-YYYYMM" (<start>-<end>),
    though several abbreviated formats are supported.

    Supported formats for each <start> and <end> component (separated by a dash)::
    - YYYYMM (e.g. "202106")
    - YYYY (maps to YYYY01, or January of year YYYY)
    - YYMM (e.g. "1306" → "201306")
    - YY (e.g. "14" → "2014" → "201401")

    Supported abbreviations for the overall `<start>-<end>` range:
    - YYYYMM- (no end, defaults to the latest available month)
    - -YYYYMM (no start, defaults to the earliest available month, 201306)
    - YYYYMM-MM (e.g. "202106-08" → "202106-202108")
    """
    ym_ranges_str = ym_ranges_str or '-'
    ym_range_strs = ym_ranges_str.split(',')
    yms = []
    for ym_range_str in ym_range_strs:
        start, *rest = ym_range_str.split('-', 1)
        if re.fullmatch(r'\d{4}', start):
            start = parse_yymm(start)
        elif re.fullmatch(r'\d{2}', start):
            start = f'20{start}'
        if start:
            start = YM(start)
        elif callable(default_start):
            start = default_start()
        elif default_start:
            start = default_start
        else:
            raise ValueError("No `start` or `default_start` provided")

        if rest:
            [end] = rest
            if re.fullmatch(r'\d{4}', end):
                end = parse_yymm(end)
            elif re.fullmatch(r'\d{2}', end):
                dd = int(end)
                if dd <= 12:
                    end = f'{start.y}{end}'
                else:
                    end = f'20{end}'
            if end:
                end = YM(end)
            elif callable(default_end):
                end = default_end()
            elif default_end:
                end = default_end
            else:
                end = YM()
        else:
            end = start + 1
        yms.extend(start.until(end))

    return list(sorted(list(set(yms))))
