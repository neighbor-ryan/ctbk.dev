from click import option
from utz import YM

from .base import ctbk
from ..has_root_cli import yms_arg


@ctbk.command()
@option('-p', '--prefix', help='Prepend this string to each "YM" output line')
@option('-r', '--reverse', is_flag=True, help='Output months in reverse order')
@option('-s', '--suffix', help='Append this string to each "YM" output line')
@yms_arg
def yms(
    prefix: str | None,
    reverse: bool,
    suffix: str | None,
    yms: list[YM],
):
    """Print one or more YM (year-month) ranges, e.g.:

    yms 24-25  # 202401, 202402, …, 202412

    yms -r     # All YMs, reverse order
    """
    for ym in reversed(yms) if reverse else yms:
        print(f"{prefix or ''}{ym}{suffix or ''}")
