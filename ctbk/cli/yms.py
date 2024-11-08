from click import argument, option

from .base import ctbk, roots_opt, load_roots
from ..has_root_cli import default_end
from ..util import GENESIS
from ..util.ym import parse_ym_ranges_str


@ctbk.command()
@option('-r', '--reverse', is_flag=True, help='Output months in reverse order')
@roots_opt
@argument('ym-ranges', required=False)
def yms(reverse, roots, ym_ranges):
    roots = load_roots(roots)
    if not ym_ranges:
        ym_ranges = '-'  # Default to all available months
    yms = parse_ym_ranges_str(
        ym_ranges,
        default_start=GENESIS,
        default_end=lambda: default_end(roots=roots),
    )
    for ym in reversed(yms) if reverse else yms:
        print(ym)
