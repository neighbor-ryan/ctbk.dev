import typing
from functools import wraps
from typing import Literal, Tuple

from click import option, Choice
from utz.ym import Monthy, YM

from ctbk.util.ym import NJ_GENESIS

Region = Literal[ 'NYC', 'JC', ]
REGIONS: Tuple[Region, ...] = typing.get_args(Region)

def get_regions(ym: Monthy):
    if YM(ym) >= NJ_GENESIS:
        return ['NYC', 'JC']
    else:
        return ['NYC']


def region(fn):
    @option('-r', '--region', type=Choice(REGIONS), help=f"Region to process ({', '.join(REGIONS)}); default: both/all. \"JC\" actually means \"NJ\" (i.e. Jersey City and Hoboken).")
    @wraps(fn)
    def _fn(*args, region=None, **kwargs):
        regions = [region] if region else REGIONS
        fn(*args, regions=regions, **kwargs)

    return _fn
