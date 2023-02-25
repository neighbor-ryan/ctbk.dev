import typing
from functools import wraps
from typing import Literal, Tuple

from click import option, Choice

from ctbk.util.ym import Monthy, YM

Region = Literal[ 'NYC', 'JC', ]
REGIONS: Tuple[Region, ...] = typing.get_args(Region)


def get_regions(ym: Monthy):
    if YM(ym) >= YM(201509):
        return ['NYC', 'JC']
    else:
        return ['NYC']


def region(fn):
    @option('-r', '--region', type=Choice(REGIONS))
    @wraps(fn)
    def _fn(*args, region=None, **kwargs):
        regions = [region] if region else REGIONS
        fn(*args, regions=regions, **kwargs)

    return _fn
