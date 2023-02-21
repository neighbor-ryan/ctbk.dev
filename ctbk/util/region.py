import typing

from typing import Literal, Tuple

from ctbk.util.ym import Monthy, YM

Region = Literal[ 'NYC', 'JC', ]
REGIONS: Tuple[Region, ...] = typing.get_args(Region)


def get_regions(ym: Monthy):
    if YM(ym) >= YM(201509):
        return ['NYC', 'JC']
    else:
        return ['NYC']
