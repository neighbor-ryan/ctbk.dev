import typing
from click import pass_context, option, Choice
from typing import Literal, Optional, Tuple

from ctbk import YM, Monthy
from ctbk.cli.base import ctbk
from ctbk.month_data import MonthURL
from ctbk.util import cached_property
from ctbk.util.constants import GENESIS, S3

Region = Literal[ 'NYC', 'JC', ]
REGIONS: Tuple[Region, ...] = typing.get_args(Region)


DIR = 'tripdata'


class TripdataZip(MonthURL):
    DIR = DIR

    def __init__(self, ym, region, root=S3):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.region = region
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        ym = YM(ym)
        super().__init__(ym)

    @cached_property
    def url(self):
        ym = self.ym
        region = self.region

        # Typos in 202206, 202207 and JC-202207
        citbike_typo_months = [ (202206, 'NYC'), (202207, 'NYC'), (202207, 'JC'), ]
        citibike = 'citbike' if (int(ym), region) in citbike_typo_months else 'citibike'

        extension = 'csv.zip'
        if region == 'NYC':
            if ym.y < 2017:
                extension = 'zip'
            url = f'{self.dir}/{ym}-{citibike}-tripdata.{extension}'
        else:
            url = f'{self.dir}/{region}-{ym}-{citibike}-tripdata.{extension}'
        return url


class TripdataZips:
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, regions: Optional[list[str]] = None):
        self.start: YM = YM(start or GENESIS)
        end = end or YM()
        self.regions = regions or REGIONS

        # "month" to "region" to "url" map
        m2r2u = [
            (
                ym,
                {
                    region: TripdataZip(ym=ym, region=region, root=S3)
                    for region in self.regions
                    if region == 'NYC' or ym >= YM(201509)
                }
            )
            for ym in self.start.until(end)
        ]

        # Default "end": current or previous calendar month
        end1, last1 = m2r2u[-1]
        missing1 = [ region for region, month in last1.items() if not month.exists() ]
        if missing1:
            end2, last2 = m2r2u[-2]
            missing2 = [ region for region, month in last2.items() if not month.exists() ]
            if missing2:
                raise RuntimeError(f"Missing regions from {end1} ({', ' .join(missing1)}) and {end2} ({', '.join(missing2)})")
            end = end2 + 1
            m2r2u = dict(m2r2u[:-1])
        else:
            m2r2u = dict(m2r2u)

        self.m2r2u: dict[YM, dict[Region, TripdataZip]] = m2r2u
        self.end: YM = end

    @property
    def zips(self) -> list[TripdataZip]:
        return [
            u
            for r2u in self.m2r2u.values()
            for u in r2u.values()
        ]


@ctbk.group()
def zips():
    pass


@zips.command()
@pass_context
@option('-r', '--region', type=Choice(REGIONS))
def urls(ctx, region):
    o = ctx.obj
    months = TripdataZips(start=o.start, end=o.end, regions=[region] if region else None)
    urls = months.zips
    if region:
        urls = [ url for url in urls if url.region == region ]
    for url in urls:
        print(url.url)
