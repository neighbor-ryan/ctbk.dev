import typing
from click import pass_context, option, Choice

from typing import Literal, Tuple

from ctbk import YM, Monthy
from ctbk.cli.base import ctbk
from ctbk.month_data import MonthsData, MonthURL
from ctbk.util import cached_property
from ctbk.util.constants import GENESIS, S3

Region = Literal[ 'NYC', 'JC', ]
REGIONS: Tuple[Region, ...] = typing.get_args(Region)


DIR = 'tripdata'


class TripdataMonth(MonthURL):
    DIR = DIR

    def __init__(self, ym, region, root=S3):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.region = region
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        ym = YM(ym)

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

        super().__init__(ym, url)


class TripdataMonths:
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, root=S3):
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        self.start: YM = YM(start or GENESIS)
        end = end or YM()

        # "month" to "region" to "url" map
        m2r2u = [
            (
                ym,
                {
                    region: TripdataMonth(ym=ym, region=region, root=root)
                    for region in REGIONS
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
            end = end2
            m2r2u = dict(m2r2u[:-1])
        else:
            m2r2u = dict(m2r2u)

        self.m2r2u: dict[YM, dict[Region, TripdataMonth]] = m2r2u
        self.end: YM = end

    @property
    def urls(self) -> list[TripdataMonth]:
        return [
            u
            for r2u in self.m2r2u.values()
            for u in r2u.values()
        ]


@ctbk.group()
def tripdata():
    pass


@tripdata.command()
@pass_context
@option('-r', '--region', type=Choice(REGIONS))
def urls(ctx, region):
    o = ctx.obj
    months = TripdataMonths(start=o.start, end=o.end, root=o.root)
    urls = months.urls
    if region:
        urls = [ url for url in urls if url.region == region ]
    for url in urls:
        print(url.url)




class Tripdata(MonthsData):
    ROOT = 'tripdata'
    RGX = r'^(?:(?P<region>JC)-)?(?P<month>\d{6})[ \-]citi?bike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'

    @cached_property
    def parsed_basenames(self):
        df = super().parsed_basenames
        df = df.dropna(subset=['month']).astype({ 'month': int })
        df['region'] = df['region'].fillna('NYC')
        return df
