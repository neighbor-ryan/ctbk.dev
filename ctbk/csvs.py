#!/usr/bin/env python

import dask.dataframe as dd
from click import pass_context, option, Choice
from utz import *
from zipfile import ZipFile

from ctbk import YM, Monthy
from ctbk.cli.base import ctbk
from ctbk.month_data import MonthData, Compute
from ctbk.tripdata import REGIONS, TripdataMonths, TripdataMonth, Region
from ctbk.util.constants import S3, BKT

DIR = f'{BKT}/csvs'


class TripdataCsv(MonthData):
    DIR = DIR

    def __init__(self, ym, region, root=S3, compute: Compute = None):
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        self.compute = compute
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.region = region
        ym = YM(ym)
        region_str = 'JC-' if region == 'JC' else ''
        url = f'{self.dir}/{region_str}{ym}-citibike-tripdata.csv'
        super().__init__(ym, url)

    @property
    def src(self) -> TripdataMonth:
        return TripdataMonth(ym=self.ym, region=self.region, root=self.root)

    def create_fn(self):
        src = self.src
        z = ZipFile(src.fd('rb'))
        names = z.namelist()
        print(f'{src.url}: zip names: {names}')

        csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
        name = singleton(csvs)

        with z.open(name, 'r') as i, self.fs.open(self.url, 'w') as o:
            copyfileobj(i, o)

    def compute_df(self):
        self.create_fn()
        df = pd.read_csv(self.url, dtype=str)
        df['region'] = self.region
        return df

    def compute_dd(self):
        self.create_fn()
        df = dd.read_csv(self.url, dtype=str)
        df['region'] = self.region
        return df


class TripdataCsvs:
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, root=S3, compute: Compute = None):
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        self.compute = compute
        src = self.src = TripdataMonths(start=start, end=end, root=root)
        self.start: YM = src.start
        self.end: YM = src.end

    @cached_property
    def csvs(self) -> list[TripdataCsv]:
        return [
            TripdataCsv(ym=u.ym, region=u.region, root=self.root, compute=self.compute)
            for u in self.src.urls
        ]

    @property
    def m2r2csv(self) -> dict[YM, dict[Region, TripdataCsv]]:
        m2r2u = self.src.m2r2u
        return {
            ym: {
                region: TripdataCsv(ym=ym, region=region, root=self.root, compute=self.compute)
                for region in r2u
            }
            for ym, r2u in m2r2u.items()
        }

    def m2df(self):
        return {
            m: pd.concat([
                csv.df
                for r, csv in r2csv.items()
            ])
            for m, r2csv in self.m2r2csv.items()
        }

    def m2dd(self):
        return {
            m: dd.concat([
                csv.dd
                for r, csv in r2csv.items()
            ])
            for m, r2csv in self.m2r2csv.items()
        }

    @cached_property
    def df(self):
        raise NotImplementedError("Unified DataFrame is large, you probably want .dd instead (.dd.compute() if you must)")

    @cached_property
    def dd(self):
        return dd.concat([
            csv.dd
            for m, r2csv in self.m2r2csv.items()
            for r, csv in r2csv.items()
        ])

    def create(self, dask=True):
        for csv in self.csvs:
            compute_fn = csv.compute_dd if dask else csv.compute_df
            csv.create(compute_fn=compute_fn)


@ctbk.group()
def csvs():
    pass


@csvs.command()
@pass_context
@option('-r', '--region', type=Choice(REGIONS))
def urls(ctx, region):
    o = ctx.obj
    months = TripdataCsvs(start=o.start, end=o.end, root=o.root, compute=o.compute)
    csvs = months.csvs
    if region:
        csvs = [ csv for csv in csvs if csv.region == region ]
    for csv in csvs:
        print(csv.url)
