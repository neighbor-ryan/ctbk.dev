#!/usr/bin/env python

import dask.dataframe as dd
from click import pass_context, option, Choice
from utz import *
from zipfile import ZipFile

from ctbk import YM, Monthy
from ctbk.cli.base import ctbk
from ctbk.month_data import MonthData, HasRoot
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.zips import REGIONS, TripdataZips, TripdataZip, Region

DIR = f'{BKT}/csvs'


class TripdataCsv(MonthData):
    DIR = DIR
    WRITE_CONFIG_NAMES = ['csv']

    def __init__(self, ym, region, **kwargs):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.region = region
        ym = YM(ym)
        super().__init__(ym, **kwargs)

    @property
    def url(self):
        region_str = 'JC-' if self.region == 'JC' else ''
        return f'{self.dir}/{region_str}{self.ym}-citibike-tripdata.csv'

    @property
    def src(self) -> TripdataZip:
        return TripdataZip(ym=self.ym, region=self.region)  # TODO: allow zips to be local?

    def create_fn(self):
        src = self.src
        z = ZipFile(src.fd('rb'))
        names = z.namelist()
        print(f'{src.url}: zip names: {names}')

        csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
        name = singleton(csvs)

        with z.open(name, 'r') as i, self.fd('wb') as o:
            copyfileobj(i, o)

    def read(self) -> DataFrame:
        if self.dask:
            return dd.read_csv(self.url, dtype=str)
        else:
            return pd.read_csv(self.url, dtype=str)

    def compute(self):
        self.create_fn()
        df = self.read()
        df['region'] = self.region
        return df


class TripdataCsvs(HasRoot):
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, **kwargs):
        src = self.src = TripdataZips(start=start, end=end)
        self.start: YM = src.start
        self.end: YM = src.end
        super().__init__(**kwargs)

    @cached_property
    def csvs(self) -> list[TripdataCsv]:
        return [
            TripdataCsv(ym=u.ym, region=u.region, **self.kwargs)
            for u in self.src.urls
        ]

    @property
    def m2r2csv(self) -> dict[YM, dict[Region, TripdataCsv]]:
        m2r2u = self.src.m2r2u
        return {
            ym: {
                region: TripdataCsv(ym=ym, region=region, **self.kwargs)
                for region in r2u
            }
            for ym, r2u in m2r2u.items()
        }

    def concat(self, dfs):
        if self.dask:
            return dd.concat(dfs)
        else:
            return pd.concat(dfs)

    def m2df(self):
        return {
            m: self.concat([
                csv.df
                for r, csv in r2csv.items()
            ])
            for m, r2csv in self.m2r2csv.items()
        }

    @cached_property
    def df(self):
        if self.dask:
            return self.concat([ csv.df for csv in self.csvs ])
        else:
            raise NotImplementedError("Unified DataFrame is large, you probably want .dd instead (.dd.compute() if you must)")

    def create(self):
        for csv in self.csvs:
            csv.create()


@ctbk.group()
def csvs():
    pass


@csvs.command()
@pass_context
@option('-r', '--region', type=Choice(REGIONS))
def urls(ctx, region):
    o = ctx.obj
    months = TripdataCsvs(start=o.start, end=o.end, root=o.root, write_configs=o.write_configs)
    csvs = months.csvs
    if region:
        csvs = [ csv for csv in csvs if csv.region == region ]
    for csv in csvs:
        print(csv.url)
