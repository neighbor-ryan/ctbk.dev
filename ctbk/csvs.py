#!/usr/bin/env python
from contextlib import contextmanager
from os.path import basename
from typing import Optional

import dask.dataframe as dd
import pandas as pd
from click import pass_context, option, Choice
from dask.delayed import delayed
from gzip_stream import GZIPCompressedStream
from shutil import copyfileobj
from utz import singleton
from zipfile import ZipFile

from ctbk import YM, Monthy
from ctbk.cli.base import ctbk
from ctbk.month_data import MonthData, HasRoot
from ctbk.util import cached_property
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame, RV, checkpoint
from ctbk.zips import REGIONS, TripdataZips, TripdataZip, Region

DIR = f'{BKT}/csvs'


class ReadsTripdataZip(MonthData):
    def __init__(self, ym, region, **kwargs):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.region = region
        ym = YM(ym)
        super().__init__(ym, **kwargs)

    @cached_property
    def src(self) -> TripdataZip:
        return TripdataZip(ym=self.ym, region=self.region)  # TODO: allow zips to be local?

    def create_fn(self):
        raise NotImplementedError

    def create_write(self, rv: RV = 'read'):
        if rv is None:
            return delayed(lambda: self.create_fn())()
        else:
            return checkpoint(self._df(), self.url, rv=rv)


class TripdataCsv(ReadsTripdataZip):
    DIR = DIR
    WRITE_CONFIG_NAMES = ['csv']

    @cached_property
    def url(self):
        region_str = 'JC-' if self.region == 'JC' else ''
        return f'{self.dir}/{region_str}{self.ym}-citibike-tripdata.csv.gz'

    def create_fn(self):
        with self.zip_csv_fd() as i, self.fd('wb') as o:
            copyfileobj(GZIPCompressedStream(i, compression_level=7), o)

    @contextmanager
    def zip_csv_fd(self):
        src = self.src
        with src.fd('rb') as z_in:
            z = ZipFile(z_in)
            names = z.namelist()
            print(f'{src.url}: zip names: {names}')

            csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
            name = singleton(csvs)

            yield z.open(name, 'r')

    def meta(self):
        if self.exists():
            return pd.read_csv(self.url, dtype=str, nrows=0)
        else:
            with self.zip_csv_fd() as i:
                return pd.read_csv(i, dtype=str, nrows=0)

    @cached_property
    def df(self) -> DataFrame:
        def create_and_read(created):
            return pd.read_csv(self.url, dtype=str)

        meta = self.meta()
        df = dd.from_delayed([ delayed(create_and_read)(self.create(rv=None)) ], meta=meta)
        df['region'] = self.region
        return df


class TripdataCsvs(HasRoot):
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, regions: Optional[list[str]] = None, **kwargs):
        src = self.src = TripdataZips(start=start, end=end, regions=regions)
        self.start: YM = src.start
        self.end: YM = src.end
        super().__init__(**kwargs)

    @cached_property
    def csvs(self) -> list[TripdataCsv]:
        return [
            TripdataCsv(ym=u.ym, region=u.region, **self.kwargs)
            for u in self.src.zips
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
        creates = [ csv.create(rv=None) for csv in self.csvs ]
        if self.dask:
            return delayed(lambda x: x)(creates)


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


@csvs.command()
@pass_context
@option('--dask', is_flag=True)
@option('-r', '--region', type=Choice(REGIONS))
def create(ctx, dask, region):
    o = ctx.obj
    csvs = TripdataCsvs(
        start=o.start, end=o.end,
        regions=[region] if region else None,
        dask=dask,
        root=o.root,
        write_configs=o.write_configs,
    )
    created = csvs.create()
    if dask:
        created.compute()
