#!/usr/bin/env python
from abc import ABC

from contextlib import contextmanager
from os.path import basename
from typing import Optional, Union

import dask.dataframe as dd
import pandas as pd
from click import pass_context, option, Choice
from dask.delayed import delayed, Delayed
from gzip_stream import GZIPCompressedStream
from shutil import copyfileobj
from utz import singleton
from zipfile import ZipFile

from ctbk import YM, Monthy
from ctbk.cli.base import ctbk, dask
from ctbk.month_data import MonthData, HasRoot, MonthDataDF
from ctbk.read import Read
from ctbk.util import cached_property
from ctbk.util.constants import BKT, GENESIS
from ctbk.util.defaultdict import Unset
from ctbk.util.df import DataFrame, RV, checkpoint
from ctbk.zips import REGIONS, TripdataZips, TripdataZip, Region

DIR = f'{BKT}/csvs'


class ReadsTripdataZip(MonthData, ABC):
    def __init__(self, ym, region, **kwargs):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.region = region
        ym = YM(ym)
        super().__init__(ym, **kwargs)

    @cached_property
    def src(self) -> TripdataZip:
        return TripdataZip(ym=self.ym, region=self.region)  # TODO: allow zips to be local?


class TripdataCsv(ReadsTripdataZip, MonthDataDF):
    DIR = DIR
    NAMES = ['csv']

    @cached_property
    def url(self):
        region_str = 'JC-' if self.region == 'JC' else ''
        return f'{self.dir}/{region_str}{self.ym}-citibike-tripdata.csv.gz'

    def extract_csv_from_zip(self):
        with self.zip_csv_fd() as i, self.fd('wb') as o:
            copyfileobj(GZIPCompressedStream(i, compression_level=7), o)

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        read = self.read if read is Unset else read
        if read is None:
            if self.dask:
                return delayed(self.extract_csv_from_zip)()
            else:
                self.extract_csv_from_zip()
        else:
            return checkpoint(self._df(), self.url, rv=self.read)

    @contextmanager
    def zip_csv_fd(self):
        """Return a read fd for the single CSV in the source .zip."""
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

    def _df(self) -> DataFrame:
        def create_and_read(created):
            return pd.read_csv(self.url, dtype=str)

        meta = self.meta()
        df = dd.from_delayed([ delayed(create_and_read)(self.create()) ], meta=meta)
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


@ctbk.group('csvs')
@pass_context
@option('-d', '--dates')
@option('-r', '--region', type=Choice(REGIONS))
def csvs(ctx, dates, region):
    if dates:
        pcs = dates.split('-')
        if len(pcs) == 2:
            [ start, end ] = pcs
            start = YM(start) if start else GENESIS
            end = YM(end) if end else None
        elif len(pcs) == 1:
            [ym] = pcs
            ym = YM(ym)
            start = ym
            end = ym + 1
        else:
            raise ValueError(f"Unrecognized -d/--dates: {dates}")
    else:
        start, end = GENESIS, None

    ctx.obj.start = start
    ctx.obj.end = end
    ctx.obj.region = [region] if region else None


@csvs.command()
@pass_context
@dask
def urls(ctx, dask):
    csvs = TripdataCsvs(dask=dask, **ctx.obj)
    for csv in csvs.csvs:
        print(csv.url)


@csvs.command()
@pass_context
@dask
def create(ctx, dask):
    csvs = TripdataCsvs(dask=dask, **ctx.obj)
    created = csvs.create()
    if dask:
        created.compute()
