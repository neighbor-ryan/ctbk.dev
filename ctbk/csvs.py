#!/usr/bin/env python
from os.path import basename

import gzip
from abc import ABC
from contextlib import contextmanager
from shutil import copyfileobj
from typing import Optional, Union, Iterator, IO
from zipfile import ZipFile, BadZipFile

import dask.dataframe as dd
import pandas as pd
from ctbk.has_root_cli import HasRootCLI
from ctbk.table import Table
from ctbk.task import Task
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.util.read import Read
from ctbk.util.region import REGIONS, Region, region
from ctbk.util.ym import dates, YM, Monthy
from ctbk.zips import TripdataZips, TripdataZip
from dask.delayed import delayed, Delayed
from gzip_stream import GZIPCompressedStream
from utz import cached_property, singleton, Unset, err

DIR = f'{BKT}/csvs'


class ReadsTripdataZip(Task, ABC):
    def __init__(self, ym, region, **kwargs):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.ym = YM(ym)
        self.region = region
        super().__init__(**kwargs)

    @cached_property
    def src(self) -> TripdataZip:
        return TripdataZip(ym=self.ym, region=self.region, roots=self.roots)


class TripdataCsv(ReadsTripdataZip, Table):
    DIR = DIR
    NAMES = ['csv']
    COMPRESSION_LEVEL = 9

    @cached_property
    def url(self):
        region_str = 'JC-' if self.region == 'JC' else ''
        return f'{self.dir}/{region_str}{self.ym}-citibike-tripdata.csv.gz'

    def extract_csv_from_zip(self):
        with self.fd('wb') as raw_o:
            with gzip.open(raw_o, 'wb', compresslevel=self.COMPRESSION_LEVEL) as o:
                header = None
                for fdno, i in enumerate(self.zip_csv_fds()):
                    line = next(i)
                    if fdno == 0:
                        header = line
                        o.write(line)
                    else:
                        header2 = line
                        if header != header2:
                            raise RuntimeError(f"Header mismatch in {self.src.url} (CSV idx {fdno}): {header} != {header2}")
                        o.write(b'\n')
                    copyfileobj(i, o)

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        read = self.read if read is Unset else read
        if read is None:
            if self.dask:
                return delayed(self.extract_csv_from_zip)()
            else:
                self.extract_csv_from_zip()
        else:
            return self.checkpoint(read=read)

    def zip_csv_fds(self) -> Iterator[IO]:
        """Return a read fd for the single CSV in the source .zip."""
        src = self.src
        with src.fd('rb') as z_in:
            z = ZipFile(z_in)
            names = z.namelist()
            print(f'{src.url}: zip names: {names}')

            csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
            if len(csvs) > 1:
                err(f"Found {len(csvs)} CSVs in {src.url}: {csvs}")

            if not csvs:
                # 202409-citibike-tripdata.zip contains 5 `.csv.zip`s
                csv_zip_names = [
                    f
                    for f in names
                    if f.endswith('.csv.zip')
                       and not f.startswith('_')
                       and not basename(f).startswith('_')
                ]
                for csv_zip_name in csv_zip_names:
                    with z.open(csv_zip_name, 'r') as csv_zip_fd:
                        try:
                            csv_zip = ZipFile(csv_zip_fd)
                        except BadZipFile:
                            raise ValueError(f"Failed to open nested zip file {csv_zip_name} inside {src.url}")
                        csv_zip_names = csv_zip.namelist()
                        csvs = [ f for f in csv_zip_names if f.endswith('.csv') and not f.startswith('_') ]
                        for csv in csvs:
                            yield csv_zip.open(csv, 'r')
            else:
                for name in csvs:
                    yield z.open(name, 'r')

    def meta(self):
        if self.exists():
            return pd.read_csv(self.url, dtype=str, nrows=0)
        else:
            with next(self.zip_csv_fds()) as i:
                return pd.read_csv(i, dtype=str, nrows=0)

    def _df(self) -> DataFrame:
        if self.dask:
            def create_and_read(created):
                return pd.read_csv(self.url, dtype=str)

            meta = self.meta()
            df = dd.from_delayed([ delayed(create_and_read)(self._create(read=None)) ], meta=meta)
        else:
            self._create(read=None)
            df = pd.read_csv(self.url, dtype=str)
        return df

    @property
    def checkpoint_kwargs(self):
        return dict(fmt='csv', read_kwargs=dict(dtype=str), write_kwargs=dict(index=False))

    def _read(self) -> DataFrame:
        if self.dask:
            return dd.read_csv(self.url, dtype=str, blocksize=None)
        else:
            return pd.read_csv(self.url, dtype=str)


class TripdataCsvs(HasRootCLI):
    DIR = DIR
    CHILD_CLS = TripdataCsv

    def __init__(self, start: Monthy = None, end: Monthy = None, regions: Optional[list[str]] = None, **kwargs):
        src = self.src = TripdataZips(start=start, end=end, regions=regions, roots=kwargs.get('roots'))
        self.start: YM = src.start
        self.end: YM = src.end
        self.regions = regions or REGIONS
        super().__init__(**kwargs)

    @cached_property
    def children(self) -> list[TripdataCsv]:
        return [
            TripdataCsv(ym=u.ym, region=u.region, **self.kwargs)
            for u in self.src.children
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
            return self.concat([ child.df for child in self.children ])
        else:
            raise NotImplementedError("Unified DataFrame is large, you probably want .dd instead (.dd.compute() if you must)")


cli = TripdataCsvs.cli(
    help=f"Extract CSVs from \"tripdata\" .zip files. Writes to <root>/{DIR}.",
    decos=[ dates, region ],
)
