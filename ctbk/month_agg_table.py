from os.path import exists
from urllib.parse import urlparse

import dask.dataframe as dd
import fsspec
import pandas as pd
from abc import ABC
from click import command, argument, option
from typing import Generator
from utz import err

from ctbk.cli.base import dask
from ctbk.util import S3
from ctbk.util.df import DataFrame
from ctbk.util.ym import dates, YM, Monthy


class MonthAggTable(ABC):
    ROOT = S3
    SRC = None
    OUT = None

    def __init__(self, start, end, root=None, overwrite=False, dask=False, out=None):
        self.root = root or self.ROOT
        if not self.SRC:
            raise RuntimeError(f"Set {self.__class__.__name__}.SRC")
        self.src = self.SRC
        self.dir = f'{self.root}/{self.src}'
        self.overwrite = overwrite
        self.dask = dask
        self.out = out or self.OUT
        self.dpd = dd if dask else pd

        self.start = start
        if end:
            self.end = end
        else:
            last = YM()
            scheme = urlparse(self.dir).scheme
            fs = fsspec.filesystem(scheme)
            last_urls = []
            found = False
            # In the worst case, the most recent data will be 2 months behind the current calendar month (e.g. at the
            # beginning of a month, when the previous month's data has not been published yet
            for i in range(3):
                last_url = self.url(last)
                last_urls.append(last_url)

                if fs.exists(last_url):
                    found = True
                    break
                else:
                    last -= 1
            if not found:
                raise ValueError(f"Couldn't find any of: {last_urls}")
            self.end = last + 1

    def url(self, ym: Monthy) -> str:
        return f'{self.dir}/{ym}.parquet'

    def read(self, url):
        return self.dpd.read_parquet(url)

    def load(self, ym: Monthy) -> DataFrame:
        url = self.url(ym)
        try:
            df = self.dpd.read_parquet(url)
        except FileNotFoundError as e:
            err(f'FileNotFoundError: {url}')
            raise
        return df

    @property
    def months(self) -> Generator['YM', None, None]:
        return self.start.until(self.end)

    @property
    def dfs(self) -> list[DataFrame]:
        return [ self.load(ym) for ym in self.months ]

    def mapped_dfs(self) -> list[DataFrame]:
        return [ self.map(df) for df in self.dfs ]

    def map(self, df):
        return df

    def reduce(self, mapped_dfs) -> DataFrame:
        return self.dpd.concat(mapped_dfs)

    def write(self, df: DataFrame):
        if isinstance(df, dd.DataFrame):
            df = df.compute()

        self.write_df(df)

    def write_df(self, df: pd.DataFrame):
        df.to_json(self.out, 'records')

    def run(self):
        out = self.out
        if exists(out):
            if self.overwrite:
                err(f'Overwriting {out}')
            else:
                err(f'{out} exists')
                return
        else:
            err(f'Writing {out}')

        mapped_dfs = self.mapped_dfs()
        df = self.reduce(mapped_dfs)
        self.write(df)

    @classmethod
    def main(cls):
        @command()
        @option('-r', '--root')
        @option('-f', '--overwrite', is_flag=True)
        @argument('out', required=False)
        @dates
        @dask
        def _main(*args, **kwargs):
            task = cls(*args, **kwargs)
            task.run()
        return _main()
