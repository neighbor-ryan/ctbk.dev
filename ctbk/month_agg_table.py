from os.path import exists

import dask.dataframe as dd
import pandas as pd
from abc import ABC
from click import command, argument, option
from typing import Generator

from ctbk.cli.base import dask
from ctbk.util import stderr, S3
from ctbk.util.df import DataFrame
from ctbk.util.ym import dates, YM, Monthy


class MonthAggTable(ABC):
    ROOT = S3
    SRC = None
    OUT = None

    def __init__(self, start, end, root=None, overwrite=False, dask=False, out=None):
        self.start = start
        self.end = end or YM()
        self.root = root or self.ROOT
        if not self.SRC:
            raise RuntimeError(f"Set {self.__class__.__name__}.SRC")
        self.src = self.SRC
        self.dir = f'{self.root}/{self.src}'
        self.overwrite = overwrite
        self.dask = dask
        self.out = out or self.OUT
        self.dpd = dd if dask else pd

    def url(self, ym: Monthy) -> str:
        return f'{self.dir}/{ym}.parquet'

    def read(self, url):
        return self.dpd.read_parquet(url)

    def load(self, ym: Monthy) -> DataFrame:
        url = self.url(ym)
        try:
            df = self.dpd.read_parquet(url)
        except FileNotFoundError as e:
            stderr(f'FileNotFoundError: {url}')
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
                stderr(f'Overwriting {out}')
            else:
                stderr(f'{out} exists')
                return
        else:
            stderr(f'Writing {out}')

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
