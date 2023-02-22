from os.path import exists

import dask.dataframe as dd
import pandas as pd
from abc import ABC
from click import command, argument, option
from typing import Generator

from ctbk.cli.base import dask
from ctbk.util import stderr
from ctbk.util.df import DataFrame
from ctbk.util.ym import dates, YM, Monthy


class MonthAggTable(ABC):
    ROOT = None
    OUT = None

    def load(self, ym: Monthy) -> DataFrame:
        raise NotImplementedError

    def __init__(self, start, end, root=None, overwrite=False, dask=False, out=None):
        self.start = start
        self.end = end or YM()
        self.root = root or self.ROOT
        self.overwrite = overwrite
        self.dask = dask
        self.out = out or self.OUT
        self.dpd = dd if dask else pd

    @property
    def months(self) -> Generator['YM', None, None]:
        return self.start.until(self.end)

    @property
    def dfs(self) -> list[DataFrame]:
        return [ self.load(ym) for ym in self.months ]

    @property
    def df(self) -> DataFrame:
        return self.dpd.concat(self.dfs)

    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def write(self, df: DataFrame):
        if isinstance(df, dd.DataFrame):
            df = df.compute()

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

        df = self.df
        out_df = self.transform(df)

        self.write(out_df)

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
