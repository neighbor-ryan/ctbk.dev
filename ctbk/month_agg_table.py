from abc import ABC
from os.path import exists
from typing import Generator

import dask.dataframe as dd
import pandas as pd
from click import command, argument, option
from utz import err, cached_property, DefaultDict

from ctbk.cli.base import dask
from ctbk.tasks import MonthTasks, MonthTables
from ctbk.util import S3
from ctbk.util.df import DataFrame
from ctbk.util.ym import dates, YM, Monthy


class MonthAggTable(ABC):
    ROOT = S3
    SRC_CLS = None
    OUT = None

    def __init__(self, start, end, root=None, overwrite=False, dask=False, out=None):
        self.root = root or self.ROOT
        self.overwrite = overwrite
        self.dask = dask
        self.out = out or self.OUT
        self.dpd = dd if dask else pd

        self.src = src = self.SRC_CLS(start=start, end=end, **self.src_kwargs())
        self.start = src.start
        self.end = src.end

    def src_kwargs(self):
        return dict(roots=DefaultDict({}, self.root), dask=self.dask)

    @property
    def dir(self):
        return self.src.dir

    def url(self, ym: Monthy) -> str:
        return self.src.url(ym)

    @property
    def months(self) -> Generator[YM, None, None]:
        return self.start.until(self.end)

    @property
    def dfs(self) -> list[DataFrame]:
        return [ self.src.df(ym) for ym in self.months ]

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

        self._run()

    def _run(self):
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
