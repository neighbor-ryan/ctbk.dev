from typing import Union, Literal

import fsspec
from urllib.parse import urlparse

import dask.dataframe as dd
import pandas as pd

from ctbk.util import cached_property, YM, Monthy, stderr


class MonthURL:
    def __init__(self, ym, url):
        self.ym = YM(ym)
        self.url = url

    def __str__(self):
        return f'{self.__class__.__name__}({self.url})'

    def __repr__(self):
        return str(self)

    @property
    def scheme(self):
        return self.parsed.scheme

    @property
    def path(self):
        return self.parsed.path

    @cached_property
    def parsed(self):
        return urlparse(self.url)

    @cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.scheme)

    def exists(self):
        return self.fs.exists(self.path)

    def fd(self, mode):
        return self.fs.open(self.path, mode)


Compute = Union[bool, Literal['overwrite', 'overread'], None]


class MonthData(MonthURL):
    def create_fn(self):
        self.compute_dd().to_parquet(self.url)

    def create(self, compute_fn=None, write_fn=None, compute: Compute = None):
        if compute is True:
            if not compute_fn:
                raise ValueError("Can't compute, compute_fn is None")
            return compute_fn()
        elif compute in ['overwrite', 'overread']:
            if not compute_fn:
                raise ValueError("Can't compute, compute_fn is None")
            if not write_fn:
                raise ValueError("Can't write, write_fn is None")
            df = compute_fn()
            stderr(f'Overwriting {self.url}')
            write_fn(df)
            if compute == 'overwrite':
                return df
            else:
                return None
        elif self.exists():
            return None
        elif compute is None:
            stderr(f'Creating {self.url}')
            self.create_fn()
        elif compute is False:
            raise RuntimeError(f"{self.url} doesn't exist")
        else:
            raise ValueError(f"Unrecognized `compute`: {compute}")

    def get(self, compute_fn, read_fn, write_fn, compute: Compute = None):
        rv = self.create(compute_fn=compute_fn, write_fn=write_fn, compute=compute)
        if rv:
            # compute in { True, 'overwrite' }
            return rv
        return read_fn(self.url)

    def df(self, compute: Compute = None) -> pd.DataFrame:
        return self.get(compute_fn=self.compute_df, read_fn=pd.read_parquet, write_fn=lambda df: df.to_parquet(self.url), compute=compute)

    def dd(self, compute: Compute = None) -> dd.DataFrame:
        return self.get(compute_fn=self.compute_dd, read_fn=dd.read_parquet, write_fn=lambda df: df.to_parquet(self.url), compute=compute)

    def compute_df(self):
        raise NotImplementedError

    def compute_dd(self):
        raise NotImplementedError


class MonthsData:
    def __init__(self, start: Monthy, end: Monthy, url: str):
        self.start: YM = YM(start)
        self.end: YM = YM(end)
        self.url_fn = url

    def url(self, ym: Monthy) -> str:
        ym = YM(ym)
        if not (self.start <= ym < self.end):
            raise ValueError(f'ym {ym} is outside [{self.start},{self.end})')
        url = ym.format(self.url_fn)
        if url == self.url_fn:
            raise ValueError(f"Invalid url? {self.url_fn} render no-ops at {ym}")
        return url

    def ym_df(self, ym: Monthy, add=False):
        ym = YM(ym)
        url = self.url(ym)
        df = MonthData(ym, url)
        if add:
            df['ym'] = ym
        return df

    @cached_property
    def dfs(self):
        return [
            self.ym_df(ym, add=True)
            for ym in self.start.until(self.end)
        ]

    @cached_property
    def ym2df(self):
        return {
            ym: self.ym_df(ym).df
            for ym in self.start.until(self.end)
        }

    @cached_property
    def df(self):
        return pd.concat([ mdf.df for mdf in self.dfs ])

    @cached_property
    def dd(self):
        return pd.concat([ mdf.dd for mdf in self.dfs ])

    def urls(self, name='url', add=False):
        if add:
            return pd.DataFrame([
                { 'ym': ym, name: self.url(ym) }
                for ym in self.start.until(self.end)
            ])
        else:
            return pd.DataFrame([
                { name: self.url(ym) }
                for ym in self.start.until(self.end)
            ])
