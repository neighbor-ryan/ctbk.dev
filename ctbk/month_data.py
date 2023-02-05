import dask.dataframe as dd
import fsspec
import pandas as pd
from urllib.parse import urlparse

from ctbk.compute import Compute, Always, Overread, Overwrite, IfAbsent, Never
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

    @cached_property
    def parsed(self):
        return urlparse(self.url)

    @cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.scheme)

    def exists(self):
        return self.fs.exists(self.url)

    def fd(self, mode):
        return self.fs.open(self.url, mode)


class MonthData(MonthURL):
    def __init__(self, *args, compute: Compute = None, **kwargs):
        self.compute = compute
        super().__init__(*args, **kwargs)

    def create_fn(self):
        self.compute_dd().to_parquet(self.url)

    def create(self, compute_fn=None, write_fn=None):
        compute = self.compute
        if compute is Always:
            if not compute_fn:
                raise ValueError("Can't compute, compute_fn is None")
            return compute_fn()
        elif compute in [Overwrite, Overread]:
            if not compute_fn:
                raise ValueError("Can't compute, compute_fn is None")
            if not write_fn:
                raise ValueError("Can't write, write_fn is None")
            df = compute_fn()
            stderr(f'Overwriting {self.url}')
            if not write_fn:
                write_fn = lambda df: df.to_parquet(self.url)
            write_fn(df)
            if compute is Overwrite:
                return df
            else:
                return None
        elif self.exists():
            return None
        elif compute is IfAbsent:
            stderr(f'Creating {self.url}')
            self.create_fn()
        elif compute is Never:
            raise RuntimeError(f"{self.url} doesn't exist")
        else:
            raise ValueError(f"Unrecognized `compute`: {compute}")

    def get(self, compute_fn, read_fn, write_fn=None):
        rv = self.create(compute_fn=compute_fn, write_fn=write_fn)
        if rv:
            # compute in { True, 'overwrite' }
            return rv
        return read_fn(self.url)

    @cached_property
    def df(self) -> pd.DataFrame:
        return self.get(compute_fn=self.compute_df, read_fn=pd.read_parquet)

    @cached_property
    def dd(self) -> dd.DataFrame:
        return self.get(compute_fn=self.compute_dd, read_fn=dd.read_parquet)

    def compute_df(self):
        raise NotImplementedError

    def compute_dd(self):
        raise NotImplementedError


class MonthsData:
    def __init__(self, start: Monthy, end: Monthy, url_fn: str, compute: Compute = None):
        self.start: YM = YM(start)
        self.end: YM = YM(end)
        self.url_fn = url_fn
        self.compute = compute

    def url(self, ym: Monthy) -> str:
        ym = YM(ym)
        if not (self.start <= ym < self.end):
            raise ValueError(f'ym {ym} is outside [{self.start},{self.end})')
        url = ym.format(self.url_fn)
        if url == self.url_fn:
            raise ValueError(f"Invalid url? {self.url_fn} render no-ops at {ym}")
        return url

    def month(self, ym: Monthy) -> MonthData:
        ym = YM(ym)
        url = self.url(ym)
        return MonthData(ym, url, compute=self.compute)

    @cached_property
    def months(self):
        return [
            self.month(ym)
            for ym in self.start.until(self.end)
        ]

    def ym_df(self, ym: Monthy, add=False) -> pd.DataFrame:
        month = self.month(ym)
        df = month.df
        if add:
            df['ym'] = ym
        return df

    def ym_dd(self, ym: Monthy, add=False) -> dd.DataFrame:
        month = self.month(ym)
        df = month.dd
        if add:
            df['ym'] = ym
        return df

    @cached_property
    def dfs(self) -> list[pd.DataFrame]:
        return [
            self.ym_df(ym, add=True)
            for ym in self.start.until(self.end)
        ]

    @cached_property
    def dds(self) -> list[dd.DataFrame]:
        return [
            self.ym_dd(ym, add=True)
            for ym in self.start.until(self.end)
        ]

    @cached_property
    def df(self):
        return pd.concat(self.dfs)

    @cached_property
    def dd(self):
        return dd.concat(self.dds)

    def create(self, dask=True):
        for month in self.months:
            compute_fn = month.compute_dd if dask else month.compute_df
            month.create(compute_fn=compute_fn)
