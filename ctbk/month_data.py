from os.path import dirname

import dask.dataframe as dd
import fsspec
import pandas as pd
from abc import ABC, abstractmethod
from urllib.parse import urlparse

from ctbk.util import cached_property, YM, Monthy, stderr
from ctbk.util.constants import DEFAULT_ROOT
from ctbk.util.df import DataFrame
from ctbk.write import Overread, Overwrite, IfAbsent, Never, WriteConfigs


class HasRoot:
    DIR = None
    WRITE_CONFIG_NAMES = None

    def __init__(self, *args, root=DEFAULT_ROOT, write_configs: WriteConfigs = None, dask: bool = False):
        self.root = root
        if not self.DIR:
            raise RuntimeError(f"{self}.DIR not defined")
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        self.write_configs = write_configs
        self.write = IfAbsent
        for write_config_name in self.WRITE_CONFIG_NAMES or []:
            if write_config_name in write_configs:
                self.write = write_configs[write_config_name]
                break
        self.dask = dask
        super().__init__(*args)

    @property
    def kwargs(self):
        return dict(root=self.root, write_configs=self.write_configs, dask=self.dask)


class MonthURL(ABC):
    def __init__(self, ym):
        self.ym = YM(ym)

    @property
    @abstractmethod
    def url(self):
        raise NotImplementedError

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

    def mkdirs(self):
        fs = self.fs
        url = self.url
        dir = dirname(url)
        if fs.exists(dir):
            return None
        else:
            fs.mkdirs(dir, exist_ok=True)
            return dir

    def fd(self, mode):
        made_dir = self.mkdirs()
        fs = self.fs
        url = self.url
        succeeded = False
        try:
            fd = fs.open(url, mode)
            succeeded = True
            return fd
        finally:
            if not succeeded:
                if fs.exists(url):
                    stderr(f"Removing failed write: {url}")
                    fs.delete(url)
                if made_dir:
                    stderr(f"Removing dir after failed write: {made_dir}")
                    fs.delete(made_dir)


class MonthData(MonthURL, HasRoot, ABC):
    DIR = None

    def __init__(self, *args, **kwargs):
        MonthURL.__init__(self, *args)
        HasRoot.__init__(self, **kwargs)

    def create_fn(self):
        df = self.compute()
        self.save(df)
        return df

    def create(self):
        write = self.write
        if write is Overwrite:
            stderr(f'{"Overwriting" if self.exists() else "Writing"} {self.url}')
            return self.create_fn()
        elif write is Overread:
            stderr(f'{"Overwriting" if self.exists() else "Writing"} {self.url} (then reading)')
            self.create_fn()
            return self.read()
        elif self.exists():
            return None
        elif write is IfAbsent:
            stderr(f'Creating {self.url}')
            return self.create_fn()
        elif write is Never:
            raise RuntimeError(f"{self.url} doesn't exist")
        else:
            raise ValueError(f"Unrecognized `write`: {write}")

    @cached_property
    def df(self) -> DataFrame:
        df = self.create()
        if df is not None:
            return df
        return self.read()

    def read(self) -> DataFrame:
        if self.dask:
            return dd.read_parquet(self.url)
        else:
            return pd.read_parquet(self.url)

    def save(self, df):
        if self.dask:
            df.compute().to_parquet(self.fd('wb'))
        else:
            df.to_parquet(self.fd('wb'))

    @abstractmethod
    def compute(self):
        raise NotImplementedError

    def concat(self, *args, **kwargs):
        concat_fn = dd.concat if self.dask else pd.concat
        return concat_fn(*args, **kwargs)


class MonthsData(HasRoot):
    def __init__(self, start: Monthy, end: Monthy, **kwargs):
        self.start: YM = YM(start)
        self.end: YM = YM(end)
        super().__init__(**kwargs)

    def month(self, ym: Monthy) -> MonthData:
        return MonthData(ym, **self.kwargs)

    @cached_property
    def months(self):
        return [
            self.month(ym)
            for ym in self.start.until(self.end)
        ]

    def ym_df(self, ym: Monthy, add=False) -> DataFrame:
        month = self.month(ym)
        df = month.df
        if add:
            df['ym'] = ym
        return df

    @cached_property
    def dfs(self) -> list[DataFrame]:
        return [
            self.ym_df(ym, add=True)
            for ym in self.start.until(self.end)
        ]

    @cached_property
    def df(self):
        if self.dask:
            return dd.concat(self.dfs)
        else:
            return pd.concat(self.dfs)

    def create(self):
        for month in self.months:
            month.create()
