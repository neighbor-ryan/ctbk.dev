from contextlib import contextmanager
from os.path import dirname
from typing import Union

import dask.dataframe as dd
import fsspec
import pandas as pd
from abc import ABC, abstractmethod
from dask import delayed
from dask.delayed import Delayed
from urllib.parse import urlparse

from ctbk.util import cached_property, YM, Monthy, stderr
from ctbk.util.constants import DEFAULT_ROOT
from ctbk.util.df import DataFrame, checkpoint, RV
from ctbk.write import Overwrite, IfAbsent, Never, WriteConfigs


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
        if write_configs:
            for write_config_name in self.WRITE_CONFIG_NAMES or []:
                if write_config_name in write_configs:
                    self.write = write_configs[write_config_name]
                    break
        self.dask = dask
        super().__init__(*args)

    @property
    def kwargs(self):
        return dict(root=self.root, write_configs=self.write_configs, dask=self.dask)

    def concat(self, *args, **kwargs):
        concat_fn = dd.concat if self.dask else pd.concat
        return concat_fn(*args, **kwargs)


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

    @contextmanager
    def fd(self, mode):
        made_dir = self.mkdirs()
        fs = self.fs
        url = self.url
        succeeded = False
        try:
            yield fs.open(url, mode)
            succeeded = True
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

    def _df(self) -> DataFrame:
        raise NotImplementedError

    def create_write(self, rv: RV = 'read'):
        return checkpoint(self._df(), self.url, rv=rv)

    def create(self, rv: RV = 'read') -> Union[DataFrame, Delayed]:
        write = self.write
        if self.exists():
            if write is Overwrite:
                stderr(f'Overwriting {self.url}')
                return self.create_write(rv=rv)
            elif rv is None:
                def exists_noop():
                    pass
                return delayed(exists_noop)(dask_key_name=f'read {self.url}')
            else:
                stderr(f'Reading {self.url}')
                return self.read()
        elif write is Never:
            raise RuntimeError(f"{self.url} doesn't exist")
        else:
            stderr(f'Writing {self.url}')
            return self.create_write(rv=rv)

    @cached_property
    def df(self) -> DataFrame:
        return self.create(rv='read')

    def read(self) -> DataFrame:
        if self.dask:
            return dd.read_parquet(self.url)
        else:
            return pd.read_parquet(self.url)


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
