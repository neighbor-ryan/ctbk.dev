from os.path import dirname

import dask.dataframe as dd
import fsspec
import pandas as pd
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dask import delayed
from dask.delayed import Delayed
from typing import Union
from urllib.parse import urlparse

from ctbk.read import Read, Disk
from ctbk.util import cached_property, YM, stderr
from ctbk.util.defaultdict import DefaultDict, Unset
from ctbk.util.df import DataFrame, checkpoint
from ctbk.write import Always, IfAbsent, Never, Write


class HasRoot:
    DIR = None
    NAMES = None

    def __init__(
            self, *args,
            roots: DefaultDict[str] = None,
            reads: DefaultDict[Read] = None,
            writes: DefaultDict[Write] = None,
            dask: bool = False,
    ):
        names = self.NAMES or []

        self.roots = roots
        self.reads = reads
        self.writes = writes

        root = self.root = None
        for name in names:
            if name in roots.configs:
                root = self.root = roots.configs[name]
                break

        self.read = Disk
        for name in names:
            if name in reads.configs:
                self.read = reads.configs[name]
                break

        self.write = IfAbsent
        for name in names:
            if name in writes.configs:
                self.write = writes.configs[name]
                break

        if not self.DIR:
            raise RuntimeError(f"{self}.DIR not defined")
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        self.dask = dask
        super().__init__(*args)

    @property
    def kwargs(self):
        return dict(roots=self.roots, reads=self.reads, writes=self.writes, dask=self.dask)

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
    NAMES = []

    def __init__(self, *args, **kwargs):
        MonthURL.__init__(self, *args)
        HasRoot.__init__(self, **kwargs)

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        raise NotImplementedError

    def create(self) -> Union[None, Delayed]:
        if self.exists():
            if self.write is Always:
                stderr(f'Overwriting {self.url}')
                return self._create()
            elif self.read is None:
                def exists_noop():
                    pass
                return delayed(exists_noop)(dask_key_name=f'read {self.url}')
            else:
                stderr(f'Reading {self.url}')
                return self._read()
        elif self.write is Never:
            raise RuntimeError(f"{self.url} doesn't exist, but `write` is `Never`")
        else:
            stderr(f'Writing {self.url}')
            return self._create()

    def _read(self) -> DataFrame:
        if self.dask:
            return dd.read_parquet(self.url)
        else:
            return pd.read_parquet(self.url)


class MonthDataDF(MonthData, ABC):
    def create(self) -> Union[None, DataFrame, Delayed]:
        return super().create()

    def _df(self, read: Union[None, Read] = Unset, write: Union[None, Write] = Unset) -> DataFrame:
        raise NotImplementedError

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed, DataFrame]:
        return checkpoint(self._df(), self.url, rv=self.read if read is Unset else read)

    @cached_property
    def df(self) -> DataFrame:
        if self.read is None:
            raise NotImplementedError(f"{self.url}: can't load df, self.read is None")
        return self.create()
