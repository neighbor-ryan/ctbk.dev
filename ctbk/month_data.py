from os.path import dirname, exists

import dask.dataframe as dd
import pandas as pd
from abc import ABC
from dask import delayed
from dask.delayed import Delayed
from typing import Union
from utz import Unset

from ctbk.has_root import HasRoot
from ctbk.month_url import MonthURL
from ctbk.util.read import Read
from ctbk.util import cached_property, stderr
from ctbk.util.df import DataFrame, checkpoint_dd, checkpoint_df
from ctbk.util.write import Always, Never


class MonthData(MonthURL, HasRoot, ABC):
    DIR = None
    NAMES = []

    def __init__(self, *args, **kwargs):
        MonthURL.__init__(self, *args)
        HasRoot.__init__(self, **kwargs)

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        raise NotImplementedError

    def create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        read = self.read if read is Unset else read
        url = self.url
        if self.exists():
            if self.write is Always:
                stderr(f'Overwriting {url}')
                return self._create()
            elif read is None:
                stderr(f'{url} already exists')
                if self.dask:
                    def exists_noop():
                        pass
                    return delayed(exists_noop)(dask_key_name=f'read {url}')
                else:
                    return None
            else:
                stderr(f'Reading {url}')
                return self._read()
        elif self.write is Never:
            raise RuntimeError(f"{url} doesn't exist, but `write` is `Never`")
        else:
            stderr(f'Writing {url}')
            return self._create()

    def _read(self) -> Union[None, Delayed]:
        if self.dask:
            [delayed] = dd.read_parquet(self.url).to_delayed()
            return delayed
        else:
            return None


class MonthDataDF(MonthData, ABC):
    def create(self, read: Union[None, Read] = Unset) -> Union[None, DataFrame, Delayed]:
        return super().create(read=read)

    def _df(self) -> DataFrame:
        raise NotImplementedError

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed, DataFrame]:
        return self.checkpoint(read=self.read if read is Unset else read)

    def _read(self) -> DataFrame:
        if self.dask:
            return dd.read_parquet(self.url)
        else:
            return pd.read_parquet(self.url)

    @cached_property
    def df(self) -> DataFrame:
        if self.read is None:
            raise NotImplementedError(f"{self.url}: can't load df, self.read is None")
        return self.create()

    @property
    def checkpoint_kwargs(self):
        return dict()

    def checkpoint(self, read: Union[None, Read] = Unset) -> Union[None, Delayed, DataFrame]:
        url = self.url
        parent = dirname(url)
        read = self.read if read is Unset else read
        rmdir = False
        if not exists(parent):
            self.fs.mkdirs(parent)
            rmdir = True
        try:
            if self.dask:
                df = checkpoint_dd(self._df(), url=url, read=read, **self.checkpoint_kwargs)
            else:
                df = checkpoint_df(self._df(), url=url, read=read, **self.checkpoint_kwargs)
            rmdir = False
            return df
        finally:
            if rmdir:
                stderr(f"Removing directory {parent} after failed write")
                self.fs.delete(parent)  # TODO: remove all directory levels that were created
