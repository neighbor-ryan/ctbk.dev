from abc import ABC
from functools import cache
from os.path import dirname, exists
from typing import Union

from ctbk.task import Task
from ctbk.util.df import DataFrame, checkpoint_dd, checkpoint_df
from ctbk.util.read import Read
from dask.delayed import Delayed
from utz import cached_property, err, Unset


class Table(Task, ABC):
    def create(self, read: Union[None, Read] = Unset) -> Union[None, DataFrame, Delayed]:
        return super().create(read=read)

    def _df(self) -> DataFrame:
        raise NotImplementedError

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed, DataFrame]:
        return self.checkpoint(read=self.read if read is Unset else read)

    def _read(self) -> DataFrame:
        return self.dpd.read_parquet(self.url)

    @cache
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
                err(f"Removing directory {parent} after failed write")
                self.fs.delete(parent)  # TODO: remove all directory levels that were created
