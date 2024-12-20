from abc import ABC
from functools import cache
from os.path import dirname, exists
from typing import Union, Type

import pandas as pd
from pandas import DataFrame

from ctbk.task import Task
from ctbk.util.df import checkpoint
from ctbk.util.read import Read
from utz import err, Unset


class Table(Task, ABC):
    def create(self, read: Union[None, Read] = Unset) -> DataFrame | None:
        return super().create(read=read)

    def _df(self) -> DataFrame:
        raise NotImplementedError

    def _create(self, read: Union[None, Read] = Unset) -> DataFrame | None:
        return self.checkpoint(read=self.read if read is Unset else read)

    def _read(self) -> DataFrame:
        return pd.read_parquet(self.url)

    @cache
    def df(self) -> DataFrame:
        if self.read is None:
            raise NotImplementedError(f"{self.url}: can't load df, self.read is None")
        return self.create()

    @property
    def checkpoint_kwargs(self):
        return dict()

    def checkpoint(self, read: Read | None | Type[Unset] = Unset) -> DataFrame | None:
        url = self.url
        parent = dirname(url)
        read = self.read if read is Unset else read
        rmdir = False
        if not exists(parent):
            self.fs.mkdirs(parent)
            rmdir = True
        try:
            df = checkpoint(self._df(), url=url, read=read, **self.checkpoint_kwargs)
            rmdir = False
            return df
        finally:
            if rmdir:
                err(f"Removing directory {parent} after failed write")
                self.fs.delete(parent)  # TODO: remove all directory levels that were created
