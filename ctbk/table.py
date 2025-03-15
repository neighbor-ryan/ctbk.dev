from abc import ABC
from functools import cache
from os.path import dirname, exists

import pandas as pd
from pandas import DataFrame
from utz import err

from ctbk.task import Task
from ctbk.util.df import save


class Table(Task[DataFrame], ABC):

    def _df(self) -> DataFrame:
        raise NotImplementedError

    def _create(self) -> DataFrame:
        return self.save()

    def read(self) -> DataFrame:
        return pd.read_parquet(self.url)

    @cache
    def df(self) -> DataFrame:
        return self.create()

    @property
    def save_kwargs(self) -> dict:
        return dict()

    def save(self) -> DataFrame:
        url = self.url
        parent = dirname(url)
        rmdir = False
        if not exists(parent):
            self.fs.mkdirs(parent)
            rmdir = True
        try:
            df = self._df()
            save(df, url=url, **self.save_kwargs)
            rmdir = False
            return df
        finally:
            if rmdir:
                err(f"Removing directory {parent} after failed write")
                self.fs.delete(parent)  # TODO: remove all directory levels that were created
