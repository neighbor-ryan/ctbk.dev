import dask.dataframe as dd
import pandas as pd

from ctbk import Monthy
from ctbk.month_data import HasRoot, MonthData
from ctbk.util import cached_property, YM
from ctbk.util.df import DataFrame


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
