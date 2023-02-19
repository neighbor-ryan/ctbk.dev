from typing import Union
from utz import Unset

from ctbk import Monthy
from ctbk.has_root import HasRoot
from ctbk.month_data import MonthData, MonthDataDF
from ctbk.util.read import Read
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

    def create(self, read: Union[None, Read] = Unset):
        for month in self.months:
            month.create(read=read)


class MonthsDataDF(MonthsData):
    def month(self, ym: Monthy) -> MonthDataDF:
        return MonthDataDF(ym, **self.kwargs)

    def months(self) -> list[MonthDataDF]:
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
        return self.concat(self.dfs)

    def create(self, read: Union[None, Read] = Unset):
        months = self.months
        creates = [ month.create(read=read) for month in months ]
        if self.dask:
            return delayed(lambda x: x)(creates)
