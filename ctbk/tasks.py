from abc import ABC
from dask import delayed
from typing import Union
from utz import cached_property, Unset

from ctbk.has_root import HasRoot
from ctbk.table import Table
from ctbk.task import Task
from ctbk.util import YM, Monthy
from ctbk.util.df import DataFrame
from ctbk.util.read import Read


class Tasks(HasRoot, ABC):
    @cached_property
    def children(self):
        raise NotImplementedError

    def create(self, read: Union[None, Read] = Unset):
        children = self.children
        creates = [ child.create(read=read) for child in children ]
        if self.dask:
            return delayed(lambda x: x)(creates)


class MonthTasks(Tasks, ABC):
    def __init__(self, start: Monthy, end: Monthy, **kwargs):
        self.start: YM = YM(start)
        self.end: YM = YM(end)
        super().__init__(**kwargs)

    def month(self, ym: Monthy) -> Task:
        raise NotImplementedError

    @cached_property
    def children(self) -> list[Task]:
        return [
            self.month(ym)
            for ym in self.start.until(self.end)
        ]


class MonthTables(MonthTasks, ABC):
    def month(self, ym: Monthy) -> Table:
        raise NotImplementedError

    @cached_property
    def children(self) -> list[Table]:
        # TODO: is this performing computations serially in Dask mode (when it should be delayed and later parallelized?)
        return [
            self.month(ym)
            for ym in self.start.until(self.end)
        ]

    def month_df(self, ym: Monthy, add_ym=False) -> DataFrame:
        month = self.month(ym)
        df = month.df
        if add_ym:
            df['ym'] = ym
        return df

    @cached_property
    def dfs(self) -> list[DataFrame]:
        return [
            self.month_df(ym, add_ym=True)
            for ym in self.start.until(self.end)
        ]

    @cached_property
    def df(self):
        return self.concat(self.dfs)
