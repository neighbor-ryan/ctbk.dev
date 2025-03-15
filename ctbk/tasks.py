from abc import ABC
from functools import cache
from typing import Type

from utz import cached_property, Unset
from utz.ym import YM, Monthy

from ctbk.table import Table
from ctbk.tables_dir import Tables, TablesDir
from ctbk.task import Task
from ctbk.util.df import DataFrame
from ctbk.util.read import Read


class Tasks(ABC):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @cached_property
    def children(self):
        raise NotImplementedError

    def create(self):
        children = self.children
        creates = [ child.create() for child in children ]


class MonthsTasks(Tasks, ABC):
    def __init__(self, yms: list[YM], **kwargs):
        self.yms = yms
        super().__init__(**kwargs)

    def month(self, ym: Monthy) -> Task:
        raise NotImplementedError

    @cached_property
    def children(self) -> list[Task]:
        return [
            self.month(ym)
            for ym in self.yms
        ]


class MonthsTables(MonthsTasks, ABC):
    def month(self, ym: Monthy) -> Table:
        raise NotImplementedError

    @cached_property
    def children(self) -> list[Table]:
        return [
            self.month(ym)
            for ym in self.yms
        ]

    def month_df(self, ym: Monthy, add_ym=False) -> DataFrame:
        month = self.month(ym)
        df = month.df()
        if add_ym:
            df['ym'] = ym
        return df

    @cache
    def dfs(self) -> list[DataFrame]:
        return [
            self.month_df(ym, add_ym=True)
            for ym in self.yms
        ]


class MonthsDirTables(MonthsTasks, ABC):
    def month(self, ym: Monthy) -> TablesDir:
        raise NotImplementedError

    @cached_property
    def children(self) -> list[TablesDir]:
        return [
            self.month(ym)
            for ym in self.yms
        ]

    def month_tables(self, ym: Monthy, add_ym=False) -> Tables:
        month = self.month(ym)
        dfs = month.dfs()
        if add_ym:
            dfs = {
                name: df.assign(ym=ym)
                for name, df in dfs.items()
            }
        return dfs
