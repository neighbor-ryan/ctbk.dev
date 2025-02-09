from abc import ABC

from utz.ym import Monthy, YM

from ctbk.table import Table
from ctbk.tables_dir import TablesDir
from ctbk.task import Task


class MonthTask(Task, ABC):
    def __init__(self, ym: Monthy, **kwargs):
        self.ym = YM(ym)
        Task.__init__(self, **kwargs)


class MonthTable(MonthTask, Table, ABC):
    @property
    def url(self):
        return f'{self.dir}/{self.ym}.parquet'


class MonthDirTables(MonthTask, TablesDir, ABC):
    @property
    def url(self):
        return f'{self.dir}/{self.ym}'
