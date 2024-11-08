from abc import ABC

from utz.ym import Monthy

from ctbk.table import Table
from ctbk.task import Task


class MonthTask(Task, ABC):
    def __init__(self, ym: Monthy, **kwargs):
        self.ym = ym
        super().__init__(**kwargs)

    @property
    def url(self):
        return f'{self.dir}/{self.ym}.parquet'


class MonthTable(MonthTask, Table, ABC):
    pass
