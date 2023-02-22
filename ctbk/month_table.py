from ctbk import Monthy
from ctbk.table import Table


class MonthTable(Table):
    def __init__(self, ym: Monthy, **kwargs):
        self.ym = ym
        super().__init__(**kwargs)

    @property
    def url(self):
        return f'{self.dir}/{self.ym}.parquet'
