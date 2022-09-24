from re import fullmatch

from boto3 import client
from inspect import getfullargspec

import click
from functools import cached_property

from urllib.parse import urlparse

import fsspec
from abc import abstractmethod

import pandas as pd
from typing import Literal, Type

from dataclasses import dataclass

from ctbk.util.convert import Result
from ctbk.util.month import Month, MonthSet

GENESIS = Month(2013, 6)


MONTH_DATASET_REGISTRY = {}

def register_month_dataset(root: str):
    def _register(cls):
        MONTH_DATASET_REGISTRY[root] = cls
        cls.root = root
        return cls
    return _register


MONTHS_DATASET_REGISTRY = {}

def register_months_dataset(root: str):
    def _register(cls):
        MONTHS_DATASET_REGISTRY[root] = cls
        cls.root = root
        cls.month_cls = register_month_dataset(root)(cls.month_cls)
        cls.month_cls.months_cls = cls
        return cls
    return _register


@dataclass
class MonthDataset:
    month: Month
    # root: str

    FMT = 'pqt'
    months_cls = None

    @cached_property
    def url(self):
        return f'{self.root}/{self.month}.{self.FMT}'

    @cached_property
    def dt(self):
        return self.month.dt

    @property
    def scheme(self):
        url = self.url
        parsed = urlparse(url)
        return parsed.scheme

    @cached_property
    def fs(self) -> fsspec.filesystem:
        return fsspec.filesystem(self.scheme)

    @property
    def exists(self):
        return self.fs.exists(self.url)

    @abstractmethod
    def compute(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def deps(self):
        raise NotImplementedError

    def __call__(self, error='warn', overwrite=False, **kwargs):
        overwriting = False
        if self.exists:
            if overwrite:
                overwriting = True
            else:
                with self.fs.open(self.url, 'rb') as f:
                    return pd.read_parquet(f)

        fn = self.compute
        fn_spec = getfullargspec(fn)
        fn_kwargs = {}

        deps = self.deps()
        result = self.compute(**kwargs)
        if isinstance(result, pd.DataFrame):
            result.to_parquet(self.url)
        elif isinstance(result, Result):
            raise  # TODO


@dataclass
class MonthsDataset:
    def months(self, start: Month = GENESIS, end: Month = None):
        end = end or Month()
        return [
            self.month_cls(month=month, root=self.root, fmt=self.fmt)
            for month in start.until(end)
        ]

    @cached_property
    def fs(self) -> fsspec.filesystem:
        return fsspec.filesystem(self.scheme)

    @cached_property
    def listdir(self):
        return self.fs.listdir(self.root)

    @cached_property
    def scheme(self):
        url = self.root
        parsed = urlparse(url)
        return parsed.scheme

    def __call__(self, start: Month = GENESIS, end: Month = None):
        return [ month() for month in self.months() ]


# class Csv(MonthDataset):
#     pass


RGX = r'(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
BKT = 'ctbk'


class Csvs(MonthsDataset):
    root = f's3://{BKT}/csvs'


# class NormalizedMonth(MonthDataset):
#     def compute(self, *args, **kwargs):
#         raise  # TODO
#
#     def deps(self):
#         self.months_cls.inputs()
#         entries = self.fs.listdir(Csvs.root)
#         matches = [ fullmatch(RGX, entry.name) for entry in entries ]


class NormalizedMonths(MonthsDataset):
    root = f's3://{BKT}/normalized'

    @cached_property
    def inputs(self):
        csvs = pd.DataFrame(Csvs().listdir)
        csvs = csvs[csvs['name'].str.endswith('.csv')]
        keys = csvs.Key.rename('key')
        d = pd.concat([ keys.str.extract(RGX), keys, ], axis=1)
        d['region'] = d['region'].fillna('NYC')
        d = d.astype({ 'year': int, 'month': int, })
        d['region_url'] = d[['region', 'key']].to_dict('records')
        d['month'] = d[['year', 'month']].apply(lambda r: Month(r['year'], r['month']), axis=1)
        months = d.groupby('month')['region_url'].apply(list).to_dict()
        return MonthSet(months)

    def deps(self, month):
        inputs = {
            f"{input['Region'].lower()}_df": input
            for input in self.inputs[month]
        }
        return inputs

    def compute(self, nyc_df, jc_df=None):


    def __getitem__(self, month):
        month = Month(month)
        return self.inputs[month]


#@register_month_dataset()
class StationsMonth(MonthDataset):
    def compute(self):
        pass


class StationsMonths(MonthsDataset):
    fmt = 'pqt'
    month_cls = StationsMonth

    def deps(self):
        return (NormalizedMonths,)


@click.command(help="")
@click.option('-s', '--src')
@click.option('-d', '--dst')
@click.option('-p', '--parallel/--no-parallel', help='Use joblib to parallelize execution')
@click.option('-f', '--overwrite/--no-overwrite', help='When set, write files even if they already exist')
@click.option('--public/--no-public', help='Give written objects a public ACL')
@click.option('--start', help='Month to process from (in YYYYMM form)')
@click.option('--end', help='Month to process until (in YYYYMM form; exclusive)')
def main(src, dst, parallel, overwrite, public, start, end):
    StationsMonthsCls = register_months_dataset(src)(StationsMonths)
