from dataclasses import dataclass
from functools import wraps
from typing import Union

import dask.dataframe as dd
import pandas as pd
import utz
from pandas import Series
from utz import decos

from ctbk import Monthy
from ctbk.has_root_cli import HasRootCLI
from ctbk.month_table import MonthTable
from ctbk.normalized import NormalizedMonth, NormalizedMonths
from ctbk.tasks import MonthTables
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.util.keys import Keys
from ctbk.util.ym import dates

DIR = f'{BKT}/aggregated'


@dataclass
class AggKeys(Keys):
    year: bool = False
    month: bool = False
    weekday: bool = False
    hour: bool = False
    region: bool = False
    gender: bool = False
    user_type: bool = False
    rideable_type: bool = False
    start_station: bool = False
    end_station: bool = False

    KEYS = {
        'year': 'y',
        'month': 'm',
        'weekday': 'w',
        'hour': 'h',
        'region': 'r',
        'gender': 'g',
        'user_type': 't',
        'rideable_type': 'b',
        'start_station': 's',
        'end_station': 'e',
    }


@dataclass
class SumKeys(Keys):
    count: bool = True
    duration: bool = False

    KEYS = {
        'count': 'c',
        'duration': 'd',
    }


class AggregatedMonth(MonthTable):
    DIR = DIR
    NAMES = [ 'aggregated', 'agg', ]

    def __init__(
            self,
            ym: Monthy,
            agg_keys: Union[str, AggKeys, dict],
            sum_keys: Union[str, SumKeys, dict],
            **kwargs
    ):
        self.agg_keys = AggKeys.load(agg_keys)
        self.sum_keys = SumKeys.load(sum_keys)
        super().__init__(ym, **kwargs)

    @property
    def url(self):
        return f'{self.dir}/{self.agg_keys.label}_{self.sum_keys.label}_{self.ym}.parquet'

    def _df(self) -> DataFrame:
        src = NormalizedMonth(self.ym, **self.kwargs)
        df = src.df
        agg_keys = dict(self.agg_keys)
        sum_keys = dict(self.sum_keys)
        group_keys = []
        if agg_keys.get('r'):
            df = df.rename(columns={'Start Region': 'Region'})  # assign rides to the region they originated in
            group_keys.append('Region')
        if agg_keys.get('y'):
            if 'Start Year' not in df:
                df['Start Year'] = df['Start Time'].dt.year
            group_keys.append('Start Year')
        if agg_keys.get('m'):
            if 'Start Month' not in df:
                df['Start Month'] = df['Start Time'].dt.month
            group_keys.append('Start Month')
        if agg_keys.get('d'):
            if 'Start Day' not in df:
                df['Start Day'] = df['Start Time'].dt.day
            group_keys.append('Start Day')
        if agg_keys.get('w'):
            if 'Start Weekday' not in df:
                df['Start Weekday'] = df['Start Time'].dt.weekday
            group_keys.append('Start Weekday')
        if agg_keys.get('h'):
            if 'Start Hour' not in df:
                df['Start Hour'] = df['Start Time'].dt.hour
            group_keys.append('Start Hour')
        if agg_keys.get('g'):
            group_keys.append('Gender')
        if agg_keys.get('t'):
            group_keys.append('User Type')
        if agg_keys.get('b'):
            group_keys.append('Rideable Type')
        if agg_keys.get('s'):
            group_keys.append('Start Station ID')
        if agg_keys.get('e'):
            group_keys.append('End Station ID')
        if agg_keys.get('n'):
            group_keys.append('Start Station Name')
        if agg_keys.get('N'):
            group_keys.append('End Station Name')
        if agg_keys.get('l'):
            group_keys += ['Start Station Latitude', 'Start Station Longitude']
        if agg_keys.get('L'):
            group_keys += ['End Station Latitude', 'End Station Longitude']

        select_keys = []
        if sum_keys.get('c'):
            if 'Count' not in df:
                df['Count'] = 1
            select_keys.append('Count')
        if sum_keys.get('d'):
            if 'Duration' not in df:
                df['Duration'] = (df['Stop Time'] - df['Start Time']).dt.seconds
            select_keys.append('Duration')

        grouped = df.groupby(group_keys)
        counts = (
            grouped
            [select_keys]
            .sum()
            .reset_index()
        )
        return counts


class AggregatedMonths(HasRootCLI, MonthTables):
    DIR = DIR
    CHILD_CLS = AggregatedMonth

    def __init__(
            self,
            agg_keys: AggKeys,
            sum_keys: SumKeys,
            sort_agg_keys=False,
            start: Monthy = None,
            end: Monthy = None,
            **kwargs
    ):
        src = self.src = NormalizedMonths(start=start, end=end, **kwargs)
        self.agg_keys = agg_keys
        self.sum_keys = sum_keys
        self.sort_agg_keys = sort_agg_keys
        super().__init__(start=src.start, end=src.end, **kwargs)

    def month(self, ym: Monthy) -> AggregatedMonth:
        return AggregatedMonth(ym, agg_keys=self.agg_keys, sum_keys=self.sum_keys, **self.kwargs)


def command(fn):
    @decos(*AggKeys.opts(), *SumKeys.opts())
    @wraps(fn)
    def _fn(ctx, *args, **kwargs):
        agg_keys = AggKeys(**utz.args(AggKeys, kwargs))
        sum_keys = SumKeys(**utz.args(SumKeys, kwargs))
        fn(*args, ctx=ctx, agg_keys=agg_keys, sum_keys=sum_keys, **utz.args(fn, kwargs))
    return _fn


AggregatedMonths.cli(
    help=f"Aggregate normalized ride entries by various columns, summing ride counts or durations. Writes to <root>/{DIR}/KEYS_YYYYMM.parquet.",
    decos=[dates],
    cmd_decos=[command],
)
