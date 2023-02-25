from dataclasses import dataclass
from functools import wraps
from typing import Union

import dask.dataframe as dd
import pandas as pd
from click import option, pass_context, argument
from ctbk import Monthy
from ctbk.cli.base import ctbk, dask
from ctbk.month_table import MonthTable
from ctbk.normalized import NormalizedMonth, NormalizedMonths
from ctbk.tasks import MonthTables
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.util.keys import Keys
from ctbk.util.ym import dates
from pandas import Series
import utz
from utz import decos, process

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
        if agg_keys.get('y') and agg_keys.get('m'):
            meta_kwargs = dict(meta=Series(name='Month', dtype=str)) if isinstance(counts, dd.DataFrame) else dict()
            counts['Month'] = counts.apply(
                lambda r: pd.to_datetime(
                    '%d-%02d' % (int(r['Start Year']), int(r['Start Month']))
                ),
                axis=1,
                **meta_kwargs,
            )
        return counts


class AggregatedMonths(MonthTables):
    DIR = DIR

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


GROUP_KEY_ARGS = [
    # Features to group by
    option('-y/-Y', '--year/--no-year'),
    option('-m/-M', '--month/--no-month'),
    option('-w/-W', '--weekday/--no-weekday'),
    option('-h/-H', '--hour/--no-hour'),
    option('-r/-R', '--region/--no-region'),
    option('-g/-G', '--gender/--no-gender'),
    option('-t/-T', '--user-type/--no-user-type'),
    option('-b/-B', '--rideable-type/--no-rideable-type'),
    option('-s/-S', '--start-station/--no-start-station'),
    option('-e/-E', '--end-station/--no-end-station'),
    # Features to aggregate
    option('-c/-C', '--count/--no-count', default=True),
    option('-d/-D', '--duration/--no-duration'),
]


def agg_sum_cmd(fn):
    @aggregated.command()
    @pass_context
    @decos(GROUP_KEY_ARGS)
    @wraps(fn)
    def _fn(ctx, *args, **kwargs):
        o = ctx.obj
        agg_keys = AggKeys(**utz.args(AggKeys, kwargs))
        sum_keys = SumKeys(**utz.args(SumKeys, kwargs))
        fn(*args, o=o, agg_keys=agg_keys, sum_keys=sum_keys, **spec_args(fn, kwargs))
    return _fn


@ctbk.group(help=f"Aggregate normalized ride entries by various columns, summing ride counts or durations. Writes to <root>/{DIR}/KEYS_YYYYMM.parquet.")
@pass_context
@dates
def aggregated(ctx, start, end):
    ctx.obj.start = start
    ctx.obj.end = end


@agg_sum_cmd
def urls(o, agg_keys, sum_keys):
    aggregated = AggregatedMonths(
        agg_keys=agg_keys,
        sum_keys=sum_keys,
        **o,
    )
    months = aggregated.children
    for month in months:
        print(month.url)


@dask
@agg_sum_cmd
def create(o, agg_keys, sum_keys, dask=True):
    aggregated = AggregatedMonths(
        agg_keys=agg_keys,
        sum_keys=sum_keys,
        **o,
        dask=dask,
    )
    created = aggregated.create(read=None)
    if dask:
        created.compute()


@agg_sum_cmd
@option('-O', '--no-open', is_flag=True)
@argument('filename')
def dag(o, agg_keys, sum_keys, no_open, filename):
    aggregated = AggregatedMonths(
        agg_keys=agg_keys,
        sum_keys=sum_keys,
        **o,
        dask=True,
    )
    df = aggregated.df
    df.visualize(filename)
    if not no_open:
        process.run('open', filename)
