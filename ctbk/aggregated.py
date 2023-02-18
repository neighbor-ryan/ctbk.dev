from dataclasses import dataclass

import dask.dataframe as dd
import pandas as pd
from click import option, pass_context, argument
from pandas import Series
from utz import process

from ctbk import Monthy
from ctbk.cli.base import ctbk
from ctbk.month_data import MonthData, MonthsData
from ctbk.normalized import NormalizedMonth, NormalizedMonths
from ctbk.util.constants import BKT
from ctbk.util.convert import run, args, decos
from ctbk.util.df import DataFrame

DIR = f'{BKT}/aggregated'


class Keys:
    KEYS = None

    def __iter__(self):
        for name, ch in self.KEYS.items():
            v = getattr(self, name)
            if v:
                yield ch, v

    @property
    def label(self):
        return ''.join(dict(self).keys())


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


class AggregatedMonth(MonthData):
    DIR = DIR
    NAMES = [ 'aggregated', 'agg', ]

    def __init__(
            self,
            *args,
            agg_keys: AggKeys,
            sum_keys: SumKeys,
            **kwargs
    ):
        self.agg_keys = agg_keys
        self.sum_keys = sum_keys
        super().__init__(*args, **kwargs)

    @property
    def url(self):
        return f'{self.dir}/{self.agg_keys.label}_{self.sum_keys.label}_{self.ym}.pqt'

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


class AggregatedMonths(MonthsData):
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

    def month(self, ym: Monthy) -> MonthData:
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
    option('-c/-C', '--counts/--no-counts', default=True),
    option('-d/-D', '--durations/--no-durations'),
]


@ctbk.group()
def aggregated():
    pass


@aggregated.command()
@pass_context
@decos(GROUP_KEY_ARGS)
def urls(ctx, **kwargs):
    o = ctx.obj
    agg_keys = AggKeys(**args(AggKeys, kwargs))
    sum_keys = SumKeys(**args(SumKeys, kwargs))
    aggregated = AggregatedMonths(
        agg_keys=agg_keys,
        sum_keys=sum_keys,
        **o,
    )
    months = aggregated.months
    for month in months:
        print(month.url)


@aggregated.command()
@pass_context
@decos(GROUP_KEY_ARGS)
@option('--dask', is_flag=True)
def create(ctx, dask, **kwargs):
    o = ctx.obj
    agg_keys = AggKeys(**args(AggKeys, kwargs))
    sum_keys = SumKeys(**args(SumKeys, kwargs))
    aggregated = AggregatedMonths(
        agg_keys=agg_keys,
        sum_keys=sum_keys,
        **o,
        dask=dask,
    )
    aggregated.create()
    months = aggregated.months
    for month in months:
        print(month.url)


@aggregated.command()
@pass_context
@decos(GROUP_KEY_ARGS)
@option('-O', '--no-open', is_flag=True)
@argument('filename')
def dag(ctx, no_open, filename, **kwargs):
    o = ctx.obj
    agg_keys = AggKeys(**args(AggKeys, kwargs))
    sum_keys = SumKeys(**args(SumKeys, kwargs))
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
