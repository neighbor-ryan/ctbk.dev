#!/usr/bin/env python
from typing import Union

import numpy as np
import pandas as pd
from click import pass_context
from numpy import nan
from pandas import Series

from ctbk import Monthy, cached_property
from ctbk.aggregated import AggregatedMonth, DIR
from ctbk.cli.base import ctbk, dask
from ctbk.month_table import MonthTable
from ctbk.stations.meta_hists import StationMetaHist
from ctbk.tasks import MonthTables
from ctbk.util import stderr
from ctbk.util.df import DataFrame, apply, sxs, meta
from ctbk.util.ym import dates


def row_sketch(a):
    restsum = sum(a[1:])
    total = a[0] + restsum
    num = len(a)
    return {
        'mode_count': a[0],
        'second': a[1] if num > 1 else nan,
        'restsum': restsum,
        'total': total,
        'counts': a,
        'first/second': a[0] / a[1] if num > 1 else nan,
        'mode_pct': a[0] / total,
        'num': num,
    }


def get_row_hist(df: DataFrame, groupby: Union[str, list[str]], sum_key: str = 'count') -> DataFrame:
    idx_name = df.index.name
    if not idx_name:
        raise RuntimeError('Index needs a name')
    if isinstance(groupby, str):
        groupby = [groupby]
    df = df[groupby]
    row_groups = df.reset_index().groupby([idx_name] + groupby)
    if sum_key in df:
        row_hist = row_groups[sum_key].sum()
    else:
        row_hist = row_groups.size().rename(sum_key)
    row_hist = row_hist.reset_index()
    return row_hist


def mode_sketch(row_hist: DataFrame, thresh: float = 0.5, sum_key: str = 'count', idx_name: str = 'Station ID') -> DataFrame:
    counts = row_hist.groupby(idx_name)[sum_key].apply(lambda s: list(reversed(sorted(s.values))))
    def annotate(counts: pd.DataFrame) -> pd.DataFrame:
        row_sketches = (
            counts
            .apply(row_sketch)
            .apply(Series)
        )
        below_thresh = row_sketches[row_sketches.mode_pct < thresh]
        if not below_thresh.empty:
            stderr(f'{len(below_thresh)} index entries with mode_pct < {thresh}:\n{below_thresh}')
        annotated = (
            row_hist
            .sort_values([idx_name, sum_key], ascending=False)
            .drop_duplicates(subset=idx_name)
            .set_index(idx_name)
        )
        annotated = sxs(annotated, row_sketches).drop(columns=[sum_key]).sort_values('mode_pct')
        return annotated

    return apply(
        annotate,
        meta={
            'Station Name': str,
            'mode_count': int,
            'second': float,
            'restsum': int,
            'total': int,
            'counts': object,
            'first/second': float,
            'mode_pct': float,
            'num': int,
        }
    )(counts)


def transform(df: DataFrame) -> DataFrame:
    df = df.set_index('Station ID')
    station_name_row_hist = get_row_hist(df, groupby='Station Name')
    annotated_station_names = mode_sketch(station_name_row_hist)
    station_ll_row_hist = get_row_hist(df, groupby=[ 'Latitude', 'Longitude' ])
    annotated_stations = mode_sketch(station_ll_row_hist)
    joined = sxs(
        annotated_station_names['Station Name'],
        annotated_stations[[ 'Latitude', 'Longitude', ]],
    ).rename(columns={
        'Station Name': 'name',
        'Latitude': 'lat',
        'Longitude': 'lng',
    })
    return joined


class ModesMonthJson(MonthTable):
    NAMES = [ 'modes_month_json', 'mmj', 'station_modes', 'sm', ]
    DIR = DIR

    @property
    def url(self):
        return f'{self.dir}/{self.ym}/stations.json'

    @staticmethod
    def get_mode_sketch(df):
        n = len(df)
        total = df['count'].sum()
        df['total'] = total
        df['n'] = n
        df['frac'] = df['count'] / total
        df['ratio'] = df['count'] / df['count'].shift(-1)
        return df

    @classmethod
    def compute_mode(cls, df):
        n = len(df)
        if n > 1:
            df = (
                df
                [['name', 'count']]
                .sort_values('count', ascending=False)
            )
            mode_sketch = cls.get_mode_sketch(df)
            stderr(f'{mode_sketch.head()}')
        return df['name'].iloc[0]

    @staticmethod
    def ll_mean(df):
        return Series({
            'lat': np.average(df.lat, weights=df['count']),
            'lng': np.average(df.lng, weights=df['count']),
        })

    @cached_property
    def idx2id(self):
        stations = self.df.reset_index()
        if self.dask:
            stations = stations.rename(columns={'index': 'id'})
        stations.index.name = 'idx'
        return stations.id

    @cached_property
    def id2idx(self):
        ids = self.idx2id.reset_index()
        if self.dask:
            ids = ids.rename(columns={ 'index': 'idx' })
        id2idx = ids.set_index('id').idx
        return id2idx

    def _df(self) -> DataFrame:
        dask = self.dask
        smh_in = StationMetaHist(self.ym, 'in', **self.kwargs)
        df_in = smh_in.df.set_index('id')
        name_groups = df_in.groupby('id')
        names = name_groups.apply(self.compute_mode, **meta('name', dask)).rename('name')

        smh_il = StationMetaHist(self.ym, 'il', **self.kwargs)
        df_il = smh_il.df.set_index('id')
        ll_groups = df_il.groupby('id')
        lls = ll_groups.apply(self.ll_mean, **meta({ 'lat': float, 'lng': float }, dask))
        stations = sxs(names, lls).reset_index()
        stations.index.name = 'idx'

        sc_am = AggregatedMonth(self.ym, 's', 'c', **self.kwargs)
        sc_df = sc_am.df
        starts = (
            sc_df
            .rename(columns={
                'Start Station ID': 'id',
                'Count': 'starts',
            })
            .set_index('id')
            .starts
        )
        stations = sxs(names, lls, starts)
        stations['starts'] = stations.starts.fillna(0).astype(int)
        return stations

    def _read(self) -> DataFrame:
        with self.fd('r') as f:
            df = pd.read_json(f, orient='index', convert_axes=False)
        df.index.name = 'id'
        return df

    def _write(self, df):
        with self.fd('w') as f:
            df.to_json(f, orient='index')

    @property
    def checkpoint_kwargs(self):
        return dict(
            write_kwargs=self._write,
            read_kwargs=self._read,
        )


class ModesMonthJsons(MonthTables):
    DIR = DIR

    def month(self, ym: Monthy) -> ModesMonthJson:
        return ModesMonthJson(ym, **self.kwargs)


@ctbk.group()
@pass_context
@dates
def station_modes(ctx, start, end):
    ctx.obj.start = start
    ctx.obj.end = end


@station_modes.command()
@pass_context
def urls(ctx):
    o = ctx.obj
    modes_month_jsons = ModesMonthJsons(**o)
    months = modes_month_jsons.children
    for month in months:
        print(month.url)


@station_modes.command()
@pass_context
@dask
def create(ctx, dask):
    o = ctx.obj
    modes_month_jsons = ModesMonthJsons(dask=dask, **o)
    created = modes_month_jsons.create(read=None)
    if dask:
        created.compute()
