#!/usr/bin/env python
from typing import Union

import pandas as pd
from numpy import nan
from pandas import Series

from ctbk.month_agg_table import MonthAggTable
from ctbk.util import stderr
from ctbk.util.df import DataFrame, meta, apply, sxs


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


class StationModes(MonthAggTable):
    # Used by [`index.tsx`](www/pages/index.tsx) plot
    SRC = 'ctbk/stations/llname_hists'
    OUT = 'www/public/assets/ids.json'

    def map(self, df: DataFrame) -> DataFrame:
        return transform(df)

    def write_df(self, df: pd.DataFrame):
        df.to_json(self.out, 'index')


if __name__ == '__main__':
    StationModes.main()
