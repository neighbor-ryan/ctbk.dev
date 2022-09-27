#!/usr/bin/env python

from utz import *

from ctbk import Month, MonthsDataset, cached_property, NormalizedMonths, Monthy
from ctbk.monthly import BKT, PARQUET_EXTENSION


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


def mode_sketch(df, groupby, thresh=0.5):
    idx_name = df.index.name
    if not idx_name:
        raise RuntimeError('Index needs a name')
    if isinstance(groupby, str):
        groupby = [groupby]
    row_groups = df.reset_index().groupby([idx_name] + groupby)
    row_hist = row_groups.size().rename('count').reset_index()
    counts = row_hist.groupby(idx_name)['count'].apply(lambda s: list(reversed(sorted(s.values))))
    row_sketches = counts.apply(row_sketch).apply(Series)
    below_thresh = row_sketches[row_sketches.mode_pct < thresh]
    if not below_thresh.empty:
        stderr.write(f'{len(below_thresh)} index entries with mode_pct < {thresh}:\n{below_thresh}\n')
    annotated = (
        row_hist
            .sort_values([idx_name, 'count'], ascending=False)
            .drop_duplicates(subset=idx_name)
            .set_index(idx_name)
    )
    annotated = sxs(annotated, row_sketches).drop(columns=['count']).sort_values('mode_pct')
    return annotated


class StationMetaHists(MonthsDataset):
    ROOT = f'{BKT}/stations/llname_hists'
    SRC_CLS = NormalizedMonths
    RGX = '(?P<month>\\d{6})\\' + PARQUET_EXTENSION

    def task_df(self, start: Monthy = None, end: Monthy = None):
        df = self.src.outputs(start=start, end=end)
        df['src'] = f'{self.src.root}/' + df.name
        df['dst'] = f'{self.root}/' + df.name
        df = df[['month', 'src', 'dst']]
        return df

    def compute(self, src_df):
        columns = {
            'Start Station ID': 'Station ID',
            'Start Station Name': 'Station Name',
            'Start Station Latitude': 'Latitude',
            'Start Station Longitude': 'Longitude',
        }
        starts = src_df[columns.keys()].rename(columns=columns)
        starts['Start'] = True

        columns = {
            'End Station ID': 'Station ID',
            'End Station Name': 'Station Name',
            'End Station Latitude': 'Latitude',
            'End Station Longitude': 'Longitude',
        }
        ends = src_df[columns.keys()].rename(columns=columns)
        ends['Start'] = False

        station_entries = pd.concat([starts, ends])
        stations_hist = (
            station_entries
                .groupby(['Station ID', 'Station Name', 'Latitude', 'Longitude'])
                .size()
                .rename('count')
                .reset_index()
        )
        return stations_hist


if __name__ == '__main__':
    StationMetaHists.cli()
