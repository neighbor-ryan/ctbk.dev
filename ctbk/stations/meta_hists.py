from functools import wraps

from click import pass_context, option
from dataclasses import dataclass
from typing import Union

from ctbk import Monthy
from ctbk.cli.base import ctbk, dask
from ctbk.month_table import MonthTable
from ctbk.normalized import NormalizedMonth, NormalizedMonths
from ctbk.tasks import MonthTables
from ctbk.util.constants import BKT
from ctbk.util.convert import spec_args, decos
from ctbk.util.df import DataFrame
from ctbk.util.keys import Keys
from ctbk.util.ym import dates

DIR = f'{BKT}/stations/meta_hists'


@dataclass
class AggKeys(Keys):
    year: bool = False
    month: bool = False
    id: bool = False
    name: bool = False
    lat_lng: bool = False

    KEYS = {
        'year': 'y',
        'month': 'm',
        'id': 'i',
        'name': 'n',
        'lat_lng': 'l',
    }


class StationMetaHist(MonthTable):
    DIR = DIR
    NAMES = [ 'station_meta_hist', 'smh', ]

    def __init__(
            self,
            ym: Monthy,
            agg_keys: Union[str, AggKeys, dict],
            **kwargs
    ):
        self.agg_keys = AggKeys.load(agg_keys)
        super().__init__(ym, **kwargs)

    @property
    def url(self):
        return f'{self.dir}/{self.agg_keys.label}_{self.ym}.parquet'

    def _df(self) -> DataFrame:
        src = NormalizedMonth(self.ym, **self.kwargs)
        df = src.df
        # Assign each ride to its start YM, ignore the end time (except for the "duration" sum_key)
        df = df.rename(columns={
            'Start Year': 'year',
            'Start Month': 'month',
            'Start Time': 'time'
        })
        agg_keys = dict(self.agg_keys)
        group_keys = []
        if agg_keys.get('y'):
            if 'year' not in df:
                df['year'] = df['time'].dt.year
            group_keys.append('year')
        if agg_keys.get('m'):
            if 'month' not in df:
                df['month'] = df['time'].dt.month
            group_keys.append('month')

        # Below can be factored like this; can the above, and similar in `AggregatedMonth`?
        # COL_NAMES = {
        #     'i': { 'Station ID': 'id' },
        #     'n': { 'Station Name': 'name' },
        #     'l': {
        #         'Station Latitude': 'lat',
        #         'Station Longitude': 'lng',
        #     }
        # }
        #
        # col_names = {
        #     prv_col: new_col
        #     for key in dict(agg_keys)
        #     for prv_col, new_col in COL_NAMES[key].items()
        # }

        col_names = {}
        if agg_keys.get('i'):
            col_names['Station ID'] = 'id'
        if agg_keys.get('n'):
            col_names['Station Name'] = 'name'
        if agg_keys.get('l'):
            col_names['Station Latitude'] = 'lat'
            col_names['Station Longitude'] = 'lng'

        # Combine the names and lat/lngs given for each station, from both ride starts and ride ends.
        # Histogram each one in and then combine then below
        def starts_ends_hist(starts: bool):
            prefix = 'Start' if starts else 'End'
            renames = {
                f'{prefix} {cur_col}': new_col
                for cur_col, new_col in col_names.items()
            }
            cur_cols = group_keys + list(renames.keys())
            renamed = df[cur_cols].rename(columns=renames)
            grouped = renamed.groupby(renamed.columns.tolist())
            counts = (
                grouped
                .size()
                .rename('count')
                .reset_index()
            )
            return counts

        starts = starts_ends_hist(starts=True)
        ends = starts_ends_hist(starts=False)

        # Combine the `starts` and `ends` hists
        station_entries = self.concat([starts, ends])
        cols = station_entries.columns.tolist()[:-1]
        stations_meta_hist = (
            station_entries
            .groupby(cols)
            ['count']
            .sum()
            .reset_index()
            .sort_values(cols)
        )
        return stations_meta_hist

    @property
    def checkpoint_kwargs(self):
        return dict(write_kwargs=dict(index=False))


class StationMetaHists(MonthTables):
    DIR = DIR

    def __init__(self, agg_keys: AggKeys, start: Monthy = None, end: Monthy = None, **kwargs):
        src = self.src = NormalizedMonths(start=start, end=end, **kwargs)
        self.agg_keys = agg_keys
        super().__init__(start=src.start, end=src.end, **kwargs)

    def month(self, ym: Monthy) -> StationMetaHist:
        return StationMetaHist(ym, agg_keys=self.agg_keys, **self.kwargs)


GROUP_KEY_ARGS = [
    # Features to group by
    option('-y/-Y', '--year/--no-year'),
    option('-m/-M', '--month/--no-month'),
    option('-i/-I', '--id/--no-id'),
    option('-n/-N', '--name/--no-name'),
    option('-l/-L', '--lat-lng/--no-lat-lng'),
]


# TODO: factor common CLI subcommands
@ctbk.group()
@pass_context
@dates
def station_meta_hists(ctx, start, end):
    ctx.obj.start = start
    ctx.obj.end = end


def agg_cmd(fn):
    @station_meta_hists.command(fn.__name__)
    @pass_context
    @decos(GROUP_KEY_ARGS)
    @wraps(fn)
    def _fn(ctx, *args, **kwargs):
        o = ctx.obj
        agg_keys = AggKeys.load(spec_args(AggKeys, kwargs))
        fn(*args, o=o, agg_keys=agg_keys, **spec_args(fn, kwargs))
    return _fn


@agg_cmd
def urls(o, agg_keys):
    station_meta_hists = StationMetaHists(agg_keys=agg_keys, **o)
    months = station_meta_hists.children
    for month in months:
        print(month.url)


@agg_cmd
@dask
def create(o, agg_keys, dask):
    station_meta_hists = StationMetaHists(agg_keys=agg_keys, dask=dask, **o)
    created = station_meta_hists.create(read=None)
    if dask:
        created.compute()
