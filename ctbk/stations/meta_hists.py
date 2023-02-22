import pandas as pd
from click import pass_context

from ctbk import Monthy
from ctbk.cli.base import ctbk, dask
from ctbk.month_table import MonthTable
from ctbk.normalized import NormalizedMonth, NormalizedMonths
from ctbk.tasks import MonthTables
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.util.ym import dates

DIR = f'{BKT}/stations/llname_hists'


class StationMetaHist(MonthTable):
    DIR = DIR
    NAMES = [ 'station_meta_hist', 'smh', ]

    def _df(self) -> DataFrame:
        src = NormalizedMonth(self.ym, **self.kwargs)
        df = src.df
        columns = {
            'Start Station ID': 'Station ID',
            'Start Station Name': 'Station Name',
            'Start Station Latitude': 'Latitude',
            'Start Station Longitude': 'Longitude',
        }
        starts = df[list(columns.keys())].rename(columns=columns)
        starts['Start'] = True

        columns = {
            'End Station ID': 'Station ID',
            'End Station Name': 'Station Name',
            'End Station Latitude': 'Latitude',
            'End Station Longitude': 'Longitude',
        }
        ends = df[list(columns.keys())].rename(columns=columns)
        ends['Start'] = False

        station_entries = self.concat([starts, ends])
        stations_meta_hist = (
            station_entries
            .groupby(['Station ID', 'Station Name', 'Latitude', 'Longitude'])
            .size()
            .rename('count')
            .reset_index()
            .sort_values(['Station ID', 'Station Name', 'Latitude', 'Longitude'])
        )
        return stations_meta_hist


class StationMetaHists(MonthTables):
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, **kwargs):
        src = self.src = NormalizedMonths(start=start, end=end, **kwargs)
        super().__init__(start=src.start, end=src.end, **kwargs)

    def month(self, ym: Monthy) -> StationMetaHist:
        return StationMetaHist(ym, **self.kwargs)


@ctbk.group()
@pass_context
@dates
def station_meta_hists(ctx, start, end):
    ctx.obj.start = start
    ctx.obj.end = end


@station_meta_hists.command()
@pass_context
def urls(ctx):
    o = ctx.obj
    station_meta_hists = StationMetaHists(**o)
    months = station_meta_hists.children
    for month in months:
        print(month.url)


@station_meta_hists.command()
@pass_context
@dask
def create(ctx, dask):
    o = ctx.obj
    station_meta_hists = StationMetaHists(dask=dask, **o)
    created = station_meta_hists.create(read=None)
    if dask:
        created.compute()
