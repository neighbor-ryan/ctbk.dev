import json
from typing import Union

import pandas as pd
from click import pass_context
from dask.delayed import Delayed
from utz import Unset

from ctbk import Monthy
from ctbk.aggregated import AggregatedMonth, DIR
from ctbk.cli.base import ctbk, dask
from ctbk.month_table import MonthTable
from ctbk.stations.modes import ModesMonthJson
from ctbk.tasks import MonthTables
from ctbk.util.df import DataFrame
from ctbk.util.read import Read
from ctbk.util.ym import dates


class StationPairsJson(MonthTable):
    DIR = DIR
    NAMES = ['station_pairs_json', 'spj']

    @property
    def url(self):
        return f'{self.dir}/{self.ym}/se_c.json'

    def _df(self) -> DataFrame:
        mmj = ModesMonthJson(self.ym, **self.kwargs)
        id2idx = mmj.id2idx

        se_am = AggregatedMonth(self.ym, 'se', 'c', **self.kwargs)
        se = se_am.df

        se_ids = (
            se
            .rename(columns={
                'Start Station ID': 'sid',
                'End Station ID': 'eid',
                'Count': 'count',
            })
            .merge(id2idx.rename('sidx').to_frame(), left_on='sid', right_index=True, how='left')
            .merge(id2idx.rename('eidx').to_frame(), left_on='eid', right_index=True, how='left')
            [['sidx', 'eidx', 'count']]
        )
        return se_ids

    @property
    def checkpoint_kwargs(self):
        return dict(
            fmt='json',
            read_kwargs=self._read,
            write_kwargs=self._write,
        )

    def checkpoint(self, read: Union[None, Read] = Unset) -> Union[None, Delayed, DataFrame]:
        return super().checkpoint(read)

    def _write(self, df):
        se_ids_obj = self.df_to_json(df)
        with self.fd('w') as f:
            json.dump(se_ids_obj, f, separators=(',', ':'))

    def _read(self) -> DataFrame:
        with self.fd('r') as f:
            se_ids_obj = json.load(f)
        return self.json_to_df(se_ids_obj)

    @staticmethod
    def df_to_json(se_ids):
        return (
            se_ids
            .groupby('sidx')
            .apply(lambda df: df.set_index('eidx')['count'].to_dict())
            .to_dict()
        )

    @staticmethod
    def json_to_df(se_ids_obj):
        return pd.DataFrame([
            dict(sidx=sidx, eidx=eidx, count=count)
            for sidx, eidxs in se_ids_obj.items()
            for eidx, count in eidxs.items()
        ])


class StationPairsJsons(MonthTables):
    DIR = DIR

    def month(self, ym: Monthy) -> StationPairsJson:
        return StationPairsJson(ym, **self.kwargs)


@ctbk.group(help=f"Write station-pair ride_counts keyed by StationModes' JSON indices. Writes to <root>/{DIR}/YYYYMM/se_c.json.")
@pass_context
@dates
def station_pair_jsons(ctx, start, end):
    ctx.obj.start = start
    ctx.obj.end = end


@station_pair_jsons.command()
@pass_context
def urls(ctx):
    o = ctx.obj
    station_pair_jsons = StationPairsJsons(**o)
    months = station_pair_jsons.children
    for month in months:
        print(month.url)


@station_pair_jsons.command()
@pass_context
@dask
def create(ctx, dask):
    o = ctx.obj
    station_pair_jsons = StationPairsJsons(dask=dask, **o)
    created = station_pair_jsons.create(read=None)
    if dask:
        created.compute()
