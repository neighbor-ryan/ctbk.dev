import json
import pandas as pd
from click import pass_context
from dask.delayed import Delayed, delayed
from typing import Union
from utz import Unset

from ctbk import Monthy
from ctbk.cli.base import ctbk, dask
from ctbk.aggregated import AggregatedMonth, SumKeys, AggKeys, DIR
from ctbk.month_table import MonthTask
from ctbk.stations.meta_hists import StationMetaHist
from ctbk.stations.modes import transform
from ctbk.tasks import MonthTables
from ctbk.util import stderr
from ctbk.util.read import Read
from ctbk.util.write import Always, Never
from ctbk.util.ym import dates


class StationPairsJson(MonthTask):
    DIR = DIR
    NAMES = ['station_pairs_json', 'spj']

    @property
    def stations_url(self):
        return f'{self.url}/stations.json'

    @property
    def idx2id_url(self):
        return f'{self.url}/idx2id.json'

    @property
    def se_c_url(self):
        return f'{self.url}/se_c.json'

    @property
    def s_c_url(self):
        return f'{self.url}/s_c.json'

    @property
    def urls(self):
        return [
            self.idx2id_url,
            self.stations_url,
            self.se_c_url,
            self.s_c_url,
        ]

    @property
    def dirname(self):
        return self.url

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        read = self.read if read is Unset else read
        if read is not None:
            raise ValueError("Can only read=None")

        ymse_c = AggregatedMonth(
            ym=self.ym,
            agg_keys=AggKeys(start_station=True, end_station=True),
            sum_keys=SumKeys(count=True),
            **self.kwargs,
        )
        src_df = ymse_c.df
        smh = StationMetaHist(ym=self.ym, **self.kwargs)

        def run(src_df: pd.DataFrame):
            with self.mkdirs():
                stations = transform(smh.df)
                stations.to_json(self.stations_url, 'index')

                idx2id = pd.Series(
                    list(sorted(
                        self.concat([
                            src_df['Start Station ID'],
                            src_df['End Station ID'],
                        ])
                        .unique()
                    )),
                    name='ID',
                )
                idx2id.index.name = 'idx'
                id2idx = idx2id.reset_index().set_index('ID')
                idx2id.to_json(self.idx2id_url)

                counts = (
                    src_df
                    .merge(id2idx.idx.rename('Start Station Idx'), left_on='Start Station ID', right_index=True, how='left')
                    .merge(id2idx.idx.rename('End Station Idx'), left_on='End Station ID', right_index=True, how='left')
                    .set_index([ 'Start Station Idx', ])
                    [['End Station Idx', 'Count']]
                )
                cj = counts.rename(columns={'End Station Idx': 'e', 'Count': 'c'})
                cj.index.name = 's'
                cj = cj.set_index('e', append=True)
                stations = {
                    s: cj.loc[s].c.to_dict()
                    for s in cj.index.levels[0]
                }
                with self.fs.open(self.se_c_url, 'w') as f:
                    json.dump(stations, f, separators=(',', ':'), )

                s_c = {
                    s: sum(vs.values())
                    for s, vs in stations.items()
                }
                with self.fs.open(self.s_c_url, 'w') as f:
                    json.dump(s_c, f, separators=(',', ':'), )

        if self.dask:
            [ddf] = src_df.to_delayed()
            return delayed(run)(ddf)
        else:
            return run(src_df)

    @property
    def url(self):
        return f'{self.dir}/{self.ym}'

    def exists(self):
        exists_obj = { url: self.fs.exists(url) for url in self.urls }
        exists = list(exists_obj.values())
        if all(exists):
            return True
        if not any(exists):
            return False
        msg = f'{self.url} partially exists: {exists_obj}'
        if self.write is Always or self.write is Never:
            stderr(msg)
            return True
        else:
            raise ValueError(msg)


class StationPairsJsons(MonthTables):
    DIR = DIR

    def month(self, ym: Monthy) -> StationPairsJson:
        return StationPairsJson(ym, **self.kwargs)


@ctbk.group()
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
        for url in month.urls:
            print(url)


@station_pair_jsons.command()
@pass_context
@dask
def create(ctx, dask):
    o = ctx.obj
    station_pair_jsons = StationPairsJsons(dask=dask, **o)
    created = station_pair_jsons.create(read=None)
    if dask:
        created.compute()
