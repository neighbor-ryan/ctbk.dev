import json
from os.path import splitext, dirname

import pandas as pd
from utz import sxs

from ctbk import MonthsDataset, GroupCounts, Monthy
from ctbk.monthly import BKT, SQLITE_EXTENSION


class StartEndDB(MonthsDataset):
    ROOT = f'{BKT}/normalized'
    SRC_CLS = GroupCounts

    @classmethod
    def main(cls, **kwargs):
        # Set {s,e} in SRC_CLS init
        return super().main(src_kwargs=dict(start_station=True, end_station=True), **kwargs)

    @classmethod
    def build_task(cls, r):
        out_dir = f'{dirname(splitext(r.src)[0])}/{r.month}'
        dst = f'{out_dir}/se_c.json'
        return dict(dst=dst, se_c_path=dst, s_c_path=f'{out_dir}/s_c.json', idx2id_path=f'{out_dir}/idx2id.json')

    def task_df(self, start: Monthy = None, end: Monthy = None):
        start, end = self.month_range(start, end)
        # List intermediate ("reduced") DFs from GroupCounts src
        df = pd.DataFrame([ dict(src=self.src.reduced_df_path(month), month=month) for month in start.until(end) ])
        # Convert them to SQLite
        df = sxs(df, df.apply(self.build_task, axis=1).apply(pd.Series))
        return df

    def compute(self, src_df, idx2id_path, se_c_fdw, s_c_fdw):
        idx2id = pd.DataFrame(list(sorted(set(pd.concat([ src_df['Start Station ID'], src_df['End Station ID'] ])))), columns=['ID'])
        idx2id.index.name = 'idx'
        id2idx = idx2id.reset_index().set_index('ID')
        idx2id.ID.to_json(idx2id_path)

        # idx2id.to_sql('stations', con)
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
        json.dump(stations, se_c_fdw, separators=(',', ':'), )
        s_c = {
            s: sum(vs.values())
            for s, vs in stations.items()
        }
        json.dump(s_c, s_c_fdw, separators=(',', ':'), )
        #counts.to_sql('counts', con)


if __name__ == '__main__':
    StartEndDB.cli()
