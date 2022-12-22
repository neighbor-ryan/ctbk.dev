import pandas as pd

from ctbk import StationModes, MonthsDataset, Monthy, GroupCounts
from ctbk.aggregator import Aggregator
from ctbk.monthly import BKT, SQLITE_EXTENSION
from ctbk.util.convert import WROTE


class StationsDB(Aggregator, MonthsDataset):
    SRC_CLS = StationModes
    ROOT = f'{BKT}/stations'
    EXTENSION = SQLITE_EXTENSION

    def __init__(self, namespace: str = None, root: str = None, src: 'Dataset' = None, **kwargs,):
        self.group_counts = GroupCounts(namespace=namespace, **kwargs,)
        super().__init__(namespace=namespace, root=root, src=src, **kwargs)

    def task_list(self, start: Monthy = None, end: Monthy = None):
        ids = self.src.output(start, end)
        with self.src.fs.open(ids['name'], 'rb') as f:
            ids_df = pd.read_parquet(f)
        grouped_path = self.group_counts.path(start, end)
        with self.group_counts.fs.open(grouped_path, 'rb') as f:
            grouped_df = pd.read_parquet(f)
        start, end = self.month_range(start, end)
        dst = self.path(start, end)
        task = {
            'ids_df': ids_df,
            'grouped_df': grouped_df,
            'dst': dst,
        }
        return [task]

    def compute(self, ids_df, grouped_df, con):
        id_renames = {
            'Station ID': 'id',
            'Station Name': 'name',
            'Latitude': 'lat',
            'Longitude': 'lng',
        }
        ids_df = (
            ids_df
            .reset_index()
            .reset_index(drop=True)
            .rename(columns=id_renames)
        )
        ids_df.index.name = 'idx'
        idx2id = ids_df.id
        id2idx = ids_df.reset_index().set_index('id').idx
        agg_keys = self.agg_keys
        grouped_renames = {
            'Start Station ID': 'start',
            'End Station ID': 'end',
            'Count': 'count',
        }
        grouped_cols = []
        if 'y' in agg_keys:
            grouped_renames['Start Year'] = 'year'
            grouped_cols.append('year')
        if 'm' in agg_keys:
            grouped_renames['Start Month'] = 'month'
            grouped_cols.append('month')
        grouped_cols += [ 'start', 'end', ]
        sort_cols = grouped_cols.copy()
        grouped_cols += ['count']
        grouped_df = grouped_df.rename(columns=grouped_renames)
        grouped_df = grouped_df.merge(id2idx, left_on='start', right_index=True, how='left')
        grouped_df = grouped_df.drop(columns='start').rename(columns={'idx': 'start'})
        grouped_df = grouped_df.merge(id2idx, left_on='end', right_index=True, how='left')
        grouped_df = grouped_df.drop(columns='end').rename(columns={'idx': 'end'})
        grouped_df = grouped_df[grouped_cols].sort_values(sort_cols)
        idx2id.to_sql('idx2id', con)
        id2idx.to_sql('id2idx', con)
        tbl = "_".join([
            self.agg_keys_label,
            self.sum_keys_label,
        ])
        grouped_df.to_sql(tbl, con, index=False)
        return WROTE


if __name__ == '__main__':
    StationsDB.cli()
