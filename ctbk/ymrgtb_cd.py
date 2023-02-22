#!/usr/bin/env python
import dask.dataframe as dd

from ctbk import Monthy
from ctbk.month_agg_table import MonthAggTable
from ctbk.util import stderr
from ctbk.util.df import meta, DataFrame
from ctbk.util.ym import YM


# ymrgtb_cd = read_parquet('s3://ctbk/aggregated/ymrgtb_cd.parquet')


# renames = {
#     'r': ['NYC', 'JC', 'HOB'],
#     't': ['Annual', 'Daily'],
#     'b': ['classic_bike', 'docked_bike', 'electric_bike', 'unknown'],
# }
# renames = {
#     k: { v: idx for idx, v in enumerate(vs) }
#     for k, vs in renames.items()
# }
# print(renames)
#
# ymr_json2 = (
#     ymr_json
#     .rename(columns={
#         'Region': 'r',
#         'Year': 'y',
#         'Month': 'm',
#         'Gender': 'g',
#         'User Type': 't',
#         'Rideable Type': 'b',
#         'Count': 'c',
#         'Duration': 'd',
#     })
# )
# ymr_json2 = (
#     ymr_json2
#     .assign(
#         **{
#             k: ymr_json2[k].apply(lambda v: o[v])
#             for k, o in renames.items()
#         },
#         y=ymr_json2.y.apply(lambda y: y - 2000)
#     )
# )
# print(ymr_json2)
# ymr_json2.to_json('www/public/assets/ymrgtb_cd.short.json', 'records')


class YmrgtbCdJson(MonthAggTable):
    # Used by [`index.tsx`](www/pages/index.tsx) plot
    ROOT = 's3://ctbk/aggregated'
    OUT = 'www/public/assets/ymrgtb_cd.json'

    def load(self, ym: Monthy) -> DataFrame:
        url = f'{self.root}/ymrgtb_cd_{ym}.parquet'
        try:
            df = self.dpd.read_parquet(url)
        except FileNotFoundError as e:
            stderr(f'FileNotFoundError: {url}')
            raise

        if len(df[(df['Start Year'] == 2020) & (df['Start Month'] == 10)]) and ym != YM(202010):
            stderr(f"Found 202010's in {url}")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        ymr_json = (
            df
            .assign(**{
                'Region': df.Region.replace('HB', 'HOB'),
                'User Type': df['User Type'].apply(
                    lambda u: {'Customer': 'Daily', 'Subscriber': 'Annual'}[u],
                    **meta('User Type', self.dask),
                )
            })
            .drop(columns='Month')
            .rename(columns={
                'Start Year': 'Year',
                'Start Month': 'Month',
            })
        )
        return ymr_json


if __name__ == '__main__':
    YmrgtbCdJson.main()
