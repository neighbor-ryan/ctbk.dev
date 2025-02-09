#!/usr/bin/env python
import pandas as pd
from pandas import DataFrame
from utz.ym import Monthy

from ctbk.month_agg_table import MonthAggTable


class YmrgtbCdJson(MonthAggTable):
    # Used by [`index.tsx`](www/pages/index.tsx) plot
    NAME = 'ymrgtb-cd'
    SRC = 'ctbk/aggregated'
    OUT = 'www/public/assets/ymrgtb_cd.json'

    def url(self, ym: Monthy) -> str:
        return f'{self.dir}/ymrgtb_cd_{ym}.parquet'

    def reduce(self, mapped_dfs: list[DataFrame]) -> DataFrame:
        df = pd.concat(mapped_dfs)
        sort_cols = ['Year', 'Month', 'Region', 'User Type', 'Rideable Type', 'Gender']
        ymr_json = (
            df
            .assign(**{
                'Region': df.Region.replace('HB', 'HOB'),
                'User Type': df['User Type'].apply(lambda u: {'Customer': 'Daily', 'Subscriber': 'Annual'}[u])
            })
            .rename(columns={
                'Start Year': 'Year',
                'Start Month': 'Month',
            })
            .sort_values(sort_cols)
        )
        cols = list(sorted(list(set(ymr_json.columns) - set(sort_cols))))
        return ymr_json[sort_cols + cols]


YmrgtbCdJson.init_cli(
    help="Read aggregated {year,month,region,gender,user-type,bike-type} x {counts,durations}, output `ymrgtb_cd.json` used by webapp."
)
