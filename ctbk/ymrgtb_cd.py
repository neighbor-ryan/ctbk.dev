#!/usr/bin/env python
from ctbk import Monthy
from ctbk.month_agg_table import MonthAggTable
from ctbk.util.df import meta, DataFrame


class YmrgtbCdJson(MonthAggTable):
    # Used by [`index.tsx`](www/pages/index.tsx) plot
    SRC = 'ctbk/aggregated'
    OUT = 'www/public/assets/ymrgtb_cd.json'

    def url(self, ym: Monthy) -> str:
        return f'{self.dir}/ymrgtb_cd_{ym}.parquet'

    def reduce(self, mapped_dfs: list[DataFrame]) -> DataFrame:
        df = self.dpd.concat(mapped_dfs)
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
