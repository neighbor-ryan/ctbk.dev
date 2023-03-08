#!/usr/bin/env python

from ctbk import AggregatedMonths
from ctbk.aggregated import AggKeys, SumKeys
from ctbk.month_agg_table import MonthAggTable
from ctbk.util.df import meta, DataFrame


class YmrgtbCdJson(MonthAggTable):
    # Used by [`index.tsx`](www/pages/index.tsx) plot
    SRC_CLS = AggregatedMonths
    OUT = 'www/public/assets/ymrgtb_cd.json'

    def src_kwargs(self):
        return dict(
            agg_keys=AggKeys.load('ymrgtb'),
            sum_keys=SumKeys.load('cd'),
            **super().src_kwargs(),
        )

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
            .rename(columns={
                'Start Year': 'Year',
                'Start Month': 'Month',
            })
        )
        return ymr_json


if __name__ == '__main__':
    YmrgtbCdJson.main()
