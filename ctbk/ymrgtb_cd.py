#!/usr/bin/env python
from ctbk import Monthy
from ctbk.month_agg_table import MonthAggTable
from ctbk.util import stderr
from ctbk.util.df import meta, DataFrame
from ctbk.util.ym import YM


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
