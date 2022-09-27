from pandas import concat
from utz import sxs

from ctbk import cached_property, Month
from ctbk.monthly import Reducer, BKT
from ctbk.stations.meta_hists import StationMetaHists


class StationMetaHist(Reducer):
    SRC_CLS = StationMetaHists
    ROOT = f's3://{BKT}/stations/llname_hists/all'

    @cached_property
    def inputs_df(self):
        df = self.src.listdir_df
        df['src'] = 's3://' + df.name
        df = (
            sxs(
                df.name.str.extract(r'(?P<month>\d{6})\.parquet')['month'].dropna().apply(Month),
                df.src,
            )
            .sort_values('month')
        )
        return df

    def compute(self, src_dfs):
        srcs_df = concat(src_dfs)
        return (
            srcs_df
                .groupby(['Station ID', 'Station Name', 'Latitude', 'Longitude'])
                ['count']
                .sum()
                .reset_index()
                .sort_values('count')
        )


if __name__ == '__main__':
    StationMetaHist.cli()
