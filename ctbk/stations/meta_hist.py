import pandas as pd

from ctbk import NormalizedMonths
from ctbk.monthly import Reducer
from ctbk.util import cached_property
from ctbk.util.constants import BKT


class StationMetaHist(Reducer):
    ROOT = f'{BKT}/stations/llname_hists/all'
    SRC_CLS = NormalizedMonths
    RGX = r'(?P<month>\d{6})\.parquet'

    def reduced_df_path(self, month):
        return f'{self.parent}/{month}.parquet'

    @cached_property
    def listdir(self):
        return self.fs.listdir(self.parent)

    def reduce(self, df):
        columns = {
            'Start Station ID': 'Station ID',
            'Start Station Name': 'Station Name',
            'Start Station Latitude': 'Latitude',
            'Start Station Longitude': 'Longitude',
        }
        starts = df[columns.keys()].rename(columns=columns)
        starts['Start'] = True

        columns = {
            'End Station ID': 'Station ID',
            'End Station Name': 'Station Name',
            'End Station Latitude': 'Latitude',
            'End Station Longitude': 'Longitude',
        }
        ends = df[columns.keys()].rename(columns=columns)
        ends['Start'] = False

        station_entries = pd.concat([starts, ends])
        stations_hist = (
            station_entries
            .groupby(['Station ID', 'Station Name', 'Latitude', 'Longitude'])
            .size()
            .rename('count')
            .reset_index()
        )
        return stations_hist

    def compute(self, combined_df, **kwargs):
        return (
            combined_df
            .groupby(['Station ID', 'Station Name', 'Latitude', 'Longitude'])
            ['count']
            .sum()
            .reset_index()
            .sort_values('count')
        )


if __name__ == '__main__':
    StationMetaHist.cli()
