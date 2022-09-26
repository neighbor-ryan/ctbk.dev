import pandas as pd

from ctbk import MonthsDataset, cached_property

TRIPDATA_BKT = 'tripdata'
TRIPDATA_URL = f's3://{TRIPDATA_BKT}'


class Tripdata(MonthsDataset):
    ROOT = TRIPDATA_URL

    @cached_property
    def inputs_df(self):
        return pd.DataFrame(self.fs.listdir())
