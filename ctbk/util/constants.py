import dask.dataframe as dd
import pandas as pd
from typing import Union

from ctbk.util import YM

S3 = 's3:/'
PARQUET_EXTENSION = '.parquet'
SQLITE_EXTENSION = '.db'
JSON_EXTENSION = '.json'
GENESIS = YM(2013, 6)
END = None
BKT = 'ctbk'

DataFrame = Union[pd.DataFrame, dd.DataFrame]
