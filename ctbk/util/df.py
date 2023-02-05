import dask.dataframe as dd
import pandas as pd
from typing import Union


DataFrame = Union[pd.DataFrame, dd.DataFrame]


def value_counts(df: DataFrame) -> pd.Series:
    if isinstance(df, dd.DataFrame):
        return df.assign(n=1).groupby(df.columns.tolist())['n'].sum().compute()
    else:
        return df.value_counts()
