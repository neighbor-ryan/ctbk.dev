import dask.dataframe as dd
import pandas as pd
from dask.delayed import Delayed, delayed
from typing import Literal
from typing import Union

DataFrame = Union[pd.DataFrame, dd.DataFrame]
RV = Literal['read', 'orig', None]

def value_counts(df: DataFrame) -> pd.Series:
    if isinstance(df, dd.DataFrame):
        return df.assign(n=1).groupby(df.columns.tolist())['n'].sum().compute()
    else:
        return df.value_counts()


def checkpoint(df: dd.DataFrame, url: str, rv: RV = 'read') -> Union[None, DataFrame, Delayed]:
    if isinstance(df, pd.DataFrame):
        df.to_parquet(url)
        if rv == 'read':
            return pd.read_parquet(url)
        elif rv == 'orig':
            return df
        else:
            return None
    else:
        name = f'{url} ({rv})'
        if rv is None:
            def none_checkpoint(df):
                df.to_parquet(url)

            [partition] = df.to_delayed()
            return delayed(none_checkpoint)(partition, dask_key_name=name)
        else:
            df = df.repartition(npartitions=1)
            print(f'url: {url}')
            return df.map_partitions(checkpoint, url, rv=rv, token=name, meta=df._meta)


if __name__ == '__main__':
    df = dd.read_parquet('s3/ctbk/aggregated/ymse_c_202212.pqt')
    d1 = checkpoint(df, 'd1.pqt', rv='read')
    # d2 = checkpoint(df, 'd2.pqt', rv='orig')
    # d3 = checkpoint(df, 'd3.pqt', rv=None)
    d1.visualize(filename='d1.png')
