import dask.dataframe as dd
import pandas as pd
from dask.delayed import Delayed, delayed
from functools import wraps
from typing import Optional, Literal, Tuple, Callable
from typing import Union

from ctbk.util.read import Read, Disk, Memory

DataFrame = Union[pd.DataFrame, dd.DataFrame]


def sxs(*dfs) -> DataFrame:
    concat = dd.concat if isinstance(dfs[0], (dd.DataFrame, dd.Series)) else pd.concat
    return concat(list(dfs), axis=1)


def apply(fn: Callable[[pd.DataFrame], pd.DataFrame], meta=None) -> Callable[[DataFrame], DataFrame]:
    @wraps(fn)
    def _fn(df: DataFrame):
        if isinstance(df, dd.DataFrame):
            return df.map_partitions(fn, meta=meta)
        else:
            return fn(df)
    return _fn


def meta(arg: Union[DataFrame, str, dict, Tuple], dask: Union[DataFrame, bool] = True) -> dict:
    dask = dask is True or isinstance(dask, dd.DataFrame)
    if dask:
        if isinstance(arg, str):
            return dict(meta=(arg, str))
        elif isinstance(arg, Tuple):
            return dict(meta=arg)
        elif isinstance(arg, dict):
            return dict(meta=arg)
        else:
            raise ValueError(f"Unrecognized arg: {arg}")
    else:
        return dict()


def value_counts(df: DataFrame) -> pd.Series:
    if isinstance(df, dd.DataFrame):
        return df.assign(n=1).groupby(df.columns.tolist())['n'].sum().compute()
    else:
        return df.value_counts()


def checkpoint_df(
    df: pd.DataFrame,
    url: str,
    read: Optional[Read] = Disk,
    fmt: Literal['pqt', 'csv', 'json'] = 'pqt',
    read_kwargs: Optional[dict] = None,
    write_kwargs: Optional[dict] = None,
) -> Union[None, pd.DataFrame]:
    write_kwargs = write_kwargs or {}
    if callable(write_kwargs):
        write_kwargs(df)
    elif fmt == 'pqt':
        df.to_parquet(url, **write_kwargs)
    elif fmt == 'csv':
        df.to_csv(url, **write_kwargs)
    elif fmt == 'json':
        df.to_json(url, **write_kwargs)
    else:
        raise ValueError(f"Unrecognized fmt: {fmt}")
    if read is Disk:
        read_kwargs = read_kwargs or dict()
        if callable(read_kwargs):
            return read_kwargs()
        elif fmt == 'pqt':
            return pd.read_parquet(url, **read_kwargs)
        elif fmt == 'csv':
            return pd.read_csv(url, **read_kwargs)
        elif fmt == 'json':
            return pd.read_json(url, **read_kwargs)
        else:
            raise ValueError(f"Unrecognized fmt: {fmt}")
    elif read is Memory:
        return df
    else:
        return None


def checkpoint_dd(
        df: dd.DataFrame,
        url: str, read: Optional[Read] = Disk,
        fmt: Literal['pqt', 'csv', 'json'] = 'pqt',
        write_kwargs: Optional[dict] = None,
        read_kwargs: Optional[dict] = None,
) -> Union[None, Delayed, dd.DataFrame]:
    name = f'{url} ({read})'
    if read is None:
        [partition] = df.repartition(npartitions=1).to_delayed()
        return delayed(checkpoint_df)(
            df=partition,
            url=url,
            read=read,
            fmt=fmt,
            read_kwargs=read_kwargs,
            write_kwargs=write_kwargs,
            dask_key_name=name,
        )
    else:
        df = df.repartition(npartitions=1)
        print(f'url: {url}')
        return df.map_partitions(
            checkpoint_df,
            url=url,
            read=read,
            fmt=fmt,
            read_kwargs=read_kwargs,
            write_kwargs=write_kwargs,
            token=name,
            meta=df._meta,
        )


if __name__ == '__main__':
    df = dd.read_parquet('s3/ctbk/aggregated/ymse_c_202212.pqt')
    d1 = checkpoint_dd(df, 'd1.pqt',)
    # d2 = checkpoint(df, 'd2.pqt', read='orig')
    # d3 = checkpoint(df, 'd3.pqt', read=None)
    d1.visualize(filename='d1.png')
