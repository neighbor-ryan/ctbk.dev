from typing import Optional, Literal
from typing import Union

import pandas as pd
from pandas import DataFrame

from ctbk.util.read import Read, Disk, Memory


def checkpoint(
    df: DataFrame,
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
