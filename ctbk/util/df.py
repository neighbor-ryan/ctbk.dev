from typing import Literal, Callable

from pandas import DataFrame


def save(
    df: DataFrame,
    url: str,
    fmt: Literal['pqt', 'csv', 'json'] = 'pqt',
    write_kwargs: dict | Callable | None = None,
) -> None:
    write_kwargs = write_kwargs or {}
    if callable(write_kwargs):
        write_kwargs(df)
    elif fmt == 'pqt':
        try:
            df.to_parquet(url, **write_kwargs)
        except ValueError as e:
            raise RuntimeError(f"Error saving {url}") from e
    elif fmt == 'csv':
        df.to_csv(url, **write_kwargs)
    elif fmt == 'json':
        df.to_json(url, **write_kwargs)
    else:
        raise ValueError(f"Unrecognized fmt: {fmt}")
