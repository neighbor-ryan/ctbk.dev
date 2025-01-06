from os import makedirs
from os.path import dirname

import pandas as pd
from utz import YM, err

from ctbk.cli.base import ctbk
from ctbk.has_root_cli import dates
from ctbk.normalized import normalize_df

V0_DIR = 's3/ctbk/normalized/v0'


@ctbk.command
@dates
def partition(yms: list[YM]):
    """Separate pre-2024 parquets (`normalized/v0`) by {src,start,end} months."""
    for ym in yms:
        pqt_path = f'{V0_DIR}/{ym}.parquet'
        ym_dir = f'{V0_DIR}/{ym}'
        ym_df = pd.read_parquet(pqt_path)
        dfs = normalize_df(ym_df, src=pqt_path)
        for yms_str, df in dfs.items():
            out_path = f'{ym_dir}/{yms_str}.parquet'
            out_dir = dirname(out_path)
            makedirs(out_dir, exist_ok=True)
            df.to_parquet(out_path, index=False)
            err(f"Wrote {len(df)} rows to {out_path}")
