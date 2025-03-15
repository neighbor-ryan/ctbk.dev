from dataclasses import dataclass
from functools import cached_property
from os.path import exists, join, splitext, basename
from tempfile import mkdtemp

import pandas as pd
from fsspec.implementations.local import LocalFileSystem
from pandas import DataFrame
from utz import YM, err

from ctbk.blob import DvcBlob
from ctbk.normalized import ParquetEngine, normalize_df, dedupe_sort
from ctbk.paths import S3
from ctbk.tables_dir import Tables
from ctbk.tripdata_month import TripdataMonth
from ctbk.util.df import save
from ctbk.util.region import Region, get_regions


@dataclass
class NormalizedChunks(DvcBlob):
    ym: YM
    engine: ParquetEngine = 'auto'

    DIR = 'normalized'

    @property
    def path(self) -> str:
        return join(S3, self.DIR, str(self.ym))

    @cached_property
    def fs(self):
        return LocalFileSystem()

    def paths(self) -> dict[str, str]:
        fs = self.fs
        paths = fs.glob(f'{self.path}/*_*.parquet')
        return { splitext(basename(path))[0]: path for path in paths }

    def normalized_region(self, region: Region) -> Tables:
        ym = self.ym
        csv = TripdataMonth(ym=ym, region=region)
        return normalize_df(csv.df(), src=csv.zip_basename, region=region)

    def dfs(self) -> Tables:
        dfs_dict: dict[str, list[DataFrame]] = {}
        for region in get_regions(self.ym):
            for name, df in self.normalized_region(region).items():
                if name not in dfs_dict:
                    dfs_dict[name] = []
                dfs_dict[name].append(df)

        rv = {}
        for name, dfs in dfs_dict.items():
            df = pd.concat(dfs)
            df = dedupe_sort(df, f'{self.ym}/{name}')
            rv[name] = df
        return rv

    @property
    def save_kwargs(self):
        return dict(write_kwargs=dict(index=False, engine=self.engine))

    def save(self) -> Tables:
        path = self.path
        rmdir = False
        if not exists(path):
            self.fs.mkdir(path)
            rmdir = True
        rm_paths = []
        try:
            _dfs = self.dfs()
            dfs: Tables = {}
            for name, df in _dfs.items():
                path = f'{path}/{name}.parquet'
                save(df, url=path, **self.save_kwargs)
                dfs[name] = df
            rmdir = False
            rm_paths = {
                name: path
                for name, path in self.paths().items()
                if name not in dfs
            }
            return dfs
        finally:
            if rmdir:
                err(f"Removing directory {path} after failed write")
                self.fs.delete(path)  # TODO: remove all directory levels that were created
            if rm_paths:
                tmpdir = mkdtemp()
                err(f"Moving untracked parquets to {tmpdir}: {rm_paths}")
                for name, path in rm_paths.items():
                    self.fs.mv(path, f'{tmpdir}/{name}.parquet')
