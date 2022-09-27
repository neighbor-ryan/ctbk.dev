from os import cpu_count
from os.path import basename, splitext

import click
from joblib import Parallel, delayed
from typing import Type, Literal

from sys import stderr

import fsspec
import pandas as pd
from abc import abstractmethod
from inspect import getfullargspec
from urllib.parse import urlparse
from utz import sxs, singleton

from ctbk import cached_property, Month, contexts, Monthy
from ctbk.util.convert import Result, run, BadKey, BAD_DST, OVERWROTE, FOUND, WROTE


PARQUET_EXTENSION = '.parquet'
GENESIS = Month(2013, 6)
RGX = r'(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
BKT = 'ctbk'

Error = Literal["warn", "error"]


class Dataset:
    NAMESPACE = 's3'  # by default, operate on a local directory called "s3", which mirrors the "s3://" namespace
    ROOT: str = None
    SRC_CLS: Type['Dataset'] = None
    RGX = None

    def __init__(
            self,
            root: str = None,
            src: 'Dataset' = None,
    ):
        self.src = src or (self.SRC_CLS and self.SRC_CLS())
        if root is None:
            if self.ROOT is None:
                raise RuntimeError('root/ROOT required')
            root = f'{self.NAMESPACE}/{self.ROOT}'
        self.root = root

    def path(self, start=None, end=None, extension=PARQUET_EXTENSION, root=None):
        root = root or self.root
        path, _extension = splitext(root)
        extension = _extension or extension
        if start and end:
            path = f'{path}_{start}:{end}'
        elif start or end:
            raise ValueError(f'Pass both of (start, end) or neither: {start}, {end}')
        path = f'{path}{extension}'
        return path

    @property
    def parent(self):
        return self.root.rsplit('/', 1)[0]

    @cached_property
    def scheme(self):
        url = self.root
        parsed = urlparse(url)
        return parsed.scheme

    @cached_property
    def fs(self) -> fsspec.filesystem:
        return fsspec.filesystem(self.scheme)

    @cached_property
    def listdir(self):
        return self.fs.listdir(self.root)

    @cached_property
    def listdir_df(self):
        return pd.DataFrame(self.listdir)

    @cached_property
    def src_names(self):
        return self.src.listdir_df.name

    @cached_property
    def src_basenames(self):
        return self.src_names.apply(basename)

    def parsed_srcs(self, rgx=None, endswith=None):
        src_basenames = self.src_basenames
        if endswith:
            src_basenames = src_basenames[src_basenames.str.endswith(endswith)]
        if rgx:
            return sxs(src_basenames.str.extract(rgx), src_basenames)
        else:
            return src_basenames

    def task_list(self, start: Monthy = None, end: Monthy = None):
        return self.task_df(start=start, end=end).to_dict('records')

    def task_df(self, start: Monthy = None, end: Monthy = None):
        return self.src.outputs(start=start, end=end)

    def outputs(self, start: Monthy = None, end: Month = None):
        df = self.listdir_df
        basenames = df.name.apply(basename)

        if not self.RGX:
            raise RuntimeError('No regex found for parsing output paths')

        df = sxs(basenames.str.extract(self.RGX), df)
        df['month'] = df['month'].apply(Month)
        if start and end:
            df = df[(start <= df.month) & (df.month < end)]
        elif start or end:
            raise ValueError(f'Pass both of (start, end) or neither: {start}, {end}')
        return df

    @classmethod
    def cli_opts(cls):
        return [
            click.command(help="Group+Count rides by station info (ID, name, lat, lng)"),
            click.option('-r', '--root', help='Namespace to read/write files, e.g. "s3://" or "s3" (for a local dir mirroring the S3 layout)'),
            click.option('--src', 'src_url', help='`src`-specific url base'),
            click.option('--dst', 'dst_url', help='`dst`-specific url base'),
            click.option('-p/-P', '--parallel/--no-parallel', default=True, help='Use joblib to parallelize execution'),
            click.option('-f', '--overwrite/--no-overwrite', help='When set, write files even if they already exist'),
            click.option('--start', help='Month to process from (in YYYYMM form)'),
            click.option('--end', help='Month to process until (in YYYYMM form; exclusive)'),
        ]

    @classmethod
    def cli(cls):
        fn = cls.main
        for cli_opt in cls.cli_opts():
            fn = cli_opt(fn)
        return fn()

    @classmethod
    def main(
            cls,
            root,
            src_url,
            dst_url,
            parallel,
            overwrite,
            start,
            end,
    ):
        src = cls.SRC_CLS(root=src_url or root)
        self = cls(root=dst_url or root, src=src)
        results = self.convert(start=start, end=end, overwrite=overwrite, parallel=parallel)
        if isinstance(results, Result):
            print(results)
        else:
            results_df = pd.DataFrame(results)
            print(results_df)

    @abstractmethod
    def compute(self, **kwargs):
        pass


class MonthsDataset(Dataset):
    def convert(
            self,
            start: Month = None,
            end: Month = None,
            error: Error = 'warn',
            overwrite: bool = False,
            parallel: bool = False,
    ):
        # Optionally filter months to operate on
        tasks = self.task_list(start=start, end=end)

        # Execute, serial or in parallel
        if parallel:
            p = Parallel(n_jobs=cpu_count())
            convert_one = delayed(self.convert_one)
            results = p(convert_one(task, error=error, overwrite=overwrite) for task in tasks)
        else:
            results = [
                self.convert_one(task, error=error, overwrite=overwrite)
                for task in tasks
            ]

        return results

    def convert_one(
            self,
            task,
            error='warn',
            overwrite=False,
    ):
        ctx = {
            'error': error,
            'overwriting': False,
            'overwrite': overwrite,
        }
        src = task.get('src')
        if 'src' in task:
            assert 'srcs' not in task
            ctx['src_name'] = basename(task['src'])
        else:
            assert 'srcs' in task
        ctx.update(task)

        dst = task['dst']
        if callable(dst):
            try:
                dst = run(dst, ctx)
            except BadKey as e:
                if error == 'raise':
                    raise e
                msg = 'Unrecognized key: %s' % src
                if error == 'warn':
                    stderr.write('%s\n' % msg)
                return Result(msg=msg, status=BAD_DST)

        ctx.update({
            'dst': dst,
            'dst_name': basename(dst),
        })

        fn = self.compute
        args = getfullargspec(fn).args

        if self.fs.exists(dst):
            if overwrite:
                ctx['overwriting'] = True
                msg = f'Overwrote {dst}'
                status = OVERWROTE
            else:
                msg = f'Found {dst}; skipping'
                status = FOUND
                return Result(msg=msg, status=status, dst=dst)
        else:
            msg = f'Wrote {dst}'
            status = WROTE

        ctxs = []
        if 'src_fd' in args:
            assert src
            assert 'src_df' not in args
            src_fd = ctx['src_fd'] = self.fs.open(src, 'rb')
            ctxs.append(src_fd)
        elif 'src_df' in args:
            if src.endswith(PARQUET_EXTENSION):
                with self.src.fs.open(src, 'rb') as f:
                    src_df = pd.read_parquet(f)
                ctx['src_df'] = src_df
            else:
                raise RuntimeError(f"Unrecognized src_df extension: {src}")

        if 'dst_fd' in args:
            dst_fd = ctx['dst_fd'] = self.fs.open(dst, 'rb')
            ctxs.append(dst_fd)

        with contexts(ctxs):
            value = run(fn, ctx)

        if isinstance(value, pd.DataFrame):
            if not self.fs.exists(self.root):
                self.fs.mkdir(self.root)
            value.to_parquet(dst)

        extra_dst = task.get('extra_dst')
        if extra_dst:
            self.fs.copy(dst, extra_dst)

        return Result(msg=msg, status=status, dst=dst, value=value)


class Reducer(Dataset):
    def task_list(self, start: Monthy = None, end: Monthy = None):
        df = self.src.outputs(start=start, end=end)
        srcs = df.name.values
        task = { 'srcs': srcs, }
        return [task]

    def reduce_wrapper(self, src):
        fn = self.reduce
        spec = getfullargspec(fn)
        args = spec.args
        ctx = { 'src': src }
        if 'df' in args:
            with self.src.fs.open(src, 'rb') as f:
                ctx['df'] = pd.read_parquet(f)
        return run(fn, ctx)

    @abstractmethod
    def reduce(self, **kwargs):
        pass

    def reduced_dfs(self, srcs, parallel: bool = False):
        if parallel:
            p = Parallel(n_jobs=cpu_count())
            dfs = p(delayed(self.reduce)(url) for url in srcs)
        else:
            dfs = [self.reduce(url) for url in srcs]
        return dfs

    def reduced_df(self, srcs, parallel: bool = False):
        return pd.concat(self.reduced_dfs(srcs=srcs, parallel=parallel))

    def combine(self, reduced_dfs):
        return pd.concat(reduced_dfs)

    def convert(
            self,
            start: Month = None,
            end: Month = None,
            parallel: bool = False,
            **kwargs,
    ):
        tasks = self.task_list(start, end)
        kwargs = dict(start=start, end=end, parallel=parallel, **kwargs)
        if parallel:
            p = Parallel(n_jobs=cpu_count())
            results = p(delayed(self.convert_one)(task, **kwargs) for task in tasks)
        else:
            results = [ self.convert_one(task, **kwargs) for task in tasks ]
        return results

    def compute(self, combined_df, **kwargs):
        return combined_df

    def convert_one(
            self,
            task,
            start: Month = None,
            end: Month = None,
            error: Error = 'warn',
            overwrite: bool = False,
            parallel: bool = False,
    ):
        latest = not start and not end

        start = Month(start) if start else GENESIS
        end = Month(end)
        dst = self.path(start, end)
        all_dst = self.path()

        fn = self.compute
        args = getfullargspec(fn).args
        ctx = {
            'dst': dst,
            'error': error,
            'overwrite': overwrite,
            'overwriting': False,
            'parallel': parallel,
        }

        if self.fs.exists(dst):
            if overwrite:
                ctx['overwriting'] = True
                msg = f'Overwrote {dst}'
                status = OVERWROTE
            else:
                msg = f'Found {dst}; skipping'
                status = FOUND
                return Result(msg=msg, status=status, dst=dst)
        else:
            msg = f'Wrote {dst}'
            status = WROTE

        srcs = task['srcs']
        if parallel:
            p = Parallel(n_jobs=cpu_count())
            reduced_dfs = p(delayed(self.reduce_wrapper)(src) for src in srcs)
        else:
            reduced_dfs = [ self.reduce_wrapper(src) for src in srcs ]
        ctx['reduced_dfs'] = reduced_dfs
        if 'combined_df' in args:
            ctx['combined_df'] = self.combine(reduced_dfs)

        value = run(fn, ctx)

        if isinstance(value, pd.DataFrame):
            if not self.fs.exists(self.parent):
                self.fs.mkdir(self.parent)
            print(f'Writing DataFrame to {dst}')
            value.to_parquet(dst)

        if latest:
            print(f'Copying {dst} to {all_dst}')
            self.fs.copy(dst, all_dst, recursive=True)

        return Result(msg=msg, status=status, dst=dst, value=value)
