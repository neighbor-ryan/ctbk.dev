from os import cpu_count
from os.path import basename, splitext

from sqlalchemy import create_engine
from tempfile import TemporaryDirectory

import click
import fsspec
import pandas as pd
from abc import abstractmethod
from inspect import getfullargspec
from joblib import Parallel, delayed
from sys import stderr
from typing import Type, Literal
from urllib.parse import urlparse
from utz import sxs

from ctbk import cached_property, Month, contexts, Monthy
from ctbk.util.context import copy_ctx
from ctbk.util.convert import Result, run, BadKey, BAD_DST, OVERWROTE, FOUND, WROTE

PARQUET_EXTENSION = '.parquet'
SQLITE_EXTENSION = '.db'
JSON_EXTENSION = '.json'
GENESIS = Month(2013, 6)
END = Month(2022, 11)
RGX = r'(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
BKT = 'ctbk'

Error = Literal["warn", "error"]


class Dataset:
    NAMESPACE = 's3/'  # by default, operate on a local directory called "s3", which mirrors the "s3://" namespace
    ROOT: str = None
    SRC_CLS: Type['Dataset'] = None
    RGX = None
    EXTENSION = PARQUET_EXTENSION

    def __init__(
            self,
            namespace: str = None,
            root: str = None,
            src: 'Dataset' = None,
    ):
        # Try to infer whether a scheme/namespace was passed in `namespace` or already prepended to `root`
        if namespace:
            self.namespace = namespace
            root = root or self.default_root
            self.root = f'{namespace}{root}'
        else:
            self.namespace = self.NAMESPACE
            if root:
                parsed = urlparse(root)
                if parsed.scheme:
                    # `root` came in as a URL
                    self.root = root
                else:
                    self.root = f'{self.namespace}{root}'
            else:
                self.root = f'{self.namespace}{self.default_root}'

        self.src = src or (self.SRC_CLS and self.SRC_CLS(namespace=self.namespace))

    @property
    def default_root(self):
        if self.ROOT is None:
            raise RuntimeError('root/ROOT required')
        return self.ROOT

    def path(self, start=None, end=None, extension=None, root=None):
        root = root or self.root
        path, _extension = splitext(root)
        extension = _extension or extension or self.EXTENSION
        if start and end:
            path = f'{path}_{start}:{end}'
        elif start or end:
            raise ValueError(f'Pass both of (start, end) or neither: {start}, {end}')
        path = f'{path}{extension}'
        return path

    def ensure_dir(self, path):
        parent = path.rsplit('/', 1)[0]
        if parent != path and not self.fs.exists(parent):
            self.fs.mkdir(parent)

    @staticmethod
    def month_range(start: Monthy = None, end: Monthy = None):
        start = Month(start) if start else GENESIS
        end = Month(end) if end else END
        return start, end

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

    @cached_property
    def parsed_basenames(self):
        df = self.listdir_df
        basenames = df.name.apply(basename).rename('basename')

        if not self.RGX:
            raise RuntimeError('No regex found for parsing output paths')

        return sxs(basenames.str.extract(self.RGX), df, basenames)

    def outputs(self, start: Monthy = None, end: Month = None):
        df = self.parsed_basenames
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
            click.option('--root', help='Namespace to read/write files, e.g. "s3://" or "s3" (for a local dir mirroring the S3 layout)'),
            click.option('--src', 'src_url', help='`src`-specific url base'),
            click.option('--dst', 'dst_url', help='`dst`-specific url base'),
            click.option('-p/-P', '--parallel/--no-parallel', default=True, help='Use joblib to parallelize execution'),
            click.option('-f', '--overwrite', count=True, help='When set, write files even if they already exist'),
            click.option('--start', help='Month to process from (in YYYYMM form)'),
            click.option('--end', help='Month to process until (in YYYYMM form; exclusive)'),
            click.option('--namespace', help='Prepend to "roots"/paths to obtain valid URLs, e.g. "s3://" (default: "s3" for local folder mimicking S3 tree)'),
            click.option('--s3', is_flag=True, help='Set --namespace to "s3://"'),
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
            namespace,
            s3,
            **kwargs,
    ):
        if s3:
            if namespace and namespace != 's3://':
                raise ValueError(f'`--s3` conflicts with `--namespace {namespace}`')
            namespace = 's3://'
        src = cls.SRC_CLS(namespace=namespace, root=src_url or root)
        self = cls(namespace=namespace, root=dst_url or root, src=src, **kwargs)
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
            dst_fd = ctx['dst_fd'] = self.fs.open(dst, 'wb')
            ctxs.append(dst_fd)

        if 'con' in args:
            tmpdir = TemporaryDirectory()
            path = f'{tmpdir.name}/{basename(dst)}'
            url = f'sqlite:///{path}'
            engine = create_engine(url)
            con = engine.connect()
            ctx['con'] = con
            copy = copy_ctx(src=path, dst=dst, dst_fs=self.fs)
            ctxs += [ tmpdir, con, copy, ]

        with contexts(ctxs):
            value = run(fn, ctx)

        if isinstance(value, pd.DataFrame):
            self.ensure_dir(dst)
            value.to_parquet(dst)

        extra_dst = task.get('extra_dst')
        if extra_dst:
            self.fs.copy(dst, extra_dst)

        return Result(msg=msg, status=status, dst=dst, value=value)


class Reducer(Dataset):
    def task_list(self, start: Monthy = None, end: Monthy = None):
        start, end = self.month_range(start, end)
        df = self.src.outputs(start=start, end=end)
        cols = [df.name.rename('src')]
        dsts = df.month.apply(self.reduced_df_path).rename('dst')
        if all(~dsts.isna()):
            cols.append(dsts)
        subtasks = sxs(*cols).to_dict('record')
        task = { 'subtasks': subtasks, }
        return [task]

    def reduced_df_path(self, month):
        return None

    def reduce_wrapper(self, src, dst=None, overwrite=False):
        fn = self.reduce
        spec = getfullargspec(fn)
        args = spec.args
        fs = self.fs
        exists = fs.exists(dst)
        if dst and exists and not overwrite:
            print(f'Found reduced_df: {dst}')
            return pd.read_parquet(dst)

        ctx = { 'src': src }
        if 'df' in args:
            with self.src.fs.open(src, 'rb') as f:
                ctx['df'] = pd.read_parquet(f)
        df = run(fn, ctx)
        if dst and (not exists or overwrite):
            if exists:
                print(f'Overwriting reduced_df: {dst}')
            else:
                print(f'Writing reduced_df: {dst}')
            with fs.open(dst, 'wb') as f:
                df.to_parquet(f)
        return df

    @abstractmethod
    def reduce(self, **kwargs):
        pass

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
        if parallel and len(tasks) > 1:
            n_jobs = min(cpu_count(), len(tasks))
            p = Parallel(n_jobs=n_jobs)
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
        start, end = self.month_range(start, end)
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

        overwrite_reduced_dfs = overwrite > 1
        subtasks = task['subtasks']
        if parallel and len(subtasks) > 1:
            n_jobs = min(cpu_count(), len(subtasks))
            p = Parallel(n_jobs=n_jobs)
            reduced_dfs = p(delayed(self.reduce_wrapper)(**subtask, overwrite=overwrite_reduced_dfs) for subtask in subtasks)
        else:
            reduced_dfs = [ self.reduce_wrapper(**subtask, overwrite=overwrite_reduced_dfs) for subtask in subtasks ]
        ctx['reduced_dfs'] = reduced_dfs
        if 'combined_df' in args:
            ctx['combined_df'] = self.combine(reduced_dfs)

        value = run(fn, ctx)

        if isinstance(value, pd.DataFrame):
            self.ensure_dir(dst)
            print(f'Writing DataFrame to {dst}')
            value.to_parquet(dst)

        attrs = {}
        if latest:
            print(f'Copying {dst} to {all_dst}')
            self.fs.copy(dst, all_dst, recursive=True)
            attrs['all_dst'] = all_dst

        return Result(msg=msg, status=status, dst=dst, value=value, attrs=attrs)
