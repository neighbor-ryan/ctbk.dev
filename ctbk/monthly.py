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
from utz import sxs

from ctbk import cached_property, Month, contexts, Monthy
from ctbk.util.convert import Result, run, BadKey, BAD_DST, OVERWROTE, FOUND, WROTE


PARQUET_EXTENSION = '.parquet'
GENESIS = Month(2013, 6)
RGX = r'(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
BKT = 'ctbk'

Error = Literal["warn", "error"]


class Dataset:
    ROOT: str = None
    SRC_CLS: Type['MonthsDataset'] = None

    def __init__(
            self,
            root: str = None,
            src: 'MonthsDataset' = None,
    ):
        self.src = src or (self.SRC_CLS and self.SRC_CLS())
        self.root = root or self.ROOT
        if self.root is None:
            raise RuntimeError('root/ROOT required')

    def path(self, start=None, end=None, extension=PARQUET_EXTENSION):
        root = self.root
        path, _extension = splitext(root)
        extension = _extension or extension
        if start and end:
            path = f'{path}_{start}-{end}'
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

    @cached_property
    def inputs_df(self):
        raise NotImplementedError

    def filter(self, **kwargs):
        inputs = self.inputs
        for k, v in kwargs.items():
            inputs = inputs[inputs[k] == v]
        return inputs

    @cached_property
    def inputs(self):
        return self.inputs_df.to_dict('records')

    def input_range_df(self, start: Monthy = None, end: Monthy = None):
        start = Month(start) if start else GENESIS
        end = Month(end) or Month()
        df = self.inputs_df
        df = df[start <= df.month < end]
        return df

    def input_range(self, start: Monthy = None, end: Monthy = None):
        start = Month(start) if start else GENESIS
        end = Month(end) or Month()
        inputs = []
        for input in self.inputs:
            month = input['month']
            if start <= month < end:
                inputs.append(input)
            else:
                print(f'Skipping month {month}: {input}')
        return inputs

    @classmethod
    def cli_opts(cls):
        return [
            click.command(help="Group+Count rides by station info (ID, name, lat, lng)"),
            click.option('-s', '--src-root', default=cls.SRC_CLS.ROOT, help='Prefix to read normalized parquets from'),
            click.option('-d', '--dst-root', default=cls.ROOT, help='Prefix to write station name/latlng hists to'),
            click.option('-p/-P', '--parallel/--no-parallel', default=True, help='Use joblib to parallelize execution'),
            click.option('-f', '--overwrite/--no-overwrite', help='When set, write files even if they already exist'),
            #click.option('--public/--no-public', help='Give written objects a public ACL'),
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
            src_root,
            dst_root,
            parallel,
            overwrite,
            # public,
            start,
            end,
    ):
        src = cls.SRC_CLS(root=src_root)
        self = cls(root=dst_root, src=src)
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
        inputs = self.input_range(start=start, end=end)

        # Execute, serial or in parallel
        if parallel:
            p = Parallel(n_jobs=cpu_count())
            convert_one = delayed(self.convert_one)
            results = p(convert_one(**ipt, error=error, overwrite=overwrite) for ipt in inputs)
        else:
            results = [
                self.convert_one(**input, error=error, overwrite=overwrite)
                for input in inputs
            ]

        return results

    def convert_one(
            self,
            dst,
            error='warn',
            overwrite=False,
            **ipt,
    ):
        ctx = {
            'error': error,
            'overwriting': False,
            'overwrite': overwrite,
        }
        src = ipt.get('src')
        if 'src' in ipt:
            assert 'srcs' not in ipt
            ctx['src_name'] = basename(ipt['src'])
        else:
            assert 'srcs' in ipt
        ctx.update(ipt)

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

        return Result(msg=msg, status=status, dst=dst, value=value)


class Reducer(Dataset):
    @cached_property
    def inputs_df(self):
        return self.src.listdir_df

    def convert(
            self,
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

        if 'src_dfs' in args:
            inputs_df = self.inputs_df
            months = inputs_df.month
            inputs_df = inputs_df[(start <= months) & (months < end)]
            srcs = inputs_df.src
            if not all(srcs.str.endswith(PARQUET_EXTENSION)):
                raise RuntimeError(f"Expected `src`s to be {PARQUET_EXTENSION} files")
            if parallel:
                p = Parallel(n_jobs=cpu_count())
                src_dfs = p(delayed(pd.read_parquet)(src) for src in srcs.values)
            else:
                src_dfs = [ pd.read_parquet(src) for src in srcs.values ]
            ctx['src_dfs'] = src_dfs

        value = run(fn, ctx)

        if isinstance(value, pd.DataFrame):
            if not self.fs.exists(self.parent):
                self.fs.mkdir(self.parent)
            print(f'Writing DataFrame to {dst}')
            value.to_parquet(dst)

        if latest and not self.fs.exists(latest):
            print(f'Copying {dst} to {all_dst}')
            self.fs.copy(dst, all_dst, recursive=True)

        return Result(msg=msg, status=status, dst=dst, value=value)
