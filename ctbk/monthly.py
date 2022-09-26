from os import cpu_count
from os.path import basename

import click
from joblib import Parallel, delayed
from typing import Type, Literal

from sys import stderr

import fsspec
import pandas as pd
from abc import abstractmethod
from inspect import getfullargspec
from urllib.parse import urlparse

from ctbk import cached_property, Month, contexts
from ctbk.util.convert import Result, run, BadKey, BAD_DST, OVERWROTE, FOUND, WROTE


GENESIS = Month(2013, 6)
RGX = r'(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
BKT = 'ctbk'

Error = Literal["warn", "error"]


class MonthsDataset:
    # start: Month = None
    # end: Month = None

    # def months(self, start: Month = GENESIS, end: Month = None):
    #     end = end or Month()
    #     return [
    #         self.month_cls(month=month, root=self.root, fmt=self.fmt)
    #         for month in start.until(end)
    #     ]

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

    @abstractmethod
    def compute(self, month, **kwargs):
        pass

    def filter(self, **kwargs):
        inputs = self.inputs
        for k, v in kwargs.items():
            inputs = inputs[inputs[k] == v]
        return inputs

    @cached_property
    def inputs_df(self):
        raise NotImplementedError

    @cached_property
    def inputs(self):
        return self.inputs_df.to_dict('records')

    def convert(
            self,
            start: Month = None,
            end: Month = None,
            error: Error = 'warn',
            overwrite: bool = False,
            parallel: bool = False,
    ):
        # Optionally filter months to operate on
        if start or end:
            start = Month(start) or GENESIS
            end = Month(end) or Month()
            inputs = []
            for input in self.inputs:
                month = input['month']
                if start <= month < end:
                    inputs.append(input)
                else:
                    print(f'Skipping month {month}: {input}')
        else:
            inputs = self.inputs

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
            **ctx,
    ):
        src = ctx.get('src')
        if 'src' in ctx:
            assert 'srcs' not in ctx
            ctx['src_name'] = basename(ctx['src'])
        else:
            assert 'srcs' in ctx

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

        ctx['dst'] = dst
        ctx['dst_name'] = basename(dst)
        ctx['error'] = error
        ctx['overwriting'] = False

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

        fn = self.compute
        args = getfullargspec(fn).args

        ctxs = []
        if 'src_fd' in args:
            assert src
            src_fd = ctx['src_fd'] = self.fs.open(src, 'rb')
            ctxs.append(src_fd)

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

    def __call__(self, start: Month = GENESIS, end: Month = None):
        return [ month() for month in self.months() ]


# @click.command(help="")
# @click.option('-s', '--src')
# @click.option('-d', '--dst')
# @click.option('-p', '--parallel/--no-parallel', help='Use joblib to parallelize execution')
# @click.option('-f', '--overwrite/--no-overwrite', help='When set, write files even if they already exist')
# @click.option('--public/--no-public', help='Give written objects a public ACL')
# @click.option('--start', help='Month to process from (in YYYYMM form)')
# @click.option('--end', help='Month to process until (in YYYYMM form; exclusive)')
# def main(src, dst, parallel, overwrite, public, start, end):
#     StationsMonthsCls = register_months_dataset(src)(StationsMonths)
