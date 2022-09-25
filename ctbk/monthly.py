from os.path import basename

from sys import stderr

import fsspec
import pandas as pd
from abc import abstractmethod
from dataclasses import dataclass
from functools import cached_property
from inspect import getfullargspec
from urllib.parse import urlparse
from utz import singleton

from ctbk.util.context import contexts
from ctbk.util.convert import Result, run, BadKey, BAD_DST, OVERWROTE, FOUND, WROTE
from ctbk.util.month import Month

GENESIS = Month(2013, 6)


RGX = r'(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
BKT = 'ctbk'


# MONTH_DATASET_REGISTRY = {}
#
#
# def register_month_dataset(root: str):
#     def _register(cls):
#         MONTH_DATASET_REGISTRY[root] = cls
#         cls.root = root
#         return cls
#     return _register
#
#
# MONTHS_DATASET_REGISTRY = {}
#
# def register_months_dataset(root: str):
#     def _register(cls):
#         MONTHS_DATASET_REGISTRY[root] = cls
#         cls.root = root
#         cls.month_cls = register_month_dataset(root)(cls.month_cls)
#         cls.month_cls.months_cls = cls
#         return cls
#     return _register


# @dataclass
# class MonthDataset:
#     month: Month
#     # root: str
#
#     FMT = 'pqt'
#     months_cls = None
#
#     @cached_property
#     def url(self):
#         return f'{self.root}/{self.month}.{self.FMT}'
#
#     @cached_property
#     def dt(self):
#         return self.month.dt
#
#     @property
#     def scheme(self):
#         url = self.url
#         parsed = urlparse(url)
#         return parsed.scheme
#
#     @cached_property
#     def fs(self) -> fsspec.filesystem:
#         return fsspec.filesystem(self.scheme)
#
#     @property
#     def exists(self):
#         return self.fs.exists(self.url)
#
#     @abstractmethod
#     def compute(self, *args, **kwargs):
#         raise NotImplementedError
#
#     @abstractmethod
#     def deps(self):
#         raise NotImplementedError
#
#     def __call__(self, error='warn', overwrite=False, **kwargs):
#         overwriting = False
#         if self.exists:
#             if overwrite:
#                 overwriting = True
#             else:
#                 with self.fs.open(self.url, 'rb') as f:
#                     return pd.read_parquet(f)
#
#         fn = self.compute
#         fn_spec = getfullargspec(fn)
#         fn_kwargs = {}
#
#         deps = self.deps()
#         result = self.compute(**kwargs)
#         if isinstance(result, pd.DataFrame):
#             result.to_parquet(self.url)
#         elif isinstance(result, Result):
#             raise  # TODO


@dataclass(init=False)
class MonthsDataset:
    root: str = None
    # start: Month = None
    # end: Month = None

    # def months(self, start: Month = GENESIS, end: Month = None):
    #     end = end or Month()
    #     return [
    #         self.month_cls(month=month, root=self.root, fmt=self.fmt)
    #         for month in start.until(end)
    #     ]

    def __init__(
            self,
            root: str = None,
            # start: Monthy = GENESIS,
            # end: Monthy = None,
    ):
        self.root = root or getattr(self, 'ROOT', None)
        if self.root is None:
            raise RuntimeError('root/ROOT not defined')
        # self.start = Month(start)
        # self.end = Month(end)

    # @abstractmethod
    # def deps(self, month, **kwargs):
    #     pass

    @abstractmethod
    def compute(self, month, **kwargs):
        pass

    def filter(self, **kwargs):
        inputs = self.inputs
        for k, v in kwargs.items():
            inputs = inputs[inputs[k] == v]
        return inputs

    @abstractmethod
    @cached_property
    def inputs_df(self):
        raise NotImplementedError

    @cached_property
    def inputs(self):
        return self.inputs_df.to_dict('records')

    def convert_all(self):
        return [ self.convert_one(**input) for input in self.inputs ]

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


# class Csv(MonthDataset):
#     pass


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
