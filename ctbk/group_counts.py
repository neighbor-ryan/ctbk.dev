#!/usr/bin/env python

import click
from utz import *

from ctbk import NormalizedMonths, Monthy
from ctbk.monthly import Reducer, BKT, PARQUET_EXTENSION

TBL = 'agg'


# def aggregate_months(urls, agg_keys, sum_keys, parallel):
#     if parallel:
#         p = Parallel(n_jobs=cpu_count())
#         dfs = p(delayed(aggregate)(url, agg_keys, sum_keys) for url in urls)
#     else:
#         dfs = [ aggregate(url, agg_keys, sum_keys) for url in urls ]
#
#     df = pd.concat(dfs)
#     return df
#
#
# def build_email(written_urls):
#     from html_dsl.common import HTML, BODY, DIV, P, UL, LI, A
#     if written_urls:
#         urls_str = "\n- ".join(written_urls)
#         text = f'''Wrote:\n- {urls_str}'''
#         html = DIV[
#             P['Wrote:'],
#             P[UL[[ LI[url] for url in written_urls ]]],
#             P[
#                 'See ', A(href='https://ctbk.s3.amazonaws.com/index.html#/aggregated?s=50')['s3://ctbk/aggregated/'],
#                 ' and ', A(href='https://ctbk.dev')['ctbk.dev'],
#             ],
#         ]
#     else:
#         text = 'No files written.'
#         html = P['No files written']
#
#     html = HTML[BODY[html]]
#
#     return str(html), text
#
#
# _df = None
# def get_df(*, urls, agg_keys, sum_keys, parallel):
#     global _df
#     if _df is None:
#         _df = aggregate_months(
#             urls=urls,
#             agg_keys=agg_keys,
#             sum_keys=sum_keys,
#             parallel=parallel,
#         )
#     return _df


class GroupCounts(Reducer):
    SRC_CLS = NormalizedMonths
    ROOT = f'{BKT}/aggregated'

    def __init__(
            self,
            # Features to group by
            year=True,
            month=True,
            weekday=False,
            hour=False,
            region=True,
            gender=True,
            user_type=True,
            rideable_type=True,
            # Features to aggregate
            counts=True,
            durations=True,
            # Misc
            sort_agg_keys=True,
            **kwargs
    ):
        self.year = year
        self.month = month
        self.weekday = weekday
        self.hour = hour
        self.region = region
        self.gender = gender
        self.user_type = user_type
        self.rideable_type = rideable_type
        self.counts = counts
        self.durations = durations
        self.sort_agg_keys = sort_agg_keys

        super().__init__(**kwargs)

    @classmethod
    def cli_opts(cls):
        return super().cli_opts() + [
            click.option('-c/-C', '--counts/--no-counts', default=True),
            click.option('-d/-D', '--durations/--no-durations', default=True),
            click.option('-g/-G', '--gender/--no-gender', default=True),
            click.option('-r/-R', '--region/--no-region', default=True),
            click.option('-t/-T', '--user-type/--no-user-type', default=True),
            click.option('-b/-B', '--rideable-type/--no-rideable-type', default=True),
            click.option('-y/-Y', '--year/--no-year', default=True),
            click.option('-m/-M', '--month/--no-month', default=True),
            click.option('-w/-W', '--weekday/--no-weekday', default=False),
            click.option('-h/-H', '--hour/--no-hour', default=False),
            click.option('--sort-agg-keys/--no-sort-agg-keys'),
        ]

    @property
    def agg_keys(self):
        agg_keys = {
            'y': self.year,
            'm': self.month,
            'w': self.weekday,
            'h': self.hour,
            'r': self.region,
            'g': self.gender,
            't': self.user_type,
            'b': self.rideable_type,
        }
        return { k: v for k, v in agg_keys.items() if v }

    @property
    def agg_keys_label(self):
        agg_keys = self.agg_keys
        if self.sort_agg_keys:
            agg_keys = dict(sorted(list(agg_keys.items()), key=lambda t: t[0]))
        return "".join(agg_keys.keys())

    @property
    def sum_keys(self):
        return { k: v for k, v in { 'c': self.counts, 'd': self.durations, }.items() if v }

    @property
    def sum_keys_label(self):
        return ''.join([ label for label, flag in self.sum_keys.items() ])

    def reduced_df_path(self, month):
        pcs = [
            self.agg_keys_label,
            self.sum_keys_label,
            f'{month}'
        ]
        name = "_".join(pcs)
        return f'{self.root}/{name}{PARQUET_EXTENSION}'

    def path(self, start=None, end=None, extension=PARQUET_EXTENSION, root=None):
        pcs = [
            self.agg_keys_label,
            self.sum_keys_label,
        ]
        if start and end:
            pcs += [f'{start}:{end}']
        name = "_".join(pcs) + extension
        return f'{root or self.root}/{name}'

    def reduce(self, df):
        agg_keys = self.agg_keys
        sum_keys = self.sum_keys
        group_keys = []
        if agg_keys.get('r'):
            df['Region'] = df['Start Region']  # assign rides to the region they originated in
            group_keys.append('Region')
        if agg_keys.get('y'):
            df['Start Year'] = df['Start Time'].dt.year
            group_keys.append('Start Year')
        if agg_keys.get('m'):
            df['Start Month'] = df['Start Time'].dt.month
            group_keys.append('Start Month')
        if agg_keys.get('d'):
            df['Start Day'] = df['Start Time'].dt.day
            group_keys.append('Start Day')
        if agg_keys.get('w'):
            df['Start Weekday'] = df['Start Time'].dt.weekday
            group_keys.append('Start Weekday')
        if agg_keys.get('h'):
            df['Start Hour'] = df['Start Time'].dt.hour
            group_keys.append('Start Hour')
        if agg_keys.get('g'):
            group_keys.append('Gender')
        if agg_keys.get('t'):
            group_keys.append('User Type')
        if agg_keys.get('b'):
            group_keys.append('Rideable Type')

        select_keys = []
        if sum_keys.get('c'):
            df['Count'] = 1
            select_keys.append('Count')
        if sum_keys.get('d'):
            df['Duration'] = (df['Stop Time'] - df['Start Time']).dt.seconds
            select_keys.append('Duration')

        grouped = df.groupby(group_keys)
        counts = (
            grouped
            [select_keys]
                .sum()
                .reset_index()
        )
        counts['Month'] = counts.apply(
            lambda r: to_dt(
                '%d-%02d' % (int(r['Start Year']), int(r['Start Month']))
            ),
            axis=1
        )
        return counts


if __name__ == '__main__':
    GroupCounts.cli()
