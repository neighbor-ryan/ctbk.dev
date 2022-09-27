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
            # Features to aggregate
            counts=True,
            durations=False,
            # Features to group by
            gender=True,
            region=True,
            user_type=True,
            rideable_type=True,
            year=True,
            month=True,
            weekday=False,
            hour=False,
            sort_agg_keys=True,
            **kwargs
    ):
        self.counts = counts
        self.durations = durations
        self.gender = gender
        self.region = region
        self.user_type = user_type
        self.rideable_type = rideable_type
        self.year = year
        self.month = month
        self.weekday = weekday
        self.hour = hour
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


# @cmd(help='Read normalized, per-month Parquet datasets, aggregate based on various features, write out to Parquet and/or SQLite')
# @opt('-c/-C', '--counts/--no-counts', default=True)
# @opt('-d/-D', '--durations/--no-durations', default=True)
# @opt('-g/-G', '--gender/--no-gender', default=True)
# @opt('-r/-R', '--region/--no-region', default=True)
# @opt('-t/-T', '--user-type/--no-user-type', default=True)
# @opt('-b/-B', '--rideable-type/--no-rideable-type', default=True)
# @opt('-y/-Y', '--year/--no-year', default=True)
# @opt('-m/-M', '--month/--no-month', default=True)
# @opt('-w/-W', '--weekday/--no-weekday', default=False)
# @opt('-h/-H', '--hour/--no-hour', default=False)
# @opt('--src-bucket', default='ctbk')
# @opt('--src-root', default='normalized')
# @opt('--dst-bucket', default='ctbk')
# @opt('--dst-root', default='aggregated')
# @opt('--sort-agg-keys/--no-sort-agg-keys')
# @opt('--parquet/--no-parquet', default=True, help='Write a Parquet version of the output data')
# @opt('--sql/--no-sql', default=True, help='Write a SQLite version of the output data')
# @opt('-p', '--parallel/--no-parallel', help='Use joblib to parallelize execution')
# @opt('-f', '--overwrite/--no-overwrite', help='When set, write files even if they already exist')
# @opt('--public/--no-public', help='Give written objects a public ACL')
# @opt('--start', help='Month to process from (inclusive; in YYYYMM form)')
# @opt('--end', help='Month to process to (exclusive; in YYYYMM form)')
# @opt('-e', '--email', help='Send email about outcome, from MAIL_USERNAME/MAIL_PASSWORD to this address')
# @opt('--smtp', help='SMTP server URL')
# def main(
#     # Eligible "y-axes"; sum these features
#     counts,
#     durations,
#     # Features to group by
#     gender,
#     region,
#     user_type,
#     rideable_type,
#     year,
#     month,
#     weekday,
#     hour,
#     # src/dst path info
#     src_bucket,
#     src_root,
#     dst_bucket,
#     dst_root,
#     # Misc configs
#     sort_agg_keys,
#     parallel,
#     overwrite,
#     public,
#     # Output format
#     parquet,
#     sql,
#     # Date range
#     start,
#     end,
#     email,
#     smtp,
# ):
#     # When running without a specified range, also upload/overwrite a default/mutable ("latest") version of the output
#     # data
#     put_latest = start is None and end is None
#     if start is None:
#         start = Month(2013, 6)
#     else:
#         start = Month(start)
#     if end is None:
#         end = Month() + 1
#     else:
#         end = Month(end)
#
#     s3 = client('s3', config=Config())
#
#     def url(month):
#         if src_root:
#             src_key = f'{src_root}/{month}.parquet'
#         else:
#             src_key = f'{month}.parquet'
#         if s3_exists(src_bucket, src_key, s3):
#             return f's3://{src_bucket}/{src_key}'
#         else:
#             return None
#
#     months = [ dict(month=m, url=url(m)) for m in start.until(end) ]
#     l = 0
#     while l < len(months) and months[l]['url'] is None:
#         l += 1
#     r = len(months) - 1
#     while r >= 0 and months[r]['url'] is None:
#         r -= 1
#
#     original_months = months
#     months = months[l:r+1]
#     if not months:
#         missing_months = original_months
#     else:
#         missing_months = [ m for m in months if m['url'] is None ]
#     if missing_months:
#         raise ValueError(f'Missing months: {[",".join([m["url"] for m in missing_months])]}')
#
#     urls = [ m['url'] for m in months ]
#     months = [ m['month'] for m in months ]
#     start = months[0]
#     end = months[-1] + 1
#
#     sum_keys = { k: v for k, v in { 'c': counts, 'd': durations, }.items() if v }
#     values_label = ''.join([ label for label, flag in sum_keys.items() ])
#     if not values_label:
#         raise ValueError('Specify at least one eligible y-axis (`counts` or `durations`)')
#
#     agg_keys = {
#         'y': year, 'm': month, 'w': weekday, 'h': hour,
#         'r': region, 'g': gender, 't': user_type, 'b': rideable_type,
#     }
#     agg_keys = { k: v for k, v in agg_keys.items() if v }
#     if sort_agg_keys:
#         agg_keys = dict(sorted(list(agg_keys.items()), key=lambda t: t[0]))
#     agg_keys_label = "".join(agg_keys.keys())
#
#     df = partial(get_df, urls=urls, agg_keys=agg_keys, sum_keys=sum_keys, parallel=parallel)
#
#     def write(fmt, dst, dst_bkt, dst_key, s3):
#         if fmt == 'parquet':
#             df().to_parquet(dst)
#         elif fmt == 'sqlite':
#             with NamedTemporaryFile() as f:
#                 df().to_sql(TBL, f'sqlite:///{f.name}', index=False)
#                 print(f'Upload {f.name} to {dst_bkt}:{dst_key}')
#                 s3.upload_file(f.name, dst_bkt, dst_key)
#         else:
#             raise RuntimeError(f'Unrecognized format: {fmt}')
#
#     def convert(fmt):
#         abs_name = "_".join([
#             agg_keys_label,
#             values_label,
#             f'{start}:{end}',
#         ])
#         abs_name += f'.{fmt}'
#
#         def put(name, overwrite=overwrite):
#             if dst_root:
#                 dst_key = f'{dst_root}/{name}'
#             else:
#                 dst_key = name
#
#             dst = f's3://{dst_bucket}/{dst_key}'
#             print(f'Computing: {dst}')
#
#             result = convert_file(
#                 write,
#                 dst=dst,
#                 fmt=fmt,
#                 public=public,
#                 overwrite=overwrite,
#             )
#             print(result.msg)
#             return result
#
#         result = put(abs_name)
#         did_write = result.status != FOUND
#         if put_latest and did_write:
#             latest_name = "_".join([
#                 agg_keys_label,
#                 values_label,
#             ])
#             latest_name += f'.{fmt}'
#             put(latest_name, overwrite=True)
#
#         return result
#
#     written_urls = []
#     if parquet:
#         result = convert('parquet')
#         if result.did_write:
#             written_urls += [ result.dst ]
#     if sql:
#         result = convert('sqlite')
#         if result.did_write:
#             written_urls += [ result.dst ]
#
#     if written_urls and email or smtp:
#         From = env.get('MAIL_FROM')
#         password = env.get('MAIL_PSWD')
#         if not From:
#             raise Exception('MAIL_FROM env var missing')
#         if not password:
#             raise Exception('MAIL_PSWD env var missing')
#         if not email:
#             raise Exception('No "To" email address found')
#         if not smtp:
#             if From.endswith('@gmail.com'):
#                 smtp = 'smtp.gmail.com:587'
#             else:
#                 raise Exception('No SMTP URL found')
#         parsed = urlparse(smtp)
#         if parsed.scheme not in {'http', 'https'}:
#             parsed = urlparse(f'https://{smtp}')
#         smtp_hostname = parsed.hostname
#         smtp_port = parsed.port or 587
#
#         html, text = build_email(written_urls)
#
#         from send_email import send_email
#         send_email(
#             From=From, To=email,
#             smtp_hostname=smtp_hostname, smtp_port=smtp_port,
#             password=password,
#             Subject='ctbk.dev aggregation result',
#             html=html, text=text,
#         )
