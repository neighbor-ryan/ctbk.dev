#!/usr/bin/env python

from click import option
from utz import *

from ctbk import NormalizedMonths
from ctbk.aggregator import Aggregator
from ctbk.monthly import Reducer, BKT, PARQUET_EXTENSION, SQLITE_EXTENSION


class GroupCounts(Aggregator, Reducer):
    SRC_CLS = NormalizedMonths
    ROOT = f'{BKT}/aggregated'
    TBL = 'agg'

    @classmethod
    def cli_opts(cls):
        return super().cli_opts() + [
            option('--email', help='Send email about outcome, from MAIL_USERNAME/MAIL_PASSWORD to this address'),
            option('--smtp', help='SMTP server URL'),
            option('--sql/--no-sql', help=f'Write a SQLite version of the output data (to default table {cls.TBL})'),
            option('--tbl', '--table', help=f'Write a SQLite version of the output data to this table name (default: {cls.TBL})'),
        ]

    def __init__(
            self,
            email=None,
            smtp=None,
            sql=False,
            tbl=None,
            **kwargs
    ):
        self.email = email
        self.smtp = smtp
        if tbl:
            sql = True
        self.sql = sql
        self.tbl = tbl or self.TBL
        super().__init__(**kwargs)

    def reduced_df_path(self, month):
        pcs = [
            self.agg_keys_label,
            self.sum_keys_label,
            f'{month}'
        ]
        name = "_".join(pcs)
        return f'{self.root}/{name}{PARQUET_EXTENSION}'

    def reduce(self, df):
        agg_keys = self.agg_keys
        sum_keys = self.sum_keys
        group_keys = []
        if agg_keys.get('r'):
            df = df.rename(columns={'Start Region': 'Region'})  # assign rides to the region they originated in
            group_keys.append('Region')
        if agg_keys.get('y'):
            if 'Start Year' not in df:
                df['Start Year'] = df['Start Time'].dt.year
            group_keys.append('Start Year')
        if agg_keys.get('m'):
            if 'Start Month' not in df:
                df['Start Month'] = df['Start Time'].dt.month
            group_keys.append('Start Month')
        if agg_keys.get('d'):
            if 'Start Day' not in df:
                df['Start Day'] = df['Start Time'].dt.day
            group_keys.append('Start Day')
        if agg_keys.get('w'):
            if 'Start Weekday' not in df:
                df['Start Weekday'] = df['Start Time'].dt.weekday
            group_keys.append('Start Weekday')
        if agg_keys.get('h'):
            if 'Start Hour' not in df:
                df['Start Hour'] = df['Start Time'].dt.hour
            group_keys.append('Start Hour')
        if agg_keys.get('g'):
            group_keys.append('Gender')
        if agg_keys.get('t'):
            group_keys.append('User Type')
        if agg_keys.get('b'):
            group_keys.append('Rideable Type')
        if agg_keys.get('s'):
            group_keys.append('Start Station ID')
        if agg_keys.get('e'):
            group_keys.append('End Station ID')

        select_keys = []
        if sum_keys.get('c'):
            if 'Count' not in df:
                df['Count'] = 1
            select_keys.append('Count')
        if sum_keys.get('d'):
            if 'Duration' not in df:
                df['Duration'] = (df['Stop Time'] - df['Start Time']).dt.seconds
            select_keys.append('Duration')

        grouped = df.groupby(group_keys)
        counts = (
            grouped
            [select_keys]
                .sum()
                .reset_index()
        )
        if agg_keys.get('y') and agg_keys.get('m'):
            counts['Month'] = counts.apply(
                lambda r: to_dt(
                    '%d-%02d' % (int(r['Start Year']), int(r['Start Month']))
                ),
                axis=1
            )
        return counts

    def combine(self, reduced_dfs):
        return self.reduce(pd.concat(reduced_dfs))

    def convert_one(self, task, overwrite: bool = False, **kwargs):
        result = super().convert_one(task, overwrite=overwrite, **kwargs)
        if result.did_write:
            dst = result.dst
            all_dst = result.attrs.get('all_dst')
            written_urls = [dst]
            if all_dst:
                written_urls.append(all_dst)
            if self.sql:
                df = result.value
                if not isinstance(df, pd.DataFrame):
                    stderr.write(f"result value is not a DataFrame, can't write SQLite db: {df}\n")
                else:
                    pqt_dsts = [ dst ]
                    if all_dst:
                        pqt_dsts.append(all_dst)
                    for pqt_dst in pqt_dsts:
                        db_dst = splitext(pqt_dst)[0] + SQLITE_EXTENSION
                        if self.fs.exists(db_dst) and not overwrite:
                            print(f'db exists, skipping: {db_dst}')
                            continue
                        if self.fs.protocol == 'file':
                            con = f'sqlite:///{db_dst}'
                            if_exists = 'replace' if overwrite else 'fail'
                            df.to_sql(self.tbl, con, if_exists=if_exists, index=False)
                        else:
                            with TemporaryDirectory() as tmpdir:
                                path = f'{tmpdir}/{basename(db_dst)}'
                                con = f'sqlite:///{path}'
                                df.to_sql(self.tbl, con, index=False)
                                with open(path, 'rb') as i, self.fs.open(db_dst, 'wb') as o:
                                    shutil.copyfileobj(i, o)
                        written_urls.append(db_dst)
            if self.email:
                self.maybe_email(written_urls)
        return result

    def maybe_email(self, written_urls):
        email, smtp = self.email, self.smtp
        if written_urls and (email or smtp):
            From = env.get('MAIL_FROM')
            password = env.get('MAIL_PSWD')
            if not From:
                raise Exception('MAIL_FROM env var missing')
            if not password:
                raise Exception('MAIL_PSWD env var missing')
            if not email:
                raise Exception('No "To" email address found')
            if not smtp:
                if From.endswith('@gmail.com'):
                    smtp = 'smtp.gmail.com:587'
                else:
                    raise Exception('No SMTP URL found')
            parsed = urlparse(smtp)
            if parsed.scheme not in {'http', 'https'}:
                parsed = urlparse(f'https://{smtp}')
            smtp_hostname = parsed.hostname
            smtp_port = parsed.port or 587

            html, text = self.build_email(written_urls)

            from send_email import send_email
            send_email(
                From=From, To=email,
                smtp_hostname=smtp_hostname, smtp_port=smtp_port,
                password=password,
                Subject='ctbk.dev aggregation result',
                html=html, text=text,
            )
            print('Emailed!')

    @staticmethod
    def build_email(written_urls):
        from html_dsl.common import HTML, BODY, DIV, P, UL, LI, A
        if written_urls:
            urls_str = "\n- ".join(written_urls)
            text = f'''Wrote:\n- {urls_str}'''
            html = DIV[
                P['Wrote:'],
                P[UL[[ LI[url] for url in written_urls ]]],
                P[
                    'See ', A(href='https://ctbk.s3.amazonaws.com/index.html#/aggregated?s=50')['s3://ctbk/aggregated/'],
                    ' and ', A(href='https://ctbk.dev')['ctbk.dev'],
                ],
            ]
        else:
            text = 'No files written.'
            html = P['No files written']

        html = HTML[BODY[html]]
        return str(html), text


if __name__ == '__main__':
    GroupCounts.cli()
