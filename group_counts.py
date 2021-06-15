#!/usr/bin/env python

from utz import *

from boto3 import client
from botocore.client import Config
from click import command as cmd, option as opt

from utils import convert_file, Month, s3_exists


def aggregate(
    url,
    agg_keys,
    sum_keys,
):
    print(f'Aggregating: {url}')
    df = read_parquet(url)
    group_keys = []
    if agg_keys.get('r'):
        # region = m['region'] or 'NYC'
        # df['Region'] = region
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
    counts = \
        grouped \
            [select_keys] \
            .sum() \
            .reset_index()
    counts['Month'] = counts.apply(
        lambda r: to_dt(
            '%d-%02d' % (int(r['Start Year']), int(r['Start Month']))
        ),
        axis=1
    )
    return counts


def aggregate_months(urls, agg_keys, sum_keys, parallel, dst):
    if parallel:
        p = Parallel(n_jobs=cpu_count())
        dfs = p(delayed(aggregate)(url, agg_keys, sum_keys) for url in urls)
    else:
        dfs = [ aggregate(url, agg_keys, sum_keys) for url in urls ]

    df = pd.concat(dfs)
    df.to_parquet(dst)


@cmd()
@opt('-c/-C','--counts/--no-counts',default=True)
@opt('-d/-D','--durations/--no-durations',default=True)
@opt('-g/-G','--gender/--no-gender',default=True)
@opt('-r/-R','--region/--no-region',default=True)
@opt('-t/-T','--user-type/--no-user-type',default=True)
@opt('-b/-B','--rideable-type/--no-rideable-type',default=True)
@opt('-y/-Y','--year/--no-year',default=True)
@opt('-m/-M','--month/--no-month',default=True)
@opt('-w/-W','--weekday/--no-weekday',default=False)
@opt('-h/-H','--hour/--no-hour',default=False)
@opt('--src-bucket',default='ctbk')
@opt('--src-root',default='normalized')
@opt('--dst-bucket',default='ctbk')
@opt('--dst-root',default='aggregated')
@opt('--sort-agg-keys/--no-sort-agg-keys')
# @opt('--parquet/--no-parquet', default=True, help='Write a Parquet version of the output data')
# @opt('--sql/--no-sql',help='Write a SQLite version of the output data')
@opt('-p','--parallel/--no-parallel',help='Use joblib to parallelize execution')
@opt('-f','--overwrite/--no-overwrite',help='When set, write files even if they already exist')
@opt('--public/--no-public',help='Give written objects a public ACL')
@opt('--start',help='Month to process from (inclusive; in YYYYMM form)')
@opt('--end',help='Month to process to (exclusive; in YYYYMM form)')
def main(
    counts,
    durations,
    gender,
    region,
    user_type,
    rideable_type,
    year,
    month,
    weekday,
    hour,
    src_bucket,
    src_root,
    dst_bucket,
    dst_root,
    sort_agg_keys,
    parallel,
    overwrite,
    # parquet,
    # sql,
    public,
    start,
    end,
):
    if start is None:
        start = Month(2013, 6)
    else:
        start = Month(start)
    if end is None:
        end = Month() + 1
    else:
        end = Month(end)

    s3 = client('s3', config=Config())

    def url(month):
        if src_root:
            src_key = f'{src_root}/{month}.parquet'
        else:
            src_key = f'{month}.parquet'
        if s3_exists(src_bucket, src_key, s3):
            return f's3://{src_bucket}/{src_key}'
        else:
            return None

    months = [ dict(month=month, url=url(month)) for month in start.until(end) ]
    l = 0
    while l < len(months) and months[l]['url'] is None:
        l += 1
    r = len(months) - 1
    while r >= 0 and months[r]['url'] is None:
        r -= 1

    original_months = months
    months = months[l:r+1]
    if not months:
        missing_months = original_months
    else:
        missing_months = [ m for m in months if m['url'] is None ]
    if missing_months:
        raise ValueError(f'Missing months: {[",".join([m["url"] for m in missing_months])]}')

    urls = [ m['url'] for m in months ]
    months = [ m['month'] for m in months ]
    start = months[0]
    end = months[-1] + 1

    sum_keys = { k:v for k,v in {'c':counts,'d':durations}.items() if v }
    values_label = ''.join([ label for label, flag in sum_keys.items() ])
    if not values_label:
        raise ValueError('Specify at least one eligible y-axis (`counts` or `durations`)')

    agg_keys = {
        'y':year, 'm':month, 'w':weekday, 'h':hour,
        'r':region, 'g':gender, 't':user_type, 'b':rideable_type,
    }
    agg_keys = { k:v for k,v in agg_keys.items() if v }
    if sort_agg_keys:
        agg_keys = dict(sorted(list(agg_keys.items()), key=lambda t: t[0]))
    agg_keys_label = "".join(agg_keys.keys())

    name = "_".join([
        agg_keys_label,
        values_label,
        f'{start}:{end}',
    ])
    name += '.parquet'

    if dst_root:
        dst_key = f'{dst_root}/{name}'
    else:
        dst_key = name

    dst = f's3://{dst_bucket}/{dst_key}'
    print(f'Computing: {dst}')
    result = convert_file(
        aggregate_months,
        urls=urls,
        agg_keys=agg_keys,
        sum_keys=sum_keys,
        parallel=parallel,
        dst=dst,
        public=public,
        overwrite=overwrite,
    )
    print(result.get('msg'))


if __name__ == '__main__':
    main()
