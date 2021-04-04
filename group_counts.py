#!/usr/bin/env python
# coding: utf-8

from utz import *

Bucket = 'ctbk'

import boto3
from boto3 import client
from botocore.client import Config
s3 = client('s3', config=Config())
s3_resource = boto3.resource('s3')
ObjectAcl = s3_resource.ObjectAcl
resp = s3.list_objects_v2(Bucket=Bucket)
contents = pd.DataFrame(resp['Contents'])
keys = contents.Key

months = keys.str.extract('^(?:JC-)?(?P<yyyy>\d{4})(?P<mm>\d{2}).*\.parquet').dropna()
cur_month = months.apply(lambda m: to_dt('%s-%s' % (m.yyyy, m.mm)), axis=1).max()


def aggregate(
    url,
    agg_keys,
    sum_keys,
):
    print(f'Aggregating: {url}')
    df = read_parquet(url)
    m = match('(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})', basename(url))
    group_keys = []
    if agg_keys.get('r'):
        region = m['region'] or 'NYC'
        df['Region'] = region
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


def inc_month(y, m):
    m += 1
    if m > 12:
        y += 1
        m = 1
    return y, m


def main(
    to,
    max_months,
    counts=True,
    durations=True,
    gender=True,
    region=True,
    user_type=True,
    year=True,
    month=True,
    weekday=False,
    hour=False,
    dst_bucket='ctbk',
    dst_root=None,
    sort_agg_keys=False,
):
    if not to:
        to = now().time
    if isinstance(to, str):
        to = parse(to)

    sum_keys = { k:v for k,v in {'c':counts,'d':durations}.items() if v }
    value = ''.join([ label for label, flag in sum_keys.items() ])
    if not value:
        raise ValueError('Specify at least one eligible y-axis (`counts` or `durations`)')

    agg_keys = {
        'y':year, 'm':month, 'w':weekday, 'h':hour,
        'r':region, 'g':gender, 't':user_type,
    }
    agg_keys = { k:v for k,v in agg_keys.items() if v }
    if sort_agg_keys:
        agg_keys = dict(sorted(list(agg_keys.items()), key=lambda t: t[0]))

    Prefix = f'{"".join(agg_keys.keys())}_{value}/'

    def s3_url(key):
        path = f's3://{dst_bucket}'
        if dst_root:
            path = f'{path}/{dst_root}'
        path = f'{path}/{key}'
        return path

    resp = s3.list_objects_v2(Bucket=Bucket, Prefix=Prefix)
    if 'Contents' in resp:
        contents = pd.DataFrame(resp['Contents'])
        keys = contents.Key
        last_date = \
            keys \
                .str \
                .extract(r'^.*/(?P<year>\d{4})(?P<month>\d{2})\.parquet$') \
                .dropna() \
                .apply(lambda r: to_dt('%s-%s' % (r['year'], r['month'])), axis=1) \
                .max()
        last_year = last_date.year
        last_month = last_date.month
        last_path = '%s%d%02d.parquet' % (Prefix, last_year, last_month)
        s3_last_url = s3_url(last_path)
        new_months = read_parquet(s3_last_url)
    else:
        last_year = 2013
        last_month = 5
        new_months = None

    n = 0
    while True:
        next_year, next_month = inc_month(last_year, last_month)

        if (to.year, to.month) < (next_year, next_month):
            print('Up to date with data through %d-%02d' % (last_year, last_month))
            return

        if max_months == 0:
            print(f'Exiting after {n} iterations')
            break

        name = '%d%02d.parquet' % (next_year, next_month)
        Key = '%s%s' % (Prefix, name)
        new_url = s3_url(Key)

        new_data = False
        for region in ['', 'JC-']:
            new_month_path = '%s%d%02d-citibike-tripdata.parquet' % (region, next_year, next_month)
            new_month_url = s3_url(new_month_path)
            try:
                new_month = aggregate(new_month_url, agg_keys, sum_keys)
            except FileNotFoundError as e:
                if (to.year, to.month) == (next_year, next_month):
                    print("Couldn't find data for final month %d-%02d" % (next_year, next_month))
                    continue
                if (to.year, to.month) == inc_month(next_year, next_month):
                    print("Couldn't find data for penultimate month %d-%02d" % (next_year, next_month))
                    continue
                if region == 'JC-':
                    print("Missing JC data for %d-%02d" % (next_year, next_month))
                    continue
                else:
                    raise e
            new_data = True
            if new_months is None:
                new_months = new_month
            else:
                new_months = concat([ new_months, new_month ])

        if new_months is None:
            return

        if new_data:
            print(f'Writing: {new_url} ({len(new_months)} entries)')
            new_months.to_parquet(new_url)
            object_acl = ObjectAcl(Bucket, Key)
            object_acl.put(ACL='public-read')
        else:
            print('No new data found; breaking')
            break

        last_year, last_month = next_year, next_month
        if max_months > 0:
            max_months -= 1
        n += 1


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-C','--no-counts',action='store_true',help="Omit ride counts from summed keys")
    # parser.add_argument('-d','--duration',action='store_true',help="Include ride duration in summed keys")
    parser.add_argument('-D','--no-duration',action='store_true',help="Omit ride duration from summed keys")
    parser.add_argument('--dst-bucket',default='ctbk',help="Bucket to read from / write to")
    parser.add_argument('--dst-root',help="Prefix within `dst` to read from / write to")

    # Flags for omitting various fields from aggregation
    parser.add_argument('-G','--no-gender',action='store_true',help="Omit rider `gender` from aggregation keys")
    parser.add_argument('--hour',action='store_true',help="Include ride-start `hour` from aggregation keys")
    #parser.add_argument('-H','--no-hour',action='store_true',help="Omit ride-start `hour` from aggregation keys")
    parser.add_argument('-M','--no-month',action='store_true',help="Omit ride-start `month` from aggregation keys")
    parser.add_argument('-R','--no-region',action='store_true',help="Omit ride `region` (JC vs NYC) from aggregation keys")
    parser.add_argument('-T','--no-user-type',action='store_true',help="Omit user `type` (daily 'Customer' vs. annual 'Subscriber') from aggregation keys")
    parser.add_argument('-w','--weekday',action='store_true',help="Include ride-start `weekday` in aggregation keys")
    #parser.add_argument('-W','--no-weekday',action='store_true',help="Omit ride `weekday` from aggregation keys")
    parser.add_argument('-Y','--no-year',action='store_true',help="Omit ride-start `year` from aggregation keys")

    parser.add_argument('--sort-agg-keys',action='store_true',help="Sort the aggregation keys in output filenames")
    parser.add_argument('-n','--max-months',default=0,type=int,help="If there is new data to compute, only compute at most this many months' worth (default: 0, means no limit)")
    parser.add_argument('-t','--to','--through',help="Date (YYYYMM or equivalent) to process through")
    args = parser.parse_args()

    dst_bucket = args.dst_bucket
    dst_root = args.dst_root
    max_months = args.max_months
    if max_months <= 0:
        max_months = -1

    to = args.to
    sort_agg_keys = args.sort_agg_keys

    counts = not args.no_counts
    duration = not args.no_duration
    # duration = args.duration

    gender = not args.no_gender
    hour = args.hour
    # hour = not args.no_hour
    month = not args.no_month
    region = not args.no_region
    user_type = not args.no_user_type
    weekday = args.weekday
    # weekday = not args.no_weekday
    year = not args.no_year

    main(
        to, max_months,
        counts, duration,
        gender, region, user_type,
        year, month, weekday, hour,
        dst_bucket=dst_bucket, dst_root=dst_root,
        sort_agg_keys=sort_agg_keys,
    )
