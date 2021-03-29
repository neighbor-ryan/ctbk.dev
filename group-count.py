#!/usr/bin/env python
# coding: utf-8

from utz import *

Bucket = 'ctbk'

from boto3 import client
from botocore.client import Config
s3 = client('s3', config=Config())
resp = s3.list_objects_v2(Bucket=Bucket)
contents = pd.DataFrame(resp['Contents'])
keys = contents.Key

months = keys.str.extract('^(?:JC-)?(?P<yyyy>\d{4})(?P<mm>\d{2}).*\.parquet').dropna()
cur_month = months.apply(lambda m: to_dt('%s-%s' % (m.yyyy, m.mm)), axis=1).max()


def sum_by_wd_gender(url):
    print(f'Aggregating: {url}')
    df = read_parquet(url)
    m = match('(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})', basename(url))
    region = m['region'] or 'NYC'
    df['Region'] = region
    df['Start Year'] = df['Start Time'].dt.year
    df['Start Month'] = df['Start Time'].dt.month
    df['Start Day'] = df['Start Time'].dt.day
    df['Start Weekday'] = df['Start Time'].dt.weekday
    df['Start Hour'] = df['Start Time'].dt.hour
    counts = \
        df \
        .groupby(['Start Year','Start Month','Start Weekday','Start Hour','Region','Gender',])['Start Year'] \
        .count() \
        .rename('Count') \
        .reset_index()
    counts['Month'] = counts.apply(
        lambda r: to_dt(
            '%d-%02d' % (int(r['Start Year']), int(r['Start Month']))
        ),
        axis=1
    )
    return counts


def main(
    to,
    max_months,
    duration=False,
    gender=True,
    region=True,
    year=True,
    month=True,
    weekday=True,
    hour=True,
    dst_bucket='ctbk',
    dst_root=None,
):
    if not to:
        to = now().time
    if isinstance(to, str):
        to = parse(to)
    if duration:
        raise NotImplementedError('Ride-duration weighted sums not implemented yet, just ride-counts')
    value = 'durations' if duration else 'rides'
    agg_keys = {
        'year':year, 'month':month, 'weekday':weekday, 'hour':hour,
        'region':region, 'gender':gender,
    }
    if not all(agg_keys.values()):
        raise NotImplementedError('TODO: secondary aggregations')
    Prefix = f'{"-".join([ k for k,v in agg_keys.items() if v ])}_{value}_'

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
                .extract(r'^.*_.*_(?P<year>\d{4})(?P<month>\d{2})') \
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
        next_month = last_month + 1
        if next_month > 12:
            next_year = last_year + 1
            next_month = 1
        else:
            next_year = last_year

        if (to.year, to.month) < (next_year, next_month):
            print('Up to date with data through %d-%02d' % (last_year, last_month))
            return

        if max_months == 0:
            print(f'Exiting after {n} iterations')
            break

        new_path = '%s%d%02d.parquet' % (Prefix, next_year, next_month)
        new_url = s3_url(new_path)

        new_data = False
        for region in ['', 'JC-']:
            new_month_path = '%s%d%02d-citibike-tripdata.parquet' % (region, next_year, next_month)
            new_month_url = s3_url(new_month_path)
            try:
                new_month = sum_by_wd_gender(new_month_url)
            except FileNotFoundError as e:
                if region == 'JC-':
                    continue
                if (to.year, to.month) == (next_year, next_month):
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
            print(f'Writing: {new_url}')
            new_months.to_parquet(new_url)
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
    parser.add_argument('-d','--duration',action='store_true',help="Aggregate/Sum ride *duration* (as opposed to default ride *count*)")
    parser.add_argument('--dst-bucket',default='ctbk',help="Bucket to read from / write to")
    parser.add_argument('--dst-root',help="Prefix within `dst` to read from / write to")
    parser.add_argument('-G','--no-gender',action='store_true',help="Don't include rider `gender` among aggregation keys")
    parser.add_argument('-H','--no-hour',action='store_true',help="Don't include ride `hour` among aggregation keys")
    parser.add_argument('-M','--no-month',action='store_true',help="Don't include ride `month` among aggregation keys")
    parser.add_argument('-n','--max-months',default=0,type=int,help="If there is new data to compute, only compute at most this many months' worth (default: 0, means no limit)")
    parser.add_argument('-R','--no-region',action='store_true',help="Don't include ride `region` (JC vs NYC) among aggregation keys")
    parser.add_argument('-t','--to','--through',help="Date (YYYYMM or equivalent) to process through")
    parser.add_argument('-W','--no-weekday',action='store_true',help="Don't include ride `weekday` among aggregation keys")
    parser.add_argument('-Y','--no-year',action='store_true',help="Don't include ride `year` among aggregation keys")
    args = parser.parse_args()
    dst_bucket = args.dst_bucket
    dst_root = args.dst_root
    duration = args.duration
    gender = not args.no_gender
    hour = not args.no_hour
    max_months = args.max_months
    if max_months <= 0:
        max_months = -1
    month = not args.no_month
    region = not args.no_region
    to = args.to
    weekday = not args.no_weekday
    year = not args.no_year
    main(to, max_months, duration, gender, region, year, month, weekday, hour, dst_bucket, dst_root)
