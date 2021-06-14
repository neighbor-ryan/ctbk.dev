#!/usr/bin/env python
# coding: utf-8

from utz import *

import boto3
from boto3 import client
from botocore.client import Config
from click import argument as arg, command as cmd, option as opt


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
        if 'Gender' not in df:
            stderr.write('%s: gender not found; setting to 0 ("unknown") for all rows\n' % (url))
            df['Gender'] = '0'
        group_keys.append('Gender')
    if agg_keys.get('t'):
        if 'User Type' not in df:
            assert 'Member/Casual' in df
            df = df.rename(columns={'Member/Casual':'User Type'})
        group_keys.append('User Type')
    if agg_keys.get('b'):
        if 'Rideable Type' not in df:
            stderr.write('%s: "Rideable Type" not found; setting to "docked bike" for all rows\n' % (url))
            df['Rideable Type'] = 'docked bike'
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


def inc_month(y, m):
    m += 1
    if m > 12:
        y += 1
        m = 1
    return y, m


@cmd()
@arg('to',nargs=1,required=False,default=None)
@opt('-n','--max-months',type=int,default=-1)
@opt('-c/-C','--counts/--no-counts',default=True)
@opt('-d/-D','--durations/--no-durations',default=True)
@opt('-g/-G','--gender/--no-gender',default=True)
@opt('-r/-R','--region/--no-region',default=True)
@opt('-t/-T','--user-type/--no-user-type',default=True)
@opt('-b/-B','--rideable-type/--no-rideable-type',default=True)
@opt('-y/-Y','--year/--no-year',default=True)
@opt('-m/-M','--month/--no-month',default=True)
@opt('-w/-W','--weekday/--no-weekday',default=False)
@opt('-h/-H','--hour/--no-hour',default=True)
@opt('--src-root',default='pqts')
@opt('--dst-bucket',default='ctbk')
@opt('--dst-root')
@opt('--sort-agg-keys/--no-sort-agg-keys')
# @opt('-f','--overwrite/--no-overwrite',help='When set, write files even if they already exist')
# @opt('--start',type=int,help='Month to process from (in YYYYMM form)')
def main(
    to,
    max_months,
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
    src_root,
    dst_bucket,
    dst_root,
    sort_agg_keys,
    # overwrite,
    # start,
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
        'r':region, 'g':gender, 't':user_type, 'b':rideable_type,
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

    Bucket = 'ctbk'
    s3 = client('s3', config=Config())
    s3_resource = boto3.resource('s3')
    ObjectAcl = s3_resource.ObjectAcl
    # resp = s3.list_objects_v2(Bucket=Bucket)
    # contents = pd.DataFrame(resp['Contents'])
    # keys = contents.Key

    # months = keys.str.extract('^(?:JC-)?(?P<yyyy>\d{4})(?P<mm>\d{2}).*\.parquet').dropna()
    #cur_month = months.apply(lambda m: to_dt('%s-%s' % (m.yyyy, m.mm)), axis=1).max()

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
            if src_root:
                new_month_path = f'{src_root}/{new_month_path}'
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
    main()
