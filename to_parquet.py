#!/usr/bin/env python
# coding: utf-8

from boto3 import client
from botocore import UNSIGNED
from botocore.client import Config, ClientError
from utz import *
from zipfile import ZipFile

rgx = r'^(?P<JC>JC-)?(?P<year>\d{4})(?P<month>\d{2})[ \-]citibike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'

fields = {
    'Trip Duration',
    'Start Time',
    'Stop Time',
    'Start Station ID',
    'Start Station Name',
    'Start Station Latitude',
    'Start Station Longitude',
    'End Station ID',
    'End Station Name',
    'End Station Latitude',
    'End Station Longitude',
    'Bike ID',
    'User Type',
    'Birth Year',
    'Gender'
}
def normalize_field(f): return sub(r'\s', '', f.lower())
normalize_fields_map = { normalize_field(f): f for f in fields }
def normalize_fields(df):
    return df.rename(columns={
        col: normalize_fields_map[normalize_field(col)]
        for col in df.columns
    })


def s3_exists(Bucket, Key, s3=None):
    if not s3:
        s3 = client('s3', config=Config(signature_version=UNSIGNED))
    try:
        s3.head_object(Bucket=Bucket, Key=Key)
        return True
    except ClientError:
        return False


def to_parquet(dst_bkt, zip_key, error='warn', overwrite=False, dst_root=None):
    name = basename(zip_key)
    m = match(rgx, name)
    if not m:
        msg = f'Unrecognized key: {name}'
        if error == 'warn':
            print(msg)
            return msg
        else:
            raise Exception(msg)
    base, ext = splitext(zip_key)
    assert ext == '.zip'
    if base.endswith('.csv'):
        base = splitext(base)[0]

    # normalize the dst path; a few src files have typos/inconsistencies
    base = '%s%s%s-citibike-tripdata' % (m['JC'] or '', m['year'], m['month'])
    if dst_root is None:
        dst_key = f'{base}.parquet'
    else:
        dst_key = f'{dst_root}/{base}.parquet'
    dst = f's3://{dst_bkt}/{dst_key}'
    s3 = client('s3', config=Config())
    if s3_exists(dst_bkt, dst_key, s3=s3):
        if overwrite:
            msg = f'Overwrote {dst}'
            print(f'Overwriting {dst}')
        else:
            msg = f'Found {dst}; skipping'
            print(msg)
            return msg
    else:
        msg = f'Wrote {dst}'

    with TemporaryDirectory() as d:
        zip_path = f'{d}/{base}.zip'
        pqt_path = f'{d}/{base}.parquet'
        s3.download_file(src_bkt, zip_key, zip_path)
        z = ZipFile(zip_path)
        names = z.namelist()
        print(f'{name}: zip names: {names}')
        [ name ] = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
        with z.open(name,'r') as i:
            df = pd.read_csv(i)
            df = normalize_fields(df)
            df = df.astype({'Start Time':'datetime64[ns]','Stop Time':'datetime64[ns]'})
            df.to_parquet(pqt_path)

        s3.upload_file(pqt_path, dst_bkt, dst_key)

        return msg


def main(src_bkt, dst_bkt, dst_root):
    s3 = client('s3', config=Config())

    resp = s3.list_objects_v2(Bucket=src_bkt)
    contents = pd.DataFrame(resp['Contents'])
    zips = contents[contents.Key.str.endswith('.zip')]

    parallel = Parallel(n_jobs=cpu_count())
    print('\n'.join(parallel(delayed(to_parquet)(dst_bkt, f, dst_root=dst_root) for f in zips.Key.values)))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-s','--src',default='tripdata',help='`src` bucket to sync from')
    parser.add_argument('-d','--dst',default='ctbk',help='`dst` bucket to sync converted/cleaned data to')
    parser.add_argument('-r','--dst-root',help='Prefix under `dst` to sync converted/cleaned data to')
    args = parser.parse_args()
    src_bkt = args.src
    dst_bkt = args.dst
    dst_root = args.dst_root

    main(src_bkt, dst_bkt, dst_root)
