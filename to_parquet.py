#!/usr/bin/env python

from utz import *

src_bkt = 'tripdata'
dst_bkt = 'runsascoded'
dst_root = 'citibike'

from boto3 import client
from botocore import UNSIGNED
from botocore.client import Config
s3 = client('s3', config=Config(signature_version=UNSIGNED))
resp = s3.list_objects_v2(Bucket=src_bkt)
contents = pd.DataFrame(resp['Contents'])
zips = contents[contents.Key.str.endswith('.zip')]

# match monthly-zip filenames
rgx = r'^(?P<JC>JC-)?(?P<year>\d{4})(?P<month>\d{2})[ \-]citibike-tripdata(?P<csv>\.csv)?(?P<zip>\.zip)?$'

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

from zipfile import ZipFile

def to_parquet(zip_path, error='warn', overwrite=False):
    name = basename(zip_path)
    m = match(rgx, name)
    if not m:
        msg = f'Unrecognized key: {name}'
        if error == 'warn':
            print(msg)
            return msg
        else:
            raise Exception(msg)
    assert name.endswith('.zip'), name
    base = splitext(zip_path)[0]
    if base.endswith('.csv'):
        base = splitext(base)[0]

    pqt_path = f'{base}.parquet'
    if exists(pqt_path):
        if overwrite:
            msg = f'Overwrote {pqt_path}'
            print(f'Overwriting {pqt_path}')
        else:
            msg = f'Found {pqt_path}; skipping'
            print(msg)
            return msg
    else:
        msg = f'Wrote {pqt_path}'

    z = ZipFile(zip_path)
    names = z.namelist()
    print(f'{name}: zip names: {names}')
    [ name ] = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
    with z.open(name,'r') as i:
        df = pd.read_csv(i)
        df = normalize_fields(df)
        df = df.astype({'Start Time':'datetime64[ns]','Stop Time':'datetime64[ns]'})
        df.to_parquet(pqt_path)

    return msg


cached_zips = sorted(glob(f'{cache}/*.zip'))

from joblib import delayed, Parallel
parallel = Parallel(n_jobs=cpu_count())

parallel(delayed(to_parquet)(f) for f in cached_zips)
