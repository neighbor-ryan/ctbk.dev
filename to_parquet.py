#!/usr/bin/env python
# coding: utf-8

import boto3
from boto3 import client
from botocore.client import Config
from utz import *
from zipfile import ZipFile

from utils import s3_exists


rgx = r'^(?P<JC>JC-)?(?P<year>\d{4})(?P<month>\d{2})[ \-]citibike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'

# s3://tripdata/(?P<JC>JC-)?(?P<month>YYYYMM)-citibike-tripdata(?:\.csv)?.zip → s3://ctbk/original/{JC}{month}-citibike-tripdata.zip
# s3://ctbk/original/{JC}{month}-citibike-tripdata.zip → s3://ctbk/cleaned/{JC}{month}-citibike-tripdata.zip


def original_to_parquet(dst_bkt, zip_key, error='warn', overwrite=False, dst_root=None):
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

    s3_resource = boto3.resource('s3')
    ObjectAcl = s3_resource.ObjectAcl

    with TemporaryDirectory() as d:
        zip_path = f'{d}/{base}.zip'
        csv_path = f'{d}/{base}.csv'
        pqt_path = f'{d}/{base}.parquet'
        s3.download_file(src_bkt, zip_key, zip_path)
        z = ZipFile(zip_path)
        names = z.namelist()
        print(f'{name}: zip names: {names}')

        csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
        if len(csvs) == 1:
            [ name ] = csvs
        elif not csvs:
            raise RuntimeError('Found no CSVs in %s' % zip_path)
        else:
            raise RuntimeError('Found %d CSVs in %s: %s' % (len(csvs), zip_path, ','.join(csvs)))

        with z.open(name,'rb') as i, open(csv_path,'wb') as o:
            o.write(i.read())

        with z.open(name,'r') as i:
            df = pd.read_csv(i)
            df.to_parquet(pqt_path)

        s3.upload_file(pqt_path, dst_bkt, dst_key)
        object_acl = ObjectAcl(dst_bkt, dst_key)
        object_acl.put(ACL='public-read')

        return msg


def main(src_bkt, dst_bkt, dst_root):
    s3 = client('s3', config=Config())

    resp = s3.list_objects_v2(Bucket=src_bkt)
    contents = pd.DataFrame(resp['Contents'])
    zips = contents[contents.Key.str.endswith('.zip')]

    parallel = Parallel(n_jobs=cpu_count())
    print(
        '\n'.join(
            parallel(
                delayed(original_to_parquet)(
                    dst_bkt,
                    f,
                    dst_root=dst_root,
                )
                for f in zips.Key.values
            )
        )
    )


if __name__ == '__main__':
    main()
    # parser = ArgumentParser()
    # parser.add_argument('-s','--src',default='tripdata',help='`src` bucket to sync from')
    # parser.add_argument('-d','--dst',default='ctbk',help='`dst` bucket to sync converted/cleaned data to')
    # parser.add_argument('-r','--dst-root',default='original',help='Prefix under `dst` to sync converted/cleaned data to')
    # args = parser.parse_args()
    # src_bkt = args.src
    # dst_bkt = args.dst
    # dst_root = args.dst_root
    #
    # main(src_bkt, dst_bkt, dst_root)
