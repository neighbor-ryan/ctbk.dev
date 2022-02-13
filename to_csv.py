#!/usr/bin/env python
# coding: utf-8

from click import command as cmd, option as opt

from boto3 import client
from botocore.client import Config
from utz import *
from zipfile import ZipFile

from utils import convert_file, BadKey, WROTE, Result

rgx = r'^(?P<JC>JC-)?(?P<year>\d{4})(?P<month>\d{2})[ \-]citibike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'

# s3://tripdata/(?P<JC>JC-)?(?P<month>YYYYMM)-citibike-tripdata(?:\.csv)?.zip → s3://ctbk/original/{JC}{month}-citibike-tripdata.zip
# s3://ctbk/original/{JC}{month}-citibike-tripdata.zip → s3://ctbk/cleaned/{JC}{month}-citibike-tripdata.zip


def to_csv(src_path, src_name, dst_name, tmpdir):
    z = ZipFile(src_path)
    names = z.namelist()
    print(f'{src_name}: zip names: {names}')

    csv_path = f'{tmpdir}/{dst_name}'

    csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
    if len(csvs) == 1:
        [ name ] = csvs
    elif not csvs:
        raise RuntimeError('Found no CSVs in %s' % src_name)
    else:
        raise RuntimeError('Found %d CSVs in %s: %s' % (len(csvs), src_name, ','.join(csvs)))

    with z.open(name,'r') as i, open(csv_path,'wb') as o:
        o.write(i.read())

    return csv_path


def original_to_csv(src_bkt, zip_key, dst_bkt, error='warn', overwrite=False, dst_root=None):
    def dst_key(src_name):
        m = match(rgx, src_name)
        if not m:
            raise BadKey(src_name)
        _, ext = splitext(src_name)
        assert ext == '.zip'

        # normalize the dst path; a few src files have typos/inconsistencies
        base = '%s%s%s-citibike-tripdata' % (m['JC'] or '', m['year'], m['month'])
        if dst_root is None:
            return f'{base}.csv'
        else:
            return f'{dst_root}/{base}.csv'

    return convert_file(
        to_csv,
        src_bkt=src_bkt, src_key=zip_key,
        dst_bkt=dst_bkt, dst_key=dst_key,
        error=error,
        overwrite=overwrite,
    ).msg


@cmd(help='Read Zip files published by Citibike (per {month,region}), extract a lone CSV from inside each, save')
@opt('-s','--src-bucket',default='tripdata',help='Source bucket to read Zip files from')
@opt('-d','--dst-bucket',default='ctbk',help='Destination bucket to write CSV files to')
@opt('-r','--dst-root',default='csvs',help='Prefix (in destination bucket) to write CSVs udner')
@opt('-p','--parallel/--no-parallel',help='Use joblib to parallelize execution')
def main(src_bucket, dst_bucket, dst_root, parallel):
    s3 = client('s3', config=Config())
    resp = s3.list_objects_v2(Bucket=src_bucket)
    contents = pd.DataFrame(resp['Contents'])
    zips = contents[contents.Key.str.endswith('.zip')]
    zips = zips.Key.values
    if parallel:
        p = Parallel(n_jobs=cpu_count())
        print(
            '\n'.join(
                p(
                    delayed(original_to_csv)(
                        src_bucket, zip, dst_bucket, dst_root=dst_root
                    )
                    for zip in zips
                )
            )
        )
    else:
        for zip in zips:
            print(original_to_csv(src_bucket, zip, dst_bucket, dst_root=dst_root))


if __name__ == '__main__':
    main()
