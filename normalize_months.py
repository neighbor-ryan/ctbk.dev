from utz import *

import boto3
from boto3 import client
from botocore.client import Config

from .utils import s3_exists

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
    rename_dict = {}
    for col in df.columns:
        normalized_col = normalize_field(col)
        if normalized_col in normalize_fields_map:
            rename_dict[col] = normalize_fields_map[normalized_col]
        else:
            stderr.write('Unexpected field: %s (%s)\n' % (normalized_col, col))
    return df.rename(columns=rename_dict)


def normalize_parquet(bkt, src_key, dst_root, overwrite=False):
    s3 = client('s3', config=Config())
    name = basename(src_key)
    dst_key = f'{dst_root}/{name}'
    src = f's3://{bkt}/{src_key}'
    dst = f's3://{bkt}/{dst_key}'
    if s3_exists(bkt, dst_key, s3=s3):
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

    df = pd.read_parquet(src)
    df = normalize_fields(df)
    df = df.astype({'Start Time':'datetime64[ns]','Stop Time':'datetime64[ns]'})
    df.to_parquet(dst)

    #s3.upload_file(pqt_path, dst_bkt, dst_key)
    object_acl = ObjectAcl(bkt, dst_key)
    object_acl.put(ACL='public-read')

    return msg


def main(bkt, src_root, dst_root):
    s3 = client('s3', config=Config())

    if not src_root.endswith('/'):
        src_root += '/'
    resp = s3.list_objects_v2(Bucket=bkt, Key=src_root)
    contents = pd.DataFrame(resp['Contents'])
    pqts = contents[contents.Key.str.endswith('.parquet')]

    parallel = Parallel(n_jobs=cpu_count())
    print(
        '\n'.join(
            parallel(
                delayed(normalize_parquet)(
                    bkt=bkt,
                    src_key=pqt,
                    dst_root=dst_root,
                )
                for pqt in pqts.Key.values
            )
        )
    )


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-b','--bucket',default='ctbk',help='Bucket to read from / write to')
    parser.add_argument('-s','--src-root',default='original',help='Prefix under `bucket` to read original data from')
    parser.add_argument('-d','--dst-root',default='cleaned',help='Prefix under `bucket` to write converted/cleaned data to')
    args = parser.parse_args()
    bkt = args.bucket
    src_root = args.src
    dst_root = args.dst_root

    main(bkt, src_root, dst_root)
