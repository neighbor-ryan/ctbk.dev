from utz import *

from boto3 import client
from botocore.client import Config
from click import command as cmd, option as opt

from utils import convert_file


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
    'Gender',
}
# All fields are ingested as strings by default; select overrides here:
dtypes = {
    'Start Time':'datetime64[ns]',
    'Stop Time':'datetime64[ns]',
    'Start Station Latitude':float,
    'Start Station Longitude':float,
    'End Station Latitude':float,
    'End Station Longitude':float,
    'Trip Duration':int,
    'Gender':int,
}
def normalize_field(f): return sub(r'[\s/]', '', f.lower())
normalize_fields_map = { normalize_field(f): f for f in fields }
normalize_fields_map['ride_id'] = 'Ride ID'
normalize_fields_map['rideable_type'] = 'Rideable Type'
normalize_fields_map['start_lng'] = 'Start Station Longitude'
normalize_fields_map['start_lat'] = 'Start Station Latitude'
normalize_fields_map['start_station_id'] = 'Start Station ID'
normalize_fields_map['start_station_name'] = 'Start Station Name'
normalize_fields_map['end_lng'] = 'End Station Longitude'
normalize_fields_map['end_lat'] = 'End Station Latitude'
normalize_fields_map['end_station_id'] = 'End Station ID'
normalize_fields_map['end_station_name'] = 'End Station Name'
normalize_fields_map['started_at'] = 'Start Time'
normalize_fields_map['ended_at'] = 'Stop Time'
def normalize_fields(df):
    rename_dict = {}
    for col in df.columns:
        normalized_col = normalize_field(col)
        if normalized_col in normalize_fields_map:
            rename_dict[col] = normalize_fields_map[normalized_col]
        else:
            stderr.write('Unexpected field: %s (%s)\n' % (normalized_col, col))
    if 'member_casual' in df:
        assert 'User Type' not in df
        df['member_casual'] = df['member_casual'].apply({'member':'Subscriber','casual':'Customer'})
        rename_dict['member_casual'] = 'User Type'
    return df.rename(columns=rename_dict)



def normalize_parquet(src, dst):
    df = read_csv(src, dtype=str)
    df = normalize_fields(df)
    df = df.astype({ k:v for k,v in dtypes.items() if k in df })
    df.to_parquet(dst)


def csv2parquet(bkt, src_key, dst_root, overwrite, start=None):
    rgx = '(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
    m = match(rgx, basename(src_key))
    if start:
        month = int('%s%s' % (m['year'], m['month']))
        if month < start:
            return 'Skipping month %d < %d' % (month, start)
    def dst_key(src_name):
        base, ext = splitext(src_name)
        assert ext == '.csv'
        dst_name = f'{base}.parquet'
        if dst_root:
            return f'{dst_root}/{dst_name}'
        else:
            return dst_name

    return convert_file(
        normalize_parquet,
        bkt=bkt, src_key=src_key, dst_key=dst_key,
        overwrite=overwrite,
    ).get('msg')


@cmd()
@opt('-b','--bucket',default='ctbk',help='Bucket to read from and write to')
@opt('-s','--src-root',default='csvs',help='Prefix to read CSVs from')
@opt('-d','--dst-root',default='pqts',help='Prefix to write normalized Parquet files to')
@opt('-p','--parallel/--no-parallel',help='Use joblib to parallelize execution')
@opt('-f','--overwrite/--no-overwrite',help='When set, write files even if they already exist')
@opt('--start',type=int,help='Month to process from (in YYYYMM form)')
def main(bucket, src_root, dst_root, parallel, overwrite, start):
    s3 = client('s3', config=Config())

    if not src_root.endswith('/'):
        src_root += '/'
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=src_root)
    contents = pd.DataFrame(resp['Contents'])
    csvs = contents[contents.Key.str.endswith('.csv')]
    csvs = csvs.Key.values

    kwargs = dict(
        bkt=bucket,
        dst_root=dst_root,
        overwrite=overwrite,
        start=start,
    )

    if parallel:
        p = Parallel(n_jobs=cpu_count())
        print(
            '\n'.join(
                p(
                    delayed(csv2parquet)(src_key=csv, **kwargs)
                    for csv in csvs
                )
            )
        )
    else:
        for csv in csvs:
            print(
                csv2parquet(src_key=csv, **kwargs)
            )


if __name__ == '__main__':
    main()
