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

# New columns from 202102
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
normalize_fields_map['member_casual'] = 'Member/Casual'


nyc_rgx = '(?:\d{1,3}|\d{4}\.\d\d)'
jc_rgx = 'JC\d{3}'
hb_rgx = 'HB\d{3}'
sys_rgx = '(?:SYS\d{3}|Lab - NYC)'
NONE = 'None'
rgxs = {
    'NYC': nyc_rgx,
    'JC': jc_rgx,
    'HB': hb_rgx,
    'SYS': sys_rgx,
    NONE: NONE,
}
def get_region(station_id, file_region=None):
    regions = [ region for region, rgx in rgxs.items() if match(rgx, station_id) ]
    if not regions:
        stderr.write('Unrecognized station: %s\n' % station_id)
        return nan
    if len(regions) > 1:
        raise ValueError(f'Station ID {station_id} matches regions {",".regions}')

    region = regions[0]
    if region == 'NYC' and file_region == 'JC':
        return file_region
    return region


def add_region(df, file_region):
    df['Start Region'] = df['Start Station ID'].fillna(NONE).apply(get_region, file_region=file_region)
    df['End Region'] = df['End Station ID'].fillna(NONE).apply(get_region, file_region=file_region)

    sys_none_start = df['Start Region'].isin({NONE, 'SYS'})
    sys_none_end = df['End Region'].isin({NONE, 'SYS'})
    sys_none = sys_none_start | sys_none_end
    sys_nones = df[sys_none]
    sys_nones[['Start Region', 'End Region']].value_counts().sort_index()

    no_end = df['End Region'] == NONE
    df.loc[no_end, 'End Region'] = df.loc[no_end, 'Start Region']  # assume incomplete rides ended in the region they started in
    print(f'Dropping {sys_none.sum()} SYS/NONE records')
    df = df[~sys_none]
    region_matrix = df[['Start Region', 'End Region']].value_counts().sort_index().rename('Count')
    print('Region matrix:')
    print(region_matrix)
    return df



def normalize_fields(df, dst, file_region):
    rename_dict = {}
    for col in df.columns:
        normalized_col = normalize_field(col)
        if normalized_col in normalize_fields_map:
            rename_dict[col] = normalize_fields_map[normalized_col]
        else:
            stderr.write('Unexpected field: %s (%s)\n' % (normalized_col, col))

    df = df.rename(columns=rename_dict)
    if 'Gender' not in df:
        stderr.write('%s: "Gender" column not found; setting to 0 ("unknown") for all rows\n' % dst)
        df['Gender'] = 0  # unknown
    if 'Rideable Type' not in df:
        stderr.write('%s: "Rideable Type" column not found; setting to "unknown" for all rows\n' % dst)
        df['Rideable Type'] = 'unknown'
    if 'Member/Casual' in df:
        assert 'User Type' not in df
        stderr.write('%s: renaming/harmonizing "member_casual" → "User Type", substituting "member" → "Subscriber", "casual" → "customer" \n' % dst)
        df['User Type'] = df['Member/Casual'].map({'member':'Subscriber','casual':'Customer'})
        del df['Member/Casual']

    df = add_region(df, file_region=file_region)

    return df


def normalize_csv(url, region, dst):
    _, ext = splitext(url)
    assert ext == '.csv'

    df = read_csv(url, dtype=str)
    df = normalize_fields(df, dst, file_region=region)
    df = df.astype({ k: v for k, v in dtypes.items() if k in df })
    return df


def normalize_csvs(entries, dst):
    df = pd.concat([ normalize_csv(**entry, dst=dst) for entry in entries ])
    df.to_parquet(dst)


def csv2pqt(
        year, month,
        entries,
        bkt, dst_root,
        overwrite, public=False,
        start=None, end=None,
):
    name = '%d%02d' % (year, month)
    if start:
        start_year, start_month = int(start[:4]), int(start[4:])
        if (year, month) < (start_year, start_month):
            return 'Skipping month %s < %d%02d' % (name, start_year, start_month)

    if end:
        end_year, end_month = int(end[:4]), int(end[4:])
        if (year, month) >= (end_year, end_month):
            return 'Skipping month %s ≥ %d%02d' % (name, end_year, end_month)

    dst_name = f'{name}.parquet'
    if dst_root:
        dst_key = f'{dst_root}/{dst_name}'
    else:
        dst_key = dst_name

    return convert_file(
        normalize_csvs,
        bkt=bkt, entries=entries, dst_key=dst_key,
        overwrite=overwrite,
        public=public,
    ).msg


@cmd(help="Normalize CSVs (harmonize field names/values), combine each month's separate JC/NYC datasets, output a single parquet per month")
@opt('-b', '--bucket', default='ctbk', help='Bucket to read from and write to')
@opt('-s', '--src-root', default='csvs', help='Prefix to read CSVs from')
@opt('-d', '--dst-root', default='normalized', help='Prefix to write normalized files to')
@opt('-p', '--parallel/--no-parallel', help='Use joblib to parallelize execution')
@opt('-f', '--overwrite/--no-overwrite', help='When set, write files even if they already exist')
@opt('--public/--no-public', help='Give written objects a public ACL')
@opt('--start', help='Month to process from (in YYYYMM form)')
@opt('--end', help='Month to process until (in YYYYMM form; exclusive)')
def main(bucket, src_root, dst_root, parallel, overwrite, public, start, end):
    s3 = client('s3', config=Config())

    if not src_root.endswith('/'):
        src_root += '/'
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=src_root)
    contents = pd.DataFrame(resp['Contents'])
    csvs = contents[contents.Key.str.endswith('.csv')]
    keys = csvs.Key.rename('key')
    urls = keys.apply(lambda key: f's3://{bucket}/{key}').rename('url')
    rgx = '(?:(?P<region>JC)-)?(?P<year>\d{4})(?P<month>\d{2})-citibike-tripdata.csv'
    d = sxs(keys.str.extract(rgx), urls)

    d['region'] = d['region'].fillna('NYC')
    d = d.astype({ 'year': int, 'month': int, })
    d['region_url'] = d[['region','url']].to_dict('records')
    months = d.groupby(['year','month'])['region_url'].apply(list).to_dict()

    kwargs = dict(
        bkt=bucket,
        dst_root=dst_root,
        overwrite=overwrite,
        public=public,
        start=start,
        end=end,
    )

    if parallel:
        p = Parallel(n_jobs=cpu_count())
        print(
            '\n'.join(
                p(
                    delayed(csv2pqt)(year, month, entries, **kwargs)
                    for (year, month), entries in months.items()
                )
            )
        )
    else:
        for (year, month), entries in months.items():
            print(
                csv2pqt(year, month, entries, **kwargs)
            )


if __name__ == '__main__':
    main()
