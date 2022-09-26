import click
from utz import *

from ctbk import Csvs, cached_property, Month
from ctbk.monthly import BKT, MonthsDataset, PARQUET_EXTENSION

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
    'Start Time': 'datetime64[ns]',
    'Stop Time': 'datetime64[ns]',
    'Start Station Latitude': float,
    'Start Station Longitude': float,
    'End Station Latitude': float,
    'End Station Longitude': float,
    'Trip Duration': int,
    'Gender': int,
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


nyc_rgx = r'(?:\d{1,3}|\d{4}\.\d\d)'
jc_rgx = r'JC\d{3}'
hb_rgx = r'HB\d{3}'
sys_rgx = r'(?:SYS\d{3}|Lab - NYC)'
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


class NormalizedMonths(MonthsDataset):
    ROOT = f's3://{BKT}/normalized'
    RGX = r'(?:(?P<region>JC)-)?(?P<month>\d{6})-citibike-tripdata.csv'
    SRC_CLS = Csvs

    @cached_property
    def inputs_df(self):
        print('computing inputs_df')
        csvs = pd.DataFrame(self.src.listdir).rename(columns={ 'name': 'src', })
        src = csvs.src
        df = sxs(src.str.extract(self.RGX), src)
        df['region'] = df['region'].fillna('NYC')
        df['month'] = df['month'].apply(Month)
        df['srcs'] = df[['region', 'src']].to_dict('records')
        df = df.groupby('month')['srcs'].apply(list).reset_index()
        df['dst'] = df['month'].apply(lambda m: f'{self.root}/{m}{PARQUET_EXTENSION}')
        return df

    def normalize_csv(self, src, region, dst):
        with self.src.fs.open(src, 'r') as f:
            df = pd.read_csv(f, dtype=str)
        df = normalize_fields(df, dst, file_region=region)
        df = df.astype({ k: v for k, v in dtypes.items() if k in df })
        return df

    def compute(self, srcs, dst):
        return pd.concat([ self.normalize_csv(**entry, dst=dst) for entry in srcs ])


if __name__ == '__main__':
    NormalizedMonths.cli()
