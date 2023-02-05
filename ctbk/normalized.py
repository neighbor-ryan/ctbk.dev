import dask.dataframe as dd
from click import pass_context
from utz import *

from ctbk.cli.base import ctbk
from ctbk import Monthy
from ctbk.csvs import TripdataCsv
from ctbk.month_data import MonthData, MonthsData, Compute
from ctbk.tripdata import REGIONS, Region
from ctbk.util.constants import DataFrame, BKT

DIR = f'{BKT}/normalized'

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
    regions = [
        region
        for region, rgx in rgxs.items()
        if match(rgx, station_id)
    ]
    if not regions:
        stderr.write('Unrecognized station: %s\n' % station_id)
        return nan
    if len(regions) > 1:
        raise ValueError(f'Station ID {station_id} matches regions {",".join(regions)}')

    region = regions[0]
    if region == 'NYC' and file_region == 'JC':
        return file_region
    return region


def add_region(df: DataFrame, region: Region) -> DataFrame:
    df['Start Region'] = df['Start Station ID'].fillna(NONE).apply(get_region, file_region=region)
    df['End Region'] = df['End Station ID'].fillna(NONE).apply(get_region, file_region=region)

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


def normalize_fields(df: DataFrame, dst, region: Region) -> DataFrame:
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

    df = add_region(df, region=region)
    df = df.astype({ k: v for k, v in dtypes.items() if k in df })

    return df


class NormalizedMonth(MonthData):
    DIR = DIR

    def csv(self, region):
        return TripdataCsv(ym=self.ym, region=region)

    @cached_property
    def csvs(self):
        return [ self.csv(region) for region in REGIONS ]

    def normalized_df(self, region):
        csv = self.csv(region)
        df = normalize_fields(csv.df, csv.url, region=region)
        return df

    def normalized_dd(self, region):
        csv = self.csv(region)
        df = normalize_fields(csv.dd, csv.url, region=region)
        return df

    def compute_df(self):
        return pd.concat([
            self.normalized_df(region)
            for region in REGIONS
        ])

    def compute_dd(self):
        return dd.concat([
            self.normalized_dd(region)
            for region in REGIONS
        ])


class NormalizedMonths(MonthsData):
    DIR = DIR

    def __init__(self, start: Monthy, end: Monthy, root=None, compute: Compute = None):
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        url_fn = '%s/{ym}.pqt' % self.dir
        super().__init__(start, end, url_fn, compute)


@ctbk.group()
def normalized():
    pass


@normalized.command()
@pass_context
def urls(ctx):
    o = ctx.obj
    normalized = NormalizedMonths(start=o.start, end=o.end, root=o.root, compute=o.compute)
    months = normalized.months
    for month in months:
        print(month.url)
