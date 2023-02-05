from re import match, sub
import dask.dataframe as dd

from click import pass_context, option
from numpy import nan

from ctbk import Monthy
from ctbk.cli.base import ctbk
from ctbk.csvs import TripdataCsv, TripdataCsvs
from ctbk.month_data import MonthData, MonthsData
from ctbk.util import cached_property, stderr
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame, value_counts
from ctbk.zips import REGIONS, Region

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
        stderr(f'Unrecognized station: {station_id}')
        return nan
    if len(regions) > 1:
        raise ValueError(f'Station ID {station_id} matches regions {",".join(regions)}')

    region = regions[0]
    if region == 'NYC' and file_region == 'JC':
        return file_region
    return region


def normalize_fields(df: DataFrame, dst, region: Region) -> DataFrame:
    rename_dict = {}
    for col in df.columns:
        normalized_col = normalize_field(col)
        if normalized_col in normalize_fields_map:
            rename_dict[col] = normalize_fields_map[normalized_col]
        else:
            stderr(f'Unexpected field: {normalized_col} ({col})')

    df = df.rename(columns=rename_dict)
    if 'Gender' not in df:
        stderr(f'{dst}: "Gender" column not found; setting to 0 ("unknown") for all rows')
        df['Gender'] = 0  # unknown
    if 'Rideable Type' not in df:
        stderr(f'{dst}: "Rideable Type" column not found; setting to "unknown" for all rows')
        df['Rideable Type'] = 'unknown'
    if 'Member/Casual' in df:
        assert 'User Type' not in df
        stderr(f'{dst}: renaming/harmonizing "member_casual" → "User Type", substituting "member" → "Subscriber", "casual" → "customer"')
        df['User Type'] = df['Member/Casual'].map({'member':'Subscriber','casual':'Customer'})
        del df['Member/Casual']

    df = add_region(df, region=region)
    df = df.astype({ k: v for k, v in dtypes.items() if k in df })

    return df


def add_region(df: DataFrame, region: Region) -> DataFrame:
    meta = lambda k: (k, 'str') if isinstance(df, dd.DataFrame) else lambda k: None
    df['Start Region'] = df['Start Station ID'].fillna(NONE).apply(get_region, file_region=region, meta=meta('Start Station ID'))
    df['End Region'] = df['End Station ID'].fillna(NONE).apply(get_region, file_region=region, meta=meta('End Station ID'))

    sys_none_start = df['Start Region'].isin({NONE, 'SYS'})
    sys_none_end = df['End Region'].isin({NONE, 'SYS'})
    sys_none = sys_none_start | sys_none_end
    sys_nones = df[sys_none]

    stderr("Computing sys_none_counts…")
    sys_none_counts = value_counts(sys_nones[['Start Region', 'End Region']]).sort_index()
    stderr("sys_none_counts:")
    stderr(sys_none_counts)

    no_end = df['End Region'] == NONE
    df.loc[no_end, 'End Region'] = df.loc[no_end, 'Start Region']  # assume incomplete rides ended in the region they started in
    stderr(f'Dropping {sys_none_counts.sum()} SYS/NONE records')
    df = df[~sys_none]
    region_matrix = value_counts(df[['Start Region', 'End Region']]).sort_index().rename('Count')
    stderr('Region matrix:')
    stderr(region_matrix)
    return df


class NormalizedMonth(MonthData):
    DIR = DIR
    WRITE_CONFIG_NAMES = [ 'normalized', 'norm', ]

    @property
    def url(self):
        return f'{self.dir}/{self.ym}.pqt'

    def csv(self, region):
        return TripdataCsv(ym=self.ym, region=region, **self.kwargs)

    @cached_property
    def csvs(self):
        return [ self.csv(region) for region in REGIONS ]

    def normalized_df(self, region):
        csv = self.csv(region)
        df = csv.df
        df = normalize_fields(df, csv.url, region=region)
        return df

    def compute(self):
        return self.concat([
            self.normalized_df(region)
            for region in REGIONS
        ])


class NormalizedMonths(MonthsData):
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, **kwargs):
        src = self.src = TripdataCsvs(start=start, end=end, **kwargs)
        super().__init__(start=src.start, end=src.end, **kwargs)

    def month(self, ym: Monthy) -> NormalizedMonth:
        return NormalizedMonth(ym, **self.kwargs)


@ctbk.group()
def normalized():
    pass


@normalized.command()
@pass_context
def urls(ctx):
    o = ctx.obj
    normalized = NormalizedMonths(**o)
    months = normalized.months
    for month in months:
        print(month.url)


@normalized.command()
@pass_context
@option('-d', '--dask', is_flag=True)
def create(ctx, dask):
    o = ctx.obj
    normalized = NormalizedMonths(dask=dask, **o)
    normalized.create()
    months = normalized.months
    for month in months:
        print(month.url)
