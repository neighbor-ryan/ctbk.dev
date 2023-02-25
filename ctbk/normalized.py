import dask.dataframe as dd
import pandas as pd
import re
from click import pass_context
from numpy import nan
from re import sub
from typing import Pattern

from ctbk import Monthy
from ctbk.cli.base import ctbk, dask
from ctbk.csvs import TripdataCsv, TripdataCsvs
from ctbk.month_table import MonthTable
from ctbk.table import Table
from ctbk.tasks import MonthTables
from ctbk.util import stderr
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.util.region import REGIONS, Region, get_regions
from ctbk.util.ym import dates

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


NONE = 'None'
RGXS: dict[str, list[Pattern]] = {
    region: [ re.compile(rgx) for rgx in rgxs ]
    for region, rgxs in {
        'NYC': [
            r'(?:\d{1,3}|\d{4}\.\d\d)',
            r'S 5th St & Kent Ave',  # 2021
            r'MTL-ECO51-1',          # 2021
            r'MTL-ECO5-LAB',         # 202209
            r'Shop Morgan',          # 202209
        ],
        'JC': [r'JC\d{3}'],
        'HB': [r'HB\d{3}'],
        'SYS': [
            r'(?:SYS\d{3}|Lab - NYC)',
            r'JCSYS',
        ],
        NONE: NONE,
    }.items()
}


def get_region(station_id, src: str, file_region: str):
    regions = [
        region
        for region, rgxs in RGXS.items()
        if any(
            rgx.match(station_id)
            for rgx in rgxs
        )
    ]
    if not regions:
        stderr(f'{src}: unrecognized station: {station_id}')
        return nan
    if len(regions) > 1:
        raise ValueError(f'{src}: station ID {station_id} matches regions {",".join(regions)}')

    region = regions[0]
    if region == 'NYC' and file_region == 'JC':
        return file_region
    return region


def normalize_fields(df: DataFrame, src, region: Region) -> DataFrame:
    rename_dict = {}
    for col in df.columns:
        normalized_col = normalize_field(col)
        if normalized_col in normalize_fields_map:
            rename_dict[col] = normalize_fields_map[normalized_col]
        else:
            stderr(f'Unexpected field: {normalized_col} ({col})')

    df = df.rename(columns=rename_dict)
    if 'Gender' not in df:
        stderr(f'{src}: "Gender" column not found; setting to 0 ("unknown") for all rows')
        df['Gender'] = 0  # unknown
    if 'Rideable Type' not in df:
        stderr(f'{src}: "Rideable Type" column not found; setting to "unknown" for all rows')
        df['Rideable Type'] = 'unknown'
    if 'Member/Casual' in df:
        assert 'User Type' not in df
        stderr(f'{src}: renaming/harmonizing "member_casual" → "User Type", substituting "member" → "Subscriber", "casual" → "customer"')
        df['User Type'] = df['Member/Casual'].map({'member':'Subscriber','casual':'Customer'})
        del df['Member/Casual']

    df = add_region(df, src=src, region=region)
    df = df.astype({ k: v for k, v in dtypes.items() if k in df })

    return df


def add_region(df: DataFrame, src: str, region: Region) -> DataFrame:
    meta = lambda k: dict(meta=(k, 'str')) if isinstance(df, dd.DataFrame) else dict()
    df['Start Region'] = df['Start Station ID'].fillna(NONE).apply(get_region, src=src, file_region=region, **meta('Start Station ID'))
    df['End Region'] = df['End Station ID'].fillna(NONE).apply(get_region, src=src, file_region=region, **meta('End Station ID'))

    sys_none_start = df['Start Region'].isin({NONE, 'SYS'})
    sys_none_end = df['End Region'].isin({NONE, 'SYS'})
    sys_none = sys_none_start | sys_none_end
    # sys_nones = df[sys_none]
    # stderr("Computing sys_none_counts…")
    # sys_none_counts = value_counts(sys_nones[['Start Region', 'End Region']]).sort_index()
    # stderr("sys_none_counts:")
    # stderr(str(sys_none_counts))

    def fill_ends(r):
        return r['Start Region'] if r['End Region'] == NONE else r['End Region']
    df['End Region'] = df[['Start Region', 'End Region']].apply(fill_ends, axis=1, **meta(None))
    # no_end = df['End Region'] == NONE
    # df.loc[no_end, 'End Region'] = df.loc[no_end, 'Start Region']  # assume incomplete rides ended in the region they started in
    # stderr(f'Dropping {sys_none_counts.sum()} SYS/NONE records')
    df = df[~sys_none]
    # region_matrix = value_counts(df[['Start Region', 'End Region']]).sort_index().rename('Count')
    # stderr('Region matrix:')
    # stderr(str(region_matrix))
    return df


class NormalizedMonth(MonthTable):
    DIR = DIR
    NAMES = [ 'normalized', 'norm', ]

    def normalized_region(self, region) -> DataFrame:
        ym = self.ym
        csv = TripdataCsv(ym=ym, region=region, **self.kwargs)
        df = csv.df
        df = normalize_fields(df, csv.url, region=region)
        def fsck_ym(df: pd.DataFrame):
            start, end = ym.dates
            date: pd.Series = df['Start Time'].dt.date
            wrong_yms = (date < start) | (date >= end)
            num_wrong_yms = wrong_yms.sum()
            if num_wrong_yms:
                wrong_dates_hist = (
                    date
                    [wrong_yms]
                    .value_counts()
                    .sort_index()
                )
                stderr(f'{num_wrong_yms} rides not in {ym}:\n{wrong_dates_hist}')
            return df[~wrong_yms]

        if self.dask:
            df = df.map_partitions(fsck_ym, meta=df._meta)
        else:
            df = fsck_ym(df)
        return df

    @property
    def checkpoint_kwargs(self):
        return dict(write_kwargs=dict(index=False))

    def _df(self) -> DataFrame:
        return self.concat([
            self.normalized_region(region)
            for region in get_regions(self.ym)
        ])


class NormalizedMonths(MonthTables):
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, **kwargs):
        src = self.src = TripdataCsvs(start=start, end=end, **kwargs)
        super().__init__(start=src.start, end=src.end, **kwargs)

    def month(self, ym: Monthy) -> NormalizedMonth:
        return NormalizedMonth(ym, **self.kwargs)


@ctbk.group(help=f"Normalize \"tripdata\" CSVs (combine regions for each month, harmonize column names, etc. Writes to <root>/{DIR}/YYYYMM.parquet.")
@pass_context
@dates
def normalized(ctx, start, end):
    ctx.obj.start = start
    ctx.obj.end = end


@normalized.command()
@pass_context
def urls(ctx):
    o = ctx.obj
    normalized = NormalizedMonths(**o)
    months = normalized.children
    for month in months:
        print(month.url)


@normalized.command()
@pass_context
@dask
def create(ctx, dask):
    o = ctx.obj
    normalized = NormalizedMonths(dask=dask, **o)
    created = normalized.create(read=None)
    if dask:
        created.compute()
