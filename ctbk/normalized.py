import re
from re import sub
from typing import Pattern

import pandas as pd
from click import option
from numpy import nan
from utz import err, sxs
from utz.ym import Monthy

from ctbk.csvs import TripdataCsv
from ctbk.has_root_cli import HasRootCLI, dates
from ctbk.month_table import MonthDirTables
from ctbk.tables_dir import Tables
from ctbk.tasks import MonthsDirTables
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.util.region import Region, get_regions

DIR = f'{BKT}/normalized'

fields = {
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
    'Start Time': 'datetime64[us]',
    'Stop Time': 'datetime64[us]',
    'Start Station Latitude': float,
    'Start Station Longitude': float,
    'Start Station Name': 'string',
    'End Station Latitude': float,
    'End Station Longitude': float,
    'End Station Name': 'string',
    'Birth Year': 'Int32',
    'Bike ID': 'string',
}
drop = [
    'Unnamed: 0',  # NY 202407
    'rideable_type_duplicate_column_name_1',  # NY 202408, 202410
    'Trip Duration',
    'tripduration',
]

def normalize_field(f):
    return sub(r'[\s/]', '', f.lower())

normalize_fields_map = { normalize_field(f): f for f in fields }
normalize_fields_map['bikeid'] = 'Bike ID'

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
        'JC': [r'JC\d{3}', 'Liberty State Park'],
        'HB': [r'HB\d{3}'],
        'SYS': [
            r'(?:SYSY?\d{3}|Lab - NYC)',
            r'JCSYS',
            r'X-MTL-AOS-5\.1',
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
        err(f'{src}: unrecognized station: {station_id}')
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
            err(f'Unexpected field: {normalized_col} ({col})')

    df = (
        df
        .rename(columns=rename_dict)
        .drop(columns=[ col for col in drop if col in df ])
    )
    if 'Gender' not in df:
        err(f'{src}: "Gender" column not found; setting to 0 ("unknown") for all rows')
        df['Gender'] = 0  # unknown
        df['Gender'] = df['Gender'].astype('Int8')
    if 'Rideable Type' not in df:
        err(f'{src}: "Rideable Type" column not found; setting to "unknown" for all rows')
        df['Rideable Type'] = 'unknown'
    df['Rideable Type'] = df['Rideable Type'].apply(lambda rt: 'classic_bike' if rt == 'docked_bike' else rt)
    df['Rideable Type'] = pd.Categorical(
        df['Rideable Type'],
        categories=['unknown', 'classic_bike', 'electric_bike'],
        ordered=True,
    )
    if 'Bike ID' in df:
        df['Bike ID'] = df['Bike ID'].astype(str)
    if 'Member/Casual' in df:
        assert 'User Type' not in df
        err(f'{src}: renaming/harmonizing "member_casual" → "User Type", substituting "member" → "Subscriber", "casual" → "customer"')
        df['User Type'] = df['Member/Casual'].map({'member':'Subscriber','casual':'Customer'})
        del df['Member/Casual']
    df['User Type'] = pd.Categorical(
        df['User Type'],
        categories=['Subscriber', 'Customer'],
        ordered=True,
    )
    df['Start Station ID'] = df['Start Station ID'].str.replace(r'^(\d+)\.0$', r'\1', regex=True)
    df['End Station ID'] = df['End Station ID'].str.replace(r'^(\d+)\.0$', r'\1', regex=True)
    df = add_region(df, src=src, region=region)
    df = df.astype({ k: v for k, v in dtypes.items() if k in df })

    return df


def add_region(df: DataFrame, src: str, region: Region) -> DataFrame:
    df['Start Region'] = df['Start Station ID'].fillna(NONE).apply(get_region, src=src, file_region=region)
    df['End Region'] = df['End Station ID'].fillna(NONE).apply(get_region, src=src, file_region=region)

    sys_none_start = df['Start Region'].isin({NONE, 'SYS'})
    sys_none_end = df['End Region'].isin({NONE, 'SYS'})
    sys_none = sys_none_start | sys_none_end
    # sys_nones = df[sys_none]
    # err("Computing sys_none_counts…")
    # sys_none_counts = value_counts(sys_nones[['Start Region', 'End Region']]).sort_index()
    # err("sys_none_counts:")
    # err(str(sys_none_counts))

    def fill_ends(r):
        return r['Start Region'] if r['End Region'] == NONE else r['End Region']
    df['End Region'] = df[['Start Region', 'End Region']].apply(fill_ends, axis=1)
    # no_end = df['End Region'] == NONE
    # df.loc[no_end, 'End Region'] = df.loc[no_end, 'Start Region']  # assume incomplete rides ended in the region they started in
    # err(f'Dropping {sys_none_counts.sum()} SYS/NONE records')
    df = df[~sys_none]
    # region_matrix = value_counts(df[['Start Region', 'End Region']]).sort_index().rename('Count')
    # err('Region matrix:')
    # err(str(region_matrix))
    return df


class NormalizedMonth(MonthDirTables):
    DIR = DIR
    NAMES = [ 'normalized', 'norm', 'n', ]

    def __init__(self, ym: Monthy, /, engine: int | None = None, **kwargs):
        self.engine = 'fastparquet' if engine == 1 else 'pyarrow' if engine == 2 else 'auto'
        super().__init__(ym, **kwargs)

    def normalized_region(self, region) -> Tables:
        ym = self.ym
        csv = TripdataCsv(ym=ym, region=region, **self.kwargs)
        df = csv.df()
        df = normalize_fields(df, csv.url, region=region)
        def ym_series(k: str):
            dt = df[k].dt
            y = dt.year.rename('y')
            m = dt.month.rename('m')
            return sxs(y, m).apply(lambda r: '%d%02d' % (r.y, r.m), axis=1)

        start_ym_strs = ym_series('Start Time')
        stop_ym_strs = ym_series('Stop Time')
        ym_strs = start_ym_strs + '_' + stop_ym_strs
        return { str(ym_str): df for ym_str, df in df.groupby(ym_strs) }

    @property
    def checkpoint_kwargs(self):
        return dict(write_kwargs=dict(index=False, engine=self.engine))

    def _dfs(self) -> Tables:
        dfs_dict: dict[str, list[DataFrame]] = {}
        for region in get_regions(self.ym):
            for name, df in self.normalized_region(region).items():
                if name not in dfs_dict:
                    dfs_dict[name] = []
                dfs_dict[name].append(df)

        rv = {}
        time_cols = ['Start Time', 'Stop Time']
        for name, dfs in dfs_dict.items():
            df = pd.concat(dfs)
            dupe_mask = df.duplicated()
            if dupe_mask.any():
                err(f"{self.ym}/{name}: removing {dupe_mask.sum()} duplicated rows")
                df = df[~dupe_mask]
            sort_keys = [ *time_cols ]
            if 'Ride ID' in df:
                sort_keys.append('Ride ID')
            elif 'Bike ID' in df:
                sort_keys.append('Bike ID')
            df = df.sort_values(sort_keys)
            dupe_mask = df[sort_keys].duplicated()
            if dupe_mask.any():
                raise AssertionError(f"{self.ym}/{name}: {sort_keys=} not unique: {dupe_mask.value_counts()}")
            rv[name] = df
        return rv

class NormalizedMonths(MonthsDirTables, HasRootCLI):
    DIR = DIR
    CHILD_CLS = NormalizedMonth

    def month(self, ym: Monthy) -> NormalizedMonth:
        return NormalizedMonth(ym, **self.kwargs, **self.extra)


NormalizedMonths.cli(
    help=f"Normalize \"tripdata\" CSVs (combine regions for each month, harmonize column names, etc. Populates directory `<root>/{DIR}/YYYYMM/` with files of the form `YYYYMM_YYYYMM.parquet`, for each pair of (start,end) months found in a given month's CSVs.",
    cmd_decos=[dates],
    create_decos=[
        option('-e', '--engine', count=True, help='1x: fastparquet, 2x: pyarrow'),
    ]
)
