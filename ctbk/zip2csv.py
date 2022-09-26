#!/usr/bin/env python

from utz import *
from zipfile import ZipFile

from ctbk import Month, MonthsDataset, cached_property
from ctbk.monthly import BKT
from ctbk.util.convert import WROTE


class Csvs(MonthsDataset):
    ROOT = f's3://{BKT}/csvs'
    SRC_BKT = 'tripdata'
    SRC_ROOT = f's3://{SRC_BKT}'

    RGX = r'^(?:(?P<region>JC)-)?(?P<month>\d{6})[ \-]citi?bike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'
    REGION_PREFIXES = { 'JC': 'JC-', 'NYC': '', }

    @cached_property
    def inputs_df(self):
        srcs_df = pd.DataFrame(self.fs.listdir(self.SRC_ROOT))
        src_keys = srcs_df.Key.rename('key')
        src_names = src_keys.apply(basename)
        zip_keys = src_names[src_names.str.endswith('.zip')]
        df = sxs(zip_keys.str.extract(self.RGX), zip_keys)
        df = df.dropna(subset=['month']).astype({ 'month': int })
        df['month'] = df['month'].apply(Month)
        df['region'] = df['region'].fillna('NYC')
        df['src'] = f'{self.SRC_ROOT}/' + df.key
        df['region_prefix'] = df.region.apply(lambda k: self.REGION_PREFIXES[k])
        df['dst_name'] = df.apply(lambda r: f'{r["region_prefix"]}{r["month"]}-citibike-tripdata', axis=1)
        df['dst'] = f'{self.root}/' + df['dst_name'] + '.csv'
        df = df[['month', 'region', 'src', 'dst']]
        return df

    def compute(self, src_fd, src_name, dst_fd):
        z = ZipFile(src_fd)
        names = z.namelist()
        print(f'{src_name}: zip names: {names}')

        csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
        name = singleton(csvs)

        with z.open(name, 'r') as i:
            dst_fd.write(i.read())

        return WROTE
