#!/usr/bin/env python

from utz import *
from zipfile import ZipFile

from ctbk import Month, MonthsDataset, Monthy
from ctbk.monthly import BKT
from ctbk.tripdata import Tripdata
from ctbk.util.convert import WROTE


class Csvs(MonthsDataset):
    ROOT = f'{BKT}/csvs'
    SRC_CLS = Tripdata
    RGX = r'(?:(?P<region>JC)-)?(?P<month>\d{6})-citibike-tripdata.csv'

    REGION_PREFIXES = { 'JC': 'JC-', 'NYC': '', }

    def task_df(self, start: Monthy = None, end: Monthy = None):
        df = self.src.outputs(start, end)
        df['src'] = f'{self.src.root}/' + df.name
        df['region_prefix'] = df.region.apply(lambda k: self.REGION_PREFIXES[k])
        df['dst_name'] = df.apply(lambda r: f'{r["region_prefix"]}{r["month"]}-citibike-tripdata', axis=1)
        df['dst'] = f'{self.root}/' + df['dst_name'] + '.csv'
        df = df[['month', 'region', 'src', 'dst']]
        return df

    def outputs(self, start: Monthy = None, end: Month = None):
        df = super().outputs(start, end)
        df['region'] = df['region'].fillna('NYC')
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


if __name__ == '__main__':
    Csvs.cli()
