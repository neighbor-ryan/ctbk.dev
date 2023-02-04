#!/usr/bin/env python

import dask.dataframe as dd
from utz import *
from zipfile import ZipFile

from ctbk import YM, MonthsDataset, Monthy
from ctbk.month_data import MonthData, Compute
from ctbk.tripdata import Tripdata, REGIONS, TripdataMonths, TripdataMonth, Region
from ctbk.util.constants import BKT
from ctbk.util.convert import WROTE

DIR = 'ctbk/csvs'


class TripdataCsv(MonthData):
    DIR = DIR

    def __init__(self, ym, region, root='s3://'):
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.region = region
        ym = YM(ym)
        region_str = 'JC-' if region == 'JC' else ''
        url = f'{self.dir}/{region_str}{ym}-citibike-tripdata.csv'
        super().__init__(ym, url)

    @property
    def src(self) -> TripdataMonth:
        return TripdataMonth(ym=self.ym, region=self.region, root=self.root)

    def create_fn(self):
        src = self.src
        z = ZipFile(src.fd('rb'))
        names = z.namelist()
        print(f'{src.url}: zip names: {names}')

        csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
        name = singleton(csvs)

        with z.open(name, 'r') as i, self.fs.open(self.path, 'w') as o:
            copyfileobj(i, o)

    def compute_df(self):
        self.create_fn()
        df = pd.read_parquet(self.url)
        df['region'] = self.region
        return df

    def compute_dd(self):
        self.create_fn()
        df = dd.read_parquet(self.url)
        df['region'] = self.region
        return df


class TripdataCsvs:
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, root='s3://'):
        self.root = root
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        src = self.src = TripdataMonths(start=start, end=end, root=root)
        self.start: YM = src.start
        self.end: YM = src.end

    @property
    def csvs(self) -> list[TripdataCsv]:
        return [ TripdataCsv(ym=u.ym, region=u.region, root=self.root) for u in self.src.urls ]

    @property
    def m2r2csv(self) -> dict[YM, dict[Region, TripdataCsv]]:
        m2r2u = self.src.m2r2u
        return {
            m: {
                r: TripdataCsv(ym=m, region=r, root=self.root)
                for r in r2u
            }
            for m, r2u in m2r2u.items()
        }

    def m2df(self, compute: Compute = None):
        return {
            m: pd.concat([
                csv.df(compute=compute)
                for r, csv in r2csv.items()
            ])
            for m, r2csv in self.m2r2csv.items()
        }

    def m2dd(self, compute: Compute = None):
        return {
            m: dd.concat([
                csv.dd(compute=compute)
                for r, csv in r2csv.items()
            ])
            for m, r2csv in self.m2r2csv.items()
        }

    def df(self, compute: Compute = None):
        raise NotImplementedError("Unified DataFrame is large, you probably want .dd instead (.dd.compute() if you must)")
        # return pd.concat([
        #     csv.df(compute=compute)
        #     for m, r2csv in self.m2r2csv.items()
        #     for r, csv in r2csv.items()
        # ])

    def dd(self, compute: Compute = None):
        return dd.concat([
            csv.dd(compute=compute)
            for m, r2csv in self.m2r2csv.items()
            for r, csv in r2csv.items()
        ])

    def create(self, compute: Compute = None):
        csvs = self.csvs
        for csv in csvs:
            csv.create(compute=compute)


class Csvs(MonthsDataset):
    ROOT = f'{BKT}/csvs'
    SRC_CLS = Tripdata
    RGX = r'(?:(?P<region>JC)-)?(?P<month>\d{6})-citibike-tripdata.csv'

    REGION_PREFIXES = { 'JC': 'JC-', 'NYC': '', }

    def task_df(self, start: Monthy = None, end: Monthy = None):
        df = self.src.outputs(start, end)
        df['src'] = f'{self.src.root}/' + df.basename
        df['region_prefix'] = df.region.apply(lambda k: self.REGION_PREFIXES[k])
        df['dst_name'] = df.apply(lambda r: f'{r["region_prefix"]}{r["month"]}-citibike-tripdata', axis=1)
        df['dst'] = f'{self.root}/' + df['dst_name'] + '.csv'
        df = df[['month', 'region', 'src', 'dst']]
        return df

    def outputs(self, start: Monthy = None, end: YM = None):
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
