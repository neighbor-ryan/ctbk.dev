#!/usr/bin/env python

import click
from utz import *
from zipfile import ZipFile

from ctbk import Month, MonthsDataset, cached_property
from ctbk.monthly import BKT
from ctbk.tripdata import Tripdata
from ctbk.util.convert import WROTE


class Csvs(MonthsDataset):
    ROOT = f's3://{BKT}/csvs'
    SRC_CLS = Tripdata

    RGX = r'^(?:(?P<region>JC)-)?(?P<month>\d{6})[ \-]citi?bike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'
    REGION_PREFIXES = { 'JC': 'JC-', 'NYC': '', }

    @cached_property
    def inputs_df(self):
        srcs_df = pd.DataFrame(self.src.fs.listdir(self.src.root))
        src_keys = srcs_df.name.rename('key')
        src_names = src_keys.apply(basename)
        zip_keys = src_names[src_names.str.endswith('.zip')]
        df = sxs(zip_keys.str.extract(self.RGX), zip_keys)
        df = df.dropna(subset=['month']).astype({ 'month': int })
        df['month'] = df['month'].apply(Month)
        df['region'] = df['region'].fillna('NYC')
        df['src'] = f'{self.src.root}/' + df.key
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


@click.command(help="Normalize CSVs (harmonize field names/values), combine each month's separate JC/NYC datasets, output a single parquet per month")
@click.option('-s', '--src-root', default=Tripdata.ROOT, help='Prefix to read CSVs from')
@click.option('-d', '--dst-root', default=Csvs.ROOT, help='Prefix to write normalized files to')
@click.option('-p/-P', '--parallel/--no-parallel', default=True, help='Use joblib to parallelize execution')
@click.option('-f', '--overwrite/--no-overwrite', help='When set, write files even if they already exist')
# @click.option('--public/--no-public', help='Give written objects a public ACL')
@click.option('--start', help='Month to process from (in YYYYMM form)')
@click.option('--end', help='Month to process until (in YYYYMM form; exclusive)')
def main(
        src_root,
        dst_root,
        parallel,
        overwrite,
        # public,
        start,
        end,
):
    src = Tripdata(root=src_root)
    csvs = Csvs(root=dst_root, src=src)
    results = csvs.convert(start=start, end=end, overwrite=overwrite, parallel=parallel)
    results_df = pd.DataFrame(results)
    print(results_df)


if __name__ == '__main__':
    main()
