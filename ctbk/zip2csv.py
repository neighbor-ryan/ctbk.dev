#!/usr/bin/env python

from utz import *
from zipfile import ZipFile

from ctbk.util.convert import BadKey, WROTE

from ctbk import MonthsDataset
from ctbk.monthly import BKT
from ctbk.util.month import Month, MonthSet


class Csvs(MonthsDataset):
    ROOT = f's3://{BKT}/csvs'
    SRC_BKT = 'tripdata'
    SRC_ROOT = f's3://{SRC_BKT}'

    RGX = r'^(?:(?P<region>JC)-)?(?P<month>\d{6})[ \-]citi?bike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'
    REGION_PREFIXES = { 'JC': 'JC-', 'NYC': '', }

    # def deps(self, month, region=None):
    #     inputs = self.inputs[month]
    #     if region:
    #         return singleton(
    #             inputs,
    #             fn=lambda e: e['region'] == region,
    #             empty_ok=True,
    #         )
    #     else:
    #         return inputs
    #
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

        # zip_keys['obj'] = zip_keys[['region', 'src', 'dst']].to_dict('records')
        # inputs = MonthSet(zip_keys[['month', 'obj']].groupby('month')['obj'].apply(list).to_dict())
        # return inputs

    def compute(self, src_fd, src_name, dst_fd):
        z = ZipFile(src_fd)
        names = z.namelist()
        print(f'{src_name}: zip names: {names}')

        csvs = [ f for f in names if f.endswith('.csv') and not f.startswith('_') ]
        name = singleton(csvs)

        with z.open(name, 'r') as i:
            dst_fd.write(i.read())

        return WROTE


# def original_to_csv(src_root, zip_key, dst_root, error='warn', overwrite=False):
#     def dst_fn(src_name):
#         m = match(RGX, src_name)
#         if not m:
#             raise BadKey(src_name)
#         _, ext = splitext(src_name)
#         assert ext == '.zip'
#
#         # normalize the dst path; a few src files have typos/inconsistencies
#         base = '%s%s%s-citibike-tripdata' % (m['JC'] or '', m['year'], m['month'])
#         if dst_root is None:
#             return f'{base}.csv'
#         else:
#             return f'{dst_root}/{base}.csv'
#
#     src = f'{src_root}/{zip_key}'
#
#     return convert_file(
#         to_csv,
#         src=src,
#         dst_key=dst_fn,
#         error=error,
#         overwrite=overwrite,
#     ).msg


# @cmd(help='Read Zip files published by Citibike (per {month,region}), extract a lone CSV from inside each, save')
# @opt('-s','--src-root',default='tripdata',help='Source bucket to read Zip files from')
# @opt('-d','--dst-bucket',default='ctbk',help='Destination bucket to write CSV files to')
# @opt('-r','--dst-root',default='csvs',help='Prefix (in destination bucket) to write CSVs udner')
# @opt('-p','--parallel/--no-parallel',help='Use joblib to parallelize execution')
# def main(src_root, dst_bucket, dst_root, parallel):
#     # src_scheme = urlparse(src_root).scheme
#     # fs =
#     s3 = client('s3', config=Config())
#     resp = s3.list_objects_v2(Bucket=src)
#     contents = pd.DataFrame(resp['Contents'])
#     zip_keys = contents[contents.Key.str.endswith('.zip')]
#     zip_keys = zip_keys.Key.values
#     if parallel:
#         p = Parallel(n_jobs=cpu_count())
#         print(
#             '\n'.join(
#                 p(
#                     delayed(original_to_csv)(
#                         src, zip_key, dst_bucket, dst_root=dst_root
#                     )
#                     for zip_key in zip_keys
#                 )
#             )
#         )
#     else:
#         for zip_key in zip_keys:
#             print(original_to_csv(src_root, zip_key, dst_bucket, dst_root=dst_root))
#
#
# if __name__ == '__main__':
#     main()
