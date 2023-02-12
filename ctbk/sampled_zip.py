from os.path import basename
from typing import Optional

from click import pass_context, option, Choice
from dask import delayed
from dask.delayed import Delayed
from shutil import copyfileobj
from zipfile import ZipFile, ZIP_LZMA

from ctbk import Monthy, YM
from ctbk.cli.base import ctbk
from ctbk.csvs import ReadsTripdataZip
from ctbk.month_data import HasRoot
from ctbk.util import cached_property, stderr
from ctbk.util.constants import BKT
from ctbk.zips import TripdataZips, REGIONS, Region

DIR = f'{BKT}/sampled'
DEFAULT_NROWS = 1000


class SampledZip(ReadsTripdataZip):
    DIR = DIR

    def __init__(self, *args, nrows=DEFAULT_NROWS, **kwargs):
        self.nrows = nrows
        super().__init__(*args, **kwargs)

    @cached_property
    def url(self):
        src = self.src
        zip_name = basename(src.url)
        return f'{self.dir}/{zip_name}'

    def create_fn(self):
        src = self.src
        with src.fd('rb') as zin:
            z_in = ZipFile(zin)
            rm_dir = self.mkdirs()
            try:
                z_out = ZipFile(self.url, 'w', compression=ZIP_LZMA)
                names = z_in.namelist()
                print(f'{src.url}: zip names: {names}')

                for name in names:
                    with z_in.open(name, 'r') as i, z_out.open(name, 'w') as o:
                        if name.endswith('.csv'):
                            for lineno, line in enumerate(i):
                                o.write(line)
                                if lineno == self.nrows:
                                    break
                        else:
                            copyfileobj(i, o)
                rm_dir = None
            finally:
                if rm_dir:
                    self.fs.delete(rm_dir)


class SampledZips(HasRoot):
    DIR = DIR

    def __init__(self, start: Monthy = None, end: Monthy = None, nrows=DEFAULT_NROWS, regions: Optional[list[str]] = None, **kwargs):
        src = self.src = TripdataZips(start=start, end=end, regions=regions)
        self.start: YM = src.start
        self.end: YM = src.end
        self.nrows = nrows
        super().__init__(**kwargs)

    def zip(self, ym: YM, region: str) -> SampledZip:
        return SampledZip(ym=ym, region=region, nrows=self.nrows, **self.kwargs)

    @cached_property
    def zips(self) -> list[SampledZip]:
        return [
            self.zip(ym=zip.ym, region=zip.region)
            for zip in self.src.zips
        ]

    def create(self):
        creates = [ zip.create(rv=None) for zip in self.zips ]
        if self.dask:
            return delayed(lambda x: x)(creates)


@ctbk.group()
def sampled_zips():
    pass


@sampled_zips.command()
@pass_context
@option('-r', '--region', type=Choice(REGIONS))
def urls(ctx, region):
    o = ctx.obj
    zips = SampledZips(start=o.start, end=o.end, root=o.root, write_configs=o.write_configs)
    for zip in zips.zips:
        if region and zip.region != region:
            continue
        print(zip.url)


@sampled_zips.command()
@pass_context
@option('--dask', is_flag=True)
@option('-r', '--region', type=Choice(REGIONS))
def create(ctx, dask, region):
    o = ctx.obj
    zips = SampledZips(
        start=o.start, end=o.end,
        regions=[region] if region else None,
        dask=dask,
        root=o.root,
        write_configs=o.write_configs,
    )
    print(zips.create().compute())
