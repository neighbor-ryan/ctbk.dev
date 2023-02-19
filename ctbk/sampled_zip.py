from os.path import basename

from click import pass_context, option, argument
from shutil import copyfileobj
from typing import Optional, Union
from utz import process, Unset
from zipfile import ZipFile, ZIP_LZMA

from ctbk import Monthy, YM
from ctbk.cli.base import ctbk, dask, region
from ctbk.csvs import ReadsTripdataZip
from ctbk.months_data import Tasks
from ctbk.util import cached_property, stderr
from ctbk.util.constants import BKT
from ctbk.util.read import Read
from ctbk.zips import TripdataZips

DIR = f'{BKT}/sampled/tripdata'
DEFAULT_NROWS = 1000


class SampledZip(ReadsTripdataZip):
    DIR = DIR
    DEFAULT_COMPRESSION = ZIP_LZMA

    def __init__(self, *args, nrows=DEFAULT_NROWS, **kwargs):
        self.nrows = nrows
        super().__init__(*args, **kwargs)

    @cached_property
    def url(self):
        src = self.src
        zip_name = basename(src.url)
        return f'{self.dir}/{zip_name}'

    def _create(self, read: Union[None, Read] = Unset) -> None:
        src = self.src
        with src.fd('rb') as zin:
            z_in = ZipFile(zin)
            rm_dir = self.mkdirs()
            try:
                z_out = ZipFile(self.url, 'w', compression=self.DEFAULT_COMPRESSION)
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


class SampledZips(Tasks):
    DIR = DIR
    NAMES = ['sampled_zip', 'szip', 'sz']

    def __init__(self, start: Monthy = None, end: Monthy = None, nrows=DEFAULT_NROWS, regions: Optional[list[str]] = None, **kwargs):
        src = self.src = TripdataZips(start=start, end=end, regions=regions, roots=kwargs.get('roots'))
        self.start: YM = src.start
        self.end: YM = src.end
        self.regions = src.regions
        self.nrows = nrows
        super().__init__(**kwargs)

    def zip(self, ym: YM, region: str) -> SampledZip:
        return SampledZip(ym=ym, region=region, nrows=self.nrows, **self.kwargs)

    @cached_property
    def children(self) -> list[SampledZip]:
        return [
            self.zip(ym=zip.ym, region=zip.region)
            for zip in self.src.children
        ]


@ctbk.group()
def sampled_zips():
    pass


@sampled_zips.command()
@pass_context
@region
def urls(ctx, region):
    o = ctx.obj
    zips = SampledZips(start=o.start, end=o.end, root=o.root, write_config=o.write_config)
    for zip in zips.children:
        if region and zip.region != region:
            continue
        print(zip.url)


@sampled_zips.command()
@pass_context
@dask
@region
def create(ctx, dask, region):
    o = ctx.obj
    zips = SampledZips(
        start=o.start, end=o.end,
        regions=[region] if region else None,
        dask=dask,
        root=o.root,
        write_config=o.write_config,
    )
    print(zips.create(read=None).compute())


@sampled_zips.command()
@pass_context
@region
@option('-O', '--no-open', is_flag=True)
@argument('filename', required=False)
def dag(ctx, region, no_open, filename):
    o = ctx.obj
    zips = SampledZips(
        start=o.start, end=o.end,
        regions=[region] if region else None,
        dask=True,
        root=o.root,
        write_config=o.write_config,
    )
    result = zips.create()
    filename = filename or 'sampled_zip_dag.png'
    stderr(f"Writing to {filename}")
    result.visualize(filename)
    if not no_open:
        process.run('open', filename)
