from os.path import basename
from shutil import copyfileobj
from typing import Optional, Union
from zipfile import ZipFile, ZIP_LZMA

from dask import delayed
from utz import cached_property, Unset
from utz.ym import YM

from ctbk.csvs import ReadsTripdataZip
from ctbk.has_root_cli import HasRootCLI, dates
from ctbk.util.constants import BKT
from ctbk.util.read import Read
from ctbk.util.region import region
from ctbk.zips import TripdataZips

DIR = f'{BKT}/sampled/tripdata'
DEFAULT_NROWS = 1000


class SampledZip(ReadsTripdataZip):
    DIR = DIR
    DEFAULT_COMPRESSION = ZIP_LZMA
    NAMES = ['sampled_zip', 'szip', 'sz']

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
        src_create = src.create(read=read)
        def make(src_create):
            with src.fd('rb') as zin:
                z_in = ZipFile(zin)
                with self.fd('w') as f:
                    z_out = ZipFile(f, 'w', compression=self.DEFAULT_COMPRESSION)
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
        if self.dask:
            return delayed(make)(src_create, dask_key_name=f'SampledZip_{self.region}_{self.ym}')
        else:
            return make(src_create)


class SampledZips(HasRootCLI):
    DIR = DIR
    CHILD_CLS = SampledZip

    def __init__(self, yms: list[YM], nrows=DEFAULT_NROWS, regions: Optional[list[str]] = None, **kwargs):
        self.yms = yms
        src = self.src = TripdataZips(yms=yms, regions=regions, roots=kwargs.get('roots'))
        self.regions = src.regions
        self.nrows = nrows
        super().__init__(**kwargs)

    def zip(self, ym: YM, region: str) -> SampledZip:
        return SampledZip(ym=ym, region=region, nrows=self.nrows, **self.kwargs)

    @cached_property
    def children(self) -> list[SampledZip]:
        return [
            self.zip(ym=zip.yym, region=zip.region)
            for zip in self.src.children
        ]


SampledZips.cli(
    help=f"Generate test data by downsampling tripdata .csv.zip files. Writes to <root>/{DIR}.",
    decos=[dates, region],
)
