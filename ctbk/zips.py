from typing import Optional

from utz import cached_property, DefaultDict
from utz.ym import YM

from ctbk.has_root_cli import HasRootCLI, dates
from ctbk.task import Task
from ctbk.util import S3
from ctbk.util.region import REGIONS, Region, get_regions, region

DIR = 'tripdata'


class TripdataZip(Task):
    DIR = DIR
    NAMES = ['zip']

    def __init__(self, ym, region, roots: Optional[DefaultDict[str]] = None):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.ym = YM(ym)
        self.region = region
        roots = roots or DefaultDict(configs={ self.NAMES[0]: S3 })
        Task.__init__(self, roots=roots)

    @cached_property
    def url(self):
        ym = self.ym
        region = self.region

        # Typos in 202206, 202207 and JC-202207
        citbike_typo_months = [ (202206, 'NYC'), (202207, 'NYC'), (202207, 'JC'), ]
        citibike = 'citbike' if (int(ym), region) in citbike_typo_months else 'citibike'

        extension = 'csv.zip'
        if region == 'NYC':
            # NYC zips are broken into multiple CSVs starting in 202404, maybe that's why the extension is just ".zip"?
            if ym.y < 2017 or int(ym) >= 202404:
                extension = 'zip'
            url = f'{self.dir}/{ym}-{citibike}-tripdata.{extension}'
        else:
            sep = ' ' if int(self.ym) == 201708 else '-'
            url = f'{self.dir}/{region}-{ym}{sep}{citibike}-tripdata.{extension}'
        return url


class TripdataZips(HasRootCLI):
    DIR = DIR
    CHILD_CLS = TripdataZip

    def __init__(
        self,
        yms: list[YM],
        regions: Optional[list[str]] = None,
        roots: Optional[DefaultDict[str]] = None,
        **kwargs,
    ):
        self.yms = yms
        self.regions = regions or REGIONS

        # "month" to "region" to "url" map
        m2r2u = [
            (
                ym,
                {
                    region: TripdataZip(ym=ym, region=region, roots=roots)
                    for region in self.regions
                    if region in get_regions(ym)
                }
            )
            for ym in self.yms
        ]

        # Default "end": current or previous calendar month
        end = max(yms) + 1
        end1, last1 = m2r2u[-1]
        missing1 = [ region for region, month in last1.items() if not month.exists() ]
        if missing1:
            if len(m2r2u) < 2:
                raise RuntimeError(f"Missing regions from {end1} ({', ' .join(missing1)})")
            else:
                end2, last2 = m2r2u[-2]
                missing2 = [ region for region, month in last2.items() if not month.exists() ]
                if missing2:
                    raise RuntimeError(f"Missing regions from {end1} ({', ' .join(missing1)}) and {end2} ({', '.join(missing2)})")
            end = end2 + 1
            m2r2u = dict(m2r2u[:-1])
        else:
            m2r2u = dict(m2r2u)

        self.m2r2u: dict[YM, dict[Region, TripdataZip]] = m2r2u
        self.end: YM = end

        super().__init__(**kwargs, roots=roots)

    @cached_property
    def children(self) -> list[TripdataZip]:
        return [
            u
            for r2u in self.m2r2u.values()
            for u in r2u.values()
        ]


TripdataZips.cli(
    help="Read .csv.zip files from s3://tripdata",
    cmd_decos=[dates, region],
    create=False,
    dag=False,
)
