from os.path import exists
from typing import Optional

from utz import cached_property
from utz.ym import YM

from ctbk.has_root_cli import HasRootCLI, yms_arg
from ctbk.task import Task
from ctbk.tripdata_month import TripdataMonth
from ctbk.util.region import REGIONS, Region, get_regions, region

DIR = 'tripdata'


class TripdataZip(Task):
    DIR = DIR
    NAMES = ['zip']

    def __init__(
        self,
        ym: YM,
        region: Region,
    ):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.ym = ym
        self.yym = ym.y if region == 'NYC' and ym.y < 2024 else ym
        self.region = region
        Task.__init__(self)

    @cached_property
    def url(self):
        ymi = int(self.yym)
        region = self.region
        return f'{self.dir}/{"JC-" if region == "JC" else ""}{ymi}-citibike-tripdata.zip'


class TripdataZips(HasRootCLI):
    DIR = DIR
    CHILD_CLS = TripdataZip

    def __init__(
        self,
        yms: list[YM],
        regions: Optional[list[str]] = None,
        **kwargs,
    ):
        self.yms = yms
        self.regions = regions or REGIONS

        # "month" to "region" to "url" map
        m2r2u = [
            (
                ym,
                {
                    region: TripdataMonth(ym=ym, region=region)
                    for region in self.regions
                    if region in get_regions(ym)
                }
            )
            for ym in self.yms
        ]

        # Default "end": current or previous calendar month
        end = max(yms) + 1
        end1, last1 = m2r2u[-1]
        missing1 = [ region for region, month in last1.items() if not exists(month.dvc_path) ]
        if missing1:
            if len(m2r2u) < 2:
                raise RuntimeError(f"Missing regions from {end1} ({', ' .join(missing1)})")
            else:
                end2, last2 = m2r2u[-2]
                missing2 = [ region for region, month in last2.items() if not exists(month.dvc_path) ]
                if missing2:
                    raise RuntimeError(f"Missing regions from {end1} ({', ' .join(missing1)}) and {end2} ({', '.join(missing2)})")
            end = end2 + 1
            m2r2u = dict(m2r2u[:-1])
        else:
            m2r2u = dict(m2r2u)

        self.m2r2u: dict[YM, dict[Region, TripdataZip]] = m2r2u
        self.end: YM = end

        super().__init__(**kwargs)

    @cached_property
    def children(self) -> list[TripdataZip]:
        return [
            u
            for r2u in self.m2r2u.values()
            for u in r2u.values()
        ]


TripdataZips.cli(
    help="Read .csv.zip files from s3://tripdata",
    cmd_decos=[yms_arg, region],
    create=False,
)
