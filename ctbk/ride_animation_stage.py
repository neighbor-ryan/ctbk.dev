"""DVX month-directory stage for deterministic packed ride animation."""
from json import dumps, loads
from pathlib import Path

from utz.ym import Monthy

from .has_root_cli import HasRootCLI, yms_arg
from .month_table import MonthTask
from .tasks import MonthsTasks


class RideAnimationMonth(MonthTask):
    DIR = 'ctbk/rides-animation'
    NAMES = ['ride-animation', 'ra']

    @property
    def url(self) -> str:
        return f'{self.dir}/{self.ym}'

    @property
    def cmd(self) -> str:
        return f'ctbk ride-animation create -w0 {self.ym}'

    def read(self) -> dict:
        return loads((Path(self.url) / 'month.json').read_text())

    def _create(self) -> dict:
        from .consolidated import ConsolidatedMonth
        from .ride_animation_cli import create
        create.callback(
            str(self.ym), Path(ConsolidatedMonth(self.ym).url).parent,
            'rides-anim-candidate/v1', Path(self.url), False,
        )
        report = self.read()
        # Storage-location fingerprints belong in the publication index;
        # the DVX stage records source content hashes in its dependencies.
        report.pop('inputs')
        (Path(self.url) / 'month.json').write_text(dumps(report, indent=2, sort_keys=True) + '\n')
        return report

    def dep_artifacts(self) -> list:
        from datetime import datetime
        from dvx.run.artifact import Artifact
        from .consolidated import ConsolidatedMonth
        from .pyramid_cascade.rides_assets import GEO_JSON_PATH, ID_MAP_PATH, STATION_LUC_PATH
        month = datetime.strptime(str(self.ym), '%Y%m')
        following = datetime(month.year + 1, 1, 1) if month.month == 12 else month.replace(month=month.month + 1)
        months = [str(self.ym)]
        next_source = Path(ConsolidatedMonth(f'{following:%Y%m}').url)
        final = self.read()['final'] if (Path(self.url) / 'month.json').exists() else next_source.exists() or Path(f'{next_source}.dvc').exists()
        if final:
            months.append(f'{following:%Y%m}')
        sources = [Path(ConsolidatedMonth(ym).url) for ym in months]
        code = Path(__file__).parent
        deps = [*sources, ID_MAP_PATH, STATION_LUC_PATH, GEO_JSON_PATH, code / 'ride_animation.py', code / 'ride_animation_cli.py', Path(__file__), code / 'pyramid_cascade' / 'rides_assets.py', code / 'pyramid_cascade' / 'rides_source.py']
        return [Artifact.from_dvc(path) or Artifact.from_path(path) for path in deps]


class RideAnimationMonths(HasRootCLI, MonthsTasks):
    CHILD_CLS = RideAnimationMonth

    def month(self, ym: Monthy) -> RideAnimationMonth:
        return RideAnimationMonth(ym)


RideAnimationMonths.cli(help='Build packed ride-animation month directories with DVX source/mapping/code provenance.', cmd_decos=[yms_arg])
