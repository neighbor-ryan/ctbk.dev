import calendar
import gzip
from abc import ABC
from os.path import basename, join
from shutil import copyfileobj, move
from tempfile import TemporaryDirectory
from typing import Optional, Union, Iterator, IO, Tuple, Literal
from zipfile import ZipFile, BadZipFile

import pandas as pd
from click import argument, option
from utz import cached_property, Unset, err, singleton
from utz.gzip import DeterministicGzipFile, deterministic_gzip_open
from utz.ym import YM

from ctbk.has_root_cli import HasRootCLI, dates
from ctbk.table import Table
from ctbk.task import Task
from ctbk.util.constants import BKT
from ctbk.util.df import DataFrame
from ctbk.util.read import Read
from ctbk.util.region import REGIONS, Region, region
from ctbk.zips import TripdataZips, TripdataZip

DIR = f'{BKT}/csvs'

NameType = Literal[1, 11, 2, 22, None]


class ReadsTripdataZip(Task, ABC):
    def __init__(
        self,
        ym: YM,
        region: Region,
        name_type: NameType = None,
        **kwargs,
    ):
        if region not in REGIONS:
            raise ValueError(f"Unrecognized region: {region}")
        self.ym = ym
        self.region = region
        self.name_type = name_type
        super().__init__(**kwargs)

    @cached_property
    def src(self) -> TripdataZip:
        return TripdataZip(ym=self.ym, region=self.region, roots=self.roots)


DEFAULT_COMPRESSION_LEVEL = 9
READ_DTYPES = {
    **{
        c: str
        for c in [
            'start station id', 'end station id',
            'Start Station ID', 'End Station ID',
            'start_station_id', 'end_station_id',
        ]
    },
    'birth year': 'Int16',
    'Birth Year': 'Int16',
    'gender': 'Int8',
    'Gender': 'Int8',
}
READ_KWARGS = dict(
    dtype=READ_DTYPES,
    na_values=r'\N',
)


class TripdataCsv(ReadsTripdataZip, Table):
    DIR = DIR
    NAMES = [ 'csv', 'c', ]

    @property
    def region_str(self) -> str:
        return 'JC-' if self.region == 'JC' else ''

    @property
    def name(self):
        return f'{self.region_str}{self.ym}'

    @property
    def basename(self) -> str:
        return f'{self.name}-citibike-tripdata.csv.gz'

    @cached_property
    def url(self) -> str:
        return f'{self.dir}/{self.basename}'

    @classmethod
    def open_path(cls, path: str, mode: str, compression_level: int = DEFAULT_COMPRESSION_LEVEL):
        if path.endswith('.gz'):
            if mode.startswith("r"):
                return gzip.open(path, mode)
            elif mode.startswith("w"):
                return deterministic_gzip_open(
                    path=path,
                    mode=mode,
                    compression_level=compression_level,
                )
            else:
                raise ValueError(f"Unsupported mode: {mode}")
        else:
            return open(path, mode)


    @classmethod
    def sort_and_write(cls, name, in_path, out_fd):
        df = pd.read_csv(in_path, **READ_KWARGS)
        df.index.name = "lineno"
        df = df.reset_index()
        path_cols = TripdataCsv.infer_sort_cols(df)
        err(f"{name}: inferred default columns: {', '.join(path_cols)}")

        df = df.sort_values(path_cols)
        linenos = df.lineno.tolist()

        with (
            cls.open_path(in_path, 'rb') as input_file,
            out_fd as output_file
        ):
            header, *lines = list(input_file)
            output_file.write(header)
            nonempty_lines = []
            empty_line_idxs = []
            for idx, line in enumerate(lines):
                if line == b'\n':
                    empty_line_idxs.append(idx)
                else:
                    nonempty_lines.append(line)
            lines = nonempty_lines
            if empty_line_idxs:
                err(f"{name}: removed {len(empty_line_idxs)} empty lines: {', '.join(map(str, empty_line_idxs))}")
            if len(lines) != len(df):
                raise ValueError(f"{name}: {len(lines)} lines != {len(df)} DF size")
            for lineno in linenos:
                output_file.write(lines[lineno])

    def extract_csv_from_zip(self):
        with TemporaryDirectory() as tmpdir:
            tmp_path = join(tmpdir, f'{self.name}.csv')
            with open(tmp_path, 'wb') as o:
                header = None
                for fdno, i in enumerate(self.zip_csv_fds()):
                    line = next(i)
                    if fdno == 0:
                        header = line
                        o.write(line)
                    else:
                        header2 = line
                        if header != header2:
                            raise RuntimeError(f"Header mismatch in {self.src.url} (CSV idx {fdno}): {header} != {header2}")
                        o.write(b'\n')
                    copyfileobj(i, o)

            with self.fd('wb') as raw_o:
                self.sort_and_write(
                    self.name,
                    tmp_path,
                    DeterministicGzipFile(
                        fileobj=raw_o,
                        mode='wb',
                        compresslevel=DEFAULT_COMPRESSION_LEVEL
                    )
                )

    def _create(self, read: Union[None, Read] = Unset) -> None:
        read = self.read if read is Unset else read
        if read is None:
            self.extract_csv_from_zip()
        else:
            return self.checkpoint(read=read)

    def zip_csv_fds(self) -> Iterator[IO]:
        """Return a read fd for the single CSV in the source .zip."""
        src = self.src
        ym = self.ym
        zip_yym = int(self.src.yym)
        with src.fd('rb') as z_in:
            z = ZipFile(z_in)
            names = z.namelist()
            err(f'{src.url}: zip names: {names}')

            if zip_yym < 2024:
                dir0 = f'{zip_yym}-citibike-tripdata'
                if zip_yym >= 2020:
                    # 2020-2023 have per-month Zips nested inside the year Zip
                    inner_zip_name = f'{dir0}/{ym}-citibike-tripdata.zip'
                    with z.open(inner_zip_name, 'r') as inner_zip_fd:
                        inner_zip = ZipFile(inner_zip_fd)
                        csvs = list(sorted([ f for f in inner_zip.namelist() if f.endswith('.csv') and not f.startswith('_') ]))
                        err(f"{ym}: loaded CSVs from annual inner zip: {csvs}")
                        for csv in csvs:
                            yield inner_zip.open(csv, 'r')
                        return
                else:
                    month = ym.m
                    month_name = calendar.month_name[month]
                    dir1 = f'{month}_{month_name}'
                    # "Type 1" name patterns:
                    # ```
                    # $ uzl $z18 | ew csv | rv MACOS | snk2 -t_ | snk3 -st_ | snk2 -st/ | last
                    # …
                    # 2018-citibike-tripdata/3_March/201803-citibike-tripdata_1.csv
                    # 2018-citibike-tripdata/4_April/201804-citibike-tripdata_1.csv
                    # 2018-citibike-tripdata/4_April/201804-citibike-tripdata_2.csv
                    # …
                    # ```
                    # In 2017, there's an extra ".csv" before the "_<N>" suffix:
                    # ```
                    # $ uzl $z17 | ew csv | rv MACOS | snk3 -st_ | snk2 -st/ | last
                    # …
                    # 2017-citibike-tripdata/3_March/201703-citibike-tripdata.csv_1.csv
                    # 2017-citibike-tripdata/4_April/201704-citibike-tripdata.csv_1.csv
                    # 2017-citibike-tripdata/4_April/201704-citibike-tripdata.csv_2.csv
                    # …
                    # ```
                    prefix1 = f'{dir0}/{dir1}/{ym}-citibike-tripdata{".csv" if ym.y == 2017 else ""}_'
                    csvs1 = list(sorted([ f for f in names if f.startswith(prefix1) and f.endswith('.csv') ]))

                    # "Type 2" name patterns:
                    # ```
                    # 2013-citibike-tripdata/201306-citibike-tripdata.csv
                    # 2018-citibike-tripdata/201801-citibike-tripdata.csv
                    # ```
                    name2 = f'{dir0}/{ym}-citibike-tripdata.csv'
                    csvs2 = list(sorted([ f for f in names if f == name2 ]))

                    if not csvs1 and not csvs2:
                        raise RuntimeError(f"Failed to find CSVs in {src.url} with name patterns: {prefix1}*.csv, {name2}")
                    pattern = self.name_type
                    if pattern is None or pattern == 1:
                        csvs = csvs1 if csvs1 else csvs2
                    elif pattern == 2:
                        csvs = csvs2 if csvs2 else csvs1
                    elif pattern == 11:
                        if csvs2 and not csvs1:
                            raise RuntimeError(f'Expected "type 1" CSV name patterns ({prefix1}*.csv), but only found {csvs2}')
                        csvs = csvs1
                    elif pattern == 22:
                        if csvs1 and not csvs2:
                            raise RuntimeError(f'Expected "type 2" CSV name pattern ({name2}), but only found {csvs1}')
                        csvs = csvs2
                    err(f"{ym}: loaded CSVs from annual zip: {csvs}")
            else:
                csvs = list(sorted([ f for f in names if f.endswith('.csv') and not f.startswith('_') ]))
            if len(csvs) > 1:
                err(f"Found {len(csvs)} CSVs in {src.url}: {csvs}")

            if not csvs:
                # 202409-citibike-tripdata.zip contains 5 `.csv.zip`s
                csv_zip_names = [
                    f
                    for f in names
                    if f.endswith('.csv.zip')
                       and not f.startswith('_')
                       and not basename(f).startswith('_')
                ]
                for csv_zip_name in csv_zip_names:
                    with z.open(csv_zip_name, 'r') as csv_zip_fd:
                        try:
                            csv_zip = ZipFile(csv_zip_fd)
                        except BadZipFile:
                            raise ValueError(f"Failed to open nested zip file {csv_zip_name} inside {src.url}")
                        csv_zip_names = csv_zip.namelist()
                        csvs = list(sorted([ f for f in csv_zip_names if f.endswith('.csv') and not f.startswith('_') ]))
                        for csv in csvs:
                            yield csv_zip.open(csv, 'r')
            else:
                for name in csvs:
                    yield z.open(name, 'r')

    def meta(self):
        if self.exists():
            return pd.read_csv(self.url, **READ_KWARGS, nrows=0)
        else:
            with next(self.zip_csv_fds()) as i:
                return pd.read_csv(i, **READ_KWARGS, nrows=0)

    def _df(self) -> DataFrame:
        self._create(read=None)
        return pd.read_csv(self.url, **READ_KWARGS)

    @classmethod
    def infer_sort_cols(cls, df) -> list[str]:
        """Infer default columns to sort by.

        NY    201306-201610:  "starttime",  "stoptime",  "bikeid"
           NJ 201509-201704: "Start Time", "Stop Time", "Bike ID"
        NY NJ 201610-201704: "Start Time", "Stop Time", "Bike ID"
        NY NJ 201704-202102:  "starttime",  "stoptime",  "bikeid"
        NY NJ 202102-202411: "started_at",  "ended_at", "ride_id"
        """
        start_time_col = singleton([ c for c in [ 'Start Time', 'starttime', 'started_at' ] if c in df.columns ])
        stop_time_col = singleton([ c for c in [ 'Stop Time', 'stoptime', 'ended_at' ] if c in df.columns ])
        id_col = singleton([ c for c in [ 'Bike ID', 'bikeid', 'ride_id' ] if c in df.columns ])
        path_cols = [ start_time_col, stop_time_col, id_col ]
        return path_cols

    @property
    def checkpoint_kwargs(self):
        return dict(fmt='csv', read_kwargs=dict(dtype=str), write_kwargs=dict(index=False))

    def _read(self) -> DataFrame:
        return pd.read_csv(self.url, **READ_KWARGS)


class TripdataCsvs(HasRootCLI):
    DIR = DIR
    CHILD_CLS = TripdataCsv

    def __init__(
        self,
        yms: list[YM],
        regions: Optional[list[str]] = None,
        name_type: NameType = None,
        **kwargs,
    ):
        self.src = TripdataZips(yms=yms, regions=regions, roots=kwargs.get('roots'))
        self.yms = yms
        self.regions = regions or REGIONS
        self.name_type = name_type
        super().__init__(**kwargs)

    @cached_property
    def children(self) -> list[TripdataCsv]:
        return [
            TripdataCsv(ym=u.ym, region=u.region, name_type=self.name_type, **self.kwargs)
            for u in self.src.children
        ]

    @property
    def m2r2csv(self) -> dict[YM, dict[Region, TripdataCsv]]:
        m2r2u = self.src.m2r2u
        return {
            ym: {
                region: TripdataCsv(ym=ym, region=region, **self.kwargs)
                for region in r2u
            }
            for ym, r2u in m2r2u.items()
        }

    def m2df(self):
        return {
            m: pd.concat([
                csv.df()
                for r, csv in r2csv.items()
            ])
            for m, r2csv in self.m2r2csv.items()
        }


cli = TripdataCsvs.cli(
    help=f"Extract CSVs from \"tripdata\" .zip files. Writes to <root>/{DIR}.",
    cmd_decos=[
        dates,
        region,
        option('-t', '--name-type', type=int, help='1: prefer type 1, 2: prefer type 2, 11: require type 1, 22: require type 2'),
    ],
)

@cli.command('sort')
@option('-n', '--dry-run', is_flag=True, help="Don't write files, just print what would be done")
@option('-z', '--compression-level', type=int, default=DEFAULT_COMPRESSION_LEVEL, help="Gzip compression level")
@argument('paths', nargs=-1)
def sort(
    dry_run: bool,
    compression_level: int,
    paths: Tuple[str, ...],
):
    """Sort one or more `.csv{,.gz}`'s in-place, remove empty lines"""
    for path in paths:
        if dry_run:
            continue

        with TemporaryDirectory() as tmpdir:
            tmp_path = join(tmpdir, basename(path))
            TripdataCsv.sort_and_write(
                basename(path),
                path,
                TripdataCsv.open_path(tmp_path, 'wb', compression_level=compression_level),
            )
            move(tmp_path, path)
