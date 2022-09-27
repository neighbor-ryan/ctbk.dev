from numpy import nan
from pandas import Series
from sys import stderr
from utz import sxs

from ctbk import StationMetaHist, Month, Monthy
from ctbk.monthly import BKT, MonthsDataset, GENESIS
from ctbk.util.convert import WROTE


def row_sketch(a):
    restsum = sum(a[1:])
    total = a[0] + restsum
    num = len(a)
    return {
        'mode_count': a[0],
        'second': a[1] if num > 1 else nan,
        'restsum': restsum,
        'total': total,
        'counts': a,
        'first/second': a[0] / a[1] if num > 1 else nan,
        'mode_pct': a[0] / total,
        'num': num,
    }


def mode_sketch(df, groupby, thresh=0.5, sum_key='count'):
    idx_name = df.index.name
    if not idx_name:
        raise RuntimeError('Index needs a name')
    if isinstance(groupby, str):
        groupby = [groupby]
    row_groups = df.reset_index().groupby([idx_name] + groupby)
    if sum_key in df:
        row_hist = row_groups[sum_key].sum()
    else:
        row_hist = row_groups.size().rename(sum_key)
    row_hist = row_hist.reset_index()
    counts = row_hist.groupby(idx_name)[sum_key].apply(lambda s: list(reversed(sorted(s.values))))
    row_sketches = counts.apply(row_sketch).apply(Series)
    below_thresh = row_sketches[row_sketches.mode_pct < thresh]
    if not below_thresh.empty:
        stderr.write(f'{len(below_thresh)} index entries with mode_pct < {thresh}:\n{below_thresh}\n')
    annotated = (
        row_hist
            .sort_values([idx_name, sum_key], ascending=False)
            .drop_duplicates(subset=idx_name)
            .set_index(idx_name)
    )
    annotated = sxs(annotated, row_sketches).drop(columns=[sum_key]).sort_values('mode_pct')
    return annotated


class StationModes(MonthsDataset):
    SRC_CLS = StationMetaHist
    ROOT = f's3://{BKT}/stations/ids'

    def input_range(self, start: Monthy = None, end: Monthy = None):
        latest = not start and not end
        start = Month(start) if start else GENESIS
        end = Month(end) or Month()
        src = self.src.path(start, end)
        dst = self.path(start, end)
        if latest:
            latest_dst = self.path()
        else:
            latest_dst = None
        return [{ 'src': src, 'dst': dst, 'latest_dst': latest_dst, }]

    def compute(self, src_df, dst, latest_dst,):
        annotated_station_names = mode_sketch(src_df.set_index('Station ID')[['Station Name', 'count']], 'Station Name')
        annotated_stations = mode_sketch(src_df.set_index('Station ID')[['Latitude', 'Longitude', 'count',]], ['Latitude', 'Longitude',])
        stations = sxs(annotated_station_names['Station Name'], annotated_stations[['Latitude', 'Longitude',]])
        stations.to_parquet(dst)
        if latest_dst:
            print(f'Copying "latest" {dst} to {latest_dst}')
            self.fs.copy(dst, latest_dst)
        return WROTE


if __name__ == '__main__':
    StationModes.cli()
