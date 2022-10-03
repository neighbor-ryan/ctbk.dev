from os.path import splitext

from click import option

from ctbk.monthly import Dataset, PARQUET_EXTENSION


class Aggregator(Dataset):
    @classmethod
    def cli_opts(cls):
        return super().cli_opts() + [
            # Features to group by
            option('-y/-Y', '--year/--no-year'),
            option('-m/-M', '--month/--no-month'),
            option('-w/-W', '--weekday/--no-weekday'),
            option('-h/-H', '--hour/--no-hour'),
            option('-r/-R', '--region/--no-region'),
            option('-g/-G', '--gender/--no-gender'),
            option('-t/-T', '--user-type/--no-user-type'),
            option('-b/-B', '--rideable-type/--no-rideable-type'),
            option('-s/-S', '--start-station/--no-start-station'),
            option('-e/-E', '--end-station/--no-end-station'),
            # Features to aggregate
            option('-c/-C', '--counts/--no-counts', default=True),
            option('-d/-D', '--durations/--no-durations'),
            option('--sort-agg-keys/--no-sort-agg-keys'),
        ]

    def __init__(
            self,
            # Features to group by
            year=False,
            month=False,
            weekday=False,
            hour=False,
            region=False,
            gender=False,
            user_type=False,
            rideable_type=False,
            start_station=False,
            end_station=False,
            # Features to aggregate
            counts=True,
            durations=False,
            # Misc
            sort_agg_keys=False,
            **kwargs
    ):
        self.year = year
        self.month = month
        self.weekday = weekday
        self.hour = hour
        self.region = region
        self.gender = gender
        self.user_type = user_type
        self.rideable_type = rideable_type
        self.start_station = start_station
        self.end_station = end_station
        self.counts = counts
        self.durations = durations
        self.sort_agg_keys = sort_agg_keys
        super().__init__(**kwargs)

    @property
    def agg_keys(self):
        agg_keys = {
            'y': self.year,
            'm': self.month,
            'w': self.weekday,
            'h': self.hour,
            'r': self.region,
            'g': self.gender,
            't': self.user_type,
            'b': self.rideable_type,
            's': self.start_station,
            'e': self.end_station,
        }
        return { k: v for k, v in agg_keys.items() if v }

    @property
    def agg_keys_label(self):
        agg_keys = self.agg_keys
        if self.sort_agg_keys:
            agg_keys = dict(sorted(list(agg_keys.items()), key=lambda t: t[0]))
        return "".join(agg_keys.keys())

    @property
    def sum_keys(self):
        return { k: v for k, v in { 'c': self.counts, 'd': self.durations, }.items() if v }

    @property
    def sum_keys_label(self):
        return ''.join([ label for label, flag in self.sum_keys.items() ])

    def path(self, start=None, end=None, extension=None, root=None):
        pcs = [
            self.agg_keys_label,
            self.sum_keys_label,
        ]
        if start and end:
            pcs += [f'{start}:{end}']
        root = root or self.root
        path, _extension = splitext(root)
        extension = _extension or extension or self.EXTENSION
        name = "_".join(pcs) + extension
        return f'{root or self.root}/{name}'
