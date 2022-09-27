from ctbk import MonthsDataset, Monthy, Month


class Tripdata(MonthsDataset):
    ROOT = 'tripdata'
    RGX = r'^(?:(?P<region>JC)-)?(?P<month>\d{6})[ \-]citi?bike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'

    def outputs(self, start: Monthy = None, end: Month = None, rgx=None, endswith=None):
        df = super().outputs(start, end, rgx, endswith)
        df = df.dropna(subset=['month']).astype({ 'month': int })
        df['region'] = df['region'].fillna('NYC')
        return df
