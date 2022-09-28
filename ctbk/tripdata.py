from ctbk import MonthsDataset, cached_property


class Tripdata(MonthsDataset):
    ROOT = 'tripdata'
    RGX = r'^(?:(?P<region>JC)-)?(?P<month>\d{6})[ \-]citi?bike-tripdata?(?P<csv>\.csv)?(?P<zip>\.zip)?$'

    @cached_property
    def parsed_basenames(self):
        df = super().parsed_basenames
        df = df.dropna(subset=['month']).astype({ 'month': int })
        df['region'] = df['region'].fillna('NYC')
        return df
