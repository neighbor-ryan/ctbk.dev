from ctbk.monthly import MonthsDataset
from ctbk.normalized import NormalizedMonths


# class StationsMonth(MonthDataset):
#     def compute(self):
#         pass


class StationsMonths(MonthsDataset):
    # fmt = 'pqt'
    # month_cls = StationsMonth

    def deps(self):
        return (NormalizedMonths,)
