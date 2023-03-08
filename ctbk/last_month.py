#!/usr/bin/env python
import json
from os.path import exists

from utz import process, cd

from ctbk import NormalizedMonths
from ctbk.month_agg_table import MonthAggTable


class LastMonth(MonthAggTable):
    SRC_CLS = NormalizedMonths
    OUT = 'www/public/assets/last_month.json'
    regenerate_screenshots = True

    def _run(self):
        last_month = self.end - 1
        last_month_str = str(last_month)
        if exists(self.out):
            with open(self.out, 'r') as f:
                cur_last_month_str = json.load(f)
        else:
            cur_last_month_str = None

        if cur_last_month_str != last_month_str:
            with open(self.out, 'w') as f:
                json.dump(last_month_str, f)

            if self.regenerate_screenshots:
                with cd('www'):
                    process.run('node', 'screenshots.js')



if __name__ == '__main__':
    LastMonth.main()
