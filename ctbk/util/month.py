from pandas.core.tools.datetimes import DatetimeScalar
from utz import *


@dataclass(init=False, order=True)
class Month:
    year: int
    month: int

    rgx = r'(?P<year>\d{4})-?(?P<month>\d\d)'

    def _init_from_str(self, arg):
        m = fullmatch(self.rgx, arg)
        if not m:
            raise ValueError('Invalid month string: %s' % arg)
        year = int(m['year'])
        month = int(m['month'])
        self.__init__(year, month)

    def _verify(self):
        if not isinstance(self.year, int):
            raise ValueError('Year %s must be int, not %s' % (str(self.year), type(self.year)))
        if not isinstance(self.month, int):
            raise ValueError('Month %s must be int, not %s' % (str(self.month), type(self.month)))

    def _init_now(self):
        now = dt.now()
        self.year = now.year
        self.month = now.month

    def __init__(self, *args):
        if len(args) == 2:
            self.year, self.month = int(args[0]), int(args[1])
            self._verify()
        elif len(args) == 1:
            arg = args[0]
            if isinstance(arg, str):
                self._init_from_str(arg)
            elif isinstance(arg, int):
                self._init_from_str(str(arg))
            elif hasattr(arg, 'year') and hasattr(arg, 'month'):
                self.year = arg.year
                self.month = arg.month
                self._verify()
            elif 'year' in arg and 'month' in arg:
                self.year = int(arg['year'])
                self.month = int(arg['month'])
                self._verify()
            elif arg is None:
                self._init_now()
            else:
                raise ValueError('Unrecognized argument: %s' % str(arg))
        elif not args:
            self._init_now()
        else:
            raise ValueError('Unrecognized arguments: %s' % str(args))

    @property
    def y(self):
        return self.year

    @property
    def m(self):
        return self.month

    def str(self, sep=''):
        return '%d%s%02d' % (self.y, sep, self.m)

    def __str__(self):
        return self.str()

    @property
    def dt(self) -> DatetimeScalar:
        return pd.to_datetime('%d-%02d' % (self.year, self.month))

    def __add__(self, n: int) -> 'Month':
        if not isinstance(n, int):
            raise ValueError('%s: can only add an integer to a Month, not %s: %s' % (str(self), str(type(n)), str(n)))
        y, m = self.y, self.m + n - 1
        y += m // 12
        m = (m % 12) + 1
        return Month(y, m)

    def __sub__(self, n: int) -> 'Month':
        if not isinstance(n, int):
            raise ValueError('%s: can only add an integer to a Month, not %s: %s' % (str(self), str(type(n)), str(n)))
        y, m = self.y, self.m - n
        if m <= 0:
            years = int(ceil(-m / 12))
            y -= years
            m += 12 * years
            assert 0 <= m < 12
        m += 1
        return Month(y, m)

    def until(self, end: 'Month' = None, step: int = 1) -> Generator['Month', None, None]:
        cur = Month(self)
        while end is None \
                or (step > 0 and cur < end) \
                or (step < 0 and cur > end):
            yield cur
            cur = cur + step
