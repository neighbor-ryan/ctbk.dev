from click import option
from pandas.core.tools.datetimes import DatetimeScalar
from utz import *

# Types that can be passed to the Month constructor
Monthy = Union['Month', str, int, None]


@dataclass(init=False, order=True, eq=True, unsafe_hash=True)
class YM:
    y: int
    m: int

    rgx = r'(?P<year>\d{4})-?(?P<month>\d\d)'

    def _init_from_str(self, arg):
        m = fullmatch(self.rgx, arg)
        if not m:
            raise ValueError('Invalid month string: %s' % arg)
        year = int(m['year'])
        month = int(m['month'])
        self.__init__(year, month)

    def _verify(self):
        if not isinstance(self.y, int):
            raise ValueError('Year %s must be int, not %s' % (str(self.y), type(self.y)))
        if not isinstance(self.m, int):
            raise ValueError('Month %s must be int, not %s' % (str(self.m), type(self.m)))

    def _init_now(self):
        now = dt.now()
        self.y = now.year
        self.m = now.month

    def __init__(self, *args):
        if len(args) == 2:
            self.y, self.m = int(args[0]), int(args[1])
            self._verify()
        elif len(args) == 1:
            arg = args[0]
            if isinstance(arg, str):
                self._init_from_str(arg)
            elif isinstance(arg, int):
                self._init_from_str(str(arg))
            elif hasattr(arg, 'year') and hasattr(arg, 'month'):
                self.y = arg.y
                self.m = arg.m
                self._verify()
            elif arg is None:
                self._init_now()
            elif 'year' in arg and 'month' in arg:
                self.y = int(arg['year'])
                self.m = int(arg['month'])
                self._verify()
            else:
                raise ValueError('Unrecognized argument: %s' % str(arg))
        elif not args:
            self._init_now()
        else:
            raise ValueError('Unrecognized arguments: %s' % str(args))

    @property
    def year(self):
        return self.y

    @property
    def month(self):
        return self.m

    def str(self, sep=''):
        return '%d%s%02d' % (self.y, sep, self.m)

    def __str__(self):
        return self.str()

    def __int__(self):
        return int(str(self))

    def format(self, url, **kwargs):
        return url.format(ym=str(self), y=str(self.y), m=str(self.m), **kwargs)

    @property
    def dt(self) -> DatetimeScalar:
        return pd.to_datetime('%d-%02d' % (self.y, self.m))

    def __add__(self, n: int) -> 'YM':
        if not isinstance(n, int):
            raise ValueError('%s: can only add an integer to a Month, not %s: %s' % (str(self), str(type(n)), str(n)))
        y, m = self.y, self.m + n - 1
        y += m // 12
        m = (m % 12) + 1
        return YM(y, m)

    def __sub__(self, n: int) -> 'YM':
        if not isinstance(n, int):
            raise ValueError('%s: can only add an integer to a Month, not %s: %s' % (str(self), str(type(n)), str(n)))
        y, m = self.y, self.m - n
        if m <= 0:
            years = int(ceil(-m / 12))
            y -= years
            m += 12 * years
            assert 0 <= m < 12
        m += 1
        return YM(y, m)

    def until(self, end: 'YM' = None, step: int = 1) -> Generator['YM', None, None]:
        cur: YM = YM(self)
        while end is None \
                or (step > 0 and cur < end) \
                or (step < 0 and cur > end):
            yield cur
            cur = cur + step


GENESIS = YM(2013, 6)
END = None


def dates(fn):
    @option('-d', '--dates')
    def _fn(*args, dates=None, **kwargs):
        if dates:
            pcs = dates.split('-')
            if len(pcs) == 2:
                [ start, end ] = pcs
                start = YM(start) if start else GENESIS
                end = YM(end) if end else None
            elif len(pcs) == 1:
                [ym] = pcs
                ym = YM(ym)
                start = ym
                end = ym + 1
            else:
                raise ValueError(f"Unrecognized -d/--dates: {dates}")
        else:
            start, end = GENESIS, None
        fn(*args, start=start, end=end, **kwargs)

    _fn.__name__ = fn.__name__
    _fn.__doc__ = fn.__doc__
    return _fn
