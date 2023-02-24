from inspect import getfullargspec


def decos(decs):
    def _fn(fn):
        for dec in reversed(decs):
            fn = dec(fn)
        return fn

    return _fn


def spec_args(fn, kwargs):
    spec = getfullargspec(fn)
    args = spec.args
    return { k: v for k, v in kwargs.items() if k in args }
