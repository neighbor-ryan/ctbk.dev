from inspect import getfullargspec

from utz import *


class BadKey(Exception):
    pass


# Result/Status "enum"
OVERWROTE = 'OVERWROTE'
FOUND = 'FOUND'
WROTE = 'WROTE'
BAD_DST = 'BAD_DST'


@dataclass
class Upload:
    path: str


@dataclass
class Result:
    msg: str
    status: str
    dst: Optional[str] = None
    value: Optional[any] = None

    @property
    def did_write(self):
        return self.status == WROTE or self.status == OVERWROTE


def run(fn, ctx, **kwargs):
    spec = getfullargspec(fn)
    args = spec.args
    defaults = spec.defaults or ()
    pos_args = args[:-len(defaults)]
    missing_args = [ arg for arg in pos_args if arg not in ctx ]
    if missing_args:
        raise ValueError('Missing arguments for function %s: %s' % (str(fn), ','.join(missing_args)))
    ctx_kwargs = { k: v for k, v in ctx.items() if k in args }
    return fn(**ctx_kwargs, **kwargs)


def verify_pieces(bkt, key, uri):
    rgx = 's3://(?P<bkt>[^/]+)(?:/(?P<key>.*))?'
    if uri is None:
        if not bkt:
            raise ValueError('No bkt found: %s %s' % (bkt, uri))
        if key:
            uri = f's3://{bkt}/{key}'
    else:
        m = match(rgx, uri)
        if not m:
            raise ValueError('Invalid uri: %s' % uri)
        if bkt is None:
            bkt = m['bkt']
        elif bkt != m['bkt']:
            raise('`bkt` %s != `uri` bucket %s' % (bkt, m['bkt']))
        if key is None:
            key = m['key']
        elif key != m['key']:
            raise('`key` %s != `uri` key %s' % (key, m['key']))

    return bkt, key, uri


def verify_bucket(bkt, default):
    if default is not None:
        if bkt is None:
            bkt = default
        elif bkt != default:
            raise ValueError('`a` %s != `b` %s' % (bkt, default))
    return bkt
