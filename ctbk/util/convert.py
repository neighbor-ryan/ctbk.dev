import boto3
from boto3 import client
from botocore import UNSIGNED
from botocore.client import Config, ClientError
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


def s3_exists(Bucket, Key, s3=None):
    if not s3:
        s3 = client('s3', config=Config(signature_version=UNSIGNED))
    try:
        s3.head_object(Bucket=Bucket, Key=Key)
        return True
    except ClientError:
        return False


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


def convert_file(
    fn,
    src_bkt=None, src_key=None, src=None,
    dst_bkt=None, dst_key=None, dst=None,
    bkt=None,
    error='warn',
    overwrite=False,
    public=False,
    **kwargs,
):
    src_bkt = verify_bucket(src_bkt, bkt)
    dst_bkt = verify_bucket(dst_bkt, bkt)

    ctx = dict()

    if any(v is not None for v in [src_bkt, src_key, src]):
        src_bkt, src_key, src = verify_pieces(src_bkt, src_key, src)
        ctx['src_bkt'] = src_bkt
        ctx['src_key'] = src_key
        ctx['src'] = src
        if src_key is not None:
            src_name = ctx['src_name'] = basename(src_key)

    if callable(dst_key):
        try:
            dst_key = ctx['dst_key'] = run(dst_key, ctx)
        except BadKey as e:
            if error == 'raise':
                raise e
            msg = 'Unrecognized key: %s' % src
            if error == 'warn':
                stderr.write('%s\n' % msg)
            return Result(msg=msg, status=BAD_DST)

    dst_bkt, dst_key, dst = verify_pieces(dst_bkt, dst_key, dst)
    ctx['dst_bkt'] = dst_bkt
    ctx['dst_key'] = dst_key
    ctx['dst'] = dst

    dst_name = ctx['dst_name'] = basename(dst_key)
    dst = ctx['dst'] = f's3://{dst_bkt}/{dst_key}'

    s3 = ctx['s3'] = client('s3', config=Config())
    ctx['error'] = error

    if s3_exists(dst_bkt, dst_key, s3=s3):
        if overwrite:
            msg = f'Overwrote {dst}'
            status = OVERWROTE
        else:
            msg = f'Found {dst}; skipping'
            status = FOUND
            return Result(msg=msg, status=status, dst=dst)
    else:
        msg = f'Wrote {dst}'
        status = WROTE

    def run_fn():
        value = run(fn, ctx, **kwargs)
        if isinstance(value, Upload):
            path = value.path
            s3.upload_file(path, dst_bkt, dst_key)
            return path
        else:
            return value

    args = getfullargspec(fn).args
    if 'src_path' in args:
        with TemporaryDirectory() as tmpdir:
            ctx['tmpdir'] = tmpdir
            name = basename(src_key)
            src_path = ctx['src_path'] = f'{tmpdir}/{name}'
            s3.download_file(src_bkt, src_key, src_path)
            value = run_fn()
    else:
        value = run_fn()

    if public:
        s3_resource = boto3.resource('s3')
        ObjectAcl = s3_resource.ObjectAcl
        object_acl = ObjectAcl(dst_bkt, dst_key)
        object_acl.put(ACL='public-read')

    return Result(msg=msg, status=status, dst=dst, value=value)
