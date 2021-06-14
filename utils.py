import boto3
from boto3 import client
from botocore import UNSIGNED
from botocore.client import Config, ClientError
from inspect import getfullargspec

from utz import *


class BadKey(Exception): pass


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
    ctx_kwargs = { k:v for k,v in ctx.items() if k in args }
    return fn(**ctx_kwargs, **kwargs)


def verify_pieces(bkt, key, uri):
    rgx = 's3://(?P<bkt>[^/]+)(?:/(?P<key>.*))?'
    if uri is None:
        if not bkt:
            raise ValueError('No bkt found: %s %s' % (bkt, uri))
        if not key:
            raise ValueError('No key found: %s %s' % (key, uri))
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


# def paths(
#     src_bkt=None, src_key=None, src=None,
#     dst_bkt=None, dst_key=None, dst=None,
#     bkt=None,
# ):
#     src_bkt = verify_bucket(src_bkt, bkt)
#     dst_bkt = verify_bucket(dst_bkt, bkt)
#
#     src_bkt, src_key, src = verify_pieces(src_bkt, src_key, src)
#     dst_bkt, dst_key, dst = verify_pieces(dst_bkt, dst_key, dst)
#
#     return src_bkt, src_key, src, dst_bkt, dst_key, dst


def convert_file(
    fn,
    src_bkt=None, src_key=None, src=None,
    dst_bkt=None, dst_key=None,
    bkt=None,
    error='warn',
    overwrite=False,
    public=False,
    **kwargs,
):
    src_bkt = verify_bucket(src_bkt, bkt)
    dst_bkt = verify_bucket(dst_bkt, bkt)

    src_bkt, src_key, src = verify_pieces(src_bkt, src_key, src)

    ctx = dict(
        src_bkt=src_bkt, src_key=src_key, src=src, src_name=basename(src_key),
        dst_bkt=dst_bkt,
        error=error,
    )

    if callable(dst_key):
        try:
            dst_key = ctx['dst_key'] = run(dst_key, ctx)
        except BadKey as e:
            if error == 'raise':
                raise e
            msg = 'Unrecognized key: %s' % src
            if error == 'warn':
                stderr.write('%s\n' % msg)
            return dict(msg=msg)

    dst_name = ctx['dst_name'] = basename(dst_key)
    dst = ctx['dst'] = f's3://{dst_bkt}/{dst_key}'
    s3 = ctx['s3'] = client('s3', config=Config())

    if s3_exists(dst_bkt, dst_key, s3=s3):
        if overwrite:
            msg = f'Overwrote {dst}'
        else:
            msg = f'Found {dst}; skipping'
            return dict(msg=msg)
    else:
        msg = f'Wrote {dst}'

    def run_fn():
        result = run(fn, ctx, **kwargs)
        dst_path = result and result.get('dst_path')
        if dst_path:
            s3.upload_file(dst_path, dst_bkt, dst_key)
        return result

    args = getfullargspec(fn).args
    if 'src_path' in args:
        with TemporaryDirectory() as tmpdir:
            ctx['tmpdir'] = tmpdir
            name = basename(src_key)
            src_path = ctx['src_path'] = f'{tmpdir}/{name}'
            s3.download_file(src_bkt, src_key, src_path)
            result = run_fn()
    else:
        result = run_fn()

    if result is None:
        result = {}
    msg = result.get('msg', msg)

    if public:
        s3_resource = boto3.resource('s3')
        ObjectAcl = s3_resource.ObjectAcl
        object_acl = ObjectAcl(dst_bkt, dst_key)
        object_acl.put(ACL='public-read')

    return o(msg=msg, **result)
