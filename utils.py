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


def run(fn, ctx):
    spec = getfullargspec(fn)
    args = spec.args
    defaults = spec.defaults or ()
    pos_args = args[:-len(defaults)]
    missing_args = [ arg for arg in pos_args if arg not in ctx ]
    if missing_args:
        raise ValueError('Missing arguments for function %s: %s' % (str(fn), ','.join(missing_args)))
    kwargs = { k:v for k,v in ctx.items() if k in args }
    return fn(**kwargs)


def convert_file(
    src_bkt, src_key,
    dst_bkt, dst_key,
    fn,
    error='warn',
    overwrite=False,
    public=False,
):
    ctx = dict(
        src_bkt=src_bkt, src_key=src_key,
        dst_bkt=dst_bkt,
        error=error,
    )

    src_name = ctx['src_name'] = basename(src_key)
    src = ctx['src'] = f's3://{src_bkt}/{src_key}'

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
        result = run(fn, ctx)
        dst_path = result.get('dst_path')
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

    msg = result.get('msg', msg)

    if public:
        s3_resource = boto3.resource('s3')
        ObjectAcl = s3_resource.ObjectAcl
        object_acl = ObjectAcl(dst_bkt, dst_key)
        object_acl.put(ACL='public-read')

    return o(msg=msg, **result)
