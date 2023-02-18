from click import pass_context, option, group, Choice
from utz import o, DefaultDict

from ctbk.util import write, read
from ctbk.util.read import Disk
from ctbk.util.constants import S3, DEFAULT_ROOT
from ctbk.util.region import REGIONS
from ctbk.util.write import IfAbsent

dask = option('--dask', is_flag=True)
region = option('-r', '--region', type=Choice(REGIONS))


@group('ctbk')
@pass_context
@option('-r', '--read', 'reads', multiple=True)
@option('-t', '--root', 'roots', multiple=True)
@option('-w', '--write', 'writes', multiple=True)
@option('--s3', is_flag=True)
def ctbk(ctx, reads, roots, writes, s3):
    if s3:
        roots = [S3] + (roots or [])
    roots = DefaultDict.load(roots, fallback=DEFAULT_ROOT)
    reads = DefaultDict.load(reads, name2value=read.parse, fallback=Disk)
    writes = DefaultDict.load(writes, name2value=write.parse, fallback=IfAbsent)
    ctx.obj = o(roots=roots, reads=reads, writes=writes)
