from click import pass_context, option, group
from utz import o

from ctbk import write, read
from ctbk.read import Disk
from ctbk.util.constants import S3, DEFAULT_ROOT
from ctbk.util.defaultdict import load_defaultdict
from ctbk.write import IfAbsent

dask = option('--dask', is_flag=True)


@group('ctbk')
@pass_context
@option('-r', '--read', 'reads', multiple=True)
@option('-t', '--root', 'roots', multiple=True)
@option('-w', '--write', 'writes', multiple=True)
@option('--s3', is_flag=True)
def ctbk(ctx, reads, roots, writes, s3):
    if s3:
        roots = [S3] + (roots or [])
    roots = load_defaultdict(roots, fallback=DEFAULT_ROOT)
    reads = load_defaultdict(reads, name2value=read.parse, fallback=Disk)
    writes = load_defaultdict(writes, name2value=write.parse, fallback=IfAbsent)
    ctx.obj = o(roots=roots, reads=reads, writes=writes)
