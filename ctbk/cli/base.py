from click import pass_context, option, group
from utz import o

from ctbk.compute import Never, IfAbsent, Overwrite, Overread, Always
from ctbk.util import YM
from ctbk.util.constants import GENESIS, S3


@group('ctbk')
@pass_context
@option('-s', '--start')
@option('-e', '--end')
@option('-r', '--root')
@option('--s3', is_flag=True)
@option('-f', '--force', count=True)
@option('-w', '--write', count=True)
def ctbk(ctx, start, end, root, s3, force, write):
    start = YM(start) if start else GENESIS
    end = YM(end) if end else None
    if s3:
        if root:
            raise ValueError(f"Pass -r/--root xor --s3")
        root = S3
    if force:
        if write:
            raise ValueError(f"Pass -f/--force (up to 2x) xor -w/--write (up to 2x)")
        compute = Overwrite if force == 1 else Overread
    elif write:
        compute = IfAbsent if write == 1 else Always
    else:
        compute = Never
    ctx.obj = o(start=start, end=end, root=root, compute=compute)
