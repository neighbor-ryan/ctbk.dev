from click import pass_context, option, group

from ctbk.util import YM
from ctbk.util.constants import GENESIS


@group('ctbk')
@pass_context
@option('-s', '--start')
@option('-e', '--end')
@option('-r', '--root')
@option('--s3', is_flag=True)
def ctbk(ctx, start, end, root, s3):
    start = YM(start) if start else GENESIS
    end = YM(end) if end else None
    if s3:
        if root:
            raise ValueError(f"Pass -r/--root xor --s3")
        root = 's3://'
    ctx.obj = o(start=start, end=end, root=root)
