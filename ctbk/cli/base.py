from click import pass_context, option, group
from utz import o

from ctbk.util import YM
from ctbk.util.constants import GENESIS, S3, DEFAULT_ROOT
from ctbk.write import WriteConfigs


@group('ctbk')
@pass_context
@option('-s', '--start')
@option('-e', '--end')
@option('-r', '--root', default=DEFAULT_ROOT)
@option('--s3', is_flag=True)
@option('-w', '--write', 'write_configs', multiple=True)
def ctbk(ctx, start, end, root, s3, write_configs):
    start = YM(start) if start else GENESIS
    end = YM(end) if end else None
    if s3:
        if root:
            raise ValueError(f"Pass -r/--root xor --s3")
        root = S3
    write_configs = WriteConfigs.load(write_configs)
    ctx.obj = o(start=start, end=end, root=root, write_configs=write_configs)
