import sys

from .ym import YM, Monthy, GENESIS
from .constants import S3
from .context import contexts
from .cached_property import cached_property


def stderr(msg=''):
    sys.stderr.write(msg)
    sys.stderr.write('\n')
