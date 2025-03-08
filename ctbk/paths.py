from os import getcwd
from os.path import dirname, relpath, join

cwd = getcwd()
CTBK = dirname(__file__)
ROOT = relpath(dirname(CTBK))
CTBK = relpath(CTBK)
S3 = relpath(join(ROOT, 's3'))
