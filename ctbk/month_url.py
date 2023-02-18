from os.path import dirname

import fsspec
from abc import ABC, abstractmethod
from contextlib import contextmanager
from urllib.parse import urlparse

from ctbk.util import cached_property, YM, stderr


class MonthURL(ABC):
    def __init__(self, ym):
        self.ym = YM(ym)

    @property
    @abstractmethod
    def url(self):
        raise NotImplementedError

    def __str__(self):
        return f'{self.__class__.__name__}({self.url})'

    def __repr__(self):
        return str(self)

    @property
    def scheme(self):
        return self.parsed.scheme

    @cached_property
    def parsed(self):
        return urlparse(self.url)

    @cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.scheme)

    def exists(self):
        return self.fs.exists(self.url)

    def mkdirs(self):
        fs = self.fs
        url = self.url
        dir = dirname(url)
        if fs.exists(dir):
            return None
        else:
            fs.mkdirs(dir, exist_ok=True)
            return dir

    @contextmanager
    def fd(self, mode):
        made_dir = self.mkdirs()
        fs = self.fs
        url = self.url
        succeeded = False
        try:
            yield fs.open(url, mode)
            succeeded = True
        finally:
            if not succeeded:
                if fs.exists(url):
                    stderr(f"Removing failed write: {url}")
                    fs.delete(url)
                if made_dir:
                    stderr(f"Removing dir after failed write: {made_dir}")
                    fs.delete(made_dir)
