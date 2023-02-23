from os.path import dirname

import fsspec
from abc import ABC, abstractmethod
from contextlib import contextmanager, nullcontext
from urllib.parse import urlparse

from ctbk.util import cached_property, YM, stderr


class HasURL(ABC):
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

    @property
    def dirname(self):
        return dirname(self.url)

    @contextmanager
    def mkdirs(self):
        # TODO: make this a contextmanager that can clean up all created dirs on failure
        fs = self.fs
        dir = self.dirname
        if fs.exists(dir):
            yield
            return
        rm_dir = True
        made_dirs = [dir]
        cur_dir = dir
        while True:
            parent = dirname(cur_dir)
            if fs.exists(parent):
                break
            made_dirs.append(parent)
            cur_dir = parent
        made_dirs = list(reversed(made_dirs))
        top_made_dir = made_dirs[0]
        fs.mkdirs(dir, exist_ok=True)
        try:
            yield
            rm_dir = False
        finally:
            if rm_dir:
                stderr(f"Removing dir after failed write: {top_made_dir}")
                fs.delete(top_made_dir)


    @contextmanager
    def fd(self, mode):
        with self.mkdirs():
            succeeded = False
            try:
                yield fs.open(url, mode)
                succeeded = True
            finally:
                if not succeeded:
                    url = self.url
                    if fs.exists(url):
                        stderr(f"Removing failed write: {url}")
                        fs.delete(url)
