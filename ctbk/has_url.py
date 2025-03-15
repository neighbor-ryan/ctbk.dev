from abc import ABC, abstractmethod
from contextlib import contextmanager
from os.path import dirname, relpath
from urllib.parse import urlparse, ParseResult

import fsspec
import yaml
from utz import cached_property, err

from ctbk.paths import S3


class HasURL(ABC):
    @property
    @abstractmethod
    def url(self) -> str:
        raise NotImplementedError

    @property
    def dvc_path(self) -> str:
        return f"{self.url}.dvc"

    @property
    def dvc_spec(self):
        dvc_path = self.dvc_path
        with open(dvc_path, 'r') as f:
            return yaml.safe_load(f)

    @property
    def path(self) -> str:
        url = self.url
        return relpath(url, S3)

    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.url})'

    def __repr__(self) -> str:
        return str(self)

    @property
    def scheme(self) -> str:
        return self.parsed.scheme

    @cached_property
    def parsed(self) -> ParseResult:
        return urlparse(self.url)

    @cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.scheme)

    def exists(self) -> bool:
        try:
            return self.fs.exists(self.url)
        except Exception:
            raise RuntimeError(f"Error checking existence of URL {self.url}")

    @property
    def dirname(self) -> str:
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
                err(f"Removing dir after failed write: {top_made_dir}")
                fs.delete(top_made_dir)

    @contextmanager
    def fd(self, mode):
        # TODO: optionally write to tmp file then move atomically
        with self.mkdirs():
            succeeded = False
            url = self.url
            fs = self.fs
            try:
                yield fs.open(url, mode)
                succeeded = True
            finally:
                if not succeeded:
                    if fs.exists(url):
                        err(f"Removing failed write: {url}")
                        fs.delete(url)
