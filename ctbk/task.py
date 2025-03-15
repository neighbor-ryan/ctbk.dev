from abc import ABC
from os.path import join
from typing import TypeVar, Generic

from utz import err

from ctbk.has_url import HasURL
from ctbk.paths import S3

T = TypeVar("T")

class Task(HasURL, ABC, Generic[T]):
    DIR = None
    NAMES = []

    def __init__(self):
        HasURL.__init__(self)

    def _create(self) -> T | None:
        raise NotImplementedError

    def create(self) -> T:
        url = self.url
        err(f'Writing {url}')
        return self._create()

    def read(self) -> T:
        raise NotImplementedError

    @property
    def dir(self) -> str:
        assert self.DIR
        return join(S3, self.DIR)
