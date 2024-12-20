from abc import ABC
from typing import Union, Type

from ctbk.has_root import HasRoot
from ctbk.has_url import HasURL
from ctbk.util.read import Read
from ctbk.util.write import Always, Never
from utz import Unset, err


class Task(HasRoot, HasURL, ABC):
    def __init__(self, **kwargs):
        HasRoot.__init__(self, **kwargs)
        HasURL.__init__(self)

    def _create(self, read: Read | None | Type[Unset] = Unset) -> None:
        raise NotImplementedError

    def create(self, read: Read | None | Type[Unset] = Unset) -> None:
        read = self.read if read is Unset else read
        url = self.url
        if self.exists():
            if self.write is Always:
                err(f'Overwriting {url}')
                return self._create(read=read)
            elif read is None:
                err(f'{url} already exists')
            else:
                err(f'Reading {url}')
                return self._read()
        elif self.write is Never:
            raise RuntimeError(f"{url} doesn't exist, but `write` is `Never`")
        else:
            err(f'Writing {url}')
            return self._create(read=read)

    def _read(self) -> None:
        raise NotImplementedError
