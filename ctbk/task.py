import dask.dataframe as dd
from abc import ABC
from dask import delayed
from dask.delayed import Delayed
from typing import Union
from utz import Unset

from ctbk.has_root import HasRoot
from ctbk.has_url import HasURL
from ctbk.util import stderr
from ctbk.util.read import Read
from ctbk.util.write import Always, Never


class Task(HasRoot, HasURL, ABC):
    DIR = None
    NAMES = []

    def __init__(self, **kwargs):
        HasRoot.__init__(self, **kwargs)
        HasURL.__init__(self)

    def _create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        raise NotImplementedError

    def create(self, read: Union[None, Read] = Unset) -> Union[None, Delayed]:
        read = self.read if read is Unset else read
        url = self.url
        if self.exists():
            if self.write is Always:
                stderr(f'Overwriting {url}')
                return self._create()
            elif read is None:
                stderr(f'{url} already exists')
                if self.dask:
                    def exists_noop():
                        pass
                    return delayed(exists_noop)(dask_key_name=f'read {url}')
                else:
                    return None
            else:
                stderr(f'Reading {url}')
                return self._read()
        elif self.write is Never:
            raise RuntimeError(f"{url} doesn't exist, but `write` is `Never`")
        else:
            stderr(f'Writing {url}')
            return self._create()

    def _read(self) -> Union[None, Delayed]:
        if self.dask:
            [delayed] = dd.read_parquet(self.url).to_delayed()
            return delayed
        else:
            return None
