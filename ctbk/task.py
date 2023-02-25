from abc import ABC
from typing import Union

import dask.dataframe as dd
from ctbk.has_root import HasRoot
from ctbk.has_url import HasURL
from ctbk.util.read import Read
from ctbk.util.write import Always, Never
from dask import delayed
from dask.delayed import Delayed
from utz import Unset, err


class Task(HasRoot, HasURL, ABC):
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
                err(f'Overwriting {url}')
                return self._create(read=read)
            elif read is None:
                err(f'{url} already exists')
                if self.dask:
                    def exists_noop():
                        pass
                    return delayed(exists_noop)(dask_key_name=f'read {url}')
                else:
                    return None
            else:
                err(f'Reading {url}')
                return self._read()
        elif self.write is Never:
            raise RuntimeError(f"{url} doesn't exist, but `write` is `Never`")
        else:
            err(f'Writing {url}')
            return self._create(read=read)

    def _read(self) -> Union[None, Delayed]:
        if self.dask:
            [delayed] = dd.read_parquet(self.url).to_delayed()
            return delayed
        else:
            return None
