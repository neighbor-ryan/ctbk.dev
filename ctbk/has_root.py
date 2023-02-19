import dask.dataframe as dd
import pandas as pd
from typing import Optional
from utz import DefaultDict

from ctbk.util.read import Read, Disk
from ctbk.util.write import IfAbsent, Write


class HasRoot:
    DIR = None
    NAMES = None

    def __init__(
            self,
            roots: Optional[DefaultDict[str]] = None,
            reads: Optional[DefaultDict[Read]] = None,
            writes: Optional[DefaultDict[Write]] = None,
            dask: bool = False,
    ):
        names = self.NAMES or []

        self.roots = roots
        root = self.root = None
        if roots:
            root = self.root = roots.get_first(names)

        self.reads = reads
        if reads:
            self.read = reads.get_first(names, Disk)
        else:
            self.read = Disk

        self.writes = writes
        if writes:
            self.write = writes.get_first(names, IfAbsent)
        else:
            self.write = IfAbsent

        if not self.DIR:
            raise RuntimeError(f"{self}.DIR not defined")
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        self.dask = dask
        super().__init__()

    @property
    def kwargs(self):
        return dict(roots=self.roots, reads=self.reads, writes=self.writes, dask=self.dask)

    def concat(self, *args, **kwargs):
        concat_fn = dd.concat if self.dask else pd.concat
        return concat_fn(*args, **kwargs)
