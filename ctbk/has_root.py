import dask.dataframe as dd
import pandas as pd
from typing import Optional
from utz import DefaultDict

from ctbk.util.read import Read, Disk
from ctbk.util.write import IfAbsent, Write


DEFAULT_ROOTS = dr = DefaultDict({'zip': 's3:/'}, 's3')


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

        roots = roots or DEFAULT_ROOTS
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

    @property
    def dpd(self):
        return dd if self.dask else pd

    def concat(self, *args, **kwargs):
        return self.dpd.concat(*args, **kwargs)
