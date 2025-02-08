from os.path import dirname, join, relpath
from typing import Optional

from utz import DefaultDict

from ctbk.util.read import Read, Disk
from ctbk.util.write import IfAbsent, Write

CTBK_ROOT = dirname(__file__)
REPO_ROOT = dirname(CTBK_ROOT)
S3_DIR = join(REPO_ROOT, 's3')
DEFAULT_ROOTS = dr = DefaultDict({}, relpath(S3_DIR))
ROOTS_ENV_VAR = 'CTBK_ROOTS'


class HasRoot:
    """Superclass providing read/write/root configs for various classes:

    - `Read`: when a dataset is computed and persisted, should the return value be:
      - `Memory`: the dataset in memory that was written, or
      - `Disk`: read from disk?
    - `Write`:
      - `Never`: read-only mode, `raise` if a needed dataset hasn't been computed (its persisted path under `root`
        doesn't already exist)
      - `IfAbsent`: compute+persist data if absent, no-op success if dataset's path already exists (in "root" tree)
      - `Always`: recompute+overwrite a dataset, even if present already under `root`
    - "root": URL str, root of a directory tree which subclasses can be pointed at (e.g. "s3:/" for Amazon S3, "s3" for
      a local folder named "s3" that will effectively mirror a directory structure on S3 (`s3/ctbk` ↔️ `s3://ctbk`)

    Using `DefaultDict`s, allows for command-line configuration like:
    ```bash
    ctbk --root zip=s3:/ --root s3 --write norm=overwrite normalized create
    ```
    This generates `NormalizedMonth` data, and its parent `TripdataCsv` data if necessary, which in turn may read
    `TripdataZip`s. Each of the 3 stages can be configured to:
    1. read/write from a given directory ("root"), in this case "s3" (a local folder) as a default (used by `norm` and
       `csv`; see subclasses' `NAMES` array) and `s3:/` (URLs will start with "s3://", Amazon S3) for just the `zip`
       datasets.
    2. compute+persist on demand: `csv`s will be read from `s3/ctbk/csvs/`, and computed if they don't exist. Computing
       them will read `zip`s from S3 though (`s3://tripdata/`), due to `--root zip=s3:/` from above. `norm`s will be
       computed and written to `s3/ctbk/normalized/` no matter what though (`--write norm=overwrite`).
    3. `read` configs are just the default `DISK` for all classes (freshly computed datasets are read from disk before
       being returned).

    An abbreviated version of the above is:
    ```bash
    ctbk -tzip=s3:/ -ts3 -wnorm=ow normalized create
    ```
    Note the abbreviations:
    - `-t`/`--root`
    - `-w`/`--write`
    - `ow`/`overwrite`

    In fact, the "root" configs above are the default anyway (see `DEFAULT_ROOTS` above):
    ```bash
    # - (re)compute and (over)write `NormalizedMonth`s
    # - compute `csv`s if necessary
    # - work locally in "s3/" (but read `zip`s from S3)
    ctbk -wnorm=ow normalized create
    ```

    There is also a special --s3 flag that is an alias for `--root s3:/`, sending all reads and writes to S3:
    ```bash
    # Ensure `norm`s exist on S3. If necessary, compute them from `csv`s on S3 , and compute+write those in S3 using
    # `zip`s from S3.
    ctbk --s3 normalized create
    ```
    """
    DIR = None    # Subdir under "root" that this class writes to / reads from
    NAMES = None  # Aliases this class can be addressed as in CLI flags (`<alias>=<value>`)

    @classmethod
    def names(cls):
        return cls.NAMES

    def __init__(
        self,
        roots: Optional[DefaultDict[str]] = None,
        reads: Optional[DefaultDict[Read]] = None,
        writes: Optional[DefaultDict[Write]] = None,
        **extra,
    ):
        names = self.NAMES or []

        roots = roots or DEFAULT_ROOTS
        self.roots = roots
        self.root = roots.get_first(names) if roots else None
        root = self.root

        self.reads = reads
        self.read = reads.get_first(names, Disk) if reads else Disk

        self.writes = writes
        self.write = writes.get_first(names, IfAbsent) if writes else IfAbsent

        if not self.DIR:
            raise RuntimeError(f"{self}.DIR not defined")
        self.dir = f'{root}/{self.DIR}' if root else self.DIR
        self.extra = extra
        super().__init__()

    @property
    def kwargs(self):
        """Useful for kwarg-forwarding in constructors."""
        return dict(roots=self.roots, reads=self.reads, writes=self.writes)
