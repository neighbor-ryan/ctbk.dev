from dataclasses import dataclass
from os.path import join, exists

import yaml
from utz import run, err, silent, Log
from utz.collections import solo

from ctbk.paths import S3
from ctbk.s3 import parse_bkt_key, get_etag


@dataclass(init=False)
class Blob:
    bkt: str
    key: str

    def __init__(self, *args: str):
        self.bkt, self.key = parse_bkt_key(args)

    @property
    def url(self) -> str:
        return f"s3://{self.bkt}/{self.key}"

    @property
    def path(self) -> str:
        return join(S3, self.bkt, self.key)

    @property
    def dvc_path(self) -> str:
        return f"{self.path}.dvc"

    @property
    def dvc_spec(self):
        with open(self.dvc_path, 'r') as f:
            return yaml.safe_load(f)

    @property
    def etag(self) -> str:
        dvc_spec = self.dvc_spec
        dep = solo(dvc_spec['deps'])
        return dep['etag']

    @property
    def s3_etag(self) -> str:
        return get_etag(self.bkt, self.key)

    def update(
        self,
        dry_run: bool = False,
        log: Log = err,
        verbose: bool = False,
    ) -> bool:
        dvc_path = self.dvc_path
        if exists(dvc_path):
            etag0 = self.etag
            etag1 = self.s3_etag
            if etag0 != etag1:
                log(f"{dvc_path} etag changed ({etag0} → {etag1}); re-importing")
                run('dvc', 'import-url', '-f', self.url, self.path, dry_run=dry_run)
                return True
            elif verbose:
                log(f"{dvc_path} (ETag {etag0}) is up to date")
            return False
        else:
            log(f"{dvc_path} not found; importing")
            run('dvc', 'import-url', self.url, self.path, dry_run=dry_run)
            return True
