#!/usr/bin/env python

from click import command, option, argument
from os.path import abspath, dirname, exists, join
import shlex
from subprocess import check_call


def run(cmd):
    print(f'Running: {shlex.join(cmd)}')
    check_call(cmd)


@command('sync')
@option('-d', '--dst', help='Bucket to sync to')
@option('-n', '--dry-run', count=True, help='Pass once for dry-run, twice for dry-run + prompt to run')
@argument('paths', nargs=-1)
def main(dst, dry_run, paths):
    dir = dirname(abspath(__file__))
    YML_PATH = join(dir, 'sync.yml')
    if exists(YML_PATH):
        import yaml
        with open(YML_PATH, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = {}

    if not dst and 'dst' in config:
        dst = config['dst']
    if not paths and 'paths' in config:
        paths = config['paths']

    if dst[-1] != '/':
        dst += '/'

    cmd = [
        'aws', 's3', 'sync'
    ]
    if dry_run:
        cmd += ['--dryrun']
    cmd += [ '--exclude', '*' ]
    for path in paths:
        cmd += [ '--include', path ]

    cmd += [ '--cache-control', 'max-age=0,public', ]

    cmd += [ dir, dst ]
    run(cmd)

    if dry_run > 1:
        cmd = [ arg for arg in cmd if arg != '--dryrun' ]
        response = input(f'Execute? {shlex.join(cmd)}\n')
        if response.lower() in {'', 'y', 'yes'}:
            run(cmd)


if __name__ == '__main__':
    main()
