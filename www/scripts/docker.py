#!/usr/bin/env python
from os.path import dirname

import click
from utz import run

SCRIPTS_DIR = dirname(__file__)
WWW_DIR = dirname(SCRIPTS_DIR)

DEFAULT_NAME = 'ctbk-www'
DEFAULT_IMAGE = 'node'


@click.command('docker.py', help='Execute commands in a `node` Docker image with the ctbk.dev/www directory mounted as the WORKDIR')
@click.option('-i', '--image', default=DEFAULT_IMAGE)
@click.option('-n', '--name', default=DEFAULT_NAME)
@click.option('-p', '--port', 'ports_strs')
@click.argument('args', nargs=-1)
def main(image, name, ports_strs, args):
    def parse_port(ports_str):
        pcs = [ int(pc) for pc in ports_str.split(':') ]
        if len(pcs) == 1:
            port = pcs[0]
            return port, port
        elif len(pcs) == 2:
            return pcs
        else:
            raise ValueError(f"Unrecognized ports_str: {ports_str}")

    ports = [
        parse_port(ports_str)
        for ports_str in ports_strs.split(',')
    ] if ports_strs else []

    dst = '/home/root/ctbk.dev/www'

    if args:
        [ entrypoint, *command ] = args
    else:
        raise ValueError("Pass <entrypoint> [...command] as positional arguments")

    cmd = [
        "docker", "run", "--rm",
        *[
            arg
            for host_port, docker_port in ports
            for arg in [ '-p', f'{host_port}:{docker_port}' ]
        ],
        '--name', name,
        '-v', f'{WWW_DIR}:{dst}',
        '-w', dst,
        '-e', 'PATH=node_modules/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
        '--entrypoint', entrypoint,
        image,
        command,
    ]

    run(*cmd)


if __name__ == '__main__':
    main()
