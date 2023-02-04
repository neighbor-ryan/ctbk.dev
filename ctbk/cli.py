#!/usr/bin/env python

import click
from click import pass_context, Choice
from utz import o

from ctbk import YM
from ctbk.tripdata import TripdataMonths, REGIONS
from ctbk.util.constants import GENESIS


@click.group('ctbk')
@pass_context
@click.option('-s', '--start')
@click.option('-e', '--end')
@click.option('-r', '--root')
@click.option('--s3', is_flag=True)
def ctbk(ctx, start, end, root, s3):
    start = YM(start) if start else GENESIS
    end = YM(end) if end else None
    if s3:
        if root:
            raise ValueError(f"Pass -r/--root xor --s3")
        root = 's3://'
    ctx.obj = o(start=start, end=end, root=root)


@ctbk.group()
def csvs():
    pass


@csvs.command()
@pass_context
def urls()

if __name__ == '__main__':
    ctbk()
