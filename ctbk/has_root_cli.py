from abc import ABC
from typing import Optional
import utz

import click
from click import pass_context

from ctbk import Monthy
from ctbk.cli.base import ctbk, dask
from ctbk.has_root import HasRoot
from ctbk.task import Task
from ctbk.tasks import Tasks
from ctbk import util


class HasRootCLI(Tasks, HasRoot, ABC):
    ROOT_DECOS = []
    CHILD_CLS: type[Task] = None

    @classmethod
    def names(cls):
        return cls.CHILD_CLS.NAMES

    # def month(self, ym: Monthy) -> type[Task]:
    #     return NormalizedMonth(ym, **self.kwargs)

    # def children(self):
    #     pass

    @classmethod
    def init_cli(cls, group: click.Group):
        @group.command()
        @pass_context
        def urls(ctx):
            o = ctx.obj
            months = cls(**o)
            children = months.children
            for month in children:
                print(month.url)

        @group.command()
        @pass_context
        @dask
        def create(ctx, dask):
            o = ctx.obj
            months = cls(dask=dask, **o)
            created = months.create(read=None)
            if dask:
                created.compute()

    @classmethod
    def cli(cls, help: str, decos: Optional[list] = None) -> click.Group:
        command_cls = cls.command_cls()
        decos = decos or []

        @utz.decos(
            ctbk.group(cls.names()[0], cls=command_cls, help=help),
            pass_context,
            *decos
        )
        def group(ctx, start, end):
            ctx.obj.start = start
            ctx.obj.end = end

        cls.init_cli(group)
        return group

    @classmethod
    def command_cls(cls):
        class Command(click.Group):
            ALIASES = cls.names()

        return Command
