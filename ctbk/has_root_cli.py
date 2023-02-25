from abc import ABC
from typing import Optional, Callable

import click
import utz
from click import argument, option, pass_context
from utz import err, process

from ctbk.cli.base import ctbk, dask
from ctbk.has_root import HasRoot
from ctbk.task import Task
from ctbk.tasks import Tasks


class HasRootCLI(Tasks, HasRoot, ABC):
    ROOT_DECOS = []
    CHILD_CLS: type[Task] = None

    @classmethod
    def names(cls):
        return cls.CHILD_CLS.NAMES

    @classmethod
    def name(cls):
        return cls.names()[0]

    # def month(self, ym: Monthy) -> type[Task]:
    #     return NormalizedMonth(ym, **self.kwargs)

    # def children(self):
    #     pass

    @classmethod
    def init_cli(cls, group: click.Group, urls=True, create=True, dag=True):
        if urls:
            @group.command()
            @pass_context
            def urls(ctx):
                o = ctx.obj
                tasks = cls(**o)
                children = tasks.children
                for month in children:
                    print(month.url)

        if create:
            @group.command()
            @pass_context
            @dask
            def create(ctx, dask):
                o = ctx.obj
                tasks = cls(dask=dask, **o)
                created = tasks.create(read=None)
                if dask:
                    created.compute()

        if dag:
            @group.command()
            @pass_context
            @option('-O', '--no-open', is_flag=True)
            @argument('filename', required=False)
            def dag(ctx, no_open, filename):
                tasks = cls(dask=True, **ctx.obj)
                result = tasks.create(read=None)
                filename = filename or f'{cls.name()}_dag.png'
                err(f"Writing to {filename}")
                result.visualize(filename)
                if not no_open:
                    process.run('open', filename)

    @classmethod
    def cli(
            cls,
            help: str,
            decos: Optional[list] = None,
            **kwargs
    ) -> click.Group:
        command_cls = cls.command_cls()
        decos = decos or []

        @utz.decos(
            ctbk.group(cls.name(), cls=command_cls, help=help),
            pass_context,
            *decos
        )
        def group(ctx, **kwargs):
            ctx.obj = dict(**ctx.obj, **kwargs)

        cls.init_cli(group, **kwargs)
        return group

    @classmethod
    def command_cls(cls):
        class Command(click.Group):
            ALIASES = cls.names()

        return Command
