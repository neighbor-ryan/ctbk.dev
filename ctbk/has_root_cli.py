from abc import ABC
from typing import Optional

import click
import utz
from click import argument, option, pass_context, Choice
from utz import err, process, decos
from utz.case import dash_case

from ctbk.cli.base import ctbk, dask, StableCommandOrder
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
    def init_cli(
            cls,
            group: click.Group,
            cmd_decos: list = None,
            group_cls: type[click.Group] = None,
            urls=True,
            create=True,
            dag=True,
    ):
        cmd_decos = cmd_decos or []

        def cmd(help):
            return decos(
                group.command(cls=group_cls, help=help),
                pass_context,
                *cmd_decos
            )

        if urls:
            @cmd(help="Print URLs for selected datasets")
            def urls(ctx, **kwargs):
                o = ctx.obj
                filtered_kwargs = utz.args(cls, dict(**o, **kwargs))
                tasks = cls(**filtered_kwargs)
                children = tasks.children
                for month in children:
                    print(month.url)

        if create:
            @cmd(help="Create selected datasets")
            @dask
            def create(ctx, dask, **kwargs):
                o = ctx.obj
                tasks = cls(dask=dask, **o, **kwargs)
                created = tasks.create(read=None)
                if dask:
                    created.compute()

        if dag:
            @cmd(help="Save and `open` a graph visualization of the datasets to be computed")
            @option('-O', '--no-open', is_flag=True)
            @option('-f', '--format', type=Choice(['png', 'svg']))
            @argument('filename', required=False)
            def dag(ctx, no_open, format, filename, **kwargs):
                o = ctx.obj
                tasks = cls(dask=True, **o, **kwargs)
                result = tasks.create(read=None)
                start = o['start']
                end = o['end']
                if filename and format and not filename.endswith(f'.{format}'):
                    raise ValueError(f"-f/--format {format} doesn't match filename {filename}")
                format = format or 'png'
                filename = filename or f'{cls.name()}_{start}-{end}_dag.{format}'
                err(f"Writing to {filename}")
                result.visualize(filename)
                if not no_open:
                    process.run('open', filename)

    @classmethod
    def cli(
            cls,
            help: str,
            decos: Optional[list] = None,
            cmd_decos: Optional[list] = None,
            **kwargs
    ) -> click.Group:
        command_cls = cls.command_cls()
        decos = decos or []

        @utz.decos(
            ctbk.group(dash_case(cls.name()), cls=command_cls, help=help),
            pass_context,
            *decos
        )
        def group(ctx, **kwargs):
            ctx.obj = dict(**ctx.obj, **kwargs)

        cls.init_cli(group, cmd_decos=cmd_decos, **kwargs)
        return group

    @classmethod
    def command_cls(cls):
        class Command(StableCommandOrder):
            ALIASES = cls.names()

        return Command
