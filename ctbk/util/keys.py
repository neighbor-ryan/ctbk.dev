from abc import ABC
from typing import Union

from click import option
from utz.case import dash_case
from utz.cli import flag


class Keys:
    KEYS = None

    def __iter__(self):
        for name, ch in self.KEYS.items():
            v = getattr(self, name)
            if v:
                yield ch, v

    @property
    def label(self):
        return ''.join(dict(self).keys())

    @classmethod
    def names(cls):
        return { key: name for name, key in cls.KEYS.items() }

    @classmethod
    def load(cls, arg: Union[str, 'AggKeys', dict]):
        if isinstance(arg, cls):
            return arg
        elif isinstance(arg, dict):
            return cls(**arg)
        else:
            names = cls.names()
            return cls(**{ names[key]: True for key in arg })

    @classmethod
    def char_name_summary(cls):
        return ", ".join([ f"'{ch}' ({name})" for name, ch in cls.KEYS.items() ])

    @classmethod
    def opt(cls):
        raise NotImplementedError

    @classmethod
    def help(cls):
        raise NotImplementedError


class GroupByKeys(Keys, ABC):
    @classmethod
    def opt(cls):
        return option('-g', '--group-by', required=True, help=cls.help())


class AggregateByKeys(Keys, ABC):
    @classmethod
    def opt(cls):
        return option('-a', '--aggregate-by', required=True, help=cls.help())
