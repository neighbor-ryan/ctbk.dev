from collections import defaultdict
from dataclasses import dataclass

from typing import Optional, Callable, TypeVar

T = TypeVar('T')


class Unset:
    def __bool__(self):
        return False


def parse_configs(configs: list[str], name2value: Optional[Callable[[str], T]] = None, fallback: Optional[T] = Unset) -> (T, dict[str, T]):
    default = fallback
    default_set = False
    kwargs: dict[str, T] = {}
    for write_config in configs:
        kv = write_config.split('=', 1)
        if len(kv) == 2:
            k, name = kv
            value = name2value(name) if name2value else name
            kwargs[k] = value
        elif len(kv) == 1:
            [name] = kv
            if default_set:
                raise ValueError(f'Multiple defaults found: {default}, {name}')
            default = name2value(name) if name2value else name
            default_set = True
        else:
            raise ValueError(f"Unrecognized defaultdict config arg: {kv}")
    return default, kwargs


def load_defaultdict(configs: list[str], name2value: Optional[Callable] = None, fallback: Optional[T] = None) -> dict[str, T]:
    default, kwargs = parse_configs(configs, name2value=name2value, fallback=fallback)
    return defaultdict(default, **kwargs)


@dataclass
class DefaultDict(dict[str, T]):
    configs: dict[str, T]
    default: T = Unset

    @staticmethod
    def load(args: list[str], name2value: Optional[Callable] = None, fallback: Optional[T] = None) -> 'DefaultDict':
        default, kwargs = parse_configs(args, name2value=name2value, fallback=fallback)
        return DefaultDict(configs=kwargs, default=default)

    def __contains__(self, item):
        return item in self.configs or self.default is not Unset

    def __getitem__(self, item) -> T:
        if item not in self.configs and self.default is not Unset:
            return self.default
        else:
            return self.configs[item]
