from typing import Union


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
