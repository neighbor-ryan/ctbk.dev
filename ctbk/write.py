from dataclasses import dataclass


class Write: pass
class Never(Write): pass
class IfAbsent(Write): pass
class Overwrite(Write): pass
class Overread(Write): pass


WRITES = [ Never, IfAbsent, Overwrite, Overread ]


WRITE_2_NAME = {
    Never: ['0', 'never', 'n', ],
    IfAbsent: [ '1', 'ifabsent', 'ifabs', 'w', ],
    Overwrite: [ '2', 'overwrite', 'write', 'ow', 'ww', ],
    Overread: [ '3', 'overread', 'or', 'www', ],
}
NAME_2_WRITE = {
    name: write for
    write, names in WRITE_2_NAME.items()
    for name in names
}


def parse(name: str) -> Write:
    if name not in NAME_2_WRITE:
        raise ValueError(f"Unrecognized write config: {name}")
    return NAME_2_WRITE[name]


@dataclass
class WriteConfigs:
    configs: dict[str, Write]
    default: Write = None

    @staticmethod
    def load(write_configs: list[str]) -> 'WriteConfigs':
        default = None
        configs = {}
        for write_config in write_configs:
            kv = write_config.split('=', 1)
            if len(kv) == 2:
                k, name = kv
                write = parse(name)
                configs[k] = write
            elif len(kv) == 1:
                [name] = kv
                default = parse(name)
            else:
                raise ValueError(f"Unrecognized write config: {kv}")
        return WriteConfigs(configs=configs, default=default)

    def __contains__(self, item):
        return self.default or item in self.configs

    def __getitem__(self, k):
        if k in self.configs:
            return self.configs[k]
        elif self.default:
            return self.default
        else:
            # raise
            return self.configs[k]
