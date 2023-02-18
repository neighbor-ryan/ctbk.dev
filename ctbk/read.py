from typing import Union


class ReadType: pass
class Memory(ReadType): pass
class Disk(ReadType): pass


Read = type[ReadType]
READS = [ Memory, Disk ]


ALIASES = {
    Memory: [ '0', 'mem', 'm', 'memory', ],
    Disk: [ '1', 'disk', 'd', 'r', 'read', ],
    None: [ '', ],
}
NAME_2_READ = {
    name: write for
    write, names in ALIASES.items()
    for name in names
}


def parse(name: str) -> Union[Read, None]:
    if name not in NAME_2_READ:
        raise ValueError(f"Unrecognized read type: {name}")
    return NAME_2_READ[name]
