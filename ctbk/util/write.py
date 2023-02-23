class WriteType: pass
class Never(WriteType): pass
class IfAbsent(WriteType): pass
class Always(WriteType): pass


Write = type[WriteType]
WRITES = [ Never, IfAbsent, Always, ]


ALIASES = {
    Never: [ '0', 'never', 'n', 'no', 'ro', '', ],
    IfAbsent: [ '1', 'ifabsent', 'ifabs', 'ia', 'w', ],
    Always: [ '2', 'overwrite', 'write', 'ow', 'ww', 'always', ],
}
NAME_2_WRITE = {
    name: write for
    write, names in ALIASES.items()
    for name in names
}


def parse(name: str) -> Write:
    if name not in NAME_2_WRITE:
        raise ValueError(f"Unrecognized write type: {name}")
    return NAME_2_WRITE[name]
