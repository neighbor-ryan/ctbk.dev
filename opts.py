from typing import Generator

def option_kv(arg):
    if isinstance(arg, tuple):
        if len(arg) == 2:
            label, value = arg
            return { 'label':label, 'value':value }
    if isinstance(arg, str):
        return { 'label':arg, 'value':arg }
    if isinstance(arg, dict):
        keys = list(arg.keys())
        if 'label' in keys or 'value' in keys and all(k in ['label','value','disabled'] for k in keys):
            v = arg.get('value', arg.get('label'))
            if 'value' not in arg:
                arg = dict(value=v, **arg)
            if 'label' not in arg:
                arg = dict(label=v, **arg)
            return arg
        if len(keys) == 1:
            return { 'label': keys[0], 'value': arg[keys[0]] }
    raise ValueError(f'Unrecognized option: {arg}')

def opts(*args):
    if len(args) == 1 and isinstance(args[0], dict):
        return [
            option_kv(dict(label=label, **value))
            if isinstance(value, dict)
            else dict(label=label, value=value)
            for label, value in args[0].items()
        ]
    elif len(args) == 1 and isinstance(args[0], Generator):
        return [ option_kv(arg) for arg in args ]
    elif len(args) == 1 and isinstance(args[0], list):
        return [ option_kv(arg) for arg in args ]
    else:
        return [ option_kv(arg) for arg in args ]
