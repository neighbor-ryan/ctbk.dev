#!/usr/bin/env python
import shlex

import click
from utz import process


@click.command()
def main():
    for arg in [
        '',
        'zip',
        'csv',
        'normalized',
        'aggregated',
        'station-meta-hist',
        'station-modes-json',
        'station-pairs-json',
        'sampled-zip',
    ]:
        # cmd = ['python', '-m', 'ctbk.cli.main']
        cmd = ['ctbk']
        if arg:
            cmd += [ arg ]
            # display_cmd += [ arg ]
        output = process.output(cmd).decode()
        md = f"""<details><summary><code>{shlex.join(cmd)}</code></summary>

```
{output}
```
</details>
"""
        print(md)
        print()


if __name__ == '__main__':
    main()
