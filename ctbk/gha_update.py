#!/usr/bin/env python

from os import environ as env

import click
from utz import err, run, lines


@click.command()
@click.option('-b', '--branch', help='Current branch; defaults to $GITHUB_HEAD_REF or $GITHUB_REF_NAME')
@click.option('-B', '--force-branch', 'force_branches', multiple=True, help='Always commit and push on these branches (even if there were no changes)')
@click.option('-n', '--dry-run', count=True, help="1x: print pushes, but don't perform them; 2x: also skip performing commits")
@click.option('-p', '--pull-branch', help='Branch to pull, before attempting to push; defaults to -b/--branch')
@click.option('-P', '--push-branch', 'push_branches', multiple=True, help='Branches to push to')
@click.option('-r', '--remote', default='origin')
@click.option('-u', '--git-user')
@click.option('-e', '--git-email')
@click.argument('paths', nargs=-1)
def main(branch, force_branches, dry_run, pull_branch, push_branches, remote, git_user, git_email, paths):
    if git_user or git_email:
        if git_user and git_email:
            run('git', 'config', '--global', 'user.name', '"GitHub Actions"')
            run('git', 'config', '--global', 'user.email', '"github@actions"')
        else:
            raise ValueError("Pass -u/--git-user and -e/--git-email, or neither")

    if not branch:
        branch = env.get("GITHUB_HEAD_REF") or env.get("GITHUB_REF_NAME")
        if not branch:
            raise ValueError("Couldn't infer branch; no -b/--branch, $GITHUB_HEAD_REF, or $GITHUB_REF_NAME")
        else:
            err(f"Read branch {branch} from GitHub Actions context")

    run('git', 'add', paths)
    staged_paths = lines('git', 'diff', '--name-only', '--cached', '--', *paths)
    if staged_paths or branch in force_branches:
        if not staged_paths:
            err(f"Branch {branch}; committing (empty) and pushing")
            run('git', 'commit', '--allow-empty', '-m', f'`{branch}` GitHub Action', dry_run=dry_run > 1)
        else:
            err(f'{staged_paths} changed; committing and pushing')
            run('git', 'commit', '-m', f'Update {", ".join(staged_paths)}', dry_run=dry_run > 1)

        run('git', 'config', '--global', 'pull.rebase', 'false')
        pull_branch = pull_branch or branch
        run('git', 'pull', remote, pull_branch)
        for push_branch in push_branches:
            run('git', 'push', remote, f'HEAD:{push_branch}', dry_run=dry_run > 0)
    else:
        err(f"No changes detected: {paths}")


if __name__ == '__main__':
    main()
