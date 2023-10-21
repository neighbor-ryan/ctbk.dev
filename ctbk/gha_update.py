#!/usr/bin/env python

from os import environ as env

import click
from utz import err, run, lines, line


@click.command()
@click.option('-b', '--current-branch', help='Current branch; defaults to $GITHUB_HEAD_REF, falling back to $GITHUB_REF_NAME then `git symbolic-ref -q --short HEAD`')
@click.option('-B', '--force-branch', 'force_branches', multiple=True, help='Always commit and push when run on these branches (even if there were no changes)')
@click.option('-n', '--dry-run', count=True, help="1x: print pushes, but don't perform them; 2x: also skip performing commits")
@click.option('-p', '--pull-branch', help='Branch to pull, before attempting to push; defaults to -b/--current-branch')
@click.option('-P', '--push-branch', 'push_branches', multiple=True, help='Branches to push to')
@click.option('-r', '--remote', default='origin')
@click.option('-R', '--pull-rebase', is_flag=True, help='Rebase onto --pull-branch (default: merge)')
@click.option('-u', '--git-user', help='Set Git user.name config (for commits)')
@click.option('-e', '--git-email', help='Set Git user.email config (for commits)')
@click.argument('paths', nargs=-1)
def main(
        current_branch,
        force_branches,
        dry_run,
        pull_branch,
        push_branches,
        remote,
        pull_rebase,
        git_user,
        git_email,
        paths
):
    if git_user or git_email:
        if git_user and git_email:
            run('git', 'config', '--global', 'user.name', git_user)
            run('git', 'config', '--global', 'user.email', git_email)
        else:
            raise ValueError("Pass -u/--git-user and -e/--git-email, or neither")

    if not current_branch:
        current_branch = env.get("GITHUB_HEAD_REF") or env.get("GITHUB_REF_NAME")
        if not current_branch:
            current_branch = line('git', 'symbolic-ref', '-q', '--short', 'HEAD')
            if not current_branch:
                raise ValueError("Couldn't infer branch; no -b/--current-branch, $GITHUB_HEAD_REF, or $GITHUB_REF_NAME")
        else:
            err(f"Read branch {current_branch} from GitHub Actions context")

    run('git', 'add', paths)
    staged_paths = lines('git', 'diff', '--name-only', '--cached', '--', *paths)
    if staged_paths or current_branch in force_branches:
        if not staged_paths:
            err(f"Branch {current_branch}; committing (empty)")
            run('git', 'commit', '--allow-empty', '-m', f'`{current_branch}` GitHub Action', dry_run=dry_run > 1)
        else:
            err(f'Committing changed paths: {staged_paths}')
            run('git', 'commit', '-m', f'Update {", ".join(staged_paths)}', dry_run=dry_run > 1)

        run('git', 'config', '--global', 'pull.rebase', str(pull_rebase))
        pull_branch = pull_branch or current_branch
        run('git', 'pull', remote, pull_branch)
        for push_branch in push_branches:
            run('git', 'push', remote, f'HEAD:{push_branch}', dry_run=dry_run > 0)
    else:
        err(f"No changes detected: {paths}")


if __name__ == '__main__':
    main()
