#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")" && pwd)"
git_dir="$(git -C "$project_dir" rev-parse --path-format=absolute --git-common-dir)"
repo_root="$(dirname "$git_dir")"
# One backend shared by every worktree, independent of the older infra stack.
export PULUMI_BACKEND_URL="file://$repo_root/infra/movie-state"
if [[ -n "${CF_PULUMI_HCCS_TOKEN:-}" ]]; then
    export CLOUDFLARE_API_TOKEN="$CF_PULUMI_HCCS_TOKEN"
fi
mkdir -p "$repo_root/infra/movie-state"
cd "$project_dir"
exec pulumi "$@"
