#!/usr/bin/env bash

echo "Running at: `date -Iseconds`"

excludes=(
    home s3 tmproot
    .idea .DS_Store __pycache__ .ipynb_checkpoints .pytest_cache ctbk.egg-info '*.iml'
    www/.next
    www/node_modules
    www/out
)
exclude_args=()
for exclude in "${excludes[@]}"; do
    exclude_args+=(--exclude "$exclude")
done

rsync -avzh "$@" "${exclude_args[@]}" ./ ec2:ctbk/

echo "Done at: `date -Iseconds`"
