#!/usr/bin/env bash

cmd=(
  aws s3 cp
  --no-guess-mime-type
  --content-type="application/json; charset=utf-8"
  --metadata-directive="REPLACE"
  s3://ctbk.dev/dist/bundle.js
  s3://ctbk.dev/dist/bundle.js
)
echo "Running: ${cmd[*]}"
"${cmd[@]}"
