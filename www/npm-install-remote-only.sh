#!/usr/bin/env bash

set -x
mv package.json{,.bak}
cat package.json.bak | jq --indent 4 'del(.dependencies."next-utils")' > package.json
npm i
mv package.json{.bak,}
