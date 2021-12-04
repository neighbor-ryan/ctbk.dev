#!/usr/bin/env bash



aws s3 sync --dryrun \
  --exclude '*' \
  --include index.html --include dist/bundle.js --include assets/index.css --include assets/index.css --include assets/citibike.css \
  . s3://ctbk/static/
