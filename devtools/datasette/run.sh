#! /usr/bin/env bash
set -eux

shopt -s nullglob

datasette serve \
  --host 0.0.0.0 \
  --immutable /data/nightly/*.sqlite
  --cors \
  --metadata metadata.yml \
  --setting sql_time_limit_ms 5000 \
  --port $DATASETTE_PORT \
  > /dev/null
