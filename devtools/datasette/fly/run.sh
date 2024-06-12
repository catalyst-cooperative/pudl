#! /usr/bin/env bash
set -eux

shopt -s nullglob

find /data/ -name '*.sqlite' -delete
ls
mv all_dbs.tar.zst /data
zstd -f -d /data/all_dbs.tar.zst -o /data/all_dbs.tar
tar -xf /data/all_dbs.tar --directory /data
datasette serve --host 0.0.0.0 ${DATABASES} --cors --inspect-file inspect-data.json --metadata metadata.yml --setting sql_time_limit_ms 5000 --port $DATASETTE_PORT > /dev/null &
nginx -c nginx.conf
