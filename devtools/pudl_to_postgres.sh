#!/usr/bin/bash

# So that we're using postgresql 13.x
export PGPORT=5434

PUDL_SQLITE="$PUDL_DIR/sqlite/pudl.sqlite"
PUDL_POSTGRES="postgresql:///pudl"

echo "Converting $PUDL_SQLITE to $PUDL_POSTGRES"

pgloader \
    --with "include drop" \
    --with "create tables" \
    --with "create indexes" \
    --with "reset sequences" \
    $PUDL_SQLITE $PUDL_POSTGRES

# Generate an SQL dumpfile compatible with PostgreSQL:
# pg_dump -U zane --format=plain --no-owner --no-acl -p 5434 -h localhost -d pudl \
#     | sed -E 's/(DROP|CREATE|COMMENT ON) EXTENSION/-- \1 EXTENSION/g' \
#     | gzip > pudl.sql.gz

# Upload that dumpfile to a storage bucket so it can be imported to CloudSQL:
# gsutil cp pudl.sql.gz gs://catalyst.coop/cloud-sql-imports/pudl-2021-11-28.sql.gz
