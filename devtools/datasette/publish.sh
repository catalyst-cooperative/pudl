#!/bin/sh

PUDL_OUT=`grep "^pudl_out" $HOME/.pudl.yml | sed -e "s/^pudl_out: //"`
SQLITE_DIR="$PUDL_OUT/sqlite"

echo "Indexing FERC1 DB FKs"
sqlite-utils index-foreign-keys $SQLITE_DIR/ferc1.sqlite
echo "Indexing PUDL DB FKs"
sqlite-utils index-foreign-keys $SQLITE_DIR/pudl.sqlite

# Other things we'll want to do...
# Install spatialite for use with census, service territories, etc.
# Enable full text search on plant and utility name and other text columns
# Full column and table level descriptions / metadata

# make metadata.yml
metadata_to_yml -o "metadata.yml"

datasette publish cloudrun \
    --service catalyst-datasette \
    --memory 4Gi \
    --install datasette-cluster-map \
    --install datasette-vega \
    --install datasette-block-robots \
    --metadata metadata.yml \
    --extra-options="--setting sql_time_limit_ms 5000" \
    $SQLITE_DIR/pudl.sqlite \
    $SQLITE_DIR/ferc1.sqlite
