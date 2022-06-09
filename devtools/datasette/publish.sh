#!/bin/sh

PUDL_OUT=`grep "^pudl_out" $HOME/.pudl.yml | sed -e "s/^pudl_out: //"`
SQLITE_DIR="$PUDL_OUT/sqlite"

echo "Indexing FERC1 DB FKs"
sqlite-utils index-foreign-keys $SQLITE_DIR/ferc1.sqlite
echo "Indexing PUDL DB FKs"
sqlite-utils index-foreign-keys $SQLITE_DIR/pudl.sqlite

# make metadata.yml
datasette_metadata_to_yml -o "metadata.yml"

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
