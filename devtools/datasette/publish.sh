#!/bin/sh

# make metadata.yml
datasette_metadata_to_yml -o "metadata.yml"

EXTRA_OPTIONS="--setting sql_time_limit_ms 5000"
EXTRA_OPTIONS="$EXTRA_OPTIONS --setting allow_download off"
EXTRA_OPTIONS="$EXTRA_OPTIONS --setting allow_csv_stream off"

datasette publish cloudrun \
    --service catalyst-datasette \
    --memory 4Gi \
    --install datasette-cluster-map \
    --install datasette-vega \
    --install datasette-block-robots \
    --metadata metadata.yml \
    --extra-options=$EXTRA_OPTIONS \
    $PUDL_OUTPUT/pudl.sqlite \
    $PUDL_OUTPUT/ferc1.sqlite \
    $PUDL_OUTPUT/ferc1_xbrl.sqlite \
    $PUDL_OUTPUT/ferc2_xbrl.sqlite \
    $PUDL_OUTPUT/ferc6_xbrl.sqlite \
    $PUDL_OUTPUT/ferc60_xbrl.sqlite \
    $PUDL_OUTPUT/ferc714_xbrl.sqlite
