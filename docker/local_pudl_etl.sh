#!/usr/bin/bash
# This script runs the entire ETL and validation tests in a docker container.
# It is mostly used for local debugging of our docker deployment and the gcp_pudl_etl.sh script.

set -eux

function bridge_settings() {
    export PUDL_CACHE="${CONTAINER_PUDL_IN}/data"
    export PUDL_OUTPUT=$CONTAINER_PUDL_OUT
}

function run_pudl_etl() {
    pudl_setup \
        --pudl_in $CONTAINER_PUDL_IN \
        --pudl_out $CONTAINER_PUDL_OUT \
    && bridge_settings \
    && ferc_to_sqlite \
        --loglevel DEBUG \
        $PUDL_SETTINGS_YML \
    && pudl_etl \
        --loglevel DEBUG \
        $PUDL_SETTINGS_YML \
    && epacems_to_parquet \
        --loglevel DEBUG \
        -y 2021 \
        -s id \
    && pytest \
        --etl-settings $PUDL_SETTINGS_YML \
        --live-dbs test
}

# Run the ETL and save the logs.
# 2>&1 redirects stderr to stdout.
run_pudl_etl 2>&1 | tee $LOGFILE

# Notify the ETL completion status.
if [[ ${PIPESTATUS[0]} == 0 ]]; then
    echo "The ETL and tests succesfully ran!"
else
    echo "Oh bummer the ETL and tests failed :/"
fi
