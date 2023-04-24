#!/usr/bin/bash
# This script runs the entire ETL and validation tests in a docker container.
# It is mostly used for local debugging of our docker deployment and the gcp_pudl_etl.sh script.

set -x

function run_pudl_etl() {
    pudl_setup \
    && alembic upgrade head \
    && ferc_to_sqlite \
        --loglevel DEBUG \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        $PUDL_SETTINGS_YML \
    && pudl_etl \
        --loglevel DEBUG \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        $PUDL_SETTINGS_YML \
    && tox parallel -e unit,integration,validation --parallel-live \
        -- \
        --gcs-cache-path=gs://internal-zenodo-cache.catalyst.coop \
        --etl-settings=$PUDL_SETTINGS_YML \
        --live-dbs
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
