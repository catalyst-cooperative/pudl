#!/usr/bin/bash
function authenticate_gcp() {
    export GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/service_account_key.json
    # Set the default gcloud project id so the zenodo-cache bucket
    # knows what project to bill for egress
    gcloud config set project ${GCLOUD_BILLING_PROJECT:?err}
}

function run_pudl_etl() {
    authenticate_gcp \
    && pudl_setup \
        --pudl_in $CONTAINER_PUDL_IN \
        --pudl_out $CONTAINER_PUDL_OUT \
    && ferc1_to_sqlite \
        --clobber \
        --loglevel DEBUG \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML \
    && pudl_etl \
        --clobber \
        --loglevel DEBUG \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML \
    && pytest \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        --etl-settings $PUDL_SETTINGS_YML \
        --live-dbs test
}

# Run the ETL and save the logs.
# 2>&1 redirects stderr to stdout.
run_pudl_etl 2>&1 | tee $LOGFILE

# Notify slack if the etl succeeded.
if [[ ${PIPESTATUS[0]} == 0 ]]; then
    echo "The ETL and tests succesfully ran!"
else
    echo "Oh bummer the ETL and tests failed :/"
fi
