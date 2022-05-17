#!/usr/bin/bash
function run_pudl_etl() {
    pudl_setup \
        --pudl_in $CONTAINER_PUDL_IN \
        --pudl_out $CONTAINER_PUDL_OUT
    ferc1_to_sqlite \
        --clobber \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML
    pudl_etl \
        --clobber \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML
}

function shutdown_vm() {
    # Create a log bucket for the deployment
    gsutil -m cp -r $CONTAINER_PUDL_OUT "gs://pudl-etl-logs/$GITHUB_SHA-$GITHUB_REF"

    # # Shut down the deploy-pudl-vm instance when the etl is done.
    ACCESS_TOKEN=`curl \
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        -H "Metadata-Flavor: Google" | jq -r '.access_token'`

    curl -X POST -H "Content-Length: 0" -H "Authorization: Bearer ${ACCESS_TOKEN}" https://compute.googleapis.com/compute/v1/projects/catalyst-cooperative-pudl/zones/us-central1-a/instances/deploy-pudl-vm/stop
}

# Run ETL. Copy outputs to GCS and shutdown VM if ETL succeeds or fails
{ # try
    run_pudl_etl 2> $LOGFILE && shutdown_vm

} || { # catch
    shutdown_vm
}
