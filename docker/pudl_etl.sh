#!/usr/bin/bash
echo "A new message from PUDL"

pudl_setup \
    --pudl_in $CONTAINER_PUDL_IN \
    --pudl_out $CONTAINER_PUDL_OUT
ferc1_to_sqlite \
    --clobber \
    --gcs-cache-path gs://zenodo-cache.catalyst.coop \
    --bypass-local-cache \
    --logfile $LOGFILE \
    $PUDL_SETTINGS_YML
pudl_etl \
    --clobber \
    --gcs-cache-path gs://zenodo-cache.catalyst.coop \
    --bypass-local-cache \
    --logfile $LOGFILE \
    $PUDL_SETTINGS_YML

# Shut down the deploy-pudl-vm instance when the etl is done.
ACCESS_TOKEN=`curl \
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
    -H "Metadata-Flavor: Google" | jq -r '.access_token'`

curl -X POST -H "Content-Length: 0" -H "Authorization: Bearer ${ACCESS_TOKEN}" https://compute.googleapis.com/compute/v1/projects/catalyst-cooperative-pudl/zones/us-central1-a/instances/deploy-pudl-vm/stop
