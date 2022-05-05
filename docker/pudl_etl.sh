#!/usr/bin/bash

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
