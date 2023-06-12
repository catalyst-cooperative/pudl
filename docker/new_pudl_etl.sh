#!/usr/bin/bash
# This script runs the entire ETL and validation tests in a docker container on Google batch.

set -x
# TODO(rousik): do we want to explicitly validate that all required env variables are present?
# TODO(rousik): logfile, loglevel and gcs-cache-path could be folded into the config.
# TODO(rousik): this script does not upload to GCP and does not send
# slack notifications. Notifications should be moved into the python.


gcloud config set project $GCP_BILLING_PROJECT
alembic upgrade head

# setup seems to only do directory setup that should be irrelevant when running in docker
pudl_setup

ferc_to_sqlite \
    --loglevel=DEBUG \
    --gcs-cache-path=gs://internal-zenodo-cache.catalyst.coop \
    --workers=8 \
    $PUDL_SETTINGS_YML

pudl_etl \
    --loglevel DEBUG \
    --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
    $PUDL_SETTINGS_YML

pytest \
    --gcs-cache-path=gs://internal-zenodo-cache.catalyst.coop \
    --etl-settings=$PUDL_SETTINGS_YML \
    --live-dbs test
