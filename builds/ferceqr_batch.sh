#!/usr/bin/bash
# This script runs the entire ferceqr ETL and validation tests in a docker container on a Google Compute Engine instance.
# This script won't work locally because it needs adequate GCP permissions.

function authenticate_gcp() {
    # Set the default gcloud project id so the gcloud storage operations know what project to bill
    echo "Authenticating to GCP"
    gcloud config set project "$GCP_BILLING_PROJECT"
}

function cleanup_ferceqr_status_files() {
    rm -f "${PUDL_OUTPUT}/FERCEQR_SUCCESS" "${PUDL_OUTPUT}/FERCEQR_FAILURE"
}

function run_ferceqr_etl() {
    echo "Running FERC EQR ETL"
    cleanup_ferceqr_status_files
    # Launch dagster-daemon in the background (handles the backfill queue)
    authenticate_gcp &&
        dagster-daemon run &

    # Kick off the ferceqr job asynchronously
    dagster job backfill --noprompt --job ferceqr
    # Wait for a file called 'FERCEQR_SUCCESS' or 'FERCEQR_FAILURE' to be created in
    # PUDL_OUTPUT indicating completion. Timeout after 6 hours if file still doesn't exist.
    inotifywait -e create -t 21600 --include 'FERCEQR_SUCCESS|FERCEQR_FAILURE' "$PUDL_OUTPUT"
    killall dagster-daemon
}

########################################################################################
# MAIN SCRIPT
########################################################################################

# Select the FERC EQR-specific dagster configuration.
cp "${DAGSTER_HOME}/dagster-ferceqr.yaml" "${DAGSTER_HOME}/dagster.yaml"

LOGFILE="${PUDL_OUTPUT}/${BUILD_ID}.log"

# Save credentials for working with AWS S3
# set +x / set -x is used to avoid printing the AWS credentials in the logs
echo "Setting AWS credentials"
mkdir -p ~/.aws
echo "[default]" >~/.aws/credentials
set +x
echo "aws_access_key_id = ${AWS_ACCESS_KEY_ID}" >>~/.aws/credentials
echo "aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}" >>~/.aws/credentials
set -x

authenticate_gcp
python -m pudl.scripts.ferceqr_deployment_preflight \
    --archive-path "$PUDL_FERCEQR_ARCHIVE_PATH" \
    --deployment-config-path "${PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH:-}" \
    --logs-path "$GCS_LOGS_BUCKET" \
    --build-id "$BUILD_ID"

run_ferceqr_etl 2>&1 | tee "$LOGFILE"

# Copy logs to GCS build directory
gcloud storage --billing-project="$GCP_BILLING_PROJECT" --quiet cp "$LOGFILE" "${GCS_LOGS_BUCKET}/${BUILD_ID}.log"

# Check if build was successful and return appropriate return value
if [ ! -f "${PUDL_OUTPUT}/FERCEQR_SUCCESS" ]; then
    echo "Build failed!"
    cleanup_ferceqr_status_files
    exit 1
fi

echo "Build successful!"
cleanup_ferceqr_status_files
