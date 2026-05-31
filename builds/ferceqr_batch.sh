#!/usr/bin/bash
# This script runs the entire ferceqr ETL and validation tests in a docker container on a Google Compute Engine instance.
# This script won't work locally because it needs adequate GCP permissions.

function authenticate_gcp() {
    # Set the default gcloud project id so the gcloud storage operations know what project to bill
    echo "Authenticating to GCP"
    gcloud config set project "$GCP_BILLING_PROJECT"
}

function write_aws_credentials() {
    # set +x / set -x is used to avoid printing the AWS credentials in the logs
    echo "Setting AWS credentials"
    mkdir -p ~/.aws
    echo "[default]" >~/.aws/credentials
    set +x
    echo "aws_access_key_id = ${AWS_ACCESS_KEY_ID}" >>~/.aws/credentials
    echo "aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}" >>~/.aws/credentials
    set -x
}

function validate_partition_range_inputs() {
    if [[ -n "${FERCEQR_START_PARTITION:-}" || -n "${FERCEQR_END_PARTITION:-}" ]]; then
        if [[ -z "${FERCEQR_START_PARTITION:-}" || -z "${FERCEQR_END_PARTITION:-}" ]]; then
            echo "FERCEQR_START_PARTITION and FERCEQR_END_PARTITION must be set together." >&2
            return 1
        fi
    fi
}

function run_ferceqr_etl() {
    echo "Running FERC EQR ETL"
    # Launch dagster-daemon in the background (handles the backfill queue)
    dagster-daemon run &

    # Kick off the ferceqr job asynchronously
    BACKFILL_ARGS=(job backfill --noprompt --job ferceqr)
    if [[ -n "${FERCEQR_START_PARTITION:-}" ]]; then
        BACKFILL_ARGS+=(--from "$FERCEQR_START_PARTITION" --to "$FERCEQR_END_PARTITION")
    fi
    dagster "${BACKFILL_ARGS[@]}"
    # Wait for a file called 'FERCEQR_SUCCESS' or 'FERCEQR_FAILURE' to be created in
    # PUDL_OUTPUT indicating completion. Timeout after 6 hours if file still doesn't exist.
    inotifywait -e create -t 21600 --include 'FERCEQR_SUCCESS|FERCEQR_FAILURE' "$PUDL_OUTPUT"
    killall dagster-daemon
}

function cleanup_on_exit() {
    if [[ -z "${LOGFILE:-}" || ! -f "$LOGFILE" ]]; then
        rm -f ~/.aws/credentials
        return 0
    fi

    if [[ -z "${GCS_LOGS_BUCKET:-}" || -z "${GCP_BILLING_PROJECT:-}" ]]; then
        echo "Skipping log upload because GCS_LOGS_BUCKET or GCP_BILLING_PROJECT is unset." >&2
        rm -f ~/.aws/credentials
        return 0
    fi

    gcloud storage --billing-project="$GCP_BILLING_PROJECT" --quiet cp "$LOGFILE" "${GCS_LOGS_BUCKET}/${BUILD_ID}.log"
    rm -f ~/.aws/credentials
}

########################################################################################
# MAIN SCRIPT
########################################################################################

# Select the FERC EQR-specific dagster configuration.
cp "${DAGSTER_HOME}/dagster-ferceqr.yaml" "${DAGSTER_HOME}/dagster.yaml"

LOGFILE="${PUDL_OUTPUT}/${BUILD_ID}.log"

write_aws_credentials

touch "$LOGFILE"
exec > >(tee -a "$LOGFILE") 2>&1
# Bash runs this trap on any script exit path, so the log upload still happens
# after an early failure as well as after a successful run.
trap cleanup_on_exit EXIT

# An empty string from the workflow means "do not deploy", which should behave the
# same as the variable being unset inside the container.
if [[ -z "${PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH:-}" ]]; then
    unset PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH
fi

if ! {
    validate_partition_range_inputs && \
    authenticate_gcp && \
    check_path_permissions --read "$PUDL_FERCEQR_ARCHIVE_PATH" && \
    check_path_permissions --write --check-ferceqr-deployment-paths "$GCS_LOGS_BUCKET" && \
    python -c "from dagster import DagsterInstance; DagsterInstance.get()"
}; then
    exit 1
fi

run_ferceqr_etl

# Check if build was successful and return appropriate return value
if [ ! -f "${PUDL_OUTPUT}/FERCEQR_SUCCESS" ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"
