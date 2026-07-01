#!/usr/bin/bash
# This script runs the entire PUDL ETL and validation tests in a docker container.
# We manage the deployment of the container using a GHA launched Google Batch VM.
# This script won't work locally because it needs adequate GCP permissions.
# It assumes that the PUDL pixi environment is activated.

# Assert that PUDL_ROOT_PATH is set by the container and points to a valid directory.
cd "${PUDL_ROOT_PATH:?PUDL_ROOT_PATH must be set by the build container}" || exit 1

# Select the PUDL-specific dagster configuration.
cp "${DAGSTER_HOME}/dagster-pudl.yaml" "${DAGSTER_HOME}/dagster.yaml"

function send_slack_msg() {
    set +x &&
        echo "sending Slack message" &&
        curl --fail-with-body -X POST -H "Content-type: application/json" -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/chat.postMessage --data "{\"channel\": \"C03FHB9N0PQ\", \"text\": \"$1\"}" &&
        set -x
}

function authenticate_gcp() {
    # Set the default gcloud project id so the gcloud storage operations know what project to bill
    echo "Authenticating to GCP"
    gcloud config set project "$GCP_BILLING_PROJECT"
}

function initialize_postgres() {
    echo "initializing postgres."
    # This is sort of a fiddly set of postgres admin tasks:
    #
    # 1. start the dagster cluster, which is set to be owned by ubuntu in the Dockerfile
    # 2. create a db within this cluster so we can do things
    # 3. tell it to actually fail when we mess up, instead of continuing blithely
    # 4. create a *dagster* user, whose creds correspond with those in builds/dagster-pudl.yaml
    # 5. make a database for dagster, which is owned by the dagster user
    pg_ctlcluster "$PG_VERSION" dagster start &&
        createdb -h127.0.0.1 -p5433 &&
        psql -v "ON_ERROR_STOP=1" -h127.0.0.1 -p5433 &&
        psql -c "CREATE USER dagster WITH SUPERUSER PASSWORD 'dagster_password'" -h127.0.0.1 -p5433 &&
        psql -c "CREATE DATABASE dagster OWNER dagster" -h127.0.0.1 -p5433
}

function run_dagster() {
    echo "Launching Dagster and running the PUDL job"
    send_slack_msg ":play: Launching Dagster and running the PUDL job for $BUILD_ID :floppy_disk:"
    pixi run pudl-with-ferc-to-sqlite-nightly
}

function save_outputs_to_gcs() {
    echo "Copying outputs to GCP bucket $PUDL_GCS_OUTPUT" &&
        gcloud storage --quiet cp -r "$PUDL_OUTPUT" "$PUDL_GCS_OUTPUT" &&
        gcloud storage --quiet cp -r "dbt/seeds/etl_full_row_counts.csv" "$PUDL_GCS_OUTPUT" &&
        gcloud storage --quiet cp "$LOGFILE" "$PUDL_GCS_OUTPUT" &&
        rm -f "$PUDL_OUTPUT/success"
}

function trigger_deployment() {
    set +x &&
        echo "Triggering the zenodo data release workflow using the GitHub API and curl" &&
        curl --fail-with-body -sS -X POST \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${PUDL_BOT_PAT}" \
            https://api.github.com/repos/catalyst-cooperative/pudl/actions/workflows/deploy-pudl.yml/dispatches \
            -d @<(
                cat <<JSON
{
  "ref": "${BUILD_REF}",
  "inputs": {
    "git_tag": "${GIT_TAG}"
  }
}
JSON
            ) &&
        set -x
}

function slack_stage_status() {
    local stage_name=$1
    local stage_status=$2
    local stage_duration=$3
    local stage_emoji=":x:"
    local duration_field="\`[--:--:--]\`"

    # Slack rows show whether a stage passed, failed, or was intentionally skipped.
    if [[ $stage_status == "$STAGE_SKIPPED" ]]; then
        stage_emoji=":ghost:"
    elif [[ $stage_status == 0 ]]; then
        stage_emoji=":check:"
    fi

    # Always render a fixed-width duration field so the stage names line up.
    if [[ -n "$stage_duration" ]]; then
        printf -v duration_field "\`[%s]\`" "$stage_duration"
    fi

    printf '%s %s %s' "$stage_emoji" "$duration_field" "$stage_name"
}

function format_stage_duration() {
    local elapsed_seconds=$1
    local elapsed_hours=$((elapsed_seconds / 3600))
    local elapsed_minutes=$(((elapsed_seconds % 3600) / 60))
    local remaining_seconds=$((elapsed_seconds % 60))

    # Always include hours to avoid ambiguity when scanning Slack stage timings.
    printf '%02d:%02d:%02d' "$elapsed_hours" "$elapsed_minutes" "$remaining_seconds"
}

function set_stage_duration() {
    local duration_var=$1
    local elapsed_seconds=$2

    # Store the formatted duration in the caller-supplied shell variable name.
    printf -v "$duration_var" '%s' "$(format_stage_duration "$elapsed_seconds")"
}

function get_total_build_duration() {
    local now_epoch_seconds

    now_epoch_seconds=$(date +%s)
    format_stage_duration "$((now_epoch_seconds - BUILD_START_EPOCH_SECONDS))"
}

function run_stage() {
    local status_var=$1
    local duration_var=$2
    shift 2

    # Most stages follow the same pattern: run, capture the command status, and
    # record how long the stage took.
    # SECONDS automatically increments each second the script is running, so if
    # we set it to 0, we get a stopwatch.
    SECONDS=0
    "$@"
    printf -v "$status_var" '%s' "$?"
    set_stage_duration "$duration_var" "$SECONDS"
}

function stage_failed() {
    local stage_status=$1

    [[ "$stage_status" != "$STAGE_SKIPPED" && "$stage_status" != 0 ]]
}

function exit_on_stage_failure() {
    local stage_status=$1

    # Required stages fail fast instead of letting later steps continue in a
    # half-broken build environment.
    if stage_failed "$stage_status"; then
        exit 1
    fi
}

function any_stage_failed() {
    local stage_status

    for stage_status in "$@"; do
        if stage_failed "$stage_status"; then
            return 0
        fi
    done

    return 1
}

function notify_slack() {
    # Notify pudl-deployment slack channel of deployment status
    local total_build_duration

    echo "Notifying Slack about deployment status"
    total_build_duration=$(get_total_build_duration)
    message="${BUILD_ID} status\n\n"
    if [[ "$1" == "success" ]]; then
        message+=":green_circle: :sunglasses: :unicorn: :rainbow: PUDL Data Build Succeeded!! :partygritty: :database_parrot: :blob-dance: :green_circle:\n\n"
    elif [[ "$1" == "failure" ]]; then
        message+=":x: Oh bummer the deployment failed :fiiiiine: :sob: :cry_spin: :x:\n\n"
    else
        echo "Invalid deployment status"
        exit 1
    fi

    message+=":time: \`[${total_build_duration}]\` Total Build Duration\n\n"
    message+="$(slack_stage_status "Run PUDL Dagster Job" "$DAGSTER_STATUS" "$DAGSTER_DURATION")\n"
    message+="$(slack_stage_status "Unit Tests" "$UNIT_TEST_STATUS" "$UNIT_TEST_DURATION")\n"
    message+="$(slack_stage_status "Integration Tests" "$INTEGRATION_TEST_STATUS" "$INTEGRATION_TEST_DURATION")\n"
    message+="$(slack_stage_status "Data Validations (FKs/dbt)" "$DATA_VALIDATION_STATUS" "$DATA_VALIDATION_DURATION")\n"
    message+="$(slack_stage_status "Row Count Checks (dbt)" "$ROW_COUNT_VALIDATION_STATUS" "$ROW_COUNT_VALIDATION_DURATION")\n"
    message+="$(slack_stage_status "Save Build Outputs" "$SAVE_OUTPUTS_STATUS" "$SAVE_OUTPUTS_DURATION")\n"
    message+="$(slack_stage_status "Trigger Deployment" "$TRIGGER_DEPLOYMENT_STATUS" "$TRIGGER_DEPLOYMENT_DURATION")\n"
    # we need to trim off the last dash-delimited section off the build ID to get a valid log link
    message+="<https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/run-etl-${BUILD_ID%-*}/logs?project=catalyst-cooperative-pudl|*Query logs online*>\n\n"
    message+="<https://storage.cloud.google.com/builds.catalyst.coop/$BUILD_ID/$BUILD_ID.log|*Download logs to your computer*>\n\n"
    message+="<https://console.cloud.google.com/storage/browser/builds.catalyst.coop/$BUILD_ID|*Browse full build outputs*>"

    send_slack_msg "$message"
}

function cleanup_on_exit() {
    local exit_code=$?

    if [[ -n "${LOGFILE:-}" && -f "$LOGFILE" && -n "${PUDL_GCS_OUTPUT:-}" ]]; then
        gcloud storage --quiet cp "$LOGFILE" "$PUDL_GCS_OUTPUT" || true
    fi

    if [[ -n "${PG_VERSION:-}" ]]; then
        pg_ctlcluster "$PG_VERSION" dagster stop || true
    fi

    rm -f ~/.aws/credentials

    if [[ $exit_code -eq 0 ]] && ! any_stage_failed \
        "$DAGSTER_STATUS" \
        "$UNIT_TEST_STATUS" \
        "$INTEGRATION_TEST_STATUS" \
        "$DATA_VALIDATION_STATUS" \
        "$ROW_COUNT_VALIDATION_STATUS" \
        "$SAVE_OUTPUTS_STATUS" \
        "$UPDATE_NIGHTLY_STATUS" \
        "$UPDATE_STABLE_STATUS" \
        "$PREP_OUTPUTS_STATUS" \
        "$DISTRIBUTION_BUCKET_STATUS" \
        "$GCS_TEMPORARY_HOLD_STATUS" \
        "$TRIGGER_DATA_VIEWER_DEPLOY_STATUS"; then
        notify_slack "success" || true
    else
        notify_slack "failure" || true
    fi

    return "$exit_code"
}

########################################################################################
# MAIN SCRIPT
########################################################################################
LOGFILE="${PUDL_OUTPUT}/${BUILD_ID}.log"
STAGE_SKIPPED="skipped"
BUILD_START_EPOCH_SECONDS=$(date +%s)

# Initialize our stage-status variables so they all definitely have a value to check
DAGSTER_STATUS="$STAGE_SKIPPED"
UNIT_TEST_STATUS="$STAGE_SKIPPED"
INTEGRATION_TEST_STATUS="$STAGE_SKIPPED"
DATA_VALIDATION_STATUS="$STAGE_SKIPPED"
ROW_COUNT_VALIDATION_STATUS="$STAGE_SKIPPED"
SAVE_OUTPUTS_STATUS="$STAGE_SKIPPED"
TRIGGER_DEPLOYMENT_STATUS="$STAGE_SKIPPED"

DAGSTER_DURATION=""
UNIT_TEST_DURATION=""
INTEGRATION_TEST_DURATION=""
DATA_VALIDATION_DURATION=""
ROW_COUNT_VALIDATION_DURATION=""
SAVE_OUTPUTS_DURATION=""
TRIGGER_DEPLOYMENT_DURATION=""

# Set these variables *only* if they are not already set by the container or workflow:
: "${PUDL_GCS_OUTPUT:=gs://builds.catalyst.coop/$BUILD_ID}"
# Keep the nightly Dagster config path repo-relative so the same pixi task commands
# work both locally and inside the nightly build container.
: "${DG_NIGHTLY_CONFIG:=src/pudl/package_data/settings/dg_nightly.yml}"
export DG_NIGHTLY_CONFIG

touch "$LOGFILE"
exec > >(tee -a "$LOGFILE") 2>&1
trap cleanup_on_exit EXIT

# Check if there are any existing builds associated with the current commit
if pixi run pudl_check_for_build "$GIT_TAG"; then
    run_stage TRIGGER_DEPLOYMENT_STATUS TRIGGER_DEPLOYMENT_DURATION trigger_deployment
    if any_stage_failed "$TRIGGER_DEPLOYMENT_STATUS"; then
        echo "Found successful build, but failed to trigger deployment"
        exit 1
    fi
    echo "Found a successful build and triggered a deployment"
    exit 0
fi

if ! {
    initialize_postgres && \
    authenticate_gcp && \
    python -c "from dagster import DagsterInstance; DagsterInstance.get()"
}; then
    exit 1
fi

run_stage DAGSTER_STATUS DAGSTER_DURATION run_dagster
run_stage UNIT_TEST_STATUS UNIT_TEST_DURATION pixi run pytest-unit-nightly
run_stage INTEGRATION_TEST_STATUS INTEGRATION_TEST_DURATION pixi run pytest-integration-nightly
run_stage DATA_VALIDATION_STATUS DATA_VALIDATION_DURATION pixi run pytest-validate-nightly
run_stage ROW_COUNT_VALIDATION_STATUS ROW_COUNT_VALIDATION_DURATION pixi run pytest-validate-row-counts-nightly

if ! any_stage_failed \
    "$DAGSTER_STATUS" \
    "$UNIT_TEST_STATUS" \
    "$INTEGRATION_TEST_STATUS" \
    "$DATA_VALIDATION_STATUS" \
    "$ROW_COUNT_VALIDATION_STATUS"; then
    touch "$PUDL_OUTPUT/success"
fi

# Generate new row counts for all tables in the PUDL database
dbt_helper update-tables --clobber --row-counts all

run_stage SAVE_OUTPUTS_STATUS SAVE_OUTPUTS_DURATION save_outputs_to_gcs

exit_on_stage_failure "$DAGSTER_STATUS"
exit_on_stage_failure "$UNIT_TEST_STATUS"
exit_on_stage_failure "$INTEGRATION_TEST_STATUS"
exit_on_stage_failure "$DATA_VALIDATION_STATUS"
exit_on_stage_failure "$ROW_COUNT_VALIDATION_STATUS"

run_stage TRIGGER_DEPLOYMENT_STATUS TRIGGER_DEPLOYMENT_DURATION trigger_deployment

# Notify slack about entire pipeline's success or failure;
if any_stage_failed \
    "$DAGSTER_STATUS" \
    "$UNIT_TEST_STATUS" \
    "$INTEGRATION_TEST_STATUS" \
    "$DATA_VALIDATION_STATUS" \
    "$ROW_COUNT_VALIDATION_STATUS" \
    "$SAVE_OUTPUTS_STATUS" \
    "$TRIGGER_DEPLOYMENT_STATUS"; then
    exit 1
fi

echo "Build succeeded!"
