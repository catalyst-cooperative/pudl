#!/usr/bin/bash
# This script runs the entire PUDL ETL and validation tests in a docker container.
# We manage the deployment of the container using a GHA launched Google Batch VM.
# This script won't work locally because it needs adequate GCP permissions.
# It assumes that the PUDL pixi environment is activated.

# Assert that PUDL_ROOT_PATH is set by the container and points to a valid directory.
cd "${PUDL_ROOT_PATH:?PUDL_ROOT_PATH must be set by the build container}" || exit 1

# Select the PUDL-specific dagster configuration.
cp "${DAGSTER_HOME}/dagster-pudl.yaml" "${DAGSTER_HOME}/dagster.yaml"

function send_zulip_msg() {
    local message="$1"
    if [[ -z "${ZULIP_API_KEY:-}" ]]; then
        echo "Skipping Zulip notification: ZULIP_API_KEY is unset." >&2
        return 0
    fi

    set +x
    curl --silent --show-error \
        -X POST "https://catalyst-cooperative.zulipchat.com/api/v1/messages" \
        -u "build-status-bot@catalyst-cooperative.zulipchat.com:${ZULIP_API_KEY}" \
        -d "type=stream" \
        -d "to=pudl-deployments" \
        -d "topic=build-deploy-pudl" \
        --data-urlencode "content=${message}" ||
        echo "Warning: Zulip notification failed." >&2
    set -x
}

function authenticate_gcp() {
    # Set the default gcloud project id so the gcloud storage operations know what project to bill
    echo "Authenticating to GCP"
    gcloud config set project "$GCP_BILLING_PROJECT"
}

function run_dagster() {
    echo "Launching Dagster and running the PUDL job"
    send_zulip_msg ":rocket: Launching Dagster and running the PUDL job for $BUILD_ID"
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

function format_stage_status() {
    local stage_name=$1
    local stage_status=$2
    local stage_duration=$3
    local stage_emoji=":x:"
    local duration_field="\`[--:--:--]\`"

    # Rows show whether a stage passed, failed, or was intentionally skipped.
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

    # Always include hours to avoid ambiguity when scanning stage timings.
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

function notify_zulip() {
    # Notify pudl-deployments Zulip stream of deployment status
    local total_build_duration
    local nl=$'\n'

    echo "Notifying Zulip about deployment status"
    total_build_duration=$(get_total_build_duration)
    message="${BUILD_ID} status${nl}${nl}"
    if [[ "$1" == "success" ]]; then
        message+=":green_circle: :sunglasses: :unicorn: :rainbow: PUDL Data Build Succeeded!! :partygritty: :database_parrot: :blob-dance: :green_circle:${nl}${nl}"
    elif [[ "$1" == "failure" ]]; then
        message+=":x: Oh bummer the deployment failed :fiiiiine: :sob: :cry_spin: :x:${nl}${nl}"
    else
        echo "Invalid deployment status"
        exit 1
    fi

    message+=":time: \`[${total_build_duration}]\` Total Build Duration${nl}${nl}"
    message+="$(format_stage_status "Run PUDL Dagster Job" "$DAGSTER_STATUS" "$DAGSTER_DURATION")${nl}"
    message+="$(format_stage_status "Unit Tests" "$UNIT_TEST_STATUS" "$UNIT_TEST_DURATION")${nl}"
    message+="$(format_stage_status "Integration Tests" "$INTEGRATION_TEST_STATUS" "$INTEGRATION_TEST_DURATION")${nl}"
    message+="$(format_stage_status "Data Validations (FKs/dbt)" "$DATA_VALIDATION_STATUS" "$DATA_VALIDATION_DURATION")${nl}"
    message+="$(format_stage_status "Row Count Checks (dbt)" "$ROW_COUNT_VALIDATION_STATUS" "$ROW_COUNT_VALIDATION_DURATION")${nl}"
    message+="$(format_stage_status "Save Build Outputs" "$SAVE_OUTPUTS_STATUS" "$SAVE_OUTPUTS_DURATION")${nl}"
    message+="$(format_stage_status "Trigger Deployment" "$TRIGGER_DEPLOYMENT_STATUS" "$TRIGGER_DEPLOYMENT_DURATION")${nl}${nl}"
    # we need to trim off the last dash-delimited section off the build ID to get a valid log link
    message+="[**Query logs online**](https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/run-etl-${BUILD_ID%-*}/logs?project=catalyst-cooperative-pudl)${nl}${nl}"
    message+="[**Download logs to your computer**](https://storage.cloud.google.com/builds.catalyst.coop/$BUILD_ID/$BUILD_ID.log)${nl}${nl}"
    message+="[**Browse full build outputs**](https://console.cloud.google.com/storage/browser/builds.catalyst.coop/$BUILD_ID)"

    send_zulip_msg "$message"
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
        notify_zulip "success" || true
    else
        notify_zulip "failure" || true
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
    authenticate_gcp &&
        python -c "from dagster import DagsterInstance; DagsterInstance.get()"
}; then
    exit 1
fi

# For notification testing, disable the ETL temporarily
# run_stage DAGSTER_STATUS DAGSTER_DURATION run_dagster
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

# Notify Zulip about entire pipeline's success or failure;
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
