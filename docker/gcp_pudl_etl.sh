#!/usr/bin/bash
# This script runs the entire PUDL ETL and validation tests in a docker container.
# We manage the deployment of the container using a GHA launched Google Batch VM.
# This script won't work locally because it needs adequate GCP permissions.
# It assumes that the PUDL pixi environment is activated.

# Assert that PUDL_REPO is set by the container and points to a valid directory.
cd "${PUDL_REPO:?PUDL_REPO must be set by the build container}" || exit 1

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
    # 4. create a *dagster* user, whose creds correspond with those in docker/dagster.yaml
    # 5. make a database for dagster, which is owned by the dagster user
    pg_ctlcluster "$PG_VERSION" dagster start &&
        createdb -h127.0.0.1 -p5433 &&
        psql -v "ON_ERROR_STOP=1" -h127.0.0.1 -p5433 &&
        psql -c "CREATE USER dagster WITH SUPERUSER PASSWORD 'dagster_password'" -h127.0.0.1 -p5433 &&
        psql -c "CREATE DATABASE dagster OWNER dagster" -h127.0.0.1 -p5433
}

function run_ferc_to_sqlite() {
    # STUB: skipping real ETL for reporting test
    echo "STUB: Skipping FERC to SQLite conversion"
    send_slack_msg ":play: Deployment started for $BUILD_ID :floppy_disk:"
}

function run_pudl_etl() {
    # STUB: skipping real ETL for reporting test
    echo "STUB: Skipping PUDL ETL"
}

function run_unit_tests() {
    # STUB: skipping real tests for reporting test
    echo "STUB: Skipping unit tests"
}

function run_integration_tests() {
    # STUB: skipping real tests for reporting test
    echo "STUB: Skipping integration tests"
}

function write_pudl_datapackage() {
    echo "Writing PUDL datapackage."
    python -c "from pudl.metadata.classes import PUDL_PACKAGE; print(PUDL_PACKAGE.to_frictionless(exclude_pattern=r'core_ferceqr.*').to_json())" >"$PUDL_OUTPUT/parquet/pudl_parquet_datapackage.json"
    return $?
}

function save_outputs_to_gcs() {
    echo "Copying outputs to GCP bucket $PUDL_GCS_OUTPUT" &&
        gcloud storage --quiet cp -r "$PUDL_OUTPUT" "$PUDL_GCS_OUTPUT" &&
        gcloud storage --quiet cp -r "dbt/seeds/etl_full_row_counts.csv" "$PUDL_GCS_OUTPUT" &&
        rm -f "$PUDL_OUTPUT/success"
}

function remove_dist_path() {
    DIST_PATH=$1
    # Only attempt to update outputs if we have an argument
    # This avoids accidentally blowing away the whole bucket if it's not set.
    if [[ -n "$DIST_PATH" ]]; then
        GCS_PATH="gs://pudl.catalyst.coop/$DIST_PATH/"
        AWS_PATH="s3://pudl.catalyst.coop/$DIST_PATH/"
        # If the old outputs don't exist, these will exit with status 1, so we
        # don't && them like with many of the other commands.
        echo "Removing old outputs from $GCS_PATH."
        gcloud storage rm --quiet --recursive --billing-project="$GCP_BILLING_PROJECT" "$GCS_PATH"
        echo "Removing old outputs from $AWS_PATH."
        gcloud storage rm --quiet --recursive "$AWS_PATH"
    else
        echo "No distribution path provided. Not updating outputs."
        exit 1
    fi
}

function upload_to_dist_path() {
    DIST_PATH=$1
    # Only attempt to update outputs if we have an argument
    # This avoids accidentally blowing away the whole bucket if it's not set.
    if [[ -n "$DIST_PATH" ]]; then
        GCS_PATH="gs://pudl.catalyst.coop/$DIST_PATH/"
        AWS_PATH="s3://pudl.catalyst.coop/$DIST_PATH/"
        # Do not && this command with the others, as it will exit with status 1 if the
        # old outputs don't exist.
        remove_dist_path "$DIST_PATH"
        echo "Copying outputs to $GCS_PATH:" &&
            gcloud storage cp --quiet --recursive --billing-project="$GCP_BILLING_PROJECT" "$PUDL_OUTPUT/*" "$GCS_PATH" &&
            echo "Copying outputs to $AWS_PATH" &&
            gcloud storage cp --quiet --recursive "$PUDL_OUTPUT/*" "$AWS_PATH"
    else
        echo "No distribution path provided. Not updating outputs."
        exit 1
    fi
}

function zenodo_data_release() {
    ZENODO_ENV=$1
    SOURCE_DIR=$2
    IGNORE_REGEX=$3
    PUBLISH=$4

    set +x &&
        echo "Triggerng the zenodo data release workflow using the GitHub API and curl" &&
        curl --fail-with-body -sS -X POST \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${PUDL_BOT_PAT}" \
            https://api.github.com/repos/catalyst-cooperative/pudl/actions/workflows/zenodo-data-release.yml/dispatches \
            -d @<(
                cat <<JSON
{
  "ref": "${BUILD_REF}",
  "inputs": {
    "env": "${ZENODO_ENV}",
    "source_dir": "${SOURCE_DIR}",
    "ignore_regex": "${IGNORE_REGEX}",
    "publish": "${PUBLISH}"
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
    local log_mode=$3
    shift 3

    # Most stages follow the same pattern: run, stream logs to the build log,
    # capture the underlying command status, and record how long the stage took.
    SECONDS=0
    if [[ "$log_mode" == "append" ]]; then
        "$@" 2>&1 | tee -a "$LOGFILE"
    else
        "$@" 2>&1 | tee "$LOGFILE"
    fi

    printf -v "$status_var" '%s' "${PIPESTATUS[0]}"
    set_stage_duration "$duration_var" "$SECONDS"
}

function stage_failed() {
    local stage_status=$1

    [[ "$stage_status" != "$STAGE_SKIPPED" && "$stage_status" != 0 ]]
}

function exit_on_stage_failure() {
    local stage_status=$1

    # Required stages fail fast and send Slack immediately instead of letting later
    # steps continue in a half-broken build environment.
    if stage_failed "$stage_status"; then
        notify_slack "failure"
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

function deploy_data_viewer() {
    set +x &&
        echo "Triggering the eel-hole/data-viewer build-deploy workflow using the GitHub API and curl" &&
        curl --fail-with-body -sS -X POST \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${PUDL_BOT_PAT}" \
            https://api.github.com/repos/catalyst-cooperative/eel-hole/actions/workflows/build-deploy.yml/dispatches \
            -g -d '{"ref":"main"}' &&
        set -x
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
    message+="$(slack_stage_status "Run FERC to SQLite" "$FERC_TO_SQLITE_STATUS" "$FERC_TO_SQLITE_DURATION")\n"
    message+="$(slack_stage_status "Run PUDL ETL" "$PUDL_ETL_STATUS" "$PUDL_ETL_DURATION")\n"
    message+="$(slack_stage_status "Unit Tests" "$UNIT_TEST_STATUS" "$UNIT_TEST_DURATION")\n"
    message+="$(slack_stage_status "Integration Tests" "$INTEGRATION_TEST_STATUS" "$INTEGRATION_TEST_DURATION")\n"
    message+="$(slack_stage_status "Write PUDL Datapackage" "$WRITE_DATAPACKAGE_STATUS" "$WRITE_DATAPACKAGE_DURATION")\n"
    message+="$(slack_stage_status "Save Build Outputs" "$SAVE_OUTPUTS_STATUS" "$SAVE_OUTPUTS_DURATION")\n"
    message+="$(slack_stage_status "Prep Outputs for Distribution" "$CLEAN_UP_OUTPUTS_STATUS" "$CLEAN_UP_OUTPUTS_DURATION")\n"
    message+="$(slack_stage_status "Update \`nightly\` Branch" "$UPDATE_NIGHTLY_STATUS" "$UPDATE_NIGHTLY_DURATION")\n"
    message+="$(slack_stage_status "Update \`stable\` Branch" "$UPDATE_STABLE_STATUS" "$UPDATE_STABLE_DURATION")\n"
    message+="$(slack_stage_status "Distribute Outputs to S3/GCS" "$DISTRIBUTION_BUCKET_STATUS" "$DISTRIBUTION_BUCKET_DURATION")\n"
    message+="$(slack_stage_status "Redeploy PUDL Data Viewer :eel: :hole:" "$TRIGGER_DATA_VIEWER_DEPLOY_STATUS" "$TRIGGER_DATA_VIEWER_DEPLOY_DURATION")\n"
    message+="$(slack_stage_status "Write-protect \`$BUILD_REF\` Outputs on GCS" "$GCS_TEMPORARY_HOLD_STATUS" "$GCS_TEMPORARY_HOLD_DURATION")\n\n"
    # we need to trim off the last dash-delimited section off the build ID to get a valid log link
    message+="<https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/run-etl-${BUILD_ID%-*}/logs?project=catalyst-cooperative-pudl|*Query logs online*>\n\n"
    message+="<https://storage.cloud.google.com/builds.catalyst.coop/$BUILD_ID/$BUILD_ID.log|*Download logs to your computer*>\n\n"
    message+="<https://console.cloud.google.com/storage/browser/builds.catalyst.coop/$BUILD_ID|*Browse full build outputs*>"

    send_slack_msg "$message"
}

function merge_tag_into_branch() {
    TAG=$1
    BRANCH=$2
    git config user.email "pudl@catalyst.coop" &&
        git config user.name "pudlbot" &&
        set +x &&
        echo "Setting authenticated git remote URL using PAT" &&
        git remote set-url origin "https://pudlbot:$PUDL_BOT_PAT@github.com/catalyst-cooperative/pudl.git" &&
        set -x &&
        echo "Updating $BRANCH branch to point at $TAG." &&
        # Check out the original row counts so the working tree is clean.
        # This is a temporary hack around the unstable row-counts in some tables.
        # TODO: fix this for real in issue #4364 / PR #4367
        git checkout -- dbt/seeds/ &&
        git fetch --force --tags origin "$TAG" &&
        git fetch origin "$BRANCH":"$BRANCH" &&
        git checkout "$BRANCH" &&
        git show-ref -d "$BRANCH" "$TAG" &&
        git merge --ff-only "$TAG" &&
        git push -u origin "$BRANCH"
}

function upload_nightly_distribution() {
    # Nightly builds publish both the canonical nightly outputs and the eel-hole copy.
    upload_to_dist_path "nightly" &&
        upload_to_dist_path "eel-hole"
}

function upload_stable_distribution() {
    # Stable releases publish both the versioned path and the rolling stable alias.
    upload_to_dist_path "$BUILD_REF" &&
        upload_to_dist_path "stable"
}

function clean_up_outputs_for_distribution() {
    # Compress the SQLite DBs for easier distribution
    pushd "$PUDL_OUTPUT" &&
        find ./ -maxdepth 1 -type f -name '*.sqlite' -print | parallel --will-cite 'zip -9 "{1}.zip" "{1}"' &&
        rm -f ./*.sqlite &&
        popd &&
        # Create a zip file of all the parquet outputs for distribution on Kaggle
        # Don't try to compress the already compressed Parquet files with Zip.
        pushd "$PUDL_OUTPUT/parquet" &&
        zip -0 "$PUDL_OUTPUT/pudl_parquet.zip" ./*.parquet ./pudl_parquet_datapackage.json &&
        # Move the individual parquet outputs to the output directory for direct access
        mv ./*.parquet "$PUDL_OUTPUT" &&
        # Move the parquet datapackage to the output directory also!
        mv ./pudl_parquet_datapackage.json "$PUDL_OUTPUT" &&
        popd &&
        # Remove any remaiining files and directories we don't want to distribute
        rm -rf "$PUDL_OUTPUT/parquet" &&
        rm -f "$PUDL_OUTPUT/pudl_dbt_tests.duckdb"
}

########################################################################################
# MAIN SCRIPT
########################################################################################
LOGFILE="${PUDL_OUTPUT}/${BUILD_ID}.log"
ZENODO_IGNORE_REGEX="(^.*\\\\.parquet$|^.*pudl_parquet_datapackage\\\\.json$)"
STAGE_SKIPPED="skipped"
BUILD_START_EPOCH_SECONDS=$(date +%s)

# Initialize our stage-status variables so they all definitely have a value to check
FERC_TO_SQLITE_STATUS="$STAGE_SKIPPED"
PUDL_ETL_STATUS="$STAGE_SKIPPED"
UNIT_TEST_STATUS="$STAGE_SKIPPED"
INTEGRATION_TEST_STATUS="$STAGE_SKIPPED"
WRITE_DATAPACKAGE_STATUS="$STAGE_SKIPPED"
SAVE_OUTPUTS_STATUS="$STAGE_SKIPPED"
UPDATE_NIGHTLY_STATUS="$STAGE_SKIPPED"
UPDATE_STABLE_STATUS="$STAGE_SKIPPED"
CLEAN_UP_OUTPUTS_STATUS="$STAGE_SKIPPED"
DISTRIBUTION_BUCKET_STATUS="$STAGE_SKIPPED"
TRIGGER_DATA_VIEWER_DEPLOY_STATUS="$STAGE_SKIPPED"
GCS_TEMPORARY_HOLD_STATUS="$STAGE_SKIPPED"

FERC_TO_SQLITE_DURATION=""
PUDL_ETL_DURATION=""
UNIT_TEST_DURATION=""
INTEGRATION_TEST_DURATION=""
WRITE_DATAPACKAGE_DURATION=""
SAVE_OUTPUTS_DURATION=""
UPDATE_NIGHTLY_DURATION=""
UPDATE_STABLE_DURATION=""
CLEAN_UP_OUTPUTS_DURATION=""
DISTRIBUTION_BUCKET_DURATION=""
TRIGGER_DATA_VIEWER_DEPLOY_DURATION=""
GCS_TEMPORARY_HOLD_DURATION=""

# Set the build type based on the action trigger and tag
if [[ "$GITHUB_ACTION_TRIGGER" == "push" && "$BUILD_REF" =~ ^v20.*$ ]]; then
    BUILD_TYPE="stable"
elif [[ "$GITHUB_ACTION_TRIGGER" == "schedule" ]]; then
    BUILD_TYPE="nightly"
elif [[ "$GITHUB_ACTION_TRIGGER" == "workflow_dispatch" ]]; then
    BUILD_TYPE="workflow_dispatch"
else
    echo "Unknown build type, exiting!"
    echo "GITHUB_ACTION_TRIGGER: $GITHUB_ACTION_TRIGGER"
    echo "BUILD_REF: $BUILD_REF"
    exit 1
fi

# Set these variables *only* if they are not already set by the container or workflow:
: "${PUDL_GCS_OUTPUT:=gs://builds.catalyst.coop/$BUILD_ID}"
: "${PUDL_SETTINGS_YML:=/home/ubuntu/pudl/src/pudl/package_data/settings/etl_full.yml}"

# Save credentials for working with AWS S3
# set +x / set -x is used to avoid printing the AWS credentials in the logs
echo "Setting AWS credentials"
mkdir -p ~/.aws
echo "[default]" >~/.aws/credentials
set +x
echo "aws_access_key_id = ${AWS_ACCESS_KEY_ID}" >>~/.aws/credentials
echo "aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}" >>~/.aws/credentials
set -x

run_stage FERC_TO_SQLITE_STATUS FERC_TO_SQLITE_DURATION overwrite run_ferc_to_sqlite
run_stage PUDL_ETL_STATUS PUDL_ETL_DURATION append run_pudl_etl
run_stage UNIT_TEST_STATUS UNIT_TEST_DURATION append run_unit_tests
run_stage INTEGRATION_TEST_STATUS INTEGRATION_TEST_DURATION append run_integration_tests

if ! any_stage_failed \
    "$FERC_TO_SQLITE_STATUS" \
    "$PUDL_ETL_STATUS" \
    "$UNIT_TEST_STATUS" \
    "$INTEGRATION_TEST_STATUS"; then
    touch "$PUDL_OUTPUT/success"
fi

# Write out a datapackage.json for external consumption
run_stage WRITE_DATAPACKAGE_STATUS WRITE_DATAPACKAGE_DURATION append write_pudl_datapackage

# Generate new row counts for all tables in the PUDL database
dbt_helper update-tables --clobber --row-counts all 2>&1 | tee -a "$LOGFILE"

# This needs to happen regardless of the ETL outcome:
pg_ctlcluster "$PG_VERSION" dagster stop 2>&1 | tee -a "$LOGFILE"

run_stage SAVE_OUTPUTS_STATUS SAVE_OUTPUTS_DURATION append save_outputs_to_gcs

exit_on_stage_failure "$FERC_TO_SQLITE_STATUS"
exit_on_stage_failure "$PUDL_ETL_STATUS"
exit_on_stage_failure "$UNIT_TEST_STATUS"
exit_on_stage_failure "$INTEGRATION_TEST_STATUS"

if [[ "$BUILD_TYPE" == "nightly" ]]; then
    run_stage UPDATE_NIGHTLY_STATUS UPDATE_NIGHTLY_DURATION append merge_tag_into_branch "$NIGHTLY_TAG" nightly
    # Remove files we don't want to distribute and zip SQLite and Parquet outputs
    run_stage CLEAN_UP_OUTPUTS_STATUS CLEAN_UP_OUTPUTS_DURATION append clean_up_outputs_for_distribution
    exit_on_stage_failure "$CLEAN_UP_OUTPUTS_STATUS"
    # Copy cleaned up outputs to the S3 and GCS distribution buckets
    run_stage DISTRIBUTION_BUCKET_STATUS DISTRIBUTION_BUCKET_DURATION append upload_nightly_distribution
    run_stage TRIGGER_DATA_VIEWER_DEPLOY_STATUS TRIGGER_DATA_VIEWER_DEPLOY_DURATION append deploy_data_viewer
    if ! stage_failed "$DISTRIBUTION_BUCKET_STATUS"; then
        zenodo_data_release \
            "sandbox" \
            "s3://pudl.catalyst.coop/nightly/" \
            "${ZENODO_IGNORE_REGEX}" \
            "publish" 2>&1 | tee -a "$LOGFILE"
    fi

elif [[ "$BUILD_TYPE" == "stable" ]]; then
    run_stage UPDATE_STABLE_STATUS UPDATE_STABLE_DURATION append merge_tag_into_branch "$BUILD_REF" stable
    # Remove files we don't want to distribute and zip SQLite and Parquet outputs
    run_stage CLEAN_UP_OUTPUTS_STATUS CLEAN_UP_OUTPUTS_DURATION append clean_up_outputs_for_distribution
    exit_on_stage_failure "$CLEAN_UP_OUTPUTS_STATUS"
    # Copy cleaned up outputs to the S3 and GCS distribution buckets
    run_stage DISTRIBUTION_BUCKET_STATUS DISTRIBUTION_BUCKET_DURATION append upload_stable_distribution
    # This is a versioned release. Ensure that outputs can't be accidentally deleted.
    # We can only do this on the GCS bucket, not S3
    run_stage GCS_TEMPORARY_HOLD_STATUS GCS_TEMPORARY_HOLD_DURATION append \
        gcloud storage --billing-project="$GCP_BILLING_PROJECT" objects update "gs://pudl.catalyst.coop/$BUILD_REF/*" --temporary-hold
    if ! stage_failed "$DISTRIBUTION_BUCKET_STATUS"; then
        zenodo_data_release \
            "production" \
            "s3://pudl.catalyst.coop/${BUILD_REF}/" \
            "${ZENODO_IGNORE_REGEX}" \
            "no-publish" 2>&1 | tee -a "$LOGFILE"
    fi

elif [[ "$BUILD_TYPE" == "workflow_dispatch" ]]; then
    # Remove files we don't want to distribute and zip SQLite and Parquet outputs
    run_stage CLEAN_UP_OUTPUTS_STATUS CLEAN_UP_OUTPUTS_DURATION append clean_up_outputs_for_distribution
    exit_on_stage_failure "$CLEAN_UP_OUTPUTS_STATUS"

    # Disable the test upload to the distribution bucket for now to avoid egress fees
    # and speed up the build. Uncomment if you need to test the distribution upload.
    # Upload to GCS / S3 just to test that it works.
    # upload_to_dist_path "$BUILD_ID" | tee -a "$LOGFILE"
    # DISTRIBUTION_BUCKET_STATUS=${PIPESTATUS[0]}
    # Remove those uploads since they were just for testing.
    # remove_dist_path "$BUILD_ID" | tee -a "$LOGFILE"

    # NOTE: because we remove the test uploads and the zenodo data release workflow
    # runs independent of the nightly build (it is not blocking), the workflow
    # has no build-specific outputs to publish. It just attempts to republish the
    # nightly outputs from s3://pudl.catalyst.coop/nightly/ to make sure that the
    # process is working.
    if ! stage_failed "$DISTRIBUTION_BUCKET_STATUS"; then
        zenodo_data_release \
            "sandbox" \
            "s3://pudl.catalyst.coop/nightly/" \
            "${ZENODO_IGNORE_REGEX}" \
            "publish" 2>&1 | tee -a "$LOGFILE"
    fi

else
    echo "Unknown build type, exiting!"
    echo "BUILD_TYPE: $BUILD_TYPE"
    echo "GITHUB_ACTION_TRIGGER: $GITHUB_ACTION_TRIGGER"
    echo "BUILD_REF: $BUILD_REF"
    notify_slack "failure"
    exit 1
fi

# This way we also save the logs from latter steps in the script
gcloud storage --quiet cp "$LOGFILE" "$PUDL_GCS_OUTPUT"
# Remove the AWS credentials file just in case the disk image sticks around
rm -f ~/.aws/credentials

# Notify slack about entire pipeline's success or failure;
if ! any_stage_failed \
    "$FERC_TO_SQLITE_STATUS" \
    "$PUDL_ETL_STATUS" \
    "$UNIT_TEST_STATUS" \
    "$INTEGRATION_TEST_STATUS" \
    "$WRITE_DATAPACKAGE_STATUS" \
    "$SAVE_OUTPUTS_STATUS" \
    "$UPDATE_NIGHTLY_STATUS" \
    "$UPDATE_STABLE_STATUS" \
    "$CLEAN_UP_OUTPUTS_STATUS" \
    "$DISTRIBUTION_BUCKET_STATUS" \
    "$TRIGGER_DATA_VIEWER_DEPLOY_STATUS" \
    "$GCS_TEMPORARY_HOLD_STATUS"; then
    notify_slack "success"
else
    notify_slack "failure"
    exit 1
fi
