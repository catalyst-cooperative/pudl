#!/usr/bin/bash
# This script runs the entire ETL and validation tests in a docker container on a Google Compute Engine instance.
# This script won't work locally because it needs adequate GCP permissions.

function send_slack_msg() {
    set +x &&
        echo "sending Slack message" &&
        curl -X POST -H "Content-type: application/json" -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/chat.postMessage --data "{\"channel\": \"C03FHB9N0PQ\", \"text\": \"$1\"}" &&
        set -x
}

function authenticate_gcp() {
    # Set the default gcloud project id so the zenodo-cache bucket
    # knows what project to bill for egress
    echo "Authenticating to GCP"
    gcloud config set project "$GCP_BILLING_PROJECT"
}

function initialize_postgres() {
    echo "initializing postgres."
    # This is sort of a fiddly set of postgres admin tasks:
    #
    # 1. start the dagster cluster, which is set to be owned by mambauser in the Dockerfile
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

function run_pudl_etl() {
    echo "Running PUDL ETL"
    send_slack_msg ":large_yellow_circle: Deployment started for $BUILD_ID :floppy_disk:"
    initialize_postgres &&
        authenticate_gcp &&
        alembic upgrade head &&
        ferc_to_sqlite \
            --loglevel DEBUG \
            --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
            --workers 8 \
            "$PUDL_SETTINGS_YML" &&
        pudl_etl \
            --loglevel DEBUG \
            --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
            "$PUDL_SETTINGS_YML" &&
        pytest \
            -n auto \
            --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
            --etl-settings "$PUDL_SETTINGS_YML" \
            --live-dbs test/integration test/unit \
            --no-cov &&
        touch "$PUDL_OUTPUT/success"
}

function write_pudl_datapackage() {
    echo "Writing PUDL datapackage."
    python -c "from pudl.metadata.classes import PUDL_PACKAGE; print(PUDL_PACKAGE.to_frictionless().to_json())" >"$PUDL_OUTPUT/parquet/pudl_parquet_datapackage.json"
    return $?
}

function save_outputs_to_gcs() {
    echo "Copying outputs to GCP bucket $PUDL_GCS_OUTPUT" &&
        gcloud storage --quiet cp -r "$PUDL_OUTPUT" "$PUDL_GCS_OUTPUT" &&
        gcloud storage --quiet cp -r "$PUDL_REPO"/dbt/seeds/etl_full_row_counts.csv "$PUDL_GCS_OUTPUT" &&
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
        curl -sS -X POST \
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

function notify_slack() {
    # Notify pudl-deployment slack channel of deployment status
    echo "Notifying Slack about deployment status"
    message="${BUILD_ID} status\n\n"
    if [[ "$1" == "success" ]]; then
        message+=":large_green_circle: :sunglasses: :unicorn_face: :rainbow: deployment succeeded!! :partygritty: :database_parrot: :blob-dance: :large_green_circle:\n\n"
    elif [[ "$1" == "failure" ]]; then
        message+=":x: Oh bummer the deployment failed :fiiiiine: :sob: :cry_spin: :x:\n\n"
    else
        echo "Invalid deployment status"
        exit 1
    fi

    message+="Stage status (0 means success):\n"
    message+="ETL_SUCCESS: $ETL_SUCCESS\n"
    message+="WRITE_DATAPACKAGE_SUCCESS: $WRITE_DATAPACKAGE_SUCCESS\n"
    message+="SAVE_OUTPUTS_SUCCESS: $SAVE_OUTPUTS_SUCCESS\n"
    message+="UPDATE_NIGHTLY_SUCCESS: $UPDATE_NIGHTLY_SUCCESS\n"
    message+="UPDATE_STABLE_SUCCESS: $UPDATE_STABLE_SUCCESS\n"
    message+="CLEAN_UP_OUTPUTS_SUCCESS: $CLEAN_UP_OUTPUTS_SUCCESS\n"
    message+="DISTRIBUTION_BUCKET_SUCCESS: $DISTRIBUTION_BUCKET_SUCCESS\n"
    message+="DEPLOY_EEL_HOLE_SUCCESS: $DEPLOY_EEL_HOLE_SUCCESS\n"
    message+="GCS_TEMPORARY_HOLD_SUCCESS: $GCS_TEMPORARY_HOLD_SUCCESS \n"
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

function clean_up_outputs_for_distribution() {
    # Compress the SQLite DBs for easier distribution
    pushd "$PUDL_OUTPUT" &&
        find ./ -maxdepth 1 -type f -name '*.sqlite' -print | parallel --will-cite 'zip -9 "{1}.zip" "{1}"' &&
        rm -f ./*.sqlite &&
        popd &&
        # Create a zip file of all the parquet outputs for distribution on Kaggle
        # Don't try to compress the already compressed Parquet files with Zip.
        pushd "$PUDL_OUTPUT/parquet" &&
        # Don't distribute the raw parquet files
        rm ./raw_*.parquet &&
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

# Initialize our success variables so they all definitely have a value to check
ETL_SUCCESS=0
SAVE_OUTPUTS_SUCCESS=0
UPDATE_NIGHTLY_SUCCESS=0
UPDATE_STABLE_SUCCESS=0
WRITE_DATAPACKAGE_SUCCESS=0
CLEAN_UP_OUTPUTS_SUCCESS=0
DISTRIBUTION_BUCKET_SUCCESS=0
DEPLOY_EEL_HOLE_SUCCESS=0
GCS_TEMPORARY_HOLD_SUCCESS=0

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
: "${PUDL_SETTINGS_YML:=/home/mambauser/pudl/src/pudl/package_data/settings/etl_full.yml}"

# Save credentials for working with AWS S3
# set +x / set -x is used to avoid printing the AWS credentials in the logs
echo "Setting AWS credentials"
mkdir -p ~/.aws
echo "[default]" >~/.aws/credentials
set +x
echo "aws_access_key_id = ${AWS_ACCESS_KEY_ID}" >>~/.aws/credentials
echo "aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}" >>~/.aws/credentials
set -x

# Run ETL. Copy outputs to GCS and shutdown VM if ETL succeeds or fails
# 2>&1 redirects stderr to stdout.
run_pudl_etl 2>&1 | tee "$LOGFILE"
ETL_SUCCESS=${PIPESTATUS[0]}

# Write out a datapackage.json for external consumption
write_pudl_datapackage 2>&1 | tee -a "$LOGFILE"
WRITE_DATAPACKAGE_SUCCESS=${PIPESTATUS[0]}

# Generate new row counts for all tables in the PUDL database
dbt_helper update-tables --clobber --row-counts all 2>&1 | tee -a "$LOGFILE"

# This needs to happen regardless of the ETL outcome:
pg_ctlcluster "$PG_VERSION" dagster stop 2>&1 | tee -a "$LOGFILE"

save_outputs_to_gcs 2>&1 | tee -a "$LOGFILE"
SAVE_OUTPUTS_SUCCESS=${PIPESTATUS[0]}

if [[ $ETL_SUCCESS != 0 ]]; then
    notify_slack "failure"
    exit 1
fi

if [[ "$BUILD_TYPE" == "nightly" ]]; then
    merge_tag_into_branch "$NIGHTLY_TAG" nightly 2>&1 | tee -a "$LOGFILE"
    UPDATE_NIGHTLY_SUCCESS=${PIPESTATUS[0]}
    # Remove files we don't want to distribute and zip SQLite and Parquet outputs
    clean_up_outputs_for_distribution 2>&1 | tee -a "$LOGFILE"
    CLEAN_UP_OUTPUTS_SUCCESS=${PIPESTATUS[0]}
    # Copy cleaned up outputs to the S3 and GCS distribution buckets
    upload_to_dist_path "nightly" | tee -a "$LOGFILE" &&
        upload_to_dist_path "eel-hole" | tee -a "$LOGFILE"
    DISTRIBUTION_BUCKET_SUCCESS=${PIPESTATUS[0]}
    gcloud run services update pudl-viewer --image us-east1-docker.pkg.dev/catalyst-cooperative-pudl/pudl-viewer/pudl-viewer:latest --region us-east1 | tee -a "$LOGFILE"
    DEPLOY_EEL_HOLE_SUCCESS=${PIPESTATUS[0]}
    if [[ $DISTRIBUTION_BUCKET_SUCCESS == 0 ]]; then
        zenodo_data_release \
            "sandbox" \
            "s3://pudl.catalyst.coop/nightly/" \
            "${ZENODO_IGNORE_REGEX}" \
            "publish" 2>&1 | tee -a "$LOGFILE"
    fi

elif [[ "$BUILD_TYPE" == "stable" ]]; then
    merge_tag_into_branch "$BUILD_REF" stable 2>&1 | tee -a "$LOGFILE"
    UPDATE_STABLE_SUCCESS=${PIPESTATUS[0]}
    # Remove files we don't want to distribute and zip SQLite and Parquet outputs
    clean_up_outputs_for_distribution 2>&1 | tee -a "$LOGFILE"
    CLEAN_UP_OUTPUTS_SUCCESS=${PIPESTATUS[0]}
    # Copy cleaned up outputs to the S3 and GCS distribution buckets
    upload_to_dist_path "$BUILD_REF" | tee -a "$LOGFILE" &&
        upload_to_dist_path "stable" | tee -a "$LOGFILE"
    DISTRIBUTION_BUCKET_SUCCESS=${PIPESTATUS[0]}
    # This is a versioned release. Ensure that outputs can't be accidentally deleted.
    # We can only do this on the GCS bucket, not S3
    gcloud storage --billing-project="$GCP_BILLING_PROJECT" objects update "gs://pudl.catalyst.coop/$BUILD_REF/*" --temporary-hold 2>&1 | tee -a "$LOGFILE"
    GCS_TEMPORARY_HOLD_SUCCESS=${PIPESTATUS[0]}
    if [[ $DISTRIBUTION_BUCKET_SUCCESS == 0 ]]; then
        zenodo_data_release \
            "production" \
            "s3://pudl.catalyst.coop/${BUILD_REF}/" \
            "${ZENODO_IGNORE_REGEX}" \
            "no-publish" 2>&1 | tee -a "$LOGFILE"
    fi

elif [[ "$BUILD_TYPE" == "workflow_dispatch" ]]; then
    # Remove files we don't want to distribute and zip SQLite and Parquet outputs
    clean_up_outputs_for_distribution 2>&1 | tee -a "$LOGFILE"
    CLEAN_UP_OUTPUTS_SUCCESS=${PIPESTATUS[0]}

    # Disable the test upload to the distribution bucket for now to avoid egress fees
    # and speed up the build. Uncomment if you need to test the distribution upload.
    # Upload to GCS / S3 just to test that it works.
    # upload_to_dist_path "$BUILD_ID" | tee -a "$LOGFILE"
    # DISTRIBUTION_BUCKET_SUCCESS=${PIPESTATUS[0]}
    # Remove those uploads since they were just for testing.
    # remove_dist_path "$BUILD_ID" | tee -a "$LOGFILE"

    # NOTE: because we remove the test uploads and the zenodo data release workflow
    # runs independent of the nightly build (it is not blocking), the workflow
    # has no build-specific outputs to publish. It just attempts to republish the
    # nightly outputs from s3://pudl.catalyst.coop/nightly/ to make sure that the
    # process is working.
    if [[ $DISTRIBUTION_BUCKET_SUCCESS == 0 ]]; then
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
if [[ $ETL_SUCCESS == 0 &&
    $SAVE_OUTPUTS_SUCCESS == 0 &&
    $UPDATE_NIGHTLY_SUCCESS == 0 &&
    $UPDATE_STABLE_SUCCESS == 0 &&
    $CLEAN_UP_OUTPUTS_SUCCESS == 0 &&
    $DISTRIBUTION_BUCKET_SUCCESS == 0 &&
    $DEPLOY_EEL_HOLE_SUCCESS == 0 &&
    $GCS_TEMPORARY_HOLD_SUCCESS == 0 ]] \
    ; then
    notify_slack "success"
else
    notify_slack "failure"
    exit 1
fi
