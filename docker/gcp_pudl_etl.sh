#!/usr/bin/bash
# This script runs the entire ETL and validation tests in a docker container on a Google Compute Engine instance.
# This script won't work locally because it needs adequate GCP permissions.

LOGFILE="${PUDL_OUTPUT}/${BUILD_ID}-pudl-etl.log"

function send_slack_msg() {
    echo "sending Slack message"
    curl -X POST -H "Content-type: application/json" -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/chat.postMessage --data "{\"channel\": \"C03FHB9N0PQ\", \"text\": \"$1\"}"
}

function upload_file_to_slack() {
    echo "Uploading file to slack with comment $2"
    curl -F "file=@$1" -F "initial_comment=$2" -F channels=C03FHB9N0PQ -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/files.upload
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
    #
    # When the PG major version changes we'll have to update this from 15 to 16
    pg_ctlcluster 15 dagster start && \
    createdb -h127.0.0.1 -p5433 && \
    psql -v "ON_ERROR_STOP=1" -h127.0.0.1 -p5433 && \
    psql -c "CREATE USER dagster WITH SUPERUSER PASSWORD 'dagster_password'" -h127.0.0.1 -p5433 && \
    psql -c "CREATE DATABASE dagster OWNER dagster" -h127.0.0.1 -p5433
}

function run_pudl_etl() {
    echo "Running PUDL ETL"
    send_slack_msg ":large_yellow_circle: Deployment started for $BUILD_ID :floppy_disk:"
    initialize_postgres && \
    authenticate_gcp && \
    alembic upgrade head && \
    ferc_to_sqlite \
        --loglevel DEBUG \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        --workers 8 \
        "$PUDL_SETTINGS_YML" \
    && pudl_etl \
        --loglevel DEBUG \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        "$PUDL_SETTINGS_YML" \
    && pytest \
        -n auto \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        --etl-settings "$PUDL_SETTINGS_YML" \
        --live-dbs test/integration test/unit \
        --no-cov \
    && pytest \
        -n auto \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        --etl-settings "$PUDL_SETTINGS_YML" \
        --live-dbs test/validate \
        --no-cov \
    && touch "$PUDL_OUTPUT/success"
}

function save_outputs_to_gcs() {
    echo "Copying outputs to GCP bucket $PUDL_GCS_OUTPUT" && \
    gsutil -q -m cp -r "$PUDL_OUTPUT" "$PUDL_GCS_OUTPUT" && \
    rm -f "$PUDL_OUTPUT/success"
}

function upload_to_dist_path() {
    GCS_PATH="gs://pudl.catalyst.coop/$1/"
    AWS_PATH="s3://pudl.catalyst.coop/$1/"

    # Only attempt to update outputs if we have an argument
    # This avoids accidentally blowing away the whole bucket if it's not set.
    if [[ -n "$1" ]]; then
        # If the old outputs don't exist, these will exit with status 1, so we
        # don't && them with the rest of the commands.
        echo "Removing old outputs from $GCS_PATH."
        gsutil -q -m -u "$GCP_BILLING_PROJECT" rm -r "$GCS_PATH"
        echo "Removing old outputs from $AWS_PATH."
        aws s3 rm --quiet --recursive "$AWS_PATH"

        echo "Copying outputs to $GCS_PATH:" && \
        gsutil -q -m -u "$GCP_BILLING_PROJECT" cp -r "$PUDL_OUTPUT/*" "$GCS_PATH" && \
        echo "Copying outputs to $AWS_PATH" && \
        aws s3 cp --quiet --recursive "$PUDL_OUTPUT/" "$AWS_PATH"
    else
        echo "No distribution path provided. Not updating outputs."
        exit 1
    fi
}

function distribute_parquet() {
    PARQUET_BUCKET="gs://parquet.catalyst.coop"
    # Only attempt to update outputs if we have a real value of BUILD_REF
    # This avoids accidentally blowing away the whole bucket if it's not set.
    echo "Copying outputs to parquet distribution bucket"
    if [[ -n "$BUILD_REF" ]]; then
        if [[ "$GITHUB_ACTION_TRIGGER" == "schedule" ]]; then
            # If running nightly builds, copy outputs to the "nightly" bucket path
            DIST_PATH="nightly"
        else
            # Otherwise we want to copy them to a directory named after the tag/ref
            DIST_PATH="$BUILD_REF"
        fi
        echo "Copying outputs to $PARQUET_BUCKET/$DIST_PATH" && \
        gsutil -q -m -u "$GCP_BILLING_PROJECT" cp -r "$PUDL_OUTPUT/parquet/*" "$PARQUET_BUCKET/$DIST_PATH"

        # If running a tagged release, ALSO update the stable distribution bucket path:
        if [[ "$GITHUB_ACTION_TRIGGER" == "push" && "$BUILD_REF" == v20* ]]; then
            echo "Copying outputs to $PARQUET_BUCKET/stable" && \
            gsutil -q -m -u "$GCP_BILLING_PROJECT" cp -r "$PUDL_OUTPUT/parquet/*" "$PARQUET_BUCKET/stable"
        fi
    fi
}

function copy_outputs_to_distribution_bucket() {
    # Only attempt to update outputs if we have a real value of BUILD_REF
    # This avoids accidentally blowing away the whole bucket if it's not set.
    echo "Copying outputs to distribution buckets"
    if [[ -n "$BUILD_REF" ]]; then
        if [[ "$GITHUB_ACTION_TRIGGER" == "schedule" ]]; then
            # If running nightly builds, copy outputs to the "nightly" bucket path
            DIST_PATH="nightly"
        else
            # Otherwise we want to copy them to a directory named after the tag/ref
            DIST_PATH="$BUILD_REF"
        fi
        upload_to_dist_path "$DIST_PATH"

        # If running a tagged release, ALSO update the stable distribution bucket path:
        if [[ "$GITHUB_ACTION_TRIGGER" == "push" && "$BUILD_REF" == v20* ]]; then
            upload_to_dist_path "stable"
        fi
    fi
}

function zenodo_data_release() {
    echo "Creating a new PUDL data release on Zenodo."

    if [[ "$1" == "production" ]]; then
        ~/pudl/devtools/zenodo/zenodo_data_release.py --no-publish --env "$1" --source-dir "$PUDL_OUTPUT"
    else
        ~/pudl/devtools/zenodo/zenodo_data_release.py --publish --env "$1" --source-dir "$PUDL_OUTPUT"
    fi
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
    message+="SAVE_OUTPUTS_SUCCESS: $SAVE_OUTPUTS_SUCCESS\n"
    message+="UPDATE_NIGHTLY_SUCCESS: $UPDATE_NIGHTLY_SUCCESS\n"
    message+="UPDATE_STABLE_SUCCESS: $UPDATE_STABLE_SUCCESS\n"
    message+="DATASETTE_SUCCESS: $DATASETTE_SUCCESS\n"
    message+="CLEAN_UP_OUTPUTS_SUCCESS: $CLEAN_UP_OUTPUTS_SUCCESS\n"
    message+="DISTRIBUTION_BUCKET_SUCCESS: $DISTRIBUTION_BUCKET_SUCCESS\n"
    message+="GCLOUD_TEMPORARY_HOLD_SUCCESS: $GCLOUD_TEMPORARY_HOLD_SUCCESS \n"
    message+="ZENODO_SUCCESS: $ZENODO_SUCCESS\n\n"

    message+="*Query* logs on <https://console.cloud.google.com/batch/jobsDetail/regions/us-west1/jobs/run-etl-$BUILD_ID/logs?project=catalyst-cooperative-pudl|Google Batch Console>.\n\n"

    message+="*Download* logs at <https://console.cloud.google.com/storage/browser/_details/builds.catalyst.coop/$BUILD_ID/$BUILD_ID-pudl-etl.log|gs://builds.catalyst.coop/${BUILD_ID}/${BUILD_ID}-pudl-etl.log>\n\n"

    message+="Get *full outputs* at <https://console.cloud.google.com/storage/browser/builds.catalyst.coop/$BUILD_ID|gs://builds.catalyst.coop/${BUILD_ID}>."

    send_slack_msg "$message"
    upload_file_to_slack "$LOGFILE" "$BUILD_ID logs:"
}

function merge_tag_into_branch() {
    TAG=$1
    BRANCH=$2
    # When building the image, GHA adds an HTTP basic auth header in git
    # config, which overrides the auth we set below. So we unset it.
    git config --unset http.https://github.com/.extraheader && \
    git config user.email "pudl@catalyst.coop" && \
    git config user.name "pudlbot" && \
    git remote set-url origin "https://pudlbot:$PUDL_BOT_PAT@github.com/catalyst-cooperative/pudl.git" && \
    echo "Updating $BRANCH branch to point at $TAG." && \
    git fetch --force --tags origin "$TAG" && \
    git fetch origin "$BRANCH":"$BRANCH" && \
    git checkout "$BRANCH" && \
    git show-ref -d "$BRANCH" "$TAG" && \
    git merge --ff-only "$TAG" && \
    git push -u origin "$BRANCH"
}

function clean_up_outputs_for_distribution() {
    # Compress the SQLite DBs for easier distribution
    gzip --verbose "$PUDL_OUTPUT"/*.sqlite && \
    # Grab the consolidated EPA CEMS outputs for distribution
    cp "$PUDL_OUTPUT/parquet/core_epacems__hourly_emissions.parquet" "$PUDL_OUTPUT" && \
    # Remove all other parquet output, which we are not yet distributing.
    rm -rf "$PUDL_OUTPUT/parquet" && \
    rm -f "$PUDL_OUTPUT/metadata.yml"
}

########################################################################################
# MAIN SCRIPT
########################################################################################
# Initialize our success variables so they all definitely have a value to check
ETL_SUCCESS=0
SAVE_OUTPUTS_SUCCESS=0
UPDATE_NIGHTLY_SUCCESS=0
UPDATE_STABLE_SUCCESS=0
DATASETTE_SUCCESS=0
DISTRIBUTE_PARQUET_SUCCESS=0
CLEAN_UP_OUTPUTS_SUCCESS=0
DISTRIBUTION_BUCKET_SUCCESS=0
ZENODO_SUCCESS=0
GCLOUD_TEMPORARY_HOLD_SUCCESS=0

# Set these variables *only* if they are not already set by the container or workflow:
: "${PUDL_GCS_OUTPUT:=gs://builds.catalyst.coop/$BUILD_ID}"
: "${PUDL_SETTINGS_YML:=home/mambauser/pudl/src/pudl/package_data/settings/etl_full.yml}"

# Run ETL. Copy outputs to GCS and shutdown VM if ETL succeeds or fails
# 2>&1 redirects stderr to stdout.
run_pudl_etl 2>&1 | tee "$LOGFILE"
ETL_SUCCESS=${PIPESTATUS[0]}

# This needs to happen regardless of the ETL outcome:
pg_ctlcluster 15 dagster stop 2>&1 | tee -a "$LOGFILE"

save_outputs_to_gcs 2>&1 | tee -a "$LOGFILE"
SAVE_OUTPUTS_SUCCESS=${PIPESTATUS[0]}

# if pipeline is successful, distribute + publish datasette
if [[ $ETL_SUCCESS == 0 ]]; then
    if [[ "$GITHUB_ACTION_TRIGGER" == "schedule" ]]; then
        merge_tag_into_branch "$NIGHTLY_TAG" nightly 2>&1 | tee -a "$LOGFILE"
        UPDATE_NIGHTLY_SUCCESS=${PIPESTATUS[0]}
    fi
    # If running a tagged release, merge the tag into the stable branch
    if [[ "$GITHUB_ACTION_TRIGGER" == "push" && "$BUILD_REF" == v20* ]]; then
        merge_tag_into_branch "$BUILD_REF" stable 2>&1 | tee -a "$LOGFILE"
        UPDATE_STABLE_SUCCESS=${PIPESTATUS[0]}
    fi

    # Deploy the updated data to datasette if we're on main
    if [[ "$BUILD_REF" == "main" ]]; then
        python ~/pudl/devtools/datasette/publish.py 2>&1 | tee -a "$LOGFILE"
        DATASETTE_SUCCESS=${PIPESTATUS[0]}
    fi

    # TODO: this behavior should be controlled by on/off switch here and this logic
    # should be moved to the triggering github action. Having it here feels fragmented.
    # Distribute outputs if branch is main or the build was triggered by tag push
    if [[ "$GITHUB_ACTION_TRIGGER" == "push" || "$BUILD_REF" == "main" ]]; then
        # Distribute Parquet outputs to a private bucket
        distribute_parquet 2>&1 | tee -a "$LOGFILE"
        DISTRIBUTE_PARQUET_SUCCESS=${PIPESTATUS[0]}
        # Remove some cruft from the builds that we don't want to distribute
        clean_up_outputs_for_distribution 2>&1 | tee -a "$LOGFILE"
        CLEAN_UP_OUTPUTS_SUCCESS=${PIPESTATUS[0]}
        # Copy cleaned up outputs to the S3 and GCS distribution buckets
        copy_outputs_to_distribution_bucket | tee -a "$LOGFILE"
        DISTRIBUTION_BUCKET_SUCCESS=${PIPESTATUS[0]}
        # TODO: this currently just makes a sandbox release, for testing. Should be
        # switched to production and only run on push of a version tag eventually.
        # Push a data release to Zenodo for long term accessiblity
        zenodo_data_release "$ZENODO_TARGET_ENV" 2>&1 | tee -a "$LOGFILE"
        ZENODO_SUCCESS=${PIPESTATUS[0]}
    fi
    # If running a tagged release, ensure that outputs can't be accidentally deleted
    # It's not clear that an object lock can be applied in S3 with the AWS CLI
    if [[ "$GITHUB_ACTION_TRIGGER" == "push" && "$BUILD_REF" == v20* ]]; then
        gcloud storage objects update "gs://pudl.catalyst.coop/$BUILD_REF/*" --temporary-hold 2>&1 | tee -a "$LOGFILE"
        GCLOUD_TEMPORARY_HOLD_SUCCESS=${PIPESTATUS[0]}
    fi
fi

# This way we also save the logs from latter steps in the script
gsutil -q cp "$LOGFILE" "$PUDL_GCS_OUTPUT"

# Notify slack about entire pipeline's success or failure;
if [[ $ETL_SUCCESS == 0 && \
      $SAVE_OUTPUTS_SUCCESS == 0 && \
      $UPDATE_NIGHTLY_SUCCESS == 0 && \
      $UPDATE_STABLE_SUCCESS == 0 && \
      $DATASETTE_SUCCESS == 0 && \
      $DISTRIBUTE_PARQUET_SUCCESS == 0 && \
      $CLEAN_UP_OUTPUTS_SUCCESS == 0 && \
      $DISTRIBUTION_BUCKET_SUCCESS == 0 && \
      $GCLOUD_TEMPORARY_HOLD_SUCCESS == 0 && \
      $ZENODO_SUCCESS == 0
]]; then
    notify_slack "success"
else
    notify_slack "failure"
    exit 1
fi
