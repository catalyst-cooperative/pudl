#!/usr/bin/bash
# This script runs the entire ETL and validation tests in a docker container on a Google Compute Engine instance.
# This script won't work locally because it needs adequate GCP permissions.

# Set PUDL_GCS_OUTPUT *only* if it is currently unset
: "${PUDL_GCS_OUTPUT:=gs://nightly-build-outputs.catalyst.coop/$BUILD_ID}"

set -x

function send_slack_msg() {
    curl -X POST -H "Content-type: application/json" -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/chat.postMessage --data "{\"channel\": \"C03FHB9N0PQ\", \"text\": \"$1\"}"
}

function upload_file_to_slack() {
    curl -F "file=@$1" -F "initial_comment=$2" -F channels=C03FHB9N0PQ -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/files.upload
}

function authenticate_gcp() {
    # Set the default gcloud project id so the zenodo-cache bucket
    # knows what project to bill for egress
    gcloud config set project "$GCP_BILLING_PROJECT"
}

function run_pudl_etl() {
    send_slack_msg ":large_yellow_circle: Deployment started for $BUILD_ID :floppy_disk:"
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
    && pytest \
        -n auto \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        --etl-settings "$PUDL_SETTINGS_YML" \
        --live-dbs test/validate \
    && touch "$PUDL_OUTPUT/success"
}

function shutdown_vm() {
    upload_file_to_slack "$LOGFILE" "pudl_etl logs for $BUILD_ID:"
    # Shut down the vm instance when the etl is done.
    echo "Shutting down VM."
    ACCESS_TOKEN=$(curl \
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        -H "Metadata-Flavor: Google" | jq -r '.access_token')
    curl -X POST -H "Content-Length: 0" -H "Authorization: Bearer ${ACCESS_TOKEN}" "https://compute.googleapis.com/compute/v1/projects/catalyst-cooperative-pudl/zones/$GCE_INSTANCE_ZONE/instances/$GCE_INSTANCE/stop"
}

function copy_outputs_to_gcs() {
    echo "Copying outputs to GCP bucket $PUDL_GCS_OUTPUT"
    gsutil -m cp -r "$PUDL_OUTPUT" "$PUDL_GCS_OUTPUT"
    rm "$PUDL_OUTPUT/success"
}

function copy_outputs_to_distribution_bucket() {
    # Only attempt to update outputs if we have a real value of BUILD_REF
    if [ -n "$BUILD_REF" ]; then
        echo "Removing old $BUILD_REF outputs from GCP distributon bucket."
        gsutil -m -u "$GCP_BILLING_PROJECT" rm -r "gs://pudl.catalyst.coop/$BUILD_REF"
        echo "Copying outputs to GCP distribution bucket"
        gsutil -m -u "$GCP_BILLING_PROJECT" cp -r "$PUDL_OUTPUT/*" "gs://pudl.catalyst.coop/$BUILD_REF"

        echo "Removing old $BUILD_REF outputs from AWS distributon bucket."
        aws s3 rm "s3://pudl.catalyst.coop/$BUILD_REF" --recursive
        echo "Copying outputs to AWS distribution bucket"
        aws s3 cp "$PUDL_OUTPUT/" "s3://pudl.catalyst.coop/$BUILD_REF" --recursive
    fi
}

function zenodo_data_release() {
    echo "Creating a new PUDL data release on Zenodo."
    ~/devtools/zenodo/zenodo_data_release.py --publish --env sandbox --source-dir "$PUDL_OUTPUT"
}

function notify_slack() {
    # Notify pudl-builds slack channel of deployment status
    if [ "$1" = "success" ]; then
        message=":large_green_circle: :sunglasses: :unicorn_face: :rainbow: The deployment succeeded!! :partygritty: :database_parrot: :blob-dance: :large_green_circle:\n\n "
        message+="<https://github.com/catalyst-cooperative/pudl/compare/main...${BUILD_REF}|Make a PR for \`${BUILD_REF}\` into \`main\`!>\n\n"
    elif [ "$1" = "failure" ]; then
        message=":large_red_square: Oh bummer the deployment failed ::fiiiiine: :sob: :cry_spin:\n\n "
    else
        echo "Invalid deployment status"
        exit 1
    fi
    message+="See https://console.cloud.google.com/storage/browser/nightly-build-outputs.catalyst.coop/$BUILD_ID for logs and outputs."

    send_slack_msg "$message"
}

# # Run ETL. Copy outputs to GCS and shutdown VM if ETL succeeds or fails
# 2>&1 redirects stderr to stdout.
run_pudl_etl 2>&1 | tee "$LOGFILE"

ETL_SUCCESS=${PIPESTATUS[0]}

copy_outputs_to_gcs

# if pipeline is successful, distribute + publish datasette
if [[ $ETL_SUCCESS == 0 ]]; then
    if [ "$GITHUB_ACTION_TRIGGER" = "workflow_dispatch" ]; then
        # Remove read-only authentication header added by git checkout
        git config --unset http.https://github.com/.extraheader
        git config user.email "pudl@catalyst.coop"
        git config user.name "pudlbot"
        git remote set-url origin "https://pudlbot:$PUDL_BOT_PAT@github.com/catalyst-cooperative/pudl.git"
        # Update the nightly branch to point at newly successful nightly build tag
        git checkout nightly
        git merge --ff-only "$NIGHTLY_TAG"
        git push
    fi
    # Deploy the updated data to datasette
    if [ "$BUILD_REF" = "dev" ] || [ "$GITHUB_ACTION_TRIGGER" = "workflow_dispatch" ]; then
        python ~/devtools/datasette/publish.py 2>&1 | tee -a "$LOGFILE"
        ETL_SUCCESS=${PIPESTATUS[0]}
    fi

    # Compress the SQLite DBs for easier distribution
    # Remove redundant multi-file EPA CEMS outputs prior to distribution
    gzip --verbose "$PUDL_OUTPUT"/*.sqlite && \
    rm -rf "$PUDL_OUTPUT/core_epacems__hourly_emissions/" && \
    rm -f "$PUDL_OUTPUT/metadata.yml"
    ETL_SUCCESS=${PIPESTATUS[0]}

    # Dump outputs to s3 bucket if branch is dev or build was triggered by a tag
    # TODO: this behavior should be controlled by on/off switch here and this logic
    # should be moved to the triggering github action. Having it here feels
    # fragmented.
    if [ "$GITHUB_ACTION_TRIGGER" = "push" ] || [ "$BUILD_REF" = "dev" ] || [ "$GITHUB_ACTION_TRIGGER" = "workflow_dispatch" ]; then
        copy_outputs_to_distribution_bucket
        ETL_SUCCESS=${PIPESTATUS[0]}
        # TEMPORARY: this currently just makes a sandbox release, for testing:
        zenodo_data_release 2>&1 | tee -a "$LOGFILE"
        ETL_SUCCESS=${PIPESTATUS[0]}
    fi
fi

# This way we also save the logs from latter steps in the script
gsutil cp "$LOGFILE" "$PUDL_GCS_OUTPUT"

# Notify slack about entire pipeline's success or failure;
# PIPESTATUS[0] either refers to the failed ETL run or the last distribution
# task that was run above
if [[ $ETL_SUCCESS == 0 ]]; then
    notify_slack "success"
else
    notify_slack "failure"
fi

shutdown_vm
