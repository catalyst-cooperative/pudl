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

function update_nightly_branch() {
    git config --unset http.https://github.com/.extraheader
    git config user.email "pudl@catalyst.coop"
    git config user.name "pudlbot"
    git remote set-url origin "https://pudlbot:$PUDL_BOT_PAT@github.com/catalyst-cooperative/pudl.git"
    echo "BOGUS: Updating nightly branch to point at $NIGHTLY_TAG."
    git fetch origin nightly:nightly
    git checkout nightly
    git merge --ff-only "$NIGHTLY_TAG"
    ETL_SUCCESS=${PIPESTATUS[0]}
    git push -u origin
}

# Short circut the script to debug the nightly branch update
update_nightly_branch
