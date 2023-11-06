#!/usr/bin/bash
# This script runs the entire ETL and validation tests in a docker container on a Google Compute Engine instance.
# This script won't work locally because it needs adequate GCP permissions.

set -x

function send_slack_msg() {
    curl -X POST -H "Content-type: application/json" -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/chat.postMessage --data "{\"channel\": \"C03FHB9N0PQ\", \"text\": \"$1\"}"
}

function upload_file_to_slack() {
    curl -F file=@$1 -F "initial_comment=$2" -F channels=C03FHB9N0PQ -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/files.upload
}

function authenticate_gcp() {
    # Set the default gcloud project id so the zenodo-cache bucket
    # knows what project to bill for egress
    gcloud config set project $GCP_BILLING_PROJECT
}

function run_pudl_etl() {
    send_slack_msg ":large_yellow_circle: Deployment started for $ACTION_SHA-$GITHUB_REF :floppy_disk:"
    authenticate_gcp \
    && alembic upgrade head \
    && pudl_setup \
    && ferc_to_sqlite \
        --loglevel=DEBUG \
        --gcs-cache-path=gs://internal-zenodo-cache.catalyst.coop \
        --workers=8 \
        $PUDL_SETTINGS_YML \
    && pudl_etl \
        --loglevel DEBUG \
        --gcs-cache-path gs://internal-zenodo-cache.catalyst.coop \
        $PUDL_SETTINGS_YML \
    && pytest \
        --gcs-cache-path=gs://internal-zenodo-cache.catalyst.coop \
        --etl-settings=$PUDL_SETTINGS_YML \
        --live-dbs test
}

function shutdown_vm() {
    # Copy the outputs to the GCS bucket
    gsutil -m cp -r $PUDL_OUTPUT "gs://nightly-build-outputs.catalyst.coop/$ACTION_SHA-$GITHUB_REF"

    upload_file_to_slack $LOGFILE "pudl_etl logs for $ACTION_SHA-$GITHUB_REF:"

    echo "Shutting down VM."
    # # Shut down the vm instance when the etl is done.
    ACCESS_TOKEN=`curl \
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        -H "Metadata-Flavor: Google" | jq -r '.access_token'`

    curl -X POST -H "Content-Length: 0" -H "Authorization: Bearer ${ACCESS_TOKEN}" https://compute.googleapis.com/compute/v1/projects/catalyst-cooperative-pudl/zones/$GCE_INSTANCE_ZONE/instances/$GCE_INSTANCE/stop
}

function copy_outputs_to_distribution_bucket() {
    echo "Copying outputs to GCP distribution bucket"
    gsutil -m -u $GCP_BILLING_PROJECT cp -r "$PUDL_OUTPUT/*" "gs://pudl.catalyst.coop/$GITHUB_REF"

    echo "Copying outputs to AWS distribution bucket"
    aws s3 cp "$PUDL_OUTPUT/" "s3://pudl.catalyst.coop/$GITHUB_REF" --recursive
    echo "Copying outputs to AWS intake bucket"
    # This is temporary as we migrate people to pudl.catalyst.coop
    aws s3 cp "$PUDL_OUTPUT/" "s3://intake.catalyst.coop/$GITHUB_REF" --recursive
}


function notify_slack() {
    # Notify pudl-builds slack channel of deployment status
    if [ $1 = "success" ]; then
        message=":large_green_circle: :sunglasses: :unicorn_face: :rainbow: The deployment succeeded!! :partygritty: :database_parrot: :blob-dance: :large_green_circle:\n\n "
        message+="<https://github.com/catalyst-cooperative/pudl/compare/main...${GITHUB_REF}|Make a PR for \`${GITHUB_REF}\` into \`main\`!>\n\n"
    elif [ $1 = "failure" ]; then
        message=":large_red_square: Oh bummer the deployment failed ::fiiiiine: :sob: :cry_spin:\n\n "
    else
        echo "Invalid deployment status"
        exit 1
    fi
    message+="See https://console.cloud.google.com/storage/browser/nightly-build-outputs.catalyst.coop/$ACTION_SHA-$GITHUB_REF for logs and outputs."

    send_slack_msg "$message"
}

# # Run ETL. Copy outputs to GCS and shutdown VM if ETL succeeds or fails
# 2>&1 redirects stderr to stdout.
run_pudl_etl 2>&1 | tee $LOGFILE

# Notify slack if the etl succeeded.
if [[ ${PIPESTATUS[0]} == 0 ]]; then
    notify_slack "success"

    # Dump outputs to s3 bucket if branch is dev or build was triggered by a tag
    if [ $GITHUB_ACTION_TRIGGER = "push" ] || [ $GITHUB_REF = "dev" ]; then
        copy_outputs_to_distribution_bucket
    fi

    # Deploy the updated data to datasette
    if [ $GITHUB_REF = "dev" ]; then
        gcloud config set run/region us-central1
        source ~/devtools/datasette/publish.sh
    fi
else
    notify_slack "failure"
fi

shutdown_vm
