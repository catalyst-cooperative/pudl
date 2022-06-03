#!/usr/bin/bash
function send_slack_msg() {
    curl -X POST -H "Content-type: application/json" -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/chat.postMessage --data "{\"channel\": \"C03FHB9N0PQ\", \"text\": \"$1\"}"
}

function authenticate_gcp() {
    # Set the default gcloud project id so the zenodo-cache bucket
    # knows what project to bill for egress
    gcloud config set project $GCLOUD_BILLING_PROJECT
}

function run_pudl_etl() {
    send_slack_msg ":large_yellow_circle: Deployment started for $ACTION_SHA-$GITHUB_REF :floppy_disk:"
    authenticate_gcp \
    && pudl_setup \
        --pudl_in $CONTAINER_PUDL_IN \
        --pudl_out $CONTAINER_PUDL_OUT \
    && ferc1_to_sqlite \
        --clobber \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML \
    && pudl_etl \
        --clobber \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML \
    && pytest \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        --etl-settings $PUDL_SETTINGS_YML \
        --live-dbs test
}

function shutdown_vm() {
    gsutil -m cp -r $CONTAINER_PUDL_OUT "gs://pudl-etl-logs/$ACTION_SHA-$GITHUB_REF"

    echo "Shutting down VM."
    # # Shut down the vm instance when the etl is done.
    ACCESS_TOKEN=`curl \
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        -H "Metadata-Flavor: Google" | jq -r '.access_token'`

    curl -X POST -H "Content-Length: 0" -H "Authorization: Bearer ${ACCESS_TOKEN}" https://compute.googleapis.com/compute/v1/projects/catalyst-cooperative-pudl/zones/us-central1-a/instances/$GCE_INSTANCE/stop
}

function notify_slack() {
    # Notify pudl-builds slack channel of deployment status
    if [ $1 = "success" ]; then
        message=":large_green_circle: :sunglasses: :unicorn_face: :rainbow: The deployment succeeded!! :rainbow: :unicorn_face: :sunglasses: :large_green_circle:\n\n "
    elif [ $1 = "failure" ]; then
        message=":large_red_square: Oh bummer the deployment failed :smiling_face_with_tear:\n\n "
    else
        echo "Invalid deployment status"
        exit 1
    fi
    message+="See https://console.cloud.google.com/storage/browser/pudl-etl-logs/$ACTION_SHA-$GITHUB_REF for logs and outputs."

    send_slack_msg "$message"
}

# # Run ETL. Copy outputs to GCS and shutdown VM if ETL succeeds or fails
# 2>&1 redirects stderr to stdout.
run_pudl_etl 2>&1 | tee $LOGFILE

# Notify slack if the etl succeeded.
if [[ ${PIPESTATUS[0]} == 0 ]]; then
    notify_slack "success"
else
    notify_slack "failure"
fi

shutdown_vm
