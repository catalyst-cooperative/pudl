#!/usr/bin/bash
function run_pudl_etl() {
    # Set the default gcloud project id so the zenodo-cache bucket
    # knows what project to bill for egress
    gcloud config set project catalyst-cooperative-pudl
    pudl_setup \
        --pudl_in $CONTAINER_PUDL_IN \
        --pudl_out $CONTAINER_PUDL_OUT
    ferc1_to_sqlite \
        --clobber \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML
    pudl_etl \
        --clobber \
        --gcs-cache-path gs://zenodo-cache.catalyst.coop \
        --bypass-local-cache \
        $PUDL_SETTINGS_YML
}

function shutdown_vm() {
    # Create a log bucket for the deployment
    echo "Google Cloud Project:"
    echo $GOOGLE_CLOUD_PROJECT
    gsutil -m cp -r $CONTAINER_PUDL_OUT "gs://pudl-etl-logs/$GITHUB_SHA-$GITHUB_REF"

    # # Shut down the deploy-pudl-vm instance when the etl is done.
    # ACCESS_TOKEN=`curl \
    #     "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
    #     -H "Metadata-Flavor: Google" | jq -r '.access_token'`

    # curl -X POST -H "Content-Length: 0" -H "Authorization: Bearer ${ACCESS_TOKEN}" https://compute.googleapis.com/compute/v1/projects/catalyst-cooperative-pudl/zones/us-central1-a/instances/deploy-pudl-vm/stop
}

function notify_slack() {
    # Notify pudl-builds slack channel of deployment status
    if [ $1 = "success" ]; then
        message=":large_green_circle: Deployment Succeeded\n\n "
    elif [ $1 = "failure" ]; then
        message=":large_red_square: Deployment Failed\n\n "
    else
        echo "Invalid deployment status"
        exit 1
    fi
    message+="See https://console.cloud.google.com/storage/browser/pudl-etl-logs/$GITHUB_SHA-$GITHUB_REF for logs and outputs."

    curl -X POST -H "Content-type: application/json" -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/chat.postMessage --data "{\"channel\": \"C03FHB9N0PQ\", \"text\": \"$message\"}"
}

# Run ETL. Copy outputs to GCS and shutdown VM if ETL succeeds or fails
{ # try
    run_pudl_etl 2>&1 | tee $LOGFILE && notify_slack success && shutdown_vm

} || { # catch
    notify_slack failure && shutdown_vm
}
