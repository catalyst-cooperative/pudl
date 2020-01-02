#!/bin/bash

GCP_REGION="us-east1"
GCP_ZONE="$GCP_REGION-c" # Should be a, c, or f

gcloud compute addresses create pudl-data-release \
  --region $GCP_REGION

STATIC_IP=`gcloud compute addresses describe pudl-data-release --region=$GCP_REGION | grep "^address:" | sed -e "s/address: //"`

gcloud compute instances create pudl-data-release \
  --zone=$GCP_ZONE \
  --image-project=ubuntu-os-cloud \
  --image-family=ubuntu-1910 \
  --machine-type=n2-highmem-4 \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-ssd \
  --address=$STATIC_IP
#  --source-snapshot=pudl-data-release
#  --metadata-from-file startup-script=data_release.sh

gcloud compute scp data-release-env.yml data-release-etl.yml data-release.sh zane@pudl-data-release: --zone=$GCP_ZONE
