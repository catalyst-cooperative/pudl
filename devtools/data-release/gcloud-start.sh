#!/bin/bash

gcloud compute addresses create pudl-data-release \
  --region us-central1

STATIC_IP=`gcloud compute addresses describe pudl-data-release --region=us-central1 | grep "^address:" | sed -e "s/address: //"`

gcloud compute instances create pudl-data-release \
  --zone=us-central1-c \
  --image-project=ubuntu-os-cloud \
  --image-family=ubuntu-1910 \
  --machine-type=n2-standard-2 \
  --boot-disk-size=20GB \
  --boot-disk-type=pd-ssd \
  --address=$STATIC_IP
#  --source-snapshot=pudl-data-release
#  --metadata-from-file startup-script=data_release.sh

gcloud compute scp data-release-env.yml data-release-etl.yml data-release.sh zane@pudl-data-release:
