#!/bin/bash

gcloud compute instances delete pudl-data-release --zone=us-central1-c --quiet
gcloud compute addresses delete pudl-data-release --region=us-central1 --quiet
