"""This script pull github traffic metrics and saves them to a GC Bucket."""

import json
import os
from pprint import pprint

import requests
from google.cloud import storage

# TODO: replace with google secret
TOKEN = os.getenv("GITHUB_TRAFFIC_TOKEN", "...")
OWNER = "catalyst-cooperative"
REPO = "pudl"
BUCKET_NAME = "github-metrics"

BIWEEKLY_METRICS = ["clones", "popular/paths", "popular/referrers", "views"]
PERSISTENT_METRICS = ["stargazers", "forks"]


def get_biweekly_metrics(metric: str) -> str:
    """
    Get json data for a biweekly github metric.

    Args:
        metric (str): The github metric name.
    Returns:
        json (str): The metric data as json text.
    """
    query_url = f"https://api.github.com/repos/{OWNER}/{REPO}/traffic/{metric}"
    headers = {
        "Authorization": f"token {TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    r = requests.get(query_url, headers=headers)
    return json.dumps(r.json())


def get_persistent_metrics(metric) -> str:
    """
    Get githubs persistent metrics: forks and stargazers.

    Args:
        metrics (str): the metric to retrieve (forks | stargazers)
    Returns:
        json (str): A json string of metrics.
    """
    query_url = f"https://api.github.com/repos/{OWNER}/{REPO}/{metric}"
    headers = {
        "Authorization": f"token {TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    metrics = []
    page = 1
    while True:
        params = {"page": page}
        metrics_json = requests.get(query_url, headers=headers, params=params).json()
        if len(metrics_json) <= 0:
            break
        metrics += metrics_json
        page += 1
    return json.dumps(metrics)


def upload_to_bucket(data):
    """Upload a gcp object."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    # https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    # /github-metrics/{metric}/{metric_date}

    # if in traffic Metrics, dump the 14 day batch
    # if forks for star gazers, rewrite the object with the new ones
    print(bucket)


for metric in BIWEEKLY_METRICS:
    print(metric)
    metric_data = get_biweekly_metrics(metric)
    pprint(metric_data)
    upload_to_bucket(metric_data)
    break

# for metric in PERSISTENT_METRICS:
#     metric_data = get_persistent_metrics(metric)
#     # rewrite_gcs_object(metric, data)
