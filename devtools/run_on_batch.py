"""This script will kick off docker based pudl ETL on google batch."""
import os
import sys
from datetime import datetime

from google.cloud import batch_v1

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "catalyst-cooperative-pudl")

# See following for supported regions:
# https://cloud.google.com/batch/docs/get-started#locations
ts = datetime.now().strftime("%Y-%m-%d-%H%M%S")
JOB_NAME = os.environ.get("JOB_NAME", f"test-{ts}")
GCP_REGION = "us-west1"
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE", "docker.io/catalystcoop/pudl-etl-ci")
DOCKER_TAG = os.environ.get("DOCKER_TAG", "latest")

# TODO: consider what is the right default machine type
MACHINE_TYPE = os.environ.get("MACHINE_TYPE", "e2-highmem-8")


def run_etl(job_name: str) -> batch_v1.Job:
    """Runs PUDL ETL on Google Batch.

    Args:
        job_name: the name of the job that will be created.

    Returns:
        A job object representing the job created.
    """
    client = batch_v1.BatchServiceClient()

    # Define what will be done as part of the job.
    task = batch_v1.TaskSpec()
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = f"{DOCKER_IMAGE}:{DOCKER_TAG}"
    runnable.environment = batch_v1.types.Environment()
    runnable.environment.variables = {
        "PUDL_INPUT": "/home/catalyst/pudl_work/data",
        "PUDL_OUTPUT": "/home/catalyst/pudl_work/output",
        "DAGSTER_HOME": "/home/catalyst/pudl_work/dagster_home",
        "CONDA_PREFIX": "/home/catalyst/env",
        "GCS_CACHE": "gs://zenodo-cache.catalyst.coop",
        "CONDA_RUN": "conda run --no-capture-output --prefix /home/catalyst/env",
        "PUDL_SETTINGS_YML": "/home/catalyst/src/pudl/package_data/settings/etl_fast.yml",
        "GOOGLE_BATCH": "true",  # This disables some legacy functionality.
        "ACTION_SHA": "foo",
        "GITHUB_REF": "bar",
        "GCP_BILLING_PROJECT": GCP_PROJECT_ID,
    }
    # Other env variables we will need are:
    # ACTION_SHA
    # GITHUB_REF

    # The following should be done via secrets:
    # SLACK_TOKEN
    # AWS_ACCESS_KEY_ID
    # AWS_SECRET_ACCESS_KEY
    # AWS_DEFAULT_REGION

    # TODO(rousik): provide mechanism for passing custom PUDL_SETTINGS_YML in, we might
    # even consider profiles that control multiple things, e.g. config, machine type,
    # timeouts.

    # Getting rid of the conde

    # set entry_point and commands
    task = batch_v1.TaskSpec()
    task.runnables = [runnable]

    # We can specify what resources are requested by each task; let's not do this and
    # let them use the full machine.

    # resources = batch_v1.ComputeResource()
    # resources.cpu_milli = 2000
    # resources.memory_mib = 16
    # task.compute_resource = resources

    task.max_retry_count = 0

    # Allow up to 3h for the ETL to complete. Run the job on big machine.
    run_secs = 3 * 3600
    task.max_run_duration = f"{run_secs}s"

    # Tasks are grouped inside a job using TaskGroups.
    # Currently, it's possible to have only one task group.
    group = batch_v1.TaskGroup()
    group.task_count = 1
    group.task_spec = task

    # Policies are used to define on what kind of virtual machines the tasks will run on.
    # In this case, we tell the system to use "e2-standard-4" machine type.
    # Read more about machine types here: https://cloud.google.com/compute/docs/machine-types
    allocation_policy = batch_v1.AllocationPolicy()

    # e2-standard-16, or e2-highmem-8 are used; figure out what's needed.
    # TODO(rousik): we might be more permissive and allow running in any region.
    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = batch_v1.AllocationPolicy.InstancePolicy()
    instances.policy.machine_type = MACHINE_TYPE
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.labels = {"env": "testing", "type": "script"}
    # We use Cloud Logging as it's an out of the box available option
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    create_request = batch_v1.CreateJobRequest()
    create_request.job = job
    create_request.job_id = job_name

    # The job's parent is the region in which the job will run, how do we set this when
    create_request.parent = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}"

    return client.create_job(create_request)


def main():
    """Runs PUDL ETL on Google Batch service."""
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    print(f"Running batch job with job name test-{ts}\n")
    job = run_etl(f"test-{ts}")
    print(f"Created job {job.name} with uuid {job.uid}")
    # TODO(rousik): add optional (on-by-default) wait for job step here.
    # That would allow us to keep the github action running/blocked until
    # the job completes.


if __name__ == "__main__":
    sys.exit(main())
