"""This script will kick off docker based pudl ETL on google batch."""
import os
import sys
from datetime import datetime

from google.cloud import batch_v1
from pydantic import BaseModel, BaseSettings

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "catalyst-cooperative-pudl")
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE", "docker.io/catalystcoop/pudl-etl-ci")
DOCKER_TAG = os.environ.get("DOCKER_TAG", "latest")


class GithubRuntime(BaseSettings):
    """Encodes parameters passed by github actions runtime.

    It is expected that GITHUB_SHA, GITHUB_REF, and GITHUB_RUN_ID
    are set before running this.
    """

    sha: str = ""
    ref: str = ""
    run_id: str = ""

    class Config:
        """Controls the loading of parameters from env variables."""

        env_prefix = "github_"


class RunProfile(BaseModel):
    """RunProfile encodes parameters that control how the job is run."""

    name: str
    gcp_region: str
    machine_type: str
    max_run_duration: str
    settings_file: str


PROFILES: dict[str, RunProfile] = {
    "fast": RunProfile(
        name="fast",
        gcp_region="us-west1",
        machine_type="e2-highmem-8",
        max_run_duration=f"{3 * 3600}s",
        settings_file="etl_fast.yml",
    ),
    "full": RunProfile(
        name="full",
        gcp_region="us-west1",
        machine_type="e2-highmem-8",
        max_run_duration=f"{5 * 3600}s",
        settings_file="etl_full.yml",
    ),
}

# TODO(rousik): Build profiles can encode variety of options, e.g.
# machine type, settings file, max run time, etc. We should use
# pydantic for that.


# Construct job name


def run_etl(profile: RunProfile, job_name: str) -> batch_v1.Job:
    """Runs PUDL ETL on Google Batch.

    Args:
        profile: runtime parameters to be used when creating the
          job.
        job_name: the name of the job that will be created.

    Returns:
        A job object representing the job created.
    """
    client = batch_v1.BatchServiceClient()
    job_labels = {
        "pudl_profile": profile.name,
        "pudl_settings_file": profile.settings_file,
        "github_sha": GithubRuntime().sha,
        "github_ref": GithubRuntime().ref,
        "github_run_id": GithubRuntime().run_id,
    }

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
        "PUDL_SETTINGS_YML": (
            "/home/catalyst/src/pudl/package_data"
            + f"/settings/{profile.settings_file}"
        ),
        "GOOGLE_BATCH": "true",  # This disables some legacy functionality.
        "ACTION_SHA": GithubRuntime().sha,
        "GITHUB_REF": GithubRuntime().ref,
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
    instances.policy.machine_type = profile.machine_type
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.labels = job_labels
    # We use Cloud Logging as it's an out of the box available option
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    create_request = batch_v1.CreateJobRequest()
    create_request.job = job
    create_request.job_id = job_name

    # The job's parent is the region in which the job will run, how do we set this when
    create_request.parent = f"projects/{GCP_PROJECT_ID}/locations/{profile.gcp_region}"

    return client.create_job(create_request)


def main():
    """Runs PUDL ETL on Google Batch service."""
    # Construct job name that encodes some useful info
    profile = os.environ.get("PUDL_PROFILE", "fast")
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_name = f"etl-{profile}-{ts}"

    print(f"Running batch job with job name {job_name}\n")
    job = run_etl(PROFILES[profile], job_name)

    print(f"Created job {job.name} with uuid {job.uid}")
    # TODO(rousik): add optional (on-by-default) wait for job step here.
    # That would allow us to keep the github action running/blocked until
    # the job completes.


if __name__ == "__main__":
    sys.exit(main())
