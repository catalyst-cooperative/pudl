.. _nightly-data-builds:

===============================================================================
Nightly Data Builds
===============================================================================

The complete ETL and tests run every night on a Google Compute Engine (GCE)
instance so new code merged into ``dev`` can be fully tested. These complete builds
also enable continuous deployment of PUDL's data outputs.

The builds are kicked off by the ``build-deploy-pudl`` GitHub Action, which builds and
pushes a Docker image with PUDL installed to `Docker Hub <https://hub.docker.com/r/catalystcoop/pudl-etl>`__ and deploys the image as a container
to a GCE instance. The container runs the ETL and tests, then copies the outputs to GCS
buckets for the PUDL Intake catalogs to consume.

Breaking the Builds
-------------------
The nightly data builds based on the ``dev`` branch are our comprehensive integration
tests. When they pass, we consider the results fit for public consumption.  The builds
are expected to pass. If they don't then someone needs to take responsibility for
getting them working again with some urgency.

Because of how long the full build & tests take, we don’t typically run them
individually before merging every PR into ``dev``. However, running ``tox -e nuke``
(the equivalent of the full builds) is recommended when you've added a new year of data
or made other changes that would be expected to break the data validations, so that the
appropriate changes can be made prior to those changes hitting ``dev`` and the nightly
builds.

If your PR causes the build to fail, you are probably the best person to fix the
problem, since you already have context on all of the changes that went into it.

Having multiple PRs merged into ``dev`` simultaneously when the builds are breaking
makes it ambiguous where the problem is coming from, makes debugging harder, and
diffuses responsibility for the breakage across several people, so it's important to fix
the breakage quickly. In some cases we may delay merging additional PRs into ``dev``
if the builds are failing to avoid ambiguity and facilitate debugging.

Therefore, we've adopted the following etiquette regarding build breakage: On the
morning after you merge a PR into ``dev``, you should check whether the nightly builds
succeeded by looking in the ``pudl-deployments`` Slack channel (which all team members
should be subscribed to). If the builds failed, look at the logging output (which is
included as an attachment to the notification) and figure out what kind of failure
occurred:

  * If the failure is due to your changes, then you are responsible for fixing the
    problem and making a new PR to ``dev`` that resolves it, and it should be a high
    priority. If you're stumped, ask for help!
  * If the failure is due to an infrastructural issue like the build server running out
    of memory and the build process getting killed, then you need to notify the member
    who is in charge of managing the builds (Currently :user:`bendnorman`), and hand off
    responsibility for debugging and fixing the issue.
  * If the failure is the result of a transient problem outside of our control like a
    network connection failing, then wait until the next morning and repeat the above
    process. If the "transient" problem persists, bring it up with the person
    managing the builds.

The GitHub Action
-----------------
The ``build-deploy-pudl`` GitHub action contains the main coordination logic for
the Nightly Data Builds. The action is triggered every night and when new tags are
pushed to the PUDL repository. This way, new data outputs are automatically updated
on code releases, and PUDL's code and data are tested every night. The action is
modeled after an `example from the setup-gcloud GitHub action repository <https://github.com/google-github-actions/setup-gcloud/tree/main/example-workflows/gce>`__.

Unfortunately, scheduled actions only run on the default branch. To run scheduled
builds on the ``dev`` branch, the `actions/checkout <https://github.com/actions/checkout>`__
step checks out the ``dev`` branch if a schedule triggers the action and the ``main``
branch if a tag triggers the action.

The ``gcloud`` command in ``build-deploy-pudl`` requires certain Google Cloud
Platform (GCP) permissions to start and update the GCE instance. The
``gcloud`` command authenticates using a service account key for the
``deploy-pudl-github-action`` service account stored in PUDL's GitHub secrets
as ``DEPLOY_PUDL_SA_KEY``. The ``deploy-pudl-github-action`` service account has
the `Compute Instance Admin (v1) IAM <https://cloud.google.com/iam/docs/understanding-roles#compute-engine>`__
role on the GCE instances to update the container and start the instance.

Google Compute Engine
---------------------
The PUDL image is deployed on a `Container Optimized GCE
<https://cloud.google.com/container-optimized-os/docs/concepts/features-and-benefits>`__
instance, a type of virtual machine (VM) built to run containers. The
``pudl-deployment-dev`` and ``pudl-deployment-tag`` instances in the
``catalyst-cooperative-pudl`` GCP project handle deployments from the ``dev`` branch and
tags, respectively. There are two VMs so a scheduled and a tag build can run
at the same time.

.. note::

    If a tag build starts before the previous tag build has finished, the previous build
    will be interrupted.

PUDL's VMs have 32 GB of RAM and 8 CPUs to accommodate the PUDL ETL's memory-intensive
steps. Currently, these VMs do not have swap space enabled.

Each GCE VM has a service account that gives the VM permissions to GCP resources.
The two PUDL deployment VMs share the ``deploy-pudl-vm-service-account``. This
service account has permissions to:

1. Write logs to Cloud Logging.
2. Start and stop the VM so the container can shut the instance off when the ETL
   is complete, so Catalyst does not incur unnecessary charges.
3. Bill the ``catalyst-cooperative-pudl`` project for egress fees from accessing
   the ``zenodo-cache.catalyst.coop`` bucket. Note: The ``catalyst-cooperative-pudl``
   won't be charged anything because the data stays within Google's network.
4. Write logs and outputs to ``pudl-etl-logs`` and ``intake.catalyst.coop`` buckets.

Docker
------
The Docker image the VMs pull installs PUDL into a mamba environment. The VMs
are configured to run the ``docker/gcp_pudl_etl.sh`` script. This script:

1. Notifies the ``pudl-deployments`` Slack channel that a deployment has started.
   Note: if the container is manually stopped, slack will not be notified.
2. Runs the ETL and full test suite.
3. Copies the outputs and logs to a directory in the ``pudl-etl-logs`` bucket. The
   directory is named using the git SHA of the commit that launched the build.
4. Copies the outputs to the ``intake.catalyst.coop`` bucket if the ETL and test
   suite run successfully.
5. Notifies the ``pudl-deployments`` Slack channel with the final build status.

The ``gcp_pudl_etl.sh script`` is only intended to run on a GCE VM with adequate
permissions. The full ETL and tests can be run locally by running these commands
from the ``pudl`` directory:

.. code-block::

    docker compose -f docker/docker-compose.yml build
    docker compose -f docker/docker-compose.yml up
