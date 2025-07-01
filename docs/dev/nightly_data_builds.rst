.. _nightly-data-builds:

===============================================================================
Nightly Data Builds
===============================================================================

The complete ETL and tests are run each night on a Google Compute Engine (GCE) instance
to ensure that any new changes merged into ``main`` are fully tested. These complete
builds also enable continuous deployment of PUDL's data outputs. If no changes have been
merged into ``main`` since the last time the builds ran, the builds are skipped.

The builds are kicked off by the ``build-deploy-pudl`` GitHub Action, which builds and
pushes a Docker image with PUDL installed to `Docker Hub <https://hub.docker.com/r/catalystcoop/pudl-etl>`__
and deploys the image as a container to a GCE instance. The container runs the ETL and
tests, then copies the outputs to a public AWS S3 bucket for distribution.

Breaking the Builds
-------------------
The nightly data builds based on the ``main`` branch are our comprehensive integration
tests. When they pass, we consider the results fit for public consumption.  The builds
are expected to pass. If they don't then someone needs to take responsibility for
getting them working again with some urgency.

Because of how long the full build & tests take, we don't typically run them
individually before merging every PR into ``main``. However, running ``make nuke``
(roughly equivalent to the full builds) is recommended when you've added a new year of
data or made other changes that would be expected to break the data validations, so that
the appropriate changes can be made prior to those changes hitting ``main`` and the
nightly builds.

If your PR causes the build to fail, you are probably the best person to fix the
problem, since you already have context on all of the changes that went into it.

Having multiple PRs merged into ``main`` simultaneously when the builds are breaking
makes it ambiguous where the problem is coming from, makes debugging harder, and
diffuses responsibility for the breakage across several people, so it's important to fix
the breakage quickly. In some cases we may delay merging additional PRs into ``main``
if the builds are failing to avoid ambiguity and facilitate debugging.

Therefore, we've adopted the following etiquette regarding build breakage: On the
morning after you merge a PR into ``main``, you should check whether the nightly builds
succeeded by looking in the ``pudl-deployments`` Slack channel (which all team members
should be subscribed to). If the builds failed, look at the logging output (which is
included as an attachment to the notification) and figure out what kind of failure
occurred:

  * If the failure is due to your changes, then you are responsible for fixing the
    problem and making a new PR to ``main`` that resolves it, and it should be a high
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

The ``gcloud`` command in ``build-deploy-pudl`` requires certain Google Cloud
Platform (GCP) permissions to start and update the GCE instance. We use Workflow
Identity Federation to authenticate the GitHub Action with GCP in the GitHub Action
workflow.

Google Compute Engine
---------------------
The PUDL image is deployed on a `Container Optimized GCE
<https://cloud.google.com/container-optimized-os/docs/concepts/features-and-benefits>`__
instance, a type of virtual machine (VM) built to run containers.

We use ephemeral VMs created with `Google Batch <https://cloud.google.com/batch/docs>`__
to run the nightly builds. Once the build has finished -- successfully or not -- the VM
is shut down.  The build VMs use the ``e2-highmem-8`` machine type (8 CPUs and 64GB of
RAM) to accommodate the PUDL ETL's memory-intensive steps. Currently, these VMs do not
have swap space enabled, so if they run out of memory, the build will immediately
terminate.

The ``deploy-pudl-vm-service-account`` service account has permissions to:

1. Write logs to Cloud Logging.
2. Start and stop the VM so the container can shut the instance off when the ETL
   is complete, so Catalyst does not incur unnecessary charges.
3. Bill the ``catalyst-cooperative-pudl`` project for egress fees from accessing
   the ``zenodo-cache.catalyst.coop`` bucket. Note: The ``catalyst-cooperative-pudl``
   won't be charged anything because the data stays within Google's network.
4. Write logs and build outputs to the ``gs://builds.catalyst.coop``,
   ``gs://pudl.catalyst.coop`` and ``s3://pudl.catalyst.coop`` buckets.
   Egress and storage costs for the S3 bucket are covered by
   `Amazon Web Services's Open Data Sponsorship Program
   <https://aws.amazon.com/opendata/open-data-sponsorship-program/>`__.

Build outputs and logs are saved to the ``gs://builds.catalyst.coop`` bucket so you can
access them later. Build logs and outputs are retained for 30 days and then deleted
automatically.

Docker
------
The Docker image the VMs pull installs PUDL into a mamba environment. The VMs
are configured to run the ``docker/gcp_pudl_etl.sh`` script. This script:

1. Notifies the ``pudl-deployments`` Slack channel that a deployment has started.
   Note: if the container is manually stopped, slack will not be notified.
2. Runs the ETL and full test suite.
3. Copies the outputs and logs to a directory in the ``gs://builds.catalyst.coop``
   bucket. The directory is named using the git SHA of the commit that launched the
   build.
4. Copies the outputs to the ``gs://pudl.catalyst.coop`` and ``s3://pudl.catalyst.coop``
   buckets if the ETL and test suite run successfully.
5. Notifies the ``pudl-deployments`` Slack channel with the final build status.

The ``gcp_pudl_etl.sh script`` is only intended to run on a GCE VM with adequate
permissions.

How to access the nightly build outputs from AWS
------------------------------------------------
You can download the outputs from a successful nightly build data directly from the
``s3://pudl.catalyst.coop`` bucket. To do this, you'll
need to `follow the instructions
<https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>`__
for installing the AWS CLI tool.

To test that the AWS CLI tool and the bucket are working properly, run:

.. code-block::

   aws s3 ls s3://pudl.catalyst.coop --no-sign-request

You should see a list of directories with version names:

.. code-block::

   PRE nightly/
   PRE stable/
   PRE v2022.11.30/
   PRE v2023.12.01/
   ...

The ``--no-sign-request`` flag allows you to make requests to the public bucket without
having to load AWS credentials. If you don't include this flag when interacting with the
``s3://pudl.catalyst.coop`` bucket, ``aws`` will give you an authentication error.

.. warning::

   If you download the files directly with ``aws`` then you'll be responsible for
   updating them, making sure you have the right version, putting them in the right
   place on your computer, etc.

To copy these files directly to your computer you can use the ``aws s3 cp`` command,
which behaves very much like the Unix ``cp`` command:

.. code::

   aws s3 cp s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip ./ --no-sign-request

If you wanted to download all of the build outputs (more than 10GB!) you can use a
recursive copy:

.. code::

   aws s3 cp --recursive s3://pudl.catalyst.coop/nightly/ ./ --no-sign-request

For more details on how to use ``aws`` in general see the
`online documentation <https://docs.aws.amazon.com/cli/latest/reference/s3/>`__ or run:

.. code::

   aws help

How to access the nightly build outputs and logs (for the Catalyst team only)
-----------------------------------------------------------------------------

Sometimes it is helpful to download the logs and data outputs of nightly builds when
debugging failures. To do this you'll need to set up the Google Cloud software
Development Kit (SDK). It is installed as part of the ``pudl-dev`` conda environment.

To authenticate with Google Cloud Platform (GCP) you'll need to run the following:

.. code::

  gcloud auth login

Initialize the ``gcloud`` command line interface and select the
``catalyst-cooperative-pudl`` project.

If it asks you whether you want to "re-initialize this configuration with new settings"
say yes.

.. code::

  gcloud init

Finally, use ``gcloud`` to establish application default credentials; this will allow
the project to be used for requester pays access through applications:

.. code::

  gcloud auth application-default login

To test whether your GCP account is set up correctly and authenticated you can run the
following command to list the contents of the cloud storage bucket containing the PUDL
data. This doesn't actually download any data, but will show you the versions
that are available:

.. code::

   gcloud storage ls --long --readable-sizes gs://builds.catalyst.coop

You should see a list of directories with build IDs that have a naming convention:
``<YYYY-MM-DD-HHMM>-<short git commit SHA>-<git branch>``.

To see what the outputs are for a given nightly build, you can use ``gcloud storage``
like this:

.. code::

    gcloud storage ls --long --readable-sizes gcloud storage ls --long --readable-sizes gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main

       6.60MiB  2024-11-15T13:28:20Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/2024-11-15-0603-60f488239-main-pudl-etl.log
     804.57MiB  2024-11-15T12:40:35Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/censusdp1tract.sqlite
     759.32MiB  2024-11-15T12:41:01Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc1_dbf.sqlite
       1.19GiB  2024-11-15T12:41:12Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc1_xbrl.sqlite
       2.16MiB  2024-11-15T12:39:23Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc1_xbrl_datapackage.json
       6.95MiB  2024-11-15T12:39:23Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc1_xbrl_taxonomy_metadata.json
     282.71MiB  2024-11-15T12:40:40Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc2_dbf.sqlite
     127.39MiB  2024-11-15T12:39:59Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc2_xbrl.sqlite
       2.46MiB  2024-11-15T12:40:54Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc2_xbrl_datapackage.json
       6.82MiB  2024-11-15T12:40:48Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc2_xbrl_taxonomy_metadata.json
       8.25MiB  2024-11-15T12:39:22Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc60_dbf.sqlite
      27.89MiB  2024-11-15T12:39:24Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc60_xbrl.sqlite
     942.19kiB  2024-11-15T12:39:22Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc60_xbrl_datapackage.json
       1.77MiB  2024-11-15T12:39:22Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc60_xbrl_taxonomy_metadata.json
     153.72MiB  2024-11-15T12:41:03Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc6_dbf.sqlite
      90.51MiB  2024-11-15T12:41:09Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc6_xbrl.sqlite
       1.32MiB  2024-11-15T12:40:47Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc6_xbrl_datapackage.json
       2.74MiB  2024-11-15T12:39:22Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc6_xbrl_taxonomy_metadata.json
       1.38GiB  2024-11-15T12:41:06Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc714_xbrl.sqlite
      83.39kiB  2024-11-15T12:40:46Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc714_xbrl_datapackage.json
     187.86kiB  2024-11-15T12:40:46Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/ferc714_xbrl_taxonomy_metadata.json
      15.06GiB  2024-11-15T12:42:17Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/pudl.sqlite
            0B  2024-11-15T12:39:22Z  gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/success
                                      gs://builds.catalyst.coop/2024-11-15-0603-60f488239-main/parquet/
   TOTAL: 23 objects, 21331056422 bytes (19.87GiB)

If you want to copy these files down directly to your computer, you can use
the ``gcloud storage cp`` command, which behaves very much like the Unix ``cp`` command:

.. code::

   gcloud storage cp gs://builds.catalyst.coop/<build ID>/pudl.sqlite ./

If you need to download all of the build outputs (~20GB!) you can do a recursive copy of
the whole directory hierarchy (note that this will incur egress charges):

.. code::

   gcloud storage cp --recursive gs://builds.catalyst.coop/<build ID>/ ./

For more background on ``gcloud storage`` see the
`quickstart guide <https://cloud.google.com/storage/docs/discover-object-storage-gcloud>`__
or check out the CLI documentation with:

.. code::

   gcloud storage --help
