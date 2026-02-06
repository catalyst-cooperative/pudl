.. _ferceqr-data-builds:

===============================================================================
FERC EQR Data Builds
===============================================================================

The FERC EQR ETL uses a very similar setup for running remote cloud builds
as the :doc:`/dev/nightly_data_builds`, however these builds do not currently
run on an automated schedule, and must be started manually. Like the nightly
builds, FERC EQR builds use a GitHub action to kick off a
`Google Batch <https://cloud.google.com/batch/docs>`__ job, which will
execute the ETL using the same Docker container as the nightly builds.

Notable differences from nightly builds
---------------------------------------
While the FERC EQR builds use much of the same infrastructure as nightly
builds, there are a few notable differences. First, the data distribution
is all done in Python using Dagster tooling. More information on how this
works can be found in the :mod:`pudl.etl.ferceqr_deployment` module. One other
major difference from the nightly builds is where data will be published.
Due to the large size of the FERC EQR dataset, we do not maintain multiple
versions of the data in S3, and instead have just one version, which will be
overwritten each time a build is executed successfully. This version can be found at
``s3://pudl.catalyst.coop/ferceqr``.
