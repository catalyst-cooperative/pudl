<a id="ferceqr-data-builds"></a>

# FERC EQR Data Builds

The FERC EQR ETL uses a very similar setup for running remote cloud builds as the
[Nightly Data Builds](nightly_data_builds.html.md), however these builds do not currently run on an
automated schedule, and must be started manually. Like the nightly builds, FERC EQR
builds use a GitHub action to kick off a [Google Batch](https://cloud.google.com/batch/docs) job, which will execute the ETL using the same
Docker container as the nightly builds.

## Notable differences from nightly builds

While the FERC EQR builds use much of the same infrastructure as nightly builds, there
are a few notable differences. First, the data distribution is all done in Python using
Dagster tooling. More information on how this works can be found in
[`pudl.dagster.assets.deploy.ferceqr`](../autoapi/pudl/dagster/assets/deploy/ferceqr/index.html.md#module-pudl.dagster.assets.deploy.ferceqr) together with the supporting resources in
[`pudl.dagster.resources`](../autoapi/pudl/dagster/resources/index.html.md#module-pudl.dagster.resources) and sensor in [`pudl.dagster.sensors`](../autoapi/pudl/dagster/sensors/index.html.md#module-pudl.dagster.sensors). One other major
difference from the nightly builds is where data will be published. Due to the large
size of the FERC EQR dataset, we do not maintain multiple versions of the data in S3,
and instead have just one version, which will be overwritten each time a build is
executed successfully. This version can be found at `s3://pudl.catalyst.coop/ferceqr`.

Before anything is published, the `deploy_ferceqr` asset runs the dbt data
validations for the FERC EQR tables (see [Data validation quickstart](data_validation_quickstart.html.md)). If any
fail, nothing is deployed and the build is reported as failed. The
`ferceqr_datapackage` asset writes the Frictionless `datapackage.json` describing
the outputs, which is published next to the data at
`s3://pudl.catalyst.coop/ferceqr/datapackage.json`.

Each quarterly input archive is also verified against the md5 checksum recorded in the
FERC EQR metadata archived on Zenodo before it is extracted. Extracting every quarter
downloads ~100 GB, so reading from remote storage must be enabled explicitly with
`PUDL_FERCEQR_ALLOW_DOWNLOADS=true`, which the build workflow sets.

The EQR extraction and transformation are implemented in DuckDB rather than pandas, so
unlike the nightly build this pipeline is limited more by CPU parallelism than by
memory. Its Batch VM is sized accordingly, with proportionally more CPU and less RAM
than the nightly build VM.
