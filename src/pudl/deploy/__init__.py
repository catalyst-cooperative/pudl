"""Post-ETL deployment: publish outputs, trigger releases, and update services.

This package handles everything that happens after a successful ETL build:
copying outputs to public cloud storage, triggering Zenodo data releases,
updating git branches, applying GCS holds on versioned artifacts, and
updating the PUDL Viewer Cloud Run service.

Modules:

- ``pudl`` — core deployment helpers for full PUDL ETL builds: preparing and
  uploading outputs to GCS/S3, updating ``nightly``/``stable`` git branches,
  triggering Zenodo releases, and redeploying the PUDL Viewer service.
- ``ferceqr`` — Dagster assets for the FERC EQR deployment pipeline: publishing
  transformed EQR Parquet outputs to cloud storage, sending Slack notifications,
  and writing the status files that signal completion to the batch job.
"""
