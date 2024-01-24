---
name: Nightly build failure report
about: Lightweight incident report for nightly build failure
title: Nightly Build Failure XXXX-XX-XX
labels:
  - bug
  - nightly-builds
assignees: ''
---

# Overview

What do you think happened, if anything?

What next steps do we need to do?

```[tasklist]
# Next steps
- [ ] verify that [S3 distribution bucket](https://s3.console.aws.amazon.com/s3/buckets/pudl.catalyst.coop?region=us-west-2&bucketType=general&prefix=nightly/&showversions=false) was updated at the expected time
- [ ] verify that [GCP distribution bucket](https://console.cloud.google.com/storage/browser/pudl.catalyst.coop/nightly;tab=objects?project=catalyst-cooperative-pudl) was updated at the expected time
- [ ] verify that [GCP internal bucket](https://console.cloud.google.com/storage/browser/builds.catalyst.coop) was updated at the expected time
- [ ] verify that [Datasette PUDL version](https://data.catalyst.coop/pudl/core_pudl__codes_datasources) points at the same hash as [nightly](https://github.com/catalyst-cooperative/pudl/tree/nightly)
- [ ] verify that [Zenodo sandbox record](https://sandbox.zenodo.org/doi/10.5072/zenodo.5563) was updated to the record number in the logs (search for `zenodo_data_release.py` and `Using metadata from` in the logs, to see what the new record number should be!)
```


# Relevant logs
[link to build logs from internal distribution bucket]()

```
```
