---
name: Nightly build failure report
about: Lightweight incident report for nightly build failure
title: Nightly Build Failure XXXX-XX-XX
labels:
  - bug
  - nightly-builds
assignees: catalyst-cooperative/inframundo
---

# Overview

What do you think happened, if anything?

# Next steps

What next steps do we need to do to understand or remediate the issue?

# Verify that everything is fixed!

Once you've applied any necessary fixes, make sure that the nightly build outputs are all in their right places.

- [ ] [S3 distribution bucket](https://s3.console.aws.amazon.com/s3/buckets/pudl.catalyst.coop?region=us-west-2&bucketType=general&prefix=nightly/&showversions=false) was updated at the expected time
- [ ] [GCP distribution bucket](https://console.cloud.google.com/storage/browser/pudl.catalyst.coop/nightly;tab=objects?project=catalyst-cooperative-pudl) was updated at the expected time
- [ ] [GCP internal bucket](https://console.cloud.google.com/storage/browser/builds.catalyst.coop) was updated at the expected time
- [ ] [Zenodo sandbox record](https://sandbox.zenodo.org/doi/10.5072/zenodo.5563) was updated to the record number reported near the end of the [associated zenodo-data-release GitHub Action logs](https://github.com/catalyst-cooperative/pudl/actions/workflows/zenodo-data-release.yml)

# Relevant logs
[link to build logs from internal distribution bucket]( PLEASE FIND THE ACTUAL LINK AND FILL IN HERE )

```
```
