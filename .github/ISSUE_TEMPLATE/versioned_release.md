---
name: PUDL Data Release
about: Create a new versioned PUDL data release for distribution.
title: PUDL Data Release vYYYY.MM.x
labels:
  - metadata
  - packaging
  - release
  - zenodo
assignees: ""
---

[Additional release process documentation](https://catalystcoop-pudl.readthedocs.io/en/nightly/dev/run_a_release.html).

## Release Checklist

- [ ] Set a release date & notify team
- [ ] Update our CITATION.cff file with the new release date and current Catalyst membership.
- [ ] Update our .zenodo.json file with current Catalyst membership.
- [ ] Close out the [PUDL Release Notes](https://catalystcoop-pudl.readthedocs.io/en/nightly/release_notes.html) with an overview of the changes in this release
- [ ] Merge those changes into `main`
- [ ] Verify that all `stable` commits are on `main` with `git log main..stable`
- [ ] Tag the release `git tag -as -m "PUDL vYYYY.M.x" vYYYY.M.x`
- [ ] Push the release tag to `main`: `git push origin vYYYY.M.x`
- [ ] Verify that the new [GitHub (software) release](https://github.com/catalyst-cooperative/pudl/releases) has been published
- [ ] Verify that [PUDL repo archive on Zenodo](https://zenodo.org/doi/10.5281/zenodo.3404014) has been updated w/ new version
- [ ] Wait several hours for a successful build to complete ([monitor here](https://console.cloud.google.com/monitoring/dashboards/builder/992bbe3f-17e6-49c4-a9e8-8f1925d4ec24))
- [ ] Activate new version on the [RTD admin panel](https://app.readthedocs.org/projects/catalystcoop-pudl/) and verify that it builds successfully.
- [ ] Verify that `stable` and the version tag point at same git ref
- [ ] Verify that [`stable` docs on RTD](https://catalystcoop-pudl.readthedocs.io/en/stable/) have been updated
- [ ] Verify `gs://pudl.catalyst.coop/vYYYY.M.x` has the new expected data.
- [ ] Verify `gs://pudl.catalyst.coop/stable` has the new expected data.
- [ ] Verify `s3://pudl.catalyst.coop/vYYYY.M.x` has the new expected data.
- [ ] Verify `s3://pudl.catalyst.coop/stable` has the new expected data.
- [ ] Verify that draft deposition for [this Zenodo archive](https://doi.org/10.5281/zenodo.3653158) has the new expected data
- [ ] Update Zenodo description and other metadata based on release notes (see task list below)
- [ ] Manually publish the new Zenodo deposition with the updated metadata
- [ ] Create an Announcement for the release in [our GitHub Discussions](https://github.com/orgs/catalyst-cooperative/discussions)
- [ ] Update the [release documentation](https://catalystcoop-pudl.readthedocs.io/en/nightly/dev/run_a_release.html) to better reflect the actual process for next time

### Zenodo Metadata Tasks

- [ ] Make sure all Catalyst members are listed in creators (automate!)
- [ ] Update Zenodo deposition description to reflect current release (automate!)
- [ ] Set correct version for Zenodo data release archive (automate!)
- [ ] Set keywords for Zenodo data release archive (automate!)
- [ ] Set language of Zenodo data release archive (automate!)
- [ ] Zenodo PUDL repo archive: update release notes (cut-and-paste from GitHub)

### Issues that we ran into

- [ ] Take notes here during the release if stuff happens.
