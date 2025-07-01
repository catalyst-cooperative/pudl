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
- [ ] Push vYYYY.M.x tag (4-digit year, month with no leading zero, and patch version) to `main`
- [ ] Verify that the new [GitHub (software) release](https://github.com/catalyst-cooperative/pudl/releases) has been published
- [ ] Verify [`catalystcoop.pudl` PyPI (software) release](https://pypi.org/project/catalystcoop.pudl/)
- [ ] Verify that [PUDL repo archive on Zenodo](https://zenodo.org/doi/10.5281/zenodo.3404014) has been updated w/ new version
- [ ] Wait 3-5 hours for a successful build to complete
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
- [ ] Wait 2-12 hours for a bot to create a PR in the [PUDL conda-forge feedstock](https://github.com/conda-forge/catalystcoop.pudl-feedstock/pulls)
- [ ] Review the `conda-forge` PR, updating dependencies and CLI entrypoints if necessary. Our direct dependencies (from `pyproject.toml`) should be pinned to the actual version appearing in `environments/conda-linux-64.lock.yml`

### Zenodo Metadata Tasks

- [ ] Make sure all Catalyst members are listed in creators (automate!)
- [ ] Update Zenodo deposition description to reflect current release (automate!)
- [ ] Set correct version for Zenodo data release archive (automate!)
- [ ] Set keywords for Zenodo data release archive (automate!)
- [ ] Set language of Zenodo data release archive (automate!)
- [ ] Zenodo PUDL repo archive: update release notes (cut-and-paste from GitHub)

### Issues that we ran into

- [ ] Take notes here during the release if stuff happens.
