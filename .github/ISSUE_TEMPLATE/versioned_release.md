---
name: Versioned Release Checklist
about: The process for creating a new release.
title: Release vYYYY.MM.DD Checklist
labels:
  - metadata
  - packaging
  - release
  - zenodo
assignees: ""
---

[Additional release process documentation](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/run_a_release.html).

```[tasklist]
## Release Checklist
- [ ] Set a release date & notify team
- [ ] Close out the [PUDL Release Notes](https://catalystcoop-pudl.readthedocs.io/en/latest/release_notes.html) with an overview of the changes in this release
- [ ] Push vYYYY.MM.DD tag
- [ ] Verify that the new [GitHub (software) release](https://github.com/catalyst-cooperative/pudl/releases) has been published
- [ ] Verify RTD build for new version is up to date (activate new version first in [RTD admin panel](https://readthedocs.org/projects/catalystcoop-pudl/versions/))
- [ ] Verify [`catalystcoop.pudl` PyPI (software) release](https://pypi.org/project/catalystcoop.pudl/)
- [ ] Verify that [PUDL repo archive on Zenodo](https://zenodo.org/doi/10.5281/zenodo.3404014) has been updated w/ new version
- [ ] Wait 6-10 hours for a successful build to complete
- [ ] Verify that `stable` and the version tag point at same git ref
- [ ] Verify that [`stable` docs on RTD](https://catalystcoop-pudl.readthedocs.io/en/stable/) have been updated
- [ ] Verify `gs://pudl.catalyst.coop/vYYYY.MM.DD` has the new expected data.
- [ ] Verify `gs://pudl.catalyst.coop/stable` has the new expected data.
- [ ] Verify `s3://pudl.catalyst.coop/vYYYY.MM.DD` has the new expected data.
- [ ] Verify `s3://pudl.catalyst.coop/stable` has the new expected data.
- [ ] Verify that draft deposition for [this Zenodo archive](https://doi.org/10.5281/zenodo.3653158) has the new expected data
- [ ] Update Zenodo description and other metadata based on release notes (see task list below)
- [ ] Manually publish the new Zenodo deposition with the updated metadata
- [ ] Create an Announcement for the release in [our GitHub Discussions](https://github.com/orgs/catalyst-cooperative/discussions)
- [ ] Update the [release documentation](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/run_a_release.html) to better reflect the actual process for next time
- [ ] Wait 2-12 hours for a bot to create a PR in the [PUDL conda-forge feedstock](https://github.com/conda-forge/catalystcoop.pudl-feedstock/pulls), and then review and merge it.
- [ ] Review the `conda-forge` PR, updating dependencies and CLI entrypoints if necessary. Our direct dependencies (from `pyproject.toml`) should be pinned to the actual version appearing in `environments/conda-linux-64.lock.yml`
```

```[tasklist]
### Zenodo Metadata Tasks
- [ ] Make sure all Catalyst members are listed in creators (automate!)
- [ ] Update Zenodo deposition description to reflect current release (automate!)
- [ ] Set correct version for Zenodo data release archive (automate!)
- [ ] Set keywords for Zenodo data release archive (automate!)
- [ ] Set language of Zenodo data release archive (automate!)
- [ ] Zenodo PUDL repo archive: update release notes (cut-and-paste from GitHub)
- [ ] Zenodo PUDL repo archive: add keywords (automate with a `.zenodo.json` file for the repo)
- [ ] Zenodo PUDL repo archive: set language to English (automate with a `.zenodo.json` file for the repo)
```

```[tasklist]
### Issues that we ran into
- [ ] Take notes here during the release if stuff happens.
```
