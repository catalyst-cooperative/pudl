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

Full documentation of process [here](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/run_a_release.html).

```[tasklist]
## Release Checklist
- [ ] set a release date & notify team
- [ ] Push vYYYY.MM.DD tag
- [ ] Verify that the GitHub (software) release has been published
- [ ] Verify RTD build for new version is up to date (activate new version first in [RTD admin panel](https://readthedocs.org/projects/catalystcoop-pudl/versions/))
- [ ] Verify [`catalystcoop.pudl` PyPI (software) release](https://pypi.org/project/catalystcoop.pudl/)
- [ ] Verify that [PUDL repo archive on Zenodo](https://zenodo.org/doi/10.5281/zenodo.3404014) has been updated w/ new version
- [ ] Wait 6-10 hours for a successful build to complete
- [ ] Verify `stable` and version tag point at same git ref
- [ ] Verify that RTD docs build for `stable` branch gets updated
- [ ] Verify `gs://pudl.catalyst.coop/vYYYY.MM.DD` has the right data.
- [ ] Verify `gs://pudl.catalyst.coop/stable` has the right data.
- [ ] Verify `s3://pudl.catalyst.coop/vYYYY.MM.DD` has the right data.
- [ ] Verify `s3://pudl.catalyst.coop/stable` has the right data.
- [ ] Verify Zenodo draft deposition has expected data
- [ ] Update Zenodo description and other metadata based on release notes / details
- [ ] Manually publish the new Zenodo deposition
- [ ] Create a GitHub Discussions Announcement (should automate with `gh release create` command, eventually)
- [ ] Update [release documentation](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/run_a_release.html) to better reflect the actual process
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
- [ ] take notes here during the release if stuff happens.
```
