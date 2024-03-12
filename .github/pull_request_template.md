<!--
Resources:
* contributing guidelines: https://catalystcoop-pudl.readthedocs.io/en/latest/CONTRIBUTING.html
* code of conduct: https://catalystcoop-pudl.readthedocs.io/en/latest/code_of_conduct.html
-->
# Overview

Closes #XXXX.

What problem does this address?

What did you change?

# Testing

How did you make sure this worked? How can a reviewer verify this?

```[tasklist]
# To-do list
- [ ] If updating analyses or data processing functions: make sure to update or write data validation tests (e.g.,  `test_minmax_rows()`)
- [ ] Update the [release notes](../docs/release_notes.rst): reference the PR and related issues.
- [ ] Ensure docs build, unit & integration tests, and test coverage pass locally with `make pytest-coverage` (otherwise the merge queue may reject your PR)
- [ ] Review the PR yourself and call out any questions or issues you have
- [ ] For minor ETL changes or data additions, once `make pytest-coverage` passes, make sure you have a fresh full PUDL DB downloaded locally, materialize new/changed assets and all their downstream assets and [run relevant data validation tests](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html#data-validation) using `pytest` and `--live-dbs`.
- [ ] For significant ETL, data coverage or analysis changes, once `make pytest-coverage` passes, ensure the full ETL runs locally and [run data validation tests](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html#data-validation) using `make pytest-validate` (a ~10 hour run). If you can't run this locally, run the `build-deploy-pudl` GitHub Action (or ask someone with permissions to). Then, check the logs on the `#pudl-deployments` Slack channel or [builds.catalyst.coop](builds.catalyst.coop).
```
