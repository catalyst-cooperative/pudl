<!--
Resources:
* contributing guidelines: https://catalystcoop-pudl.readthedocs.io/en/nightly/CONTRIBUTING.html
* code of conduct: https://catalystcoop-pudl.readthedocs.io/en/nightly/code_of_conduct.html
-->

# Overview

Closes #XXXX.

What problem does this address?

What did you change?

# Testing

How did you make sure this worked? How can a reviewer verify this?

```[tasklist]
# To-do list
- [ ] If updating analyses or data processing functions: make sure to update or write data validation tests (e.g. `test_minmax_rows()`)
- [ ] Update the [release notes](https://catalystcoop-pudl.readthedocs.io/en/nightly/release_notes.html): reference the PR and related issues.
- [ ] Run `make pytest-coverage` locally to ensure that the merge queue will accept your PR.
- [ ] Review the PR yourself and call out any questions or issues you have
- [ ] For minor ETL changes or data additions, once `make pytest-coverage` passes, make sure you have a fresh full PUDL DB downloaded locally, materialize new/changed assets and all their downstream assets and [run relevant data validation tests](https://catalystcoop-pudl.readthedocs.io/en/nightly/dev/testing.html#data-validation) using `pytest` and `--live-dbs`.
- [ ] For bigger ETL or data changes run the full ETL locally and then [run the data validations](https://catalystcoop-pudl.readthedocs.io/en/nightly/dev/testing.html#data-validation) using `make pytest-validate`.
- [ ] Alternatively, run the `build-deploy-pudl` GitHub Action manually.
```
