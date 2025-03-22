<!--
Resources:
* contributing guidelines: https://catalystcoop-pudl.readthedocs.io/en/nightly/CONTRIBUTING.html
* code of conduct: https://catalystcoop-pudl.readthedocs.io/en/nightly/code_of_conduct.html
-->

# Overview

Closes #XXXX.

## What problem does this address?

## What did you change?

## Documentation

Make sure to update relevant aspects of the documentation:

- [ ] Update the [release notes](https://catalystcoop-pudl.readthedocs.io/en/nightly/release_notes.html): reference the PR and related issues.
- [ ] Update relevant Data Source jinja templates (see `docs/data_sources/templates`).
- [ ] Update relevant table or source description metadata (see `src/metadata`).
- [ ] Review and update any other aspects of the documentation that might be affected by this PR.

# Testing

How did you make sure this worked? How can a reviewer verify this?

## To-do list

- [ ] If updating analyses or data processing functions: make sure to update row count expectations in `dbt` tests.
- [ ] Run `make pytest-coverage` locally to ensure that the merge queue will accept your PR.
- [ ] Review the PR yourself and call out any questions or issues you have.
- [ ] For minor ETL changes or data additions, once `make pytest-coverage` passes, make sure you have a fresh full PUDL DB downloaded locally, materialize new/changed assets and all their downstream assets and [run relevant data validation tests](https://catalystcoop-pudl.readthedocs.io/en/nightly/dev/testing.html#data-validation) using `pytest` and `--live-dbs`.
- [ ] For bigger ETL or data changes run the full ETL locally and then [run the data validations](https://catalystcoop-pudl.readthedocs.io/en/nightly/dev/testing.html#data-validation) using `make pytest-validate`.
- [ ] Alternatively, run the `build-deploy-pudl` GitHub Action manually.
