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
- [ ] For PRs that change the PUDL outputs significantly, run the full ETL locally and then [run the data validations](https://catalystcoop-pudl.readthedocs.io/en/nightly/dev/data_validation.html) using dbt. If you can't run the ETL locally then run the `build-deploy-pudl` GitHub Action manually and ensure that it succeeds.
