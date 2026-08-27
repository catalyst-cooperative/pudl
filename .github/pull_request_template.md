<!--
Resources:
* contributing guidelines: https://docs.catalyst.coop/pudl/en/nightly/CONTRIBUTING.html
* code of conduct: https://docs.catalyst.coop/pudl/en/nightly/code_of_conduct.html
-->

# Overview

Closes #XXXX.

## What problem does this address?

## What did you change?

## Documentation

Make sure to update relevant aspects of the documentation:

- [ ] Update `docs/release_notes.rst`
- [ ] Update relevant Data Source jinja templates (see `docs/data_sources/templates`).
- [ ] Update relevant table or source description metadata (see `src/metadata`).
- [ ] Review and update any other aspects of the documentation that might be affected by this PR.

# Testing

How did you make sure this worked? How can a reviewer verify this?

## To-do list

- [ ] Run `pixi run pytest-unit` and `pixi run pytest-integration` (2-5 minutes total) and fix any issues that come up.
- [ ] For PRs that alter manually specified dbt data tests or any table schema, update the dbt `schema.yml` files using `dbt_helper update-tables --schema`
- [ ] When you think the PR is done, run `pixi run pytest-ci` (~45 minutes) to ensure that the merge queue will accept your changes.
- [ ] Review the PR yourself and call out any questions or issues you have.
- [ ] For PRs that change the row counts of any table, update `dbt/seeds/etl_full_row_counts.csv` using `dbt_helper update-tables --row-counts`.
- [ ] Run `pixi run prek-run` to run linters and static code analysis checks.
- [ ] For PRs that change the PUDL outputs significantly, run the full ETL locally and then [run the data validations](https://docs.catalyst.coop/pudl/en/nightly/dev/data_validation_quickstart.html) using dbt. If you can't run the ETL locally then run the `build-pudl` GitHub Action manually and ensure that it succeeds.
