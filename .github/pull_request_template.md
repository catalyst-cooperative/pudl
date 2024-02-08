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
- [ ] Ensure docs build, unit & integration tests, and test coverage pass locally with `make pytest-coverage` (otherwise the merge queue may reject your PR)
- [ ] For significant ETL changes, ensure the full ETL runs locally
- [ ] For major data coverage & analysis changes, [run data validation tests](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html#data-validation)
- [ ] If updating analyses or data processing functions: make sure to update or write data validation tests
- [ ] Update the [release notes](../docs/release_notes.rst): reference the PR and related issues.
- [ ] Review the PR yourself and call out any questions or issues you have
```
