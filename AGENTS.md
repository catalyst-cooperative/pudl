# LLM coding agent instructions for the Public Utility Data Liberation (PUDL) Project

## PUDL Project Overview

- The PUDL Project implements a data processing pipeline that ingests raw energy system
  data from public agencies like the US Energy Information Administration (EIA) and the
  Federal Energy Regulatory Commission (FERC) and transforms it into clean, well
  organized tables for use in analysis and modeling.
- PUDL uses the Dagster data orchestration framework to manage dependencies between
  different assets, and to enable parallel execution of different portions of the data
  processing pipeline.
- The raw input data for the PUDL data processing pipeline can be found in the directory
  indicated by the `$PUDL_INPUT` environment variable. The raw inputs are downloaded as
  needed by the data pipeline, but can be pre-downloaded in bulk using the
  `pudl_datastore` command line interface.
- The PUDL data processing pipeline primarily generates Apache Parquet files as its
  outputs. These outputs can be found in `$PUDL_OUTPUT/parquet/` where `$PUDL_OUTPUT` is
  an environment variable which should be set by the user.

## Development environment tips

- PUDL uses mamba to manage its Python environment and dependencies.
- PUDL's conda environment is named `pudl-dev`.
- To run commands in the `pudl-dev` environment, prefix them with `mamba run -n pudl-dev`
- PUDL uses ruff to lint and automatically format python code. Before staging files for
  a commit, always run `mamba run -n pudl-dev pre-commit run ruff-check --all-files` and
  `mamba run -n pudl-dev pre-commit run ruff-format --all-files`
- A number of pre-commit hooks are defined in .pre-config-config.yaml.
- We try to use appropriate type annotations in function, class, and method definitions,
  but they are not yet checked or enforced. They are primarily to improve readability
  for humans, LLMs, and IDEs.

## Testing instructions

- PUDL uses pytest to manage its unit and integration tests.
- Tests should avoid using unittest and monkeypatch, and use pytest-mock.
- Rather than enumerating various test cases within a single test function, the
  tests should use the pytest.parametrize decorator to enumerate tests cases, specifying
  the appropriate success or failure or exception to be raised for each test as
  appropriate.
- Tests must be run inside the `pudl-dev` conda environment.
- For example, the unit tests can be run with `mamba run -n pudl-dev pytest test/unit`.
- We use dbt only for data validation, and NOT for data transformations. The PUDL data
  tests are under the `dbt/` directory.
- The PUDL integration tests process a substantial amount of data and take up to an hour
  to run, and so should not generally be run during development interactively.

## Code Style Guidelines

- Follow pandas naming conventions: use `df` for DataFrames, descriptive column names
- Prefer longer, readable, descriptive variable names over short, cryptic ones.
- Use explicit type hints for function parameters and returns where helpful.
- Prefer method chaining for pandas operations when it improves readability.
- Use `pathlib.Path` for file system operations instead of string concatenation.
- Follow snake_case for functions/variables, PascalCase for classes.
- Use f-strings for string formatting, including in logging statements.
- Write docstrings for all public functions/classes using Google style python
  docstrings.
- Limit lines to 88 characters for better readability.
- Do not use `print()` statements; use logging instead.

## PUDL-Specific Patterns

- Asset dependencies in Dagster should be explicit and well-documented
- In general, data validation should happen in dbt, not in Dagster asset checks.
- Sanity checks that validate assumptions about the data should be done as it is being
  transformed, with assertions failing loudly if expectations are not met.
- Use PUDL's existing utility functions in `pudl.helpers` when available.
- Raw data access should use the datastore pattern, not direct file I/O.
- Use nullable pandas dtypes (e.g. `pd.Int64Dtype()` or `pd.StringDtype()`) when
  possible, to avoid generic `object` dtypes and mixed NULL values.
- Parquet outputs should use snappy compression and pyarrow dtypes.
- Metadata describing the tables, columns, and data sources can be found in the
  `pudl.metadata` subpackage. "Resources" are tables and "Fields" are columns.
- Metadata classes defined in the `pudl.metadata.classes` module using Pydantic
  generally mirror the frictionless datapackage standard.
- Our documentation is built using Sphinx. The source files are in the `docs/`
  directory. The source files are in reStructuredText format.
- Whenever we make significant changes to the codebase, they should be noted in the PUDL
  release notes found at `docs/release_notes.rst`.

## Performance Considerations

- Use vectorized pandas operations instead of row-wise `apply` or loops.
- Consider using just-in-time compilation with numba for performance-critical code.
- Do not use inplace operations on pandas DataFrames.
- Avoid chained indexing in pandas to prevent SettingWithCopyWarning.
- Use efficient pandas merging and joining techniques, ensuring indexes are set
  appropriately.
- Avoid creating unnecessary intermediate DataFrames.
- Use categorical dtypes for columns with a limited set of values to save memory.
- Profile and optimize any code that processes large datasets.
- PUDL relies primarily on pandas for data processing, but in cases where performance or
  memory limitations are important, we may also use DuckDB or polars dataframes.
- For large datasets (>1GB), consider polars for aggregations before pandas.
- Use polars for memory-intensive operations or when pandas performance is limiting.
