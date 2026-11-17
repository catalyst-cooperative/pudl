# FERC EQR validation tests: working notes

Temporary handoff notes (delete along with `eqr_dictionary.pdf` when done).

## Goal

Add dbt data validation tests to the `core_ferceqr__*` tables based on the EQR Data
Dictionary v4.0 (`eqr_dictionary.pdf`, Docket RM23-9-000). Categorical columns are
already tested elsewhere, so they are out of scope.

## Where things live

- Test definitions: `dbt/schema_inputs/ferceqr/ferceqr/<table>/schema.human.yml`
- Generated (check in, don't hand-edit): `dbt/models/ferceqr/ferceqr/<table>/schema.yml`
- Regenerate: `pixi run --frozen dbt_helper update-tables --schema <table>`
- Run: `pixi run --frozen dbt_helper validate --select "source:ferceqr.<table>"`
- Commit with `pixi run --frozen git commit ...` so `prek` is on `PATH`.

## Test pattern

Most tests use the existing `expect_column_fraction_outside_range` generic test, with a
SQL *expression* passed as `column_name`, e.g. `CAST(a < b AS INTEGER)` with
`min_value: 0`, `max_value: 0` and `max_fraction_above: <tol>`. The fractions are
of non-null expression values. Filers make mistakes, so almost every rule needs a
small tolerance rather than being strict. Strict rules use
`dbt_expectations.expression_is_true`. Thresholds were set just above the maximum
observed rate and the observed rates are recorded in YAML comments.

## Done so far

`core_ferceqr__transactions` (all passing locally):

- Charge identity: `total_transaction_charge` is within $1 of
  `transaction_quantity * price + total_transmission_charge` (<=0.2% outside).
- Dates: `end >= begin` (strict); begin date falls in `year_quarter` (1e-6 tolerance,
  one midnight boundary row in 2016q4); `trade_date` vs begin date (<=2% after,
  <=1% more than 10 years before).
- Signs: standardized quantity/price signs agree with reported ones (<=0.1% and
  <=0.01%). No non-negativity tests: negative quantities (10-29%, some filers
  report everything negative) and prices (2-6%) are legitimate.

`core_ferceqr__contracts` (all passing locally):

- Dates: termination before commencement (<=1.5%); actual termination before
  commencement (<=2%); actual termination after end of filing quarter (<=0.1%);
  `end_date < begin_date` (<=1%); execution date after quarter end (<=1%) or >~68
  years before it (<=0.2%).

## Findings / not done

- **Duplicate keys** (not tested, investigate first, may be transform bugs):
  `(year_quarter, seller_company_id_ferc, transaction_unique_id)` and
  `(..., contract_unique_id)` are not unique (44k and 194k duplicates in 2013q3-q4).
  The dictionary says unique IDs are "dependent on future system design".
- **Standardized columns**: populated for many products other than ENERGY, CAPACITY and
  BOOKED OUT POWER (9.7M rows in 2013), contrary to the dictionary. About 0.2%
  of those three products are missing them.
- **ID formats**: legacy company IDs like `D004308` (not `C` + 6 digits) and agent
  filer IDs like `FA1` (not just `FS<n>`). Any format test needs `^[CD]\d{6}$` or
  per-year handling.
- Rules checked and rejected for contracts: execution date after commencement (5-6%
  of rows, retroactive amendments), `begin_date` < commencement (2-15%), `end_date` >
  termination date (1-2.6%), negative contract `rate` (up to 18%, 67% for OTHER).
- Negative `total_transmission_charge`: 3% in 2013-16, 0.2% since 2019. Too loose to be
  useful as a test.
- The local data covers only 8 quarters: 2013q3, 2013q4, 2014q4, 2016q4, 2019q4, 2022q1,
  2024q1, 2026q3. Thresholds should be rechecked against all quarters.

## Remaining ideas

- Contracts: LT/ST term name vs. duration (LT ~1.4k and ST ~4.7k violations in 2013, ~1-2%);
  `rate_minimum <= rate_maximum` (~0.04%); ID format `^C\d+$` (0 violations);
  `units` vs. `rate_units` consistency; quantity sign (<=1.4%).
- Transactions: standardized price ≈ price when `rate_units` is `$/MWH` and product is
  ENERGY; standardized-column missingness for the three products above
  (`expect_missingness_between`).
- `quarterly_identity`: ID/email formats, uniqueness of `(year_quarter, company_id_ferc,
  filer_unique_id)`.
- `quarterly_index_pub`: key uniqueness; FK to `quarterly_identity`.
- Cross-table: seller IDs in transactions/contracts exist in `quarterly_identity` per
  quarter; transactions' `(seller, contract_service_agreement_id)` appear in contracts
  (fraction test); seller name <-> ID one-to-one; row counts per partition and
  quarter-over-quarter ratios.
- Release notes (`docs/release_notes.rst`) once the set is final; run `pixi run prek-run`.

## Environment gotchas

- `$PUDL_OUTPUT/pudl_dbt_tests.duckdb` may be locked by another process (host). Work
  around by running with a scratch output dir:
  `mkdir /tmp/pudl_out && ln -s $PUDL_OUTPUT/parquet /tmp/pudl_out/parquet`, then
  `PUDL_OUTPUT=/tmp/pudl_out pixi run --frozen dbt_helper validate ...`.
- The full transactions table is ~1B rows locally, so the not-all-null test takes minutes.
- An unrelated uncommitted change to `src/pudl/scripts/dbt_helper.py` (strip trailing
  `/` from `parquet_base_path`) was in the working tree and is not part of this work.
