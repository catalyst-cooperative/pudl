==================================================
How to debug a quantile check
==================================================

Run the quantile check by selecting a local target and the table you want to check.
If you want to check all the tables, you can instead select all the quantile checks
by using ``test_name:expect_quantile_constraints`` in the select clause.

In this example, we're running quantile checks for table
``out_eia__monthly_generators``.

.. code-block:: console

    [pudl/dbt] $ dbt build --target etl-fast --select source:pudl.out_eia__monthly_generators
    [...]
    17:54:02  9 of 9 START test source_test_idle_capacity_pudl_out_eia__monthly_generators_ .. [RUN]
    17:54:02  9 of 9 PASS source_test_idle_capacity_pudl_out_eia__monthly_generators_ ........ [PASS in 0.02s]
    17:54:02
    17:54:02  Finished running 9 data tests in 0 hours 0 minutes and 0.81 seconds (0.81s).
    17:54:02
    17:54:02  Completed with 1 error, 0 partial successes, and 0 warnings:
    17:54:02
    17:54:02  Failure in test source_expect_quantile_constraints_pudl_out_eia__monthly_generators_capacity_factor___quantile_0_6_min_value_0_5_max_value_0_9____quantile_0_1_min_value_0_04____quantile_0_95_max_value_0_95___fuel_type_code_pudl_coal_and_capacity_factor_0_0__capacity_mw (models/output/out_eia__monthly_generators/schema.yml)
    17:54:02    Got 1 result, configured to fail if != 0
    17:54:02
    17:54:02    compiled code at target/compiled/pudl_dbt/models/output/out_eia__monthly_generators/schema.yml/source_expect_quantile_constra_a53737dceb68a29ccc347708c9467242.sql
    17:54:02
    17:54:02  Done. PASS=8 WARN=0 ERROR=1 SKIP=0 TOTAL=9

In this example, one quantile was out of bounds.

Grab the quantile that's failing by running the "compiled code at" SQL file against
the tests db.

.. code-block:: console

  [pudl/dbt] $ duckdb $PUDL_OUTPUT/pudl_dbt_tests.duckdb <target/compiled/pudl_dbt/models/output/out_eia__monthly_generators/schema.yml/source_expect_quantile_constra_a53737dceb68a29ccc347708c9467242.sql
  ┌──────────┬────────────┐
  │ quantile │ expression │
  │ varchar  │  boolean   │
  ├──────────┼────────────┤
  │ 0.1      │ false      │
  └──────────┴────────────┘

In this example, the quantile that failed was quantile 0.1.

Find out how severe it is by running the "debug_quantile_constraints" operation. You
will need the table name (grab from the "compiled code at" path) and the test name
(grab from the "Failure in test" line in the original output). Remember to specify
the same local target.

.. code-block:: console

  [pudl/dbt] $ dbt run-operation debug_quantile_constraints --target etl-fast --args "{table: out_eia__monthly_generators, test: source_expect_quantile_constraints_pudl_out_eia__monthly_generators_capacity_factor___quantile_0_6_min_value_0_5_max_value_0_9____quantile_0_1_min_value_0_04____quantile_0_95_max_value_0_95___fuel_type_code_pudl_coal_and_capacity_factor_0_0__capacity_mw}"
  17:59:42  Running with dbt=1.9.3
  17:59:42  Registered adapter: duckdb=1.9.2
  17:59:42  Found 2 models, 377 data tests, 2 seeds, 242 sources, 830 macros
  17:59:43  table: source.pudl_dbt.pudl.out_eia__monthly_generators
  17:59:43  test: expect_quantile_constraints
  17:59:43  column: capacity_factor
  17:59:43  row_condition: fuel_type_code_pudl='coal' and capacity_factor<>0.0
  17:59:43  description:
  17:59:43  quantile |    value |      min |      max
  17:59:43      0.60 |    0.545 |     0.50 |     0.90
  17:59:43      0.10 |    0.036 |     0.04 |     None
  17:59:43      0.95 |    0.826 |     None |     0.95

In this example, quantile 0.1 was expected to be at least 0.04, but was found to be
0.036, which is too low.

Locate the quantile check in the table's schema.yml file. The path is the same as the
"compiled code at" path with the heads and tails trimmed off -- copy starting from
``models/`` and stop at ``schema.yml``.

Find the column name and the row condition in the debug_quantile_constraints output.
In this example, the check we want is for column ``capacity_factor``, and it's the
entry with a row condition ``fuel_type_code_pudl='coal' and capacity_factor<>0.0``.

.. code-block:: console

  [pudl/dbt] $ $EDITOR models/output/out_eia__monthly_generators/schema.yml

Depending on the situation, from here you can:

* investigate further in a Python notebook
* fix a bug, re-run the pipeline, and repeat the check
* adjust the quantile constraints (& consider leaving a dated note for followup in
  case it gets worse)
