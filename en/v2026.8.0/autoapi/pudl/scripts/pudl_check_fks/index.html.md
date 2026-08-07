# pudl.scripts.pudl_check_fks

Check that foreign key constraints in the PUDL database are respected.

## Functions

| [`main`](#pudl.scripts.pudl_check_fks.main)(→ int)   | Check that foreign key constraints in the PUDL database are respected.   |
|------------------------------------------------------|--------------------------------------------------------------------------|

## Module Contents

### pudl.scripts.pudl_check_fks.main(logfile: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), loglevel: [str](https://docs.python.org/3/library/stdtypes.html#str), db_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [int](https://docs.python.org/3/library/functions.html#int)

Check that foreign key constraints in the PUDL database are respected.

Dagster manages the dependencies between various assets in our ETL pipeline,
attempting to materialize tables only after their upstream dependencies have been
satisfied. However, this order is non deterministic because they are executed in
parallel, and doesn’t necessarily correspond to the foreign-key constraints within
the database, so durint the ETL we disable foreign key constraints within
`pudl.sqlite`.

However, we still expect foreign key constraints to be satisfied once all of the
tables have been loaded, so we check that they are valid after the ETL has
completed. This script runs the same check.
