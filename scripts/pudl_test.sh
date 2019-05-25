#!/bin/sh
# This isn't *really* a script (thought maybe it should be made into one)
# Rather, it's meant to record the steps required to instantiate the DB and
# make sure everything that should be working really is working along the way.

# Test the datastore update process:
pytest --disable-warnings test/datastore_test.py

# If that worked, go ahead and populate the datastore with update_datastore.py

# Test the ETL Process:
pytest --disable-warnings test/etl_test.py

# If that worked, go ahead and populate the database with init_pudl.py

# Now run some tests that access the database and sanity check the data:
pytest --disable-warnings --live_pudl_db test/validation/

# Attempt to run some notebooks which also exercise the data, and maybe also
# generate some useful outputs (CSV files or pretty plots...)
# The --nbval-lax flag tells pytest only to check for a lack of errors in the
# notebook cells, rather than exact output matching -- unless a cell has been
# specifically marked with # NBVAL_CHECK_OUTPUT
pytest --disable-warnings --nbval-lax test/notebooks/
pytest --disable-warnings --nbval-lax docs/notebooks/
