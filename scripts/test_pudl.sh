#!/bin/sh
# This isn't *really* a script (thought maybe it should be made into one)
# Rather, it's meant to record the steps required to instantiate the DB and
# make sure everything that should be working really is working along the way.

# Test the datastore update process:
# Run this from within the top level repository directory:
pytest --disable-warnings test/datastore_test.py

# If that worked, go ahead and populate the datastore...
# Note that this may take a while unless you have a fast connection.
# Run this from within the scripts directory:
update_datastore.py --sources eia923 eia860 ferc1 epacems

# Do a minimal ETL Test:
# Run this from within the top level repository directory:
pytest --disable-warnings test/travis_ci_test.py

# Test the ETL Process more fully...:
# Run this from within the top level repository directory:
pytest --disable-warnings test/etl_test.py

# If that all worked, go ahead and populate the real FERC form 1 DB
# Run this from within the scripts directory:
ferc1_to_sqlite.py settings_ferc1_to_sqlite_default.yml

# Load the real PUDL DB. Note that you will need to edit the settings
# file to choose which data you want to load.
# Run this from within the scripts directory:
init_pudl.py settings_init_pudl_YOUR-EDITED-FILENAME.yml

# Now run some tests that access the PUDL database and sanity check the data:
# Run this from within the top level repository directory:
pytest --disable-warnings --live_ferc_db=AUTO --live_pudl_db=AUTO test/validation/

# Attempt to run some notebooks which also exercise the data,
# The --nbval-lax flag tells pytest only to check for a lack of errors in the
# notebook cells, rather than exact output matching -- unless a cell has been
# specifically marked with # NBVAL_CHECK_OUTPUT
# Run this from within the top level repository directory:
pytest --disable-warnings --nbval-lax test/notebooks src/pudl/data/notebooks
