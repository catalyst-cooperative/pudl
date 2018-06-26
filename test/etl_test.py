"""PyTest based testing of the FERC & PUDL Database initializations.

This module also contains fixtures for returning connections to the databases.
These connections can be either to the live databases (for post-ETL testing)
or to the test databases, which are created from scratch and dropped after the
tests have completed.  See the --live_ferc_db and --live_pudl_db
command line options by running pytest --help.
"""
import pytest


@pytest.mark.etl
@pytest.mark.eia860
@pytest.mark.eia923
@pytest.mark.ferc1
def test_pudl_init_db(ferc1_engine, pudl_engine):
    """
    Create a fresh PUDL DB and pull in some FERC1 & EIA data.

    If we are doing ETL (ingest) testing, then these databases are populated
    anew, in their *_test form.  If we're doing post-ETL (post-ingest) testing
    then we just grab a connection to the existing DB.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the fixtures defined in conftest.py
    """
    pass


@pytest.mark.etl
@pytest.mark.ferc1
def test_ferc1_init_db(ferc1_engine):
    """
    Create a fresh FERC Form 1 DB and attempt to access it.

    If we are doing ETL (ingest) testing, then these databases are populated
    anew, in their *_test form.  If we're doing post-ETL (post-ingest) testing
    then we just grab a connection to the existing DB.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the fixtures defined in conftest.py
    """
    pass
