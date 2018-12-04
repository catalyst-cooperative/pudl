"""Limited testing of the PUDL DB ETL for use with Travis CI."""
import pytest
import pandas as pd
from pudl import constants as pc

START_DATE_CI = pd.to_datetime(
    '{}-01-01'.format(max(pc.working_years['eia923'])))
END_DATE_CI = pd.to_datetime(
    '{}-12-31'.format(max(pc.working_years['eia923'])))


@pytest.mark.travis_ci
def test_ferc1_init_db(ferc1_engine_travis_ci):
    """
    Create a fresh FERC Form 1 DB and attempt to access it.

    If we are doing ETL (ingest) testing, then these databases are populated
    anew, in their *_test form.  If we're doing post-ETL (post-ingest) testing
    then we just grab a connection to the existing DB.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the fixtures defined in conftest.py
    """
    pass


@pytest.mark.travis_ci
def test_pudl_init_db(pudl_engine_travis_ci):
    """
    Create a fresh PUDL DB and pull in some FERC1 & EIA data.

    If we are doing ETL (ingest) testing, then these databases are populated
    anew, in their *_test form.  If we're doing post-ETL (post-ingest) testing
    then we just grab a connection to the existing DB.

    Nothing needs to be in the body of this "test" because the database
    connections are created by the fixtures defined in conftest.py
    """
    pass


# @pytest.fixture(scope='module')
# def output_monthly_minimal(live_pudl_db):
#    pudl_out = PudlTabl(
#        freq='MS', testing=(not live_pudl_db),
#        start_date=START_DATE_CI, end_date=END_DATE_CI
#    )
#    return pudl_out
#
#
# @pytest.mark.travis_ci
# @pytest.mark.post_etl
# @pytest.mark.skip(reason="Still debuting the post-ETL Travis CI tests.")
# def test_capacity_factor(output_monthly_minimal):
#    """Test the capacity factor calculation."""
#    cf = pudl.analysis.mcoe.capacity_factor(output_monthly_minimal)
#    print("capacity_factor: {} records found".format(len(cf)))
