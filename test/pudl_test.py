"""
Tests excercising the pudl module for use with PyTest.

Run this test by with 'pytest -s test/test_pudl.py'
"""

import pytest
from pudl import pudl, ferc1, eia923, eia860, settings, constants
from pudl import models, models_ferc1, models_eia923, models_eia860


def test_init_db():
    """Create a fresh PUDL DB and pull in some FERC1 & EIA923 data."""
    ferc1.init_db(ferc1_tables=constants.ferc1_default_tables,
                  refyear=max(constants.ferc1_working_years),
                  years=constants.ferc1_working_years,
                  def_db=True,
                  verbose=True,
                  testing=True)

    pudl.init_db(ferc1_tables=constants.ferc1_pudl_tables,
                 ferc1_years=constants.ferc1_working_years,
                 eia923_tables=constants.eia923_pudl_tables,
                 eia923_years=constants.eia923_working_years,
                 eia860_tables=constants.eia860_pudl_tables,
                 eia860_years=constants.eia860_working_years,
                 verbose=True,
                 debug=False,
                 testing=True)

    ferc1_engine = ferc1.db_connect_ferc1(testing=True)
    ferc1.drop_tables_ferc1(ferc1_engine)

    pudl_engine = pudl.db_connect_pudl(testing=True)
    pudl.drop_tables_pudl(pudl_engine)
