"""Tests excercising the pudl module for use with PyTest."""

import pytest
from pudl import pudl, ferc1, eia923, settings, constants
from pudl import models, models_ferc1, models_eia923


def test_init_db():
    """Create a fresh PUDL DB and pull in some FERC1 & EIA923 data."""
    ferc1.init_db(ferc1_tables=constants.ferc1_default_tables,
                  refyear=2015,
                  years=range(2007, 2016),
                  def_db=True,
                  verbose=True,
                  testing=True)

    pudl.init_db(ferc1_tables=constants.ferc1_pudl_tables,
                 ferc1_years=range(2007, 2016),
                 eia923_tables=constants.eia923_pudl_tables,
                 eia923_years=range(2011, 2016),
                 verbose=True,
                 debug=False,
                 testing=True)

    ferc1_engine = ferc1.db_connect_ferc1(testing=True)
    ferc1.drop_tables_ferc1(ferc1_engine)

    pudl_engine = pudl.db_connect_pudl(testing=True)
    pudl.drop_tables_pudl(pudl_engine)
