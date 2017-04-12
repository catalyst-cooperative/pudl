"""Tests excercising the ferc1 module for use with PyTest."""

import pytest
import pudl.ferc1
from pudl import ferc1, settings, constants


def test_init_db():
    """Create a fresh FERC Form 1 DB and attempt to access it."""
    ferc1.init_db(ferc1_tables=constants.ferc1_default_tables,
                  refyear=2015,
                  years=constants.ferc1_working_years,
                  def_db=True,
                  verbose=True,
                  testing=True)

    ferc1_engine = ferc1.db_connect_ferc1(testing=True)
    pudl.ferc1.drop_tables_ferc1(ferc1_engine)
