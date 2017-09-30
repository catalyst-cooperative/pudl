"""Tests excercising the ferc1 module for use with PyTest."""

import pytest
from pudl import ferc1, constants


def test_init_db():
    """Create a fresh FERC Form 1 DB and attempt to access it."""
    ferc1.init_db(ferc1_tables=constants.ferc1_default_tables,
                  refyear=max(constants.ferc1_working_years),
                  years=constants.ferc1_working_years,
                  def_db=True,
                  verbose=True,
                  testing=True)

    ferc1_engine = ferc1.db_connect_ferc1(testing=True)
    ferc1.drop_tables_ferc1(ferc1_engine)
