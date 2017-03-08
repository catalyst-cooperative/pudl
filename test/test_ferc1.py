"""Tests excercising the ferc1 module for use with PyTest."""

import pytest
import pudl.ferc1
import pudl.constants as pc


def test_init_db():
    """Create a fresh FERC Form 1 DB and attempt to access it."""
    pudl.ferc1.init_db(refyear=2015, years=pc.ferc1_working_years,
                       def_db=True, verbose=True, testing=True)
    ferc1_engine = pudl.ferc1.db_connect_ferc1(testing=True)
    pudl.ferc1.drop_tables_ferc1(ferc1_engine)
