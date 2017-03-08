"""Tests excercising the pudl module for use with PyTest."""

import pytest
import pudl.pudl
import pudl.ferc1
import pudl.constants as pc


def test_init_db():
    """Create a fresch PUDL DB and pull in some FERC1 & EIA923 data."""
    pudl.ferc1.init_db(refyear=2015, years=[2015, ], def_db=True,
                       verbose=True, testing=True)
    pudl.pudl.init_db(ferc1_tables=pc.ferc1_pudl_tables,
                      ferc1_years=[2015, ],
                      eia923_tables=pc.eia923_pudl_tables,
                      eia923_years=[2015, ],
                      verbose=True, debug=False, testing=True)

    ferc1_engine = pudl.ferc1.db_connect_ferc1(testing=True)
    pudl.ferc1.drop_tables_ferc1(ferc1_engine)

    pudl_engine = pudl.pudl.db_connect_pudl(testing=True)
    pudl.pudl.drop_tables_pudl(pudl_engine)
