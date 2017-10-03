"""
Test output module functionality.

This set of tests exercises the functions within the output module, to ensure
that changes to the code haven't broken our standard outputs. These tests
depend on there being an already initialized NON TEST database, but they only
read data from the DB, so that's safe.  To use these tests you need to have
initialized the DB successfully.
"""
import pytest
from pudl import pudl, outputs


def test_ferc1_output():
    """Test output routines for tables from FERC Form 1."""
    pudl_engine = pudl.db_connect_pudl(testing=False)

    print("Compiling FERC Form 1 Plants & Utilities table...")
    pu_ferc = outputs.plants_utils_ferc_df(pudl_engine)

    print("Compiling FERC Form 1 Fuel table...")
    fuel_out = outputs.fuel_ferc1_df(pudl_engine)

    print("Compiling FERC Form 1 Steam Plants table...")
    steam_out = outputs.plants_steam_ferc1_df(pudl_engine)


def test_eia_output():
    """Test output routines for tables from across EIA data sources."""
    pudl_engine = pudl.db_connect_pudl(testing=False)

    print("Compiling EIA Plants & Utilities table...")
    pu_eia = outputs.plants_utils_eia_df(pudl_engine)


def test_eia923_output():
    """Test output routines for tables from EIA Form 923."""
    pudl_engine = pudl.db_connect_pudl(testing=False)

    print("Compiling EIA 923 Fuel Receipts & Costs table...")
    frc_out = outputs.frc_eia923_df(pudl_engine)

    print("Compiling EIA 923 Generation Fuel table...")
    gf_out = outputs.gf_eia923_df(pudl_engine)

    print("Compiling EIA 923 Boiler Fuel table...")
    bf_out = outputs.bf_eia923_df(pudl_engine)

    print("Compiling EIA 923 Generation table...")
    g_out = outputs.g_eia923_df(pudl_engine)


def test_eia860_output():
    """Test output routines for tables from EIA Form 860."""
    pudl_engine = pudl.db_connect_pudl(testing=False)

    print("Compiling EIA 860 Generators table...")
    gens_out = outputs.gens_eia860_df(pudl_engine)

    print("Compiling EIA 860 Ownership table...")
    own_out = outputs.o_eia860_df(pudl_engine)
