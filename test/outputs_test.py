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


@pytest.mark.ferc1
@pytest.mark.tabular_output
@pytest.mark.post_etl
def test_ferc1_output(live_pudl_db):
    """Test output routines for tables from FERC Form 1."""
    testing = (not live_pudl_db)
    print("Compiling FERC Form 1 Plants & Utilities table...")
    pu_ferc = outputs.plants_utils_ferc1(testing=testing)
    print("    {} records found.".format(len(pu_ferc)))

    print("Compiling FERC Form 1 Fuel table...")
    fuel_out = outputs.fuel_ferc1(testing=testing)
    print("    {} records found.".format(len(fuel_out)))

    print("Compiling FERC Form 1 Steam Plants table...")
    steam_out = outputs.plants_steam_ferc1(testing=testing)
    print("    {} records found.".format(len(steam_out)))


@pytest.mark.tabular_output
@pytest.mark.eia923
@pytest.mark.eia860
@pytest.mark.post_etl
def test_eia_output(live_pudl_db):
    """Test output routines for tables from across EIA data sources."""
    testing = (not live_pudl_db)
    print("Compiling EIA Plants & Utilities table...")
    pu_eia = outputs.plants_utils_eia(testing=testing)
    print("    {} records found.".format(len(pu_eia)))


@pytest.mark.tabular_output
@pytest.mark.eia923
@pytest.mark.post_etl
def test_eia923_output(live_pudl_db):
    """Test output routines for tables from EIA Form 923."""
    testing = (not live_pudl_db)
    print("Compiling EIA 923 Fuel Receipts & Costs table...")
    frc_out = outputs.fuel_receipts_costs_eia923(testing=testing)
    print("    {} records found.".format(len(frc_out)))

    print("Compiling EIA 923 Generation Fuel table...")
    gf_out = outputs.generation_fuel_eia923(testing=testing)
    print("    {} records found.".format(len(gf_out)))

    print("Compiling EIA 923 Boiler Fuel table...")
    bf_out = outputs.boiler_fuel_eia923(testing=testing)
    print("    {} records found".format(len(bf_out)))

    print("Compiling EIA 923 Generation table...")
    g_out = outputs.generation_eia923(testing=testing)
    print("    {} records found".format(len(g_out)))


@pytest.mark.tabular_output
@pytest.mark.eia860
@pytest.mark.post_etl
def test_eia860_output(live_pudl_db):
    """Test output routines for tables from EIA Form 860."""
    testing = (not live_pudl_db)
    print("Compiling EIA 860 Utilities table...")
    utils_out = outputs.utilities_eia860(testing=testing)
    print("    {} records found".format(len(utils_out)))

    print("Compiling EIA 860 Plants table...")
    plants_out = outputs.plants_eia860(testing=testing)
    print("    {} records found".format(len(plants_out)))

    print("Compiling EIA 860 Generators table...")
    gens_out = outputs.generators_eia860(testing=testing)
    print("    {} records found".format(len(gens_out)))

    print("Compiling EIA 860 Ownership table...")
    own_out = outputs.ownership_eia860(testing=testing)
    print("    {} records found".format(len(own_out)))
