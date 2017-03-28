"""Tests exercising the eia923 module for use with PyTest."""

import pytest
import pudl.eia923
import pudl.constants as pc
import pandas as pd


def test_get_pages_eia923():
    """Pull in all pages of EIA923 that we expect to work."""
    eia923_xlsx = {}
    for yr in pc.eia923_working_years:
        print("Reading EIA 923 spreadsheet data for {}.".format(yr))
        eia923_xlsx[yr] = pd.ExcelFile(pudl.eia923.get_eia923_files([yr, ])[0])

    eia923_dfs = {}
    for page in ['generation_fuel', 'stocks', 'boiler_fuel', 'generator',
                 'fuel_receipts_costs', 'plant_frame']:
        eia923_dfs[page] = \
            pudl.eia923.get_eia923_page(page,
                                        eia923_xlsx,
                                        years=pc.eia923_working_years,
                                        verbose=True)
