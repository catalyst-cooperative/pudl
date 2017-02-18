import pandas as pd
import os.path
from pudl import settings, constants

###########################################################################
# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.
###########################################################################

"""
Retrieve data from EIA Form 923 for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

@author: alana for Catalyst Cooperative.

This code is for use analyzing EIA Form 923 data, years 2008-2016 Current
version is for years 2014-2016, which have standardized naming conventions and
file formatting.

"""

def datadir(year):
    """Given a year, return path to appropriate EIA 923 data directory."""

    # These are the only years we've got...
    assert year in range(2001, 2017)
    if(year < 2008):
        return(os.path.join(settings.EIA923_DATA_DIR,'f906920_{}'.format(year)))
    else:
        return(os.path.join(settings.EIA923_DATA_DIR,'f923_{}'.format(year)))


def get_eia923_files(years=[2014,2015,2016]):
    """
    Select desired spreadsheets containing string '2_3_4' for years 2014-2016
    of EIA Form923 data. No input required; years prior to 2014 can be imported,
    but do not fit formatting used in 'parse_eia923' function.
    """
    from glob import glob

    for yr in years:
        assert(yr > 2008), "EIA923 file selection only works for 2008 & later."

    # Find all the files matching *2_3_4* within the EIA923 data directories
    # corresponding to the years that we're looking at.
    return([ glob(os.path.join(datadir(yr),'*2_3_4*'))[0] for yr in years ])

def parse_eia923(tabname, years=[2014,2015,2016]):
    """
    Read a single table from several years of EIA923 data. Return a DataFrame.

    The tabname argument must be exactly one of the following strings:
      - 'generation_fuel'
      - 'stocks'
      - 'boiler_fuel'
      - 'generator'
      - 'fuel_receipts_costs'
      - 'plant_frame'
    """
    for year in years:
        assert(year > 2013), "EIA923 parsing only works for 2014 and later."

    # these are tabname inputs mapping to excel spreadsheet tabs
    tabmap = { "generation_fuel" : 0,
               "stocks" : 1,
               "boiler_fuel" : 2,
               "generator" : 3,
               "fuel_receipts_costs" : 4,
               "plant_frame" : 5 }

    assert(tabname in tabmap.keys()), "Unrecognized tabname: {}".format(tabname)

    # rowskip indicates the number of header rows to skip, which varies by tab
    rowskip = { "generation_fuel" : 5,
                "stocks" : 5,
                "boiler_fuel" : 5,
                "generator" : 5,
                "fuel_receipts_costs" : 4,
                "plant_frame" : 4 }

    filenames = get_eia923_files(years)

    df = pd.DataFrame()
    for (year, filename) in zip(years,filenames):
        newdata = pd.read_excel(fn, sheetname=tabmap[tabname],
                                    skiprows=rowskip[tabname])

        # stocks tab is missing a YEAR column for some reason. Add it!
        if(tabname=="stocks"):
            newdata["YEAR"]=yr

        df = df.append(newdata)

    return(df)
