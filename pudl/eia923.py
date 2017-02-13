import pandas as pd
import os.path

###########################################################################
# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.
###########################################################################

"""Retrieve data from EIA Form 923 for analysis.

For now this pulls from the published Excel spreadsheets. The same data
may also be available in a more machine readable form via the EIA's bulk
JSON download facility, but those files will require parsing.

@author: alana for Catalyst Cooperative
This code is for use analyzing EIA Form 923 data, years 2008-2016
Current version is for years 2014-2016, which have standardized naming conventions and file formatting
"""

# directory beneath which the EIA Form 923 data lives...
eia923_datadir = os.path.join(settings.PUDL_DIR,
                                         'data',
                                         'eia',
                                         'form923',
                                         'get_eia923.sh')


def get_eia923(years=[2014,2015,2016]):
    """Select desired spreadsheets containing string '2_3_4' for years 2014-2016 of EIA Form923 data.
    No input required; years prior to 2014 can be imported, but do not fit
    formatting used in 'parse_eia923' function
    """
    myfilelist=[] #Empty list to add xcel files to
    listfolder=[x[0] for x in os.walk(eia923_datadir)] #lists all folders inside eia923_datadir
    for folderitem in listfolder:   #folderitem is individual folder within listfolder
        for filename in os.listdir(folderitem): #filename is individual spreadsheet within folderitem
            filelist=[] #filelist is list of all files in folderitem
            filelist.append(filename) #adds each file name to the list
            filepath = os.path.join(folderitem, filename) #creates a full path (as string) to file
            for fileitem in filelist:
                if '2_3_4' in fileitem: #2_3_4 is the string that all desired spreadsheets have in common for years 2009-2016
                    for year in years:
                        if str(year) in filepath:
                            myfilelist.append(filepath) #Saves files in filelist that fit the 'if' terms to the myFileList outside of loop
    return myfilelist #returns list of excel file paths


def parse_eia923(tabname, years=[2014,2015,2016]):
    """Utilize get_eia923 function to parse 1 tab of EIA Form 923.

    Only 1 tabname input allowed (i.e. "generation_fuel", "stocks","boiler_fuel", "generator",
    "fuel_receipts_costs", or "plant_frame")
    """
    df = pd.DataFrame()
    tabmap = {"generation_fuel": 0, #these are tabname inputs mapping to excel spreadsheet tabs
              "stocks":1,
              "boiler_fuel":2,
              "generator":3,
              "fuel_receipts_costs":4,
              "plant_frame":5}
    rowskip = {"generation_fuel": 5, #number of header rows to skip varies by tab
              "stocks":5,
              "boiler_fuel":5,
              "generator":5,
              "fuel_receipts_costs":4,
              "plant_frame":4}
    files=get_eia923()
    for file in files:
        for year in years:
            if str(year) in file:
                data = pd.read_excel(file, sheetname=tabmap[tabname], skiprows=rowskip[tabname])
                data["YEAR"]=year #"stocks" tab is missing YEAR column; this ensures all tabs have YEAR data
        df = df.append(data)
    return df #returns data frame of all years for input tabname# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.
