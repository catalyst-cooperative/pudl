# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.

import pandas as pd
import os

"""Retrieve data from EIA Form 923 for analysis.

For now this pulls from the published Excel spreadsheets. The same data
may also be available in a more machine readable form via the EIA's bulk
JSON download facility, but those files will require parsing.

@author: alana for Catalyst Cooperative
This code is for use analyzing EIA Form 923 data, years 2008-2016
Current version is for years 2014-2016, which have standardized naming conventions and file formatting
"""

#os.chdir(os.path.join('C:\\','Users','alana','Dropbox','Catalyst_Coop', 'data', 'eia', 'form923'))
os.getcwd()
MainFolder=os.getcwd()

#Select years for analysis
#file2008= 'eia923December2008.xls' #include if using 2008 file: it has non-standard naming convention so requires manual import


def get_eia923(years=[2014,2015,2016]):
    #Selects desired spreadsheets containing string '2_3_4' for years 2014-2016 of EIA Form923 data
    #no input required; years prior to 2014 can be imported, but do not fit formatting used in 'parse_eia923' function
    myFileList=[] #Empty list to add xcel files to
    listFolder=[x[0] for x in os.walk(MainFolder)] #lists all folders inside MainFolder
    for folderItem in listFolder:   #folderItem is individual folder within listFolder
        print (folderItem)
        for filename in os.listdir(folderItem): #filename is individual spreadsheet within folderItem
            fileList=[] #fileList is list of all files in folderItem
            fileList.append(filename) #adds each file name to the list
            filepath = os.path.join(folderItem, filename) #creates a full path (as string) to file
            for fileItem in fileList:
                if '2_3_4' in fileItem: #2_3_4 is the string that all desired spreadsheets have in common for years 2009-2016
                    for year in years:
                        if str(year) in filepath:
                            myFileList.append(filepath) #Saves files in fileList that fit the 'if' terms to the myFileList outside of loop
    return myFileList #returns list of excel file paths

def parse_eia923(tabname, years=[2014,2015,2016]):
    #utilizes get_eia923 function to parse 1 tab of EIA Form 923;
    #only 1 tabname input allowed ("generation&fuel", "stocks","boiler_fuel", "generator", "fuel_receipts&costs", or "plant_frame")
    df = pd.DataFrame()
    tabmap = {"generation&fuel": 0, #these are tabname inputs mapping to excel spreadsheet tabs
              "stocks":1,
              "boiler_fuel":2,
              "generator":3,
              "fuel_receipts&costs":4,
              "plant_frame":5}
    rowskip = {"generation&fuel": 5, #number of header rows to skip varies by tab
              "stocks":5,
              "boiler_fuel":5,
              "generator":5,
              "fuel_receipts&costs":4,
              "plant_frame":4}
    files=get_eia923()
    for file in files:
        print(file.title)
        for year in years:
            if str(year) in file:
                data = pd.read_excel(file, sheetname=tabmap[tabname], skiprows=rowskip[tabname])
                data["YEAR"]=year #"stocks" tab is missing YEAR column; this ensures all tabs have YEAR data
        df = df.append(data)
    print("done")
    return df #returns data frame of all years for input tabname# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.
