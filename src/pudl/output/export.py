"""Routines for exporting data from PUDL for use elsewhere.

Function names should be indicative of the format of the thing that's being
exported (e.g. CSV, Excel spreadsheets, parquet files, HDF5).
"""

import logging

import pandas as pd

import pudl

logger = logging.getLogger(__name__)


def annotated_xlsx(df, notes_dict, tags_dict, first_cols, sheet_name,
                   xlsx_writer):
    """Outputs an annotated spreadsheet workbook based on compiled dataframes.

    Creates annotation tab and header rows for EIA 860, EIA 923, and FERC 1
    fields in a dataframe. This is done using an Excel Writer object, which
    must be created and saved outside the function, thereby allowing multiple
    sheets and associated annotations to be compiled in the same Excel file.

    Args:
        df (pandas.DataFrame): The dataframe for which annotations are being
            created
        notes_dict (dict): dictionary with column names as keys and long
            annotations as values
        tags_dict (dict): dictionary of dictionaries with tag categories as
            keys for outer dictionary and values are dictionaries with column
            names as keys and values are tag within the tag category
        first_cols (list): ordered list of columns that should come first in
            outfile
        sheet_name (string): name of data sheet in output spreadsheet
        xlsx_writer (pandas.ExcelWriter): this is an ExcelWriter object used to
            accumulate multiple tabs, which must be created outside of
            function, before calling the first time e.g.
            "xlsx_writer = pd.ExcelWriter('outfile.xlsx')"

    Returns:
        xlsx_writer (pandas.ExcelWriter): which must be called outside the
        function, after final use of function, for writing out to excel:
        "xlsx_writer.save()"

    """
    first_cols = [c for c in first_cols if c in df.columns]
    df = pudl.helpers.organize_cols(df, first_cols)

    # Transpose the original dataframe to easily add and map tags as columns
    dfnew = df.transpose()

    # For loop where tag is metadata field (e.g. data_source or data_origin) &
    # column is a nested dictionary of column name & value; maps tags_dict to
    # columns in df and creates a new column for each tag category
    for tag, column_dict in tags_dict.items():
        dfnew[tag] = dfnew.index.to_series().map(column_dict)
    # Take the new columns that were created for each tag category and add them
    # to the index
    for tag, column_dict in tags_dict.items():
        dfnew = dfnew.set_index([tag], append=True)
    # Transpose to return data fields to columns
    dfnew = dfnew.transpose()
    # Create an excel sheet for the data frame
    dfnew.to_excel(xlsx_writer, sheet_name=str(sheet_name), na_rep='NA')
    # Convert notes dictionary into a pandas series
    notes = pd.Series(notes_dict, name='notes')
    notes.index.name = 'field_name'
    # Create an excel sheet of the notes_dict
    notes.to_excel(xlsx_writer, sheet_name=str(sheet_name) + '_notes',
                   na_rep='NA')

    # ZS: Don't think we *need* to return the xlsx_writer object here, because
    # any alternations we've made are stored within the object -- and its scope
    # exists beyond this function (since it was passed in).
    # If we *are* going to return the xlsx_writer, then we should probably be
    # making copy of it up front and not alter the one that's passed in. Either
    # idiom is common, but the mix of both might be confusing.

    # Return the xlsx_writer object, which can be written out, outside of
    # function, with 'xlsx_writer.save()'
    return xlsx_writer
