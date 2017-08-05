
"""Routines specific to cleaning up EIA Form 923 data."""

import pandas as pd
import numpy as np
from pudl import clean_pudl


def yearly_to_monthly_eia923(df, md):
    """
    Convert an EIA 923 record with 12 months of data into 12 monthly records.

    Much of the data reported in EIA 923 is monthly, but all 12 months worth of
    data is reported in a single record, with one field for each of the 12
    months.  This function converts these annualized composite records into a
    set of 12 monthly records containing the same information, by parsing the
    field names for months, and adding a month field.  Non - time series data
    is retained in the same format.

    Args:
        df(pandas.DataFrame): A pandas DataFrame containing the annual
            data to be converted into monthly records.
        md(dict): a dictionary with the numbers 1 - 12 as keys, and the
            patterns used to match field names for each of the months as
            values. These patterns are also used to re - name the columns in
            the dataframe which is returned, so they need to match the entire
            portion of the column name that is month - specific.

    Returns:
        pandas.DataFrame: A dataframe containing the same data as was passed in
            via df, but with monthly records instead of annual records.
    """
    yearly = df.copy()
    all_years = pd.DataFrame()

    for y in yearly.year.unique():
        this_year = yearly[yearly.year == y].copy()
        monthly = pd.DataFrame()
        for m in md.keys():
            # Grab just the columns for the month we're working on.
            this_month = this_year.filter(regex=md[m])

            # Drop this month's data from the yearly data frame.
            this_year.drop(this_month.columns, axis=1, inplace=True)

            # Rename this month's columns to get rid of the month reference.
            this_month.columns = this_month.columns.str.replace(md[m], '')

            # Add a numerical month column corresponding to this month.
            this_month['month'] = m

            # Add this month's data to the monthly DataFrame we're building.
            monthly = pd.concat([monthly, this_month])

        # Merge the monthly data we've built up with the remaining fields in
        # the data frame we started with -- all of which should be independent
        # of the month, and apply across all 12 of the monthly records created
        # from each of the # initial annual records.
        this_year = this_year.merge(monthly, left_index=True, right_index=True)
        # Add this new year's worth of data to the big dataframe we'll return
        all_years = pd.concat([all_years, this_year])

    return(all_years)


def coalmine_cleanup(cmi_df):
    """
    """
    cmi_df = cmi_df.copy()
    # Map mine type codes, which have changed over the years, to a few
    # canonical values:
    cmi_df['coalmine_type'].replace(
        {'[pP]': 'P', 'U/S': 'US', 'S/U': 'SU', 'Su': 'S'},
        inplace=True, regex=True)

    # Because we need to pull the coalmine_id field into the FRC table,
    # but we don't know what that ID is going to be until we've populated
    # this table... we're going to functionally end up using the data in
    # the coalmine info table as a "key."  Whatever set of things we
    # drop duplicates on will be the defacto key.  Whatever massaging we do
    # of the values here (case, removing whitespace, punctuation, etc.) will
    # affect the total number of "unique" mines that we end up having in the
    # table... and we probably want to minimize it (without creating
    # collisions).  We will need to do exactly the same transofrmations in the
    # FRC ingest function before merging these values in, or they won't match
    # up.

    # Transform coalmine names to a canonical form to reduce duplicates:
    # No leading or trailing whitespace:
    cmi_df['coalmine_name'] = cmi_df['coalmine_name'].str.strip()
    # Let's use Title Case:
    cmi_df['coalmine_name'] = cmi_df['coalmine_name'].str.title()
    # compact internal whitespace:
    cmi_df['coalmine_name'] = \
        cmi_df['coalmine_name'].replace('[\s+]', ' ', regex=True)
    # remove all internal non-alphanumeric characters:
    cmi_df['coalmine_name'] = \
        cmi_df['coalmine_name'].replace('[^a-zA-Z0-9 -]', '', regex=True)

    # Homogenize the data type that we're finding inside the coalmine_county
    # field (ugh, Excel sheets!).  Mostly these are integers or NA values,
    # but for imported coal, there are both 'IMP' and 'IM' string values.
    # This should change it all to strings that are compatible with the
    # Integer type within postgresql.
    cmi_df['coalmine_county'].replace('[a-zA-Z]+',
                                      value=np.nan,
                                      regex=True,
                                      inplace=True)
    cmi_df['coalmine_county'] = cmi_df['coalmine_county'].astype(float)
    cmi_df['coalmine_county'] = \
        clean_pudl.fix_int_na(cmi_df['coalmine_county'],
                              float_na=np.nan,
                              int_na=-1,
                              str_na='')
    return(cmi_df)


def fuel_reciept_cost_clean(frc_df):
    """
    Clean fields in fuel_reciept_cost.

    Fuel cost is reported in cents per mmbtu. Convert cents to dollars.
    """
    frc_df = frc_df.copy()
    frc_df['fuel_cost_per_mmbtu'] = frc_df['fuel_cost_per_mmbtu'] / 100
    return(frc_df)
