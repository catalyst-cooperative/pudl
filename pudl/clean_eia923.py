
"""Routines specific to cleaning up EIA Form 923 data."""

import pandas as pd


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
