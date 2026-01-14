"""Code for transforming RUS data that pertains to more than one RUS Form."""

import pandas as pd

import pudl.helpers as helpers


def early_transform(raw_df: pd.DataFrame, boolean_columns_to_fix=[]) -> pd.DataFrame:
    """Standard transforms for raw RUS data."""
    df = (
        helpers.standardize_na_values(raw_df)
        # the report_month column is seemingly always 12 so this will default to Dec.
        .pipe(helpers.convert_to_date)
        .pipe(
            helpers.fix_boolean_columns,
            boolean_columns_to_fix=boolean_columns_to_fix,
        )
        .pipe(helpers.simplify_strings, ["borrower_name_rus"])
    )
    return df


def reshape(df: pd.DataFrame) -> pd.DataFrame:
    """Standard melt."""
    return df
