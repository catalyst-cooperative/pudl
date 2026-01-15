"""Code for transforming RUS data that pertains to more than one RUS Form."""

import pandas as pd

import pudl.helpers as helpers
import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


def early_check_pk(
    df: pd.DataFrame,
    pk_early: list[str] = ["report_date", "borrower_id_rus"],
    raise_fail=True,
) -> None:
    """Check the expected primary key of the table.

    By default the expected primary key is ["report_date", "borrower_id_rus"].
    """
    dupes = df[df.duplicated(subset=pk_early, keep=False)]
    if not dupes.empty:
        message = "Found early indication of "
        if raise_fail:
            raise AssertionError(message)
        logger.warning(message)
    return


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
