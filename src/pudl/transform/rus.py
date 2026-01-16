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
        message = f"Found early indication of {len(dupes)} not good bad bad duplicates:\n{dupes}"
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


def multi_index_stack(
    df: pd.DataFrame,
    idx_ish: list[str],
    data_cols: list[str],
    pattern,
    match_names: list[str],
    unstack_level: list[str],
) -> pd.DataFrame:
    """Make a multi-index with a regex pattern and stack."""
    df = df.set_index(idx_ish).filter(regex=pattern)
    df.columns = pd.MultiIndex.from_frame(
        df.columns.str.extract(pattern, expand=True)
    ).set_names(match_names)
    df = df.stack(level=unstack_level, future_stack=True).reset_index()
    # remove the remaining multi-index
    df.columns = df.columns.map("".join)
    df = df.dropna(subset=data_cols, how="all")
    return df
