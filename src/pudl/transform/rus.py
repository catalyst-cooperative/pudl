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
    pattern,
    data_cols: list[str],
    match_names: list[str],
    unstack_level: list[str],
) -> pd.DataFrame:
    """Make a multi-index with a regex pattern and stack.

    Args:
        df: table to edit.
        idx_ish: columns in df you want to set as index of stacked table.
        pattern: a regex pattern of the df's column names. This pattern should have
            match groups that correspond to the levels of the multi-index that will be
            created. One of these match groups must include the string values of the
            data_cols.
        data_cols: names of data columns - these are strings within the df's original
            column names that you will leave unstacked. The resulting dataframe will
            include these columns.
        match_names: the assigned names of each of the match groups in the pattern - in
            the order they appear in the pattern. The match group's name we won't use is
            the group containing the data_cols values - this can be named anything but for
            clarity name this 'data_cols'.
        unstack_level: list of match_names to unstack. Presumably this will be all of the
            match_names expect 'data_cols'.
    """
    df = df.set_index(idx_ish).filter(regex=pattern)
    df.columns = pd.MultiIndex.from_frame(
        df.columns.str.extract(pattern, expand=True)
    ).set_names(match_names)
    df = df.stack(level=unstack_level, future_stack=True).reset_index()
    # remove the remaining multi-index
    df.columns = df.columns.map("".join)
    df = df.dropna(subset=data_cols, how="all")
    return df
