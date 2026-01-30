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
        .assign(
            borrower_name_rus=lambda x: x.borrower_name_rus.str.replace(
                ",", ""
            ).str.replace(".", "")
        )
    )
    return df


def multi_index_stack(
    df: pd.DataFrame,
    idx_ish: list[str],
    pattern,
    data_cols: list[str],
    match_names: list[str],
    unstack_level: list[str],
    drop_zero_rows: bool = False,
) -> pd.DataFrame:
    """Stack multiple data columns - create categorical columns and data columns.

    Many RUS tables are reported in a wide format, with several columns reporting
    the same type of value, but within different categories. E.g. electricity sales by
    customer class, with each customer class in a separate column, and separate sets of
    customer class columns for the dollar value of sales, and the MWh of electricity
    sold.

    This function takes those groups of columns and stacks each of them into a single
    data column creating another categorical column describing the class to which each
    record pertains.

    Args:
        df: table to edit.
        idx_ish: columns in df you want to set as index of stacked table.
        pattern: a regex pattern of the df's column names you want to stack. This
            pattern should have match groups that correspond to the levels of the
            multi-index that will be created. One of these match groups must include
            the string values of the ``data_cols``. For example, the
            ``raw_rus7__power_requirements`` table contains a set of columns which
            have sales_kwh and revenue pertaining to many different customer
            classifications. The raw columns always have many different types of
            customer_classifications at the beginning and either the sales or revenue
            at the end. So the pattern for this table is:
            ``rf"^(.+)_(sales_kwh|revenue)$"``
        data_cols: names of data columns - these are strings within the df's original
            column names that you will leave unstacked. The resulting dataframe will
            include these columns. Using the same ``raw_rus7__power_requirements``
            example, the data columns would be: ``["sales_kwh", "revenue"]``
        match_names: the assigned names of each of the match groups in the regex
            pattern - in the order they appear in the pattern. The match group's name
            we won't use is the group containing the data_cols values - this can be
            named anything but for clarity name this 'data_cols'. Using the same
            ``raw_rus7__power_requirements`` example, the ``match_names`` would be
            ``["customer_classification", "data_cols"]`` because the customer
            classification is the first match in the ``pattern`` and the data columns
            are the second match in the pattern.
        unstack_level: list of match_names to unstack. These are the names of the
            matches that get unstacked - these end up as columns in the resulting
            table. Presumably this will be all of the match_names except 'data_cols'.
        drop_zero_rows: if True, drop rows where all data_cols are 0. Function
            already drops rows where data_cols are all NaN.
    """
    df = df.set_index(idx_ish).filter(regex=pattern)
    df.columns = pd.MultiIndex.from_frame(
        df.columns.str.extract(pattern, expand=True)
    ).set_names(match_names)
    df = df.stack(level=unstack_level, future_stack=True).reset_index()
    # remove the remaining multi-index
    df.columns = df.columns.map("".join)
    df = df.dropna(subset=data_cols, how="all")
    if drop_zero_rows:
        all_zero_mask = (df.fillna(0)[data_cols] == 0).all(axis=1)
        df = df[~all_zero_mask]
    return df
