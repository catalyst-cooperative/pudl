"""Transform the RUS7 tables."""

import pandas as pd
from dagster import asset

import pudl.transform.rus as rus


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_meeting_and_board(raw_rus7__meeting_and_board):
    """Transform the core_rus7__meeting_and_board table."""
    df = rus.early_transform(
        raw_df=raw_rus7__meeting_and_board,
        boolean_columns_to_fix=[
            "does_manager_have_written_contract",
            "was_quorum_present",
        ],
    )
    rus.early_check_pk(df)

    df.last_annual_meeting_date = pd.to_datetime(
        df.last_annual_meeting_date, format="mixed"
    )
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_balance_sheet_assets(raw_rus7__balance_sheet):
    """Transform the core_rus7__yearly_balance_sheet_assets table."""
    df = rus.early_transform(raw_df=raw_rus7__balance_sheet)
    rus.early_check_pk(df)
    # MELT
    idx_ish = ["report_date", "borrower_id_rus", "borrower_name_rus"]
    value_vars = list(df.filter(regex=r"_assets$").columns)
    df = df.melt(
        id_vars=idx_ish,
        value_vars=value_vars,
        var_name="asset_type",
        value_name="balance",
    )
    df.asset_type = df.asset_type.str.removesuffix("_assets")
    # POST-MELT
    df["is_total"] = df.asset_type.str.startswith("total_")
    return df


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus7__yearly_balance_sheet_liabilities(raw_rus7__balance_sheet):
    """Transform the core_rus7__yearly_balance_sheet_liabilities table."""
    df = rus.early_transform(raw_df=raw_rus7__balance_sheet)
    rus.early_check_pk(df)
    # MELT
    idx_ish = ["report_date", "borrower_id_rus", "borrower_name_rus"]
    value_vars = list(df.filter(regex=r"_liabilities$").columns)
    df = df.melt(
        id_vars=idx_ish,
        value_vars=value_vars,
        var_name="asset_type",
        value_name="balance",
    )
    df.asset_type = df.asset_type.str.removesuffix("_liabilities")
    # POST-MELT
    df["is_total"] = df.asset_type.str.startswith("total_")
    return df


# TODO: feed all rus7 tables into this and extract info
@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus7__scd_borrowers(raw_rus7__borrowers):
    """Transform the core_rus7__scd_borrowers table."""
    df = rus.early_transform(raw_df=raw_rus7__borrowers)
    rus.early_check_pk(df)
    # TODO: encode region_code?
    return df


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus7__yearly_employee_statistics(raw_rus7__employee_statistics):
    """Transform the core_rus7__yearly_employee_statistics table."""
    df = rus.early_transform(raw_df=raw_rus7__employee_statistics)
    rus.early_check_pk(df)
    # TODO: decide if we should break this up into three lil bb tables or not
    # (see note in metadata/resources)
    return df


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus7__yearly_energy_efficiency(raw_rus7__energy_efficiency):
    """Transform the core_rus7__yearly_energy_efficiency table."""
    df = rus.early_transform(raw_df=raw_rus7__energy_efficiency)
    rus.early_check_pk(df)
    # Multi-Stack
    data_cols = ["customers_num", "savings_mmbtu", "invested"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^({'|'.join(data_cols)})_(.+)_(cumulative|new_in_report_year)$",
        match_names=["data_cols", "customer_classification", "date_range"],
        unstack_level=["customer_classification", "date_range"],
    )
    return df


@asset
def _core_rus7__yearly_power_requirements(raw_rus7__power_requirements):
    """Early transform an internal power_requirements table.

    This main input gets used serval times and needs some dropping of duplicate
    records so we do it once.
    """
    df = rus.early_transform(
        raw_df=raw_rus7__power_requirements,
        boolean_columns_to_fix=["is_peak_coincident"],
    )
    # PK duplicate management
    dupe_mask = df.duplicated(subset=["report_date", "borrower_id_rus"], keep=False)
    # visually inspecting these two dupes i learned that most of the values in
    # one record are null. And all of the non-null values seem to be the exact same.
    # Which led me to want to drop the mostly null record.
    # First check this assumption: Are all of the non-null values the same?
    assert (
        df[dupe_mask]
        .dropna(axis=1, how="any")
        .reset_index(drop=True)
        .T.assign(is_same=lambda x: x[0] == x[1])
        .is_same.all()
    )
    # find the mostly null record of these two dupes and drop it
    more_null_loc = (
        df[dupe_mask].isna().sum(axis=1).sort_values(ascending=False).index[0]
    )
    df = df.drop(more_null_loc, axis="index")
    rus.early_check_pk(df)
    return df


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus7__yearly_power_requirements_electric_sales(
    _core_rus7__yearly_power_requirements: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the core_rus7__yearly_power_requirements_electric_sales table."""
    df = _core_rus7__yearly_power_requirements
    # Multi-Stack
    data_cols = ["sales_kwh", "revenue"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^(.+)_({'|'.join(data_cols)})$",
        match_names=["customer_classification", "data_cols"],
        unstack_level=["customer_classification"],
    )
    return df


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus7__yearly_power_requirements_electric_customers(
    _core_rus7__yearly_power_requirements: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the core_rus7__yearly_power_requirements_electric_sales table."""
    df = _core_rus7__yearly_power_requirements
    # Multi-Stack
    data_cols = ["customers_num"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^(.+)_({data_cols[0]})_(december|avg)$",
        match_names=["customer_classification", "data_cols", "date_range"],
        unstack_level=["customer_classification", "date_range"],
    )
    return df


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus7__yearly_power_requirements(
    _core_rus7__yearly_power_requirements: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the core_rus7__yearly_power_requirements_electric_sales table."""
    df = _core_rus7__yearly_power_requirements
    # The electric sales portion of this table gets reshaped and pulled into two
    # separate tables. The electric sales portion of this table ends with two totals
    # the rest of the table pertains to other utility functions. The totals show up
    # in the reshaped electric sales portion of the table but we also rename them
    # and include them here as well.
    df = df.rename(
        columns={
            "total_revenue": "electric_sales_revenue",
            "total_sales_kwh": "electric_sales_kwh",
        }
    )
    # this portion of the table does not need a reshape. Applying enforce_schema
    # will effectively drop all the other columns in this table.
    return df
