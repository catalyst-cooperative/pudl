"""Transform the RUS12 tables."""

import pandas as pd
from dagster import asset

import pudl.transform.rus as rus


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_meeting_and_board(raw_rus12__meeting_and_board):
    """Transform the core_rus12__yearly_meeting_and_board table."""
    df = rus.early_transform(
        raw_df=raw_rus12__meeting_and_board,
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
def core_rus12__yearly_balance_sheet_assets(raw_rus12__balance_sheet):
    """Transform the core_rus12__yearly_balance_sheet_assets table."""
    df = rus.early_transform(raw_df=raw_rus12__balance_sheet)
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


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_balance_sheet_liabilities(raw_rus12__balance_sheet):
    """Transform the core_rus12__yearly_balance_sheet_liabilities table."""
    df = rus.early_transform(raw_df=raw_rus12__balance_sheet)
    rus.early_check_pk(df)
    # MELT
    idx_ish = ["report_date", "borrower_id_rus", "borrower_name_rus"]
    value_vars = list(df.filter(regex=r"_liabilities$").columns)
    df = df.melt(
        id_vars=idx_ish,
        value_vars=value_vars,
        var_name="liability_type",
        value_name="balance",
    )
    df.liability_type = df.liability_type.str.removesuffix("_liabilities")
    # POST-MELT
    df["is_total"] = df.liability_type.str.startswith("total_")
    return df
