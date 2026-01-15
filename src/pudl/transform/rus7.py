"""Transform the RUS7 tables."""

import pandas as pd
from dagster import asset

import pudl.transform.rus as rus


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
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


@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
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
    idx_ish = ["report_date", "borrower_id_rus", "borrower_name_rus"]
    df = df.set_index(idx_ish)
    data_cols = ["customers_num", "savings_mmbtu", "invested"]
    df.columns = df.columns.str.split(
        rf"^({'|'.join(data_cols)})_(.+)_(cumulative|new_in_report_year)$",
        expand=True,
    ).set_names([None, "data_cols", "customer_classification", "date_range", None])
    df = df.stack(
        level=["customer_classification", "date_range"], future_stack=True
    ).reset_index()
    # remove the remaining multi-index
    df.columns = df.columns.map("".join)

    # POST-MELT
    df = df.dropna(subset=data_cols, how="all")
    return df
