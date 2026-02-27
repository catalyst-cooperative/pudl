"""Transform the RUS7 tables."""

import pandas as pd
from dagster import asset

import pudl.transform.rus as rus


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_meeting_and_board(raw_rus7__meeting_and_board):
    """Transform the meeting and board (aka governance) table."""
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
    """Transform the balance sheet assets table."""
    df = rus.early_transform(raw_df=raw_rus7__balance_sheet)
    rus.early_check_pk(df)
    # MELT
    idx_ish = ["report_date", "borrower_id_rus", "borrower_name_rus"]
    value_vars = list(df.filter(regex=r"_assets$").columns)
    df = df.melt(
        id_vars=idx_ish,
        value_vars=value_vars,
        var_name="asset_type",
        value_name="ending_balance",
    )
    df.asset_type = df.asset_type.str.removesuffix("_assets")
    # POST-MELT
    df["is_total"] = df.asset_type.str.startswith("total_")
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_balance_sheet_liabilities(raw_rus7__balance_sheet):
    """Transform the balance sheet liabilities table."""
    df = rus.early_transform(raw_df=raw_rus7__balance_sheet)
    rus.early_check_pk(df)
    # MELT
    idx_ish = ["report_date", "borrower_id_rus", "borrower_name_rus"]
    value_vars = list(df.filter(regex=r"_liabilities$").columns)
    df = df.melt(
        id_vars=idx_ish,
        value_vars=value_vars,
        var_name="liability_type",
        value_name="ending_balance",
    )
    df.liability_type = df.liability_type.str.removesuffix("_liabilities")
    # POST-MELT
    df["is_total"] = df.liability_type.str.startswith("total_")
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__scd_borrowers(raw_rus7__borrowers):
    """Transform the borrowers table."""
    df = rus.early_transform(raw_df=raw_rus7__borrowers)
    rus.early_check_pk(df)
    # TODO: encode region_code?
    return df.assign(
        state=lambda x: x.borrower_id_rus.str.extract(r"^([A-Z]{2})\d{4}$")
    )


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_employee_statistics(raw_rus7__employee_statistics):
    """Transform the employee statistics table."""
    df = rus.early_transform(raw_df=raw_rus7__employee_statistics)
    rus.early_check_pk(df)
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_energy_efficiency(raw_rus7__energy_efficiency):
    """Transform the energy efficiency table."""
    df = rus.early_transform(raw_df=raw_rus7__energy_efficiency)
    rus.early_check_pk(df)
    # Multi-Stack
    data_cols = ["customers_num", "savings_mmbtu", "invested"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^({'|'.join(data_cols)})_(.+)_(cumulative|new_in_report_year)$",
        match_names=["data_cols", "customer_class", "observation_period"],
        unstack_level=["customer_class", "observation_period"],
    )
    return df


@asset
def _core_rus7__yearly_power_requirements(raw_rus7__power_requirements):
    """Early transform an internal power_requirements table.

    This main input gets used serval times in several downstream
    ``core_rus7__yearly_power_requirements*`` assets. The raw asset needs some
    cleaning and dropping of duplicate records so we do it once.
    """
    df = rus.early_transform(
        raw_df=raw_rus7__power_requirements,
        boolean_columns_to_fix=["is_peak_coincident"],
    )
    # PK duplicate management
    df = df.reset_index(drop=True)
    dupe_mask = df.duplicated(subset=["report_date", "borrower_id_rus"], keep=False)
    if not df[dupe_mask].empty:
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


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_power_requirements_electric_sales(
    _core_rus7__yearly_power_requirements: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the power requirements of electric sales table.

    The resulting table is a portion of the power_requirements tables, which
    pertains to the sales and revenue of electricity.
    """
    df = _core_rus7__yearly_power_requirements
    # Multi-Stack
    data_cols = ["sales_kwh", "revenue"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^(.+)_({'|'.join(data_cols)})$",
        match_names=["customer_class", "data_cols"],
        unstack_level=["customer_class"],
    )
    # then convert all of the units from kWh to MWh
    df = rus.convert_units(
        df,
        old_unit="kwh",
        new_unit="mwh",
        converter=0.001,
    )
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_power_requirements_electric_customers(
    _core_rus7__yearly_power_requirements: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the power requirements of electric customers table.

    The resulting table is a portion of the power_requirements tables, which
    pertains to the number of customers in different customer classes.
    """
    df = _core_rus7__yearly_power_requirements
    # Multi-Stack
    data_cols = ["customers_num"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^(.+)_({data_cols[0]})_(december|avg)$",
        match_names=["customer_class", "data_cols", "observation_period"],
        unstack_level=["customer_class", "observation_period"],
    )
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_power_requirements(
    _core_rus7__yearly_power_requirements: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the power requirements table.

    The resulting table is a portion of the power_requirements tables, which
    pertains to the revenue from several portions of the borrower's business as well
    as several types of electricity generated, purchased or used.
    """
    df = _core_rus7__yearly_power_requirements
    # The electric sales portion of this table gets reshaped and pulled into two
    # separate tables. The electric sales portion of this table ends with two totals
    # the rest of the table pertains to other utility functions. The totals show up
    # in the reshaped electric sales portion of the table but we also rename them
    # and include them here as well. That way this table has all of the sectors of
    # power requirements reported in one place.
    df = (
        df.rename(
            columns={
                "total_revenue": "electric_sales_revenue",
                "total_sales_kwh": "electric_sales_kwh",
            }
        )
        # then convert all of the units from kW* to MW*
        .pipe(
            rus.convert_units,
            old_unit="kwh",
            new_unit="mwh",
            converter=0.001,
        )
        .pipe(
            rus.convert_units,
            old_unit="kw",
            new_unit="mw",
            converter=0.001,
        )
    )
    # this portion of the table does not need a reshape. Applying enforce_schema
    # will effectively drop all the other columns in this table.
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_investments(
    raw_rus7__investments: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the investments table."""
    df = rus.early_transform(
        raw_df=raw_rus7__investments,
        boolean_columns_to_fix=["for_rural_development"],
    )

    # No PK in this table
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_long_term_debt(
    raw_rus7__long_term_debt: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the core_rus7__yearly_investments table."""
    df = rus.early_transform(raw_df=raw_rus7__long_term_debt)
    # No PK in this table
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_patronage_capital(
    raw_rus7__patronage_capital: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the patronage capital table."""
    df = rus.early_transform(raw_df=raw_rus7__patronage_capital)
    rus.early_check_pk(df)

    def _melt_on_date(df, period):
        idx_ish = ["report_date", "borrower_id_rus", "borrower_name_rus"]
        value_vars = list(df.filter(regex=rf"_{period}$").columns)
        range_df = df.melt(
            id_vars=idx_ish,
            value_vars=value_vars,
            var_name="patronage_type",
            value_name=f"patronage_{period}",
        )
        range_df.patronage_type = range_df.patronage_type.str.removesuffix(f"_{period}")
        return range_df.set_index(idx_ish + ["patronage_type"])

    df = pd.merge(
        _melt_on_date(df, "cumulative"),
        _melt_on_date(df, "report_year"),
        right_index=True,
        left_index=True,
        how="outer",
    ).reset_index()
    df["is_total"] = df.patronage_type.str.startswith("total_")
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_statement_of_operations(
    raw_rus7__statement_of_operations: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the statement of operations table."""
    df = rus.early_transform(raw_df=raw_rus7__statement_of_operations)
    rus.early_check_pk(df)

    statement_groups = [
        "cost_of_electric_service",
        "opex",
        "patronage_and_operating_margins",
        "patronage_capital_or_margins",
    ]
    periods = ["opex_ytd", "opex_ytd_budget", "opex_report_month"]
    pattern = rf"^({'|'.join(statement_groups)})_(.+)_({'|'.join(periods)})$"
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=periods,
        pattern=pattern,
        match_names=["opex_group", "opex_type", "data_cols"],
        unstack_level=["opex_group", "opex_type"],
    )
    df["is_total"] = df.opex_group.str.startswith("total_")
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus7__yearly_long_term_leases(
    raw_rus7__long_term_leases: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the long term leases table."""
    df = rus.early_transform(raw_df=raw_rus7__long_term_leases)

    # Spot fix negative rental value that should be positive based on the same property_type
    # reported in other years to the same borrower.
    mask = (
        (df.borrower_id_rus == "LA0015")
        & (df.report_date == "2013-12-01")
        & (df.property_type == "Tower Right-Of-Way")
    )
    assert len(df[mask]) == 1, (
        "Expected exactly one record to be affected by this spot fix."
    )
    df.loc[mask, "rental_cost_ytd"] = abs(df.loc[mask, "rental_cost_ytd"])

    # TO-DO: there are some sus rows where rental cost is 0 or all categories are NA.
    # We could remove these?

    return df
