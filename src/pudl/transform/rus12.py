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


# TODO: feed all rus12 tables into this and extract info
@asset  # TODO: (io_manager_key="pudl_io_manager") once metadata is settled
def core_rus12__scd_borrowers(raw_rus12__borrowers):
    """Transform the core_rus12__scd_borrowers table."""
    df = rus.early_transform(raw_df=raw_rus12__borrowers)
    rus.early_check_pk(df)
    # TODO: encode region_code?
    return df.assign(
        state=lambda x: x.borrower_id_rus.str.extract(r"^([A-Z]{2})\d{4}$")
    )


@asset  # (io_manager_key="pudl_io_manager") # add this when units can be converted and column names have changed
def core_rus12__yearly_renewable_plants(raw_rus12__renewable_plants):
    """Transform the core_rus12__yearly_renewable_plant table."""
    df = rus.early_transform(raw_df=raw_rus12__renewable_plants)

    # Convert date_created to datetime
    df.date_created = pd.to_datetime(df.date_created, format="mixed")

    # Convert units
    # TODO: use Christina's convert_units function once it's merged in for $1000s to $1
    # TODO: use Christina's convert_units function once it's merged in for kw to mw

    # Transform ideas
    # - Make primary_renewable_fuel_type look like renewable fuels from other sources.

    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_long_term_debt(raw_rus12__long_term_debt):
    """Transform the core_rus12__yearly_long_term_debt table."""
    # TODO: the debt_description column could potentially get some cleaning.
    return rus.early_transform(raw_df=raw_rus12__long_term_debt)


@asset  # (io_manager_key="pudl_io_manager")
def core_rus12__yearly_lines_stations_labor_materials_cost(
    raw_rus12__lines_and_stations_labor_materials,
):
    """Transform the raw_rus12__lines_and_stations_labor_materials table."""
    df = rus.early_transform(raw_df=raw_rus12__lines_and_stations_labor_materials)

    data_cols = ["cost"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=r"^(labor|material)_(maintenance|operation)_(lines|stations)_(cost)$",
        match_names=[
            "labor_or_material",
            "operation_or_maintenance",
            "lines_or_stations",
            "data_cols",
        ],
        unstack_level=[
            "labor_or_material",
            "operation_or_maintenance",
            "lines_or_stations",
        ],
    )
    # NOTE: this multi_index_stack function is dropping the employees_num column for now. I'm assuming we can get this data from another table, else I can circle back.
    return df


@asset  # (io_manager_key="pudl_io_manager")
def core_rus12__yearly_loans(raw_rus12__loans):
    """Transform the raw_rus12__loans table."""
    df = rus.early_transform(raw_df=raw_rus12__loans)
    df.loan_maturity_date = pd.to_datetime(df.loan_maturity_date, format="mixed")
    df.for_rural_development = df.for_rural_development.astype("boolean")

    # Make sure loan balance isn't more than original loan amount
    loan_diff = df.loan_original_amount - df.loan_balance
    assert len(loan_diff[loan_diff < 0]) == 0, (
        "Loan balance exceeds original loan amount for some loans."
    )

    return df


@asset  # (io_manager_key="pudl_io_manager")
def core_rus12__yearly_plant_labor(raw_rus12__plant_labor):
    """Transform the raw_rus12__plant_labor table."""
    df = rus.early_transform(raw_df=raw_rus12__plant_labor)
    df.employees_fte_num = df.employees_fte_num.astype("Int64")
    df.employees_part_time_num = df.employees_part_time_num.astype("Int64")
    return df


@asset  # (io_manager_key="pudl_io_manager")
def core_rus12__yearly_sources_and_distribution_by_plant_type(
    raw_rus12__sources_and_distribution,
):
    """Transform the raw_rus12__sources_and_distribution table.

    This function pivots the cost, capacity, net-energy, and plant num data by plant type
    from the Sources and Distribution table. The rest of the table contents are in
    core_rus12__yearly_sources_and_distribution.
    """
    df = rus.early_transform(raw_df=raw_rus12__sources_and_distribution)
    data_cols = ["capacity_kw", "cost", "plants_num", "net_energy_mwh"]
    # This list excludes total values and instead includes them in the sources_and_distribution table.
    plant_types = [
        "fossil_steam",
        "hydro",
        "internal_combustion",
        "combined_cycle",
        "nuclear",
        "other",
    ]
    # This function intentionally drops all cols that aren't plant_type related.
    # They are processed in the core_rus12__yearly_sources_and_distribution function.
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^({'|'.join(plant_types)})_({'|'.join(data_cols)})$",
        match_names=["plant_type", "data_cols"],
        unstack_level=[
            "plant_type",
        ],
        drop_zeros_rows=True,
    )
    # Make sure plant num is only int values and then convert to integer
    assert (df.plants_num.dropna() % 1 == 0).all()
    df.plants_num = df.plants_num.astype("Int64")
    # TODO: use Christina's convert_units function once it's merged in for kw to mw

    return df
