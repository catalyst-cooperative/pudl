"""Transform the RUS12 tables."""

import pandas as pd
from dagster import AssetOut, Output, asset, multi_asset

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


@asset(io_manager_key="pudl_io_manager")
def core_rus12__scd_borrowers(raw_rus12__borrowers):
    """Transform the core_rus12__scd_borrowers table."""
    df = rus.early_transform(raw_df=raw_rus12__borrowers)
    rus.early_check_pk(df)
    # TODO: encode region_code?
    return df.assign(
        state=lambda x: x.borrower_id_rus.str.extract(r"^([A-Z]{2})\d{4}$")
    )


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_renewable_plants(raw_rus12__renewable_plants):
    """Transform the core_rus12__yearly_renewable_plants table."""
    df = rus.early_transform(raw_df=raw_rus12__renewable_plants)

    # Convert date_created to datetime
    df.date_created = pd.to_datetime(df.date_created, format="mixed")

    # Convert units
    df = rus.convert_units(
        df,
        old_unit="kw",
        new_unit="mw",
        converter=0.001,
    )
    df = rus.convert_units(
        df, old_unit="thousand_dollars", new_unit=None, converter=1000
    )

    # Fix typo in primary_renewable_fuel_type values
    df.primary_renewable_fuel_type = df.primary_renewable_fuel_type.replace(
        {"Solar - photvoltaic": "Solar - photovoltaic"}
    )

    # TODO: Make primary_renewable_fuel_type look like renewable fuels from other sources.

    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_long_term_debt(raw_rus12__long_term_debt):
    """Transform the core_rus12__yearly_long_term_debt table."""
    # TODO: the debt_description column could potentially get some cleaning.
    return rus.early_transform(raw_df=raw_rus12__long_term_debt)


@asset(io_manager_key="pudl_io_manager")
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


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_loans(raw_rus12__loans):
    """Transform the raw_rus12__loans table."""
    df = rus.early_transform(raw_df=raw_rus12__loans)
    df.loan_maturity_date = pd.to_datetime(df.loan_maturity_date, format="mixed")
    df.for_rural_development = df.for_rural_development.astype("boolean")

    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_plant_labor(raw_rus12__plant_labor):
    """Transform the raw_rus12__plant_labor table."""
    df = rus.early_transform(raw_df=raw_rus12__plant_labor)

    # Remove duplicate Walter Scott plant entries.
    exclude_cols = ["borrower_id_rus", "borrower_name_rus"]
    dupe_mask = (
        df["borrower_id_rus"].isin(["IA0083", "IA0084"])
        & (df["plant_name_rus"] == "Walter Scott")
        & df.drop(columns=exclude_cols).duplicated(keep=False)
        & (
            df["borrower_id_rus"] == "IA0083"
        )  # dropping the IA0083 and keeping the IA0084 so both borrowers show up in the data.
    )
    df = df.loc[~dupe_mask]

    # Test payroll_total column so can remove it in the schema
    payroll_cols = [
        "payroll_maintenance",
        "payroll_operations",
        "payroll_other_accounts",
    ]
    assert df[
        df[payroll_cols].fillna(0).sum(axis=1) != df["payroll_total"].fillna(0)
    ].empty, (
        "payroll_total column does not equal sum of payroll component columns for some rows."
    )

    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_sources_and_distribution_by_plant_type(
    raw_rus12__sources_and_distribution,
):
    """Transform the raw_rus12__sources_and_distribution table.

    This function pivots the cost, capacity, net-energy, and plant num data by plant type
    from the Sources and Distribution table. The rest of the table contents are in
    core_rus12__yearly_sources_and_distribution.
    """
    df = rus.early_transform(raw_df=raw_rus12__sources_and_distribution)
    data_cols = ["capacity_kw", "cost", "plant_num", "net_energy_received_mwh"]

    plant_types = [
        "fossil_steam",
        "hydro",
        "internal_combustion",
        "combined_cycle",
        "nuclear",
        "other",
    ]

    # Stack by plant type
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^({'|'.join(plant_types)})_({'|'.join(data_cols)})$",
        match_names=["plant_type", "data_cols"],
        unstack_level=[
            "plant_type",
        ],
        drop_zero_rows=True,
    )
    # Convert units
    df = rus.convert_units(
        df,
        old_unit="kw",
        new_unit="mw",
        converter=0.001,
    )

    # TODO: add is_total column.
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_sources_and_distribution(
    raw_rus12__sources_and_distribution,
):
    """Transform the raw_rus12__sources_and_distribution table.

    This function process all columns from the Sources and Distribution table
    that are not plant type specific. The plant type specific columns are processed
    in core_rus12__yearly_sources_and_distribution_by_plant_type.

    The multi_index_stack function intentionally drops a few columns that don't
    show up in other tables. This include:
    - ``total_energy_losses_pct`` (calculable with other cols, dropped because pct value
    was an outlier column and not easily stacked with other columns).
    - ``total_plant_num`` (calculable with the sources_and_distribution_by_plant_type table).
    - ``total_capacity_kw`` (calculable with the sources_and_distribution_by_plant_type table).

    This function keeps the ``total_plant_cost`` and ``total_plant_mwh`` columns even though
    they are also calculable with the other table, because they are components of other
    totals included in this table.
    """
    df = rus.early_transform(raw_df=raw_rus12__sources_and_distribution)
    # Remove plant type columns handled in sources_and_distribution_by_plant_type function
    plant_types = [
        "fossil_steam",
        "hydro",
        "internal_combustion",
        "combined_cycle",
        "nuclear",
        "other",
    ]
    plant_type_mask = df.columns.str.startswith(tuple(f"{p}_" for p in plant_types))
    df = df.loc[:, ~plant_type_mask]

    # Stack by cost and mwh
    data_cols = ["cost", "net_energy_received_mwh"]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^(.+)_({'|'.join(data_cols)})$",
        match_names=["source_of_energy", "data_cols"],
        unstack_level=[
            "source_of_energy",
        ],
        drop_zero_rows=True,
    )
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_statement_of_operations(raw_rus12__statement_of_operations):
    """Transform the raw_rus12__statement_of_operations table.

    This function drops a number of columns that contain per_kwh values that are
    entirely NA through all years. It then reshapes the table by stacking expense
    types by the name of the total column for which they are calculation components.
    """
    df = rus.early_transform(raw_df=raw_rus12__statement_of_operations)

    # Setting this assertion before stacking the table to make sure the PK holds
    rus.early_check_pk(
        df, pk_early=["borrower_id_rus", "borrower_name_rus", "report_date"]
    )

    # There are a bunch of cols ending in per_kwh that seem to have no information in them.
    # Verify this so we feel good dropping them. (They get dropped in multi_index_stack).
    per_kwh_cols = df.columns[df.columns.str.contains("per_kwh")]
    assert df[per_kwh_cols].notna().any().any() is not True, (
        "Expected per_kwh columns to be entirely NA."
    )

    # Stack by operating revenue group and expense type
    data_cols = [
        "opex_ytd",
        "opex_ytd_budget",
        "opex_report_month",
    ]
    opex_group = [
        "operation_revenue_and_patronage_capital",
        "operation_expense",
        "maintenance_expense",
        "cost_of_electric_service",
        "net_patronage_capital_or_margins",
    ]
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^({'|'.join(opex_group)})_(.+)_({'|'.join(data_cols)})$",
        match_names=["opex_group", "opex_type", "data_cols"],
        unstack_level=["opex_group", "opex_type"],
    )
    df["is_total"] = df.opex_type.str.startswith("total_")
    # TODO: could remove total columns that aren't used as part of the calculation for others.
    return df


@asset  # TODO: convert to (io_manager_key="pudl_io_manager")
def core_rus12__yearly_plant_costs(
    raw_rus12__combined_cycle_plant_costs: pd.DataFrame,
    raw_rus12__hydroelectric_plant_costs: pd.DataFrame,
    raw_rus12__internal_combustion_plant_costs: pd.DataFrame,
    raw_rus12__nuclear_plant_costs: pd.DataFrame,
    raw_rus12__steam_plant_costs: pd.DataFrame,
):
    """Transform the plant cost tables.

    This transform takes all of the plant production cost tables, processes
    them similarly and combines them into one plant cost table.
    """
    plant_cost_tables = {
        "combined_cycle": raw_rus12__combined_cycle_plant_costs,
        "hydroelectric": raw_rus12__hydroelectric_plant_costs,
        "internal_combustion": raw_rus12__internal_combustion_plant_costs,
        "nuclear": raw_rus12__nuclear_plant_costs,
        "steam": raw_rus12__steam_plant_costs,
    }

    df_outs = {}
    for plant_type, raw_table in plant_cost_tables.items():
        df = rus.early_transform(raw_df=raw_table)
        # there are duplicates for the "Walter Scott" plant
        if plant_type != "steam":
            rus.early_check_pk(
                df, pk_early=["report_date", "borrower_id_rus", "plant_name_rus"]
            )

        data_cols = ["amount", "per_mmbtu", "dollars_per_mwh"]
        if plant_type in ["hydroelectric", "nuclear"]:
            data_cols = ["amount", "dollars_per_mwh"]
        pattern = rf"^(capex|opex|total)_(.+)_({'|'.join(data_cols)})$"
        df = rus.multi_index_stack(
            df,
            idx_ish=[
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
            ],
            data_cols=data_cols,
            pattern=pattern,
            match_names=["cost_group", "cost_type", "data_cols"],
            unstack_level=["cost_group", "cost_type"],
            assume_no_dropped_cols=True,
        )
        df["plant_type"] = plant_type
        df_outs[plant_type] = df
    df_all = pd.concat(df_outs.values())
    # this total flag is a little different than the others. it grabs the high-level
    # cost group totals and it flags the cost_type's of net and less. Because these
    # are clearly calculated fields from looking at the form.
    df["is_total"] = df.cost_type.str.contains("total|net|less") | (
        df.cost_group == "total"
    )
    # TODO: settle on names and do this rename in the column maps
    df_all = df_all.rename(
        columns={
            "amount": "cost",
            "dollars_per_mwh": "cost_per_mwh",
            "per_mmbtu": "cost_per_mmbtu",
        }
    )
    return df_all


def _drop_bad_ownership_plant(df):
    """Drop 1 plant record with unexpected ownership label and duplicate data.

    There is a Wisdom steam plant record that is labeled to be both fully owned by
    borrower and partly owned for one year. Which is an unexpected combo based on the
    `_OR_PowerSupply Plant File Documentation.rst` documentation file in the rus12
    archive. Luckily this plant has exactly the same records as the other Wisdom steam
    plant that year with more expected ownership labels.

    So we check if the two plant records for that year have the same data, then
    drop the one badly labeled ownership record.
    """
    bad_ownership_label_mask = (
        ~df.is_full_ownership_portion & ~df.is_partly_owned_by_borrower
    )
    assert len(df[bad_ownership_label_mask]) == 1

    wisdom_steam_2019_mask = (
        (df.plant_name_rus == "Wisdom")
        & (df.report_date.dt.year == 2019)
        & (df.plant_type == "steam")
    )

    idx = [
        "borrower_id_rus",
        "borrower_name_rus",
        "plant_name_rus",
        "is_full_ownership_portion",
        "is_partly_owned_by_borrower",
    ]
    assert df[
        wisdom_steam_2019_mask
        & (~df.duplicated(subset=[col for col in df if col not in idx], keep=False))
    ].empty

    return df.drop(df[wisdom_steam_2019_mask & bad_ownership_label_mask].index)


@multi_asset(
    outs={
        "core_rus12__yearly_plant_operations_by_plant": AssetOut(),
        "core_rus12__yearly_plant_operations_by_borrower": AssetOut(),
    }  # TODO: add io_manager_key="pudl_io_manager" into AssetOut
)
def core_rus12__yearly_plant_operations(
    raw_rus12__combined_cycle_plant_operations: pd.DataFrame,
    raw_rus12__hydroelectric_plant_operations: pd.DataFrame,
    raw_rus12__internal_combustion_plant_operations: pd.DataFrame,
    raw_rus12__nuclear_plant_operations: pd.DataFrame,
    raw_rus12__steam_plant_operations: pd.DataFrame,
):
    """Transform the plant operations tables.

    This transform takes all of the plant operations tables, processes
    them similarly and combines them into one plant table. Which is then
    split out into two tables: by borrower and by plant. The details of
    which record should end up in which output table are documented in
    these tables' resource metadata.
    """
    plant_operations_tables = {
        "combined_cycle": raw_rus12__combined_cycle_plant_operations,
        "hydroelectric": raw_rus12__hydroelectric_plant_operations,
        "internal_combustion": raw_rus12__internal_combustion_plant_operations,
        "nuclear": raw_rus12__nuclear_plant_operations,
        "steam": raw_rus12__steam_plant_operations,
    }

    df_outs = {}
    for plant_type, raw_table in plant_operations_tables.items():
        df_plant_type = rus.early_transform(
            raw_df=raw_table,
            boolean_columns_to_fix=[
                "is_full_ownership_portion",
                "is_partly_owned_by_borrower",
            ],
        )
        df_plant_type["plant_type"] = plant_type
        df_outs[plant_type] = df_plant_type
    df = pd.concat(df_outs.values())
    for old_unit in ["1000_lbs", "1000_cubic_feet", "1000_gals"]:
        df = rus.convert_units(
            df,
            old_unit=old_unit,
            new_unit=old_unit.removeprefix("1000_"),
            converter=1000,
        )
    df = rus.convert_units(
        df,
        old_unit="kw",
        new_unit="mw",
        converter=0.001,
    )

    df = df.astype(
        {
            "is_full_ownership_portion": pd.BooleanDtype(),
            "is_partly_owned_by_borrower": pd.BooleanDtype(),
        }
    ).pipe(_drop_bad_ownership_plant)

    # for old years, there is no is_partly_owned_by_borrower column and no
    # accompanying documentation. We assume if its a full ownership portion,
    # it should go in both. if not full ownership portion  it should go in by borrower.

    # Older years do not have a record for
    null_partly_owned_mask = df.report_date.dt.year.isin([2006, 2007, 2008])

    # From _OR_PowerSupply Plant File Documentation.rtf in 2021 archive
    # TO FOCUS ONLY ON DATA FOR THE BORROWERS’ SHARE OF THE PLANTS
    # FullOwnershipScope    BorrowerShared
    # FALSE                 TRUE
    # TRUE                  FALSE
    df_by_borrower = df[
        (null_partly_owned_mask)
        | (
            (~df.is_full_ownership_portion & df.is_partly_owned_by_borrower)
            | (df.is_full_ownership_portion & ~df.is_partly_owned_by_borrower)
        )
    ]
    # TO FOCUS ON DATA FOR THE WHOLE PLANT
    # FullOwnershipScope    BorrowerShared
    # TRUE                  TRUE
    # TRUE                  FALSE
    df_by_plant = df[
        (null_partly_owned_mask & ~df.is_full_ownership_portion)
        | (
            (df.is_full_ownership_portion & df.is_partly_owned_by_borrower)
            | (df.is_full_ownership_portion & ~df.is_partly_owned_by_borrower)
        )
    ]

    # there are a small number of plants that have duplicated values... which seem
    # hard to reconcile.
    idx_check = [
        "report_date",
        "borrower_id_rus",
        "borrower_name_rus",
        "plant_name_rus",
        "is_full_ownership_portion",
        "is_partly_owned_by_borrower",
        "unit_id_rus",
        "plant_type",
    ]
    assert len(df_by_borrower[df_by_borrower.duplicated(idx_check)]) < 35
    assert len(df_by_plant[df_by_plant.duplicated(idx_check)]) < 38
    return (
        Output(
            output_name="core_rus12__yearly_plant_operations_by_plant",
            value=df_by_plant,
        ),
        Output(
            output_name="core_rus12__yearly_plant_operations_by_borrower",
            value=df_by_borrower,
        ),
    )
