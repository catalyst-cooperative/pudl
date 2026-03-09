"""Transform the RUS12 tables."""

import pandas as pd
from dagster import AssetIn, AssetOut, Field, Output, asset, multi_asset

import pudl.transform.rus as rus
from pudl import logging_helpers
from pudl.helpers import cleanstrings_snake
from pudl.metadata.enums import PLANT_TYPE_RUS12
from pudl.transform.eia import harvest_entity_tables

logger = logging_helpers.get_logger(__name__)


@asset
def _core_rus12__yearly_meeting_and_board(raw_rus12__meeting_and_board):
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


@asset
def _core_rus12__yearly_balance_sheet_assets(raw_rus12__balance_sheet):
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


@asset
def _core_rus12__yearly_balance_sheet_liabilities(raw_rus12__balance_sheet):
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


@asset
def _core_rus12__scd_borrowers(raw_rus12__borrowers):
    """Transform the core_rus12__scd_borrowers table."""
    df = rus.early_transform(raw_df=raw_rus12__borrowers)
    rus.early_check_pk(df)
    # TODO: encode region_code?
    return df.assign(
        state=lambda x: x.borrower_id_rus.str.extract(r"^([A-Z]{2})\d{4}$")
    )


@asset
def _core_rus12__yearly_renewable_plants(raw_rus12__renewable_plants):
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


@asset
def _core_rus12__yearly_long_term_debt(raw_rus12__long_term_debt):
    """Transform the core_rus12__yearly_long_term_debt table."""
    # TODO: the debt_description column could potentially get some cleaning.
    return rus.early_transform(raw_df=raw_rus12__long_term_debt)


@asset
def _core_rus12__yearly_lines_stations_labor_materials_cost(
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
def _core_rus12__yearly_loans(raw_rus12__loans, raw_rus12__loan_guarantees):
    """Transform the raw_rus12__loans and raw_rus12__loan_guarantees tables."""
    df_loans = rus.early_transform(
        raw_df=raw_rus12__loans,
        boolean_columns_to_fix=["for_rural_development"],
        string_cols_to_simplify=["loan_recipient"],
    ).assign(is_loan_guarantee=False)
    df_loan_guarantees = rus.early_transform(
        raw_df=raw_rus12__loan_guarantees,
        boolean_columns_to_fix=["for_rural_development"],
        string_cols_to_simplify=["loan_recipient"],
    ).assign(is_loan_guarantee=True)
    df = pd.concat([df_loans, df_loan_guarantees], ignore_index=True)
    # Convert all loan_maturity_dates to datetime
    df.loan_maturity_date = pd.to_datetime(df.loan_maturity_date, format="mixed")
    return df


@asset
def _core_rus12__yearly_plant_labor(raw_rus12__plant_labor):
    """Transform the raw_rus12__plant_labor table."""
    df = rus.early_transform(raw_df=raw_rus12__plant_labor)
    df = cleanstrings_snake(df, ["plant_type"])

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


@asset
def _core_rus12__yearly_sources_and_distribution_by_plant_type(
    raw_rus12__sources_and_distribution,
):
    """Transform the raw_rus12__sources_and_distribution table.

    This function pivots the cost, capacity, net-energy, and plant num data by plant type
    from the Sources and Distribution table. The rest of the table contents are in
    core_rus12__yearly_sources_and_distribution.
    """
    df = rus.early_transform(raw_df=raw_rus12__sources_and_distribution)
    data_cols = ["capacity_kw", "cost", "plant_num", "net_energy_received_mwh"]

    # Stack by plant type
    df = rus.multi_index_stack(
        df,
        idx_ish=["report_date", "borrower_id_rus", "borrower_name_rus"],
        data_cols=data_cols,
        pattern=rf"^({'|'.join(PLANT_TYPE_RUS12)})_({'|'.join(data_cols)})$",
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


@asset
def _core_rus12__yearly_sources_and_distribution(
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
    plant_type_mask = df.columns.str.startswith(
        tuple(f"{p}_" for p in PLANT_TYPE_RUS12)
    )
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


@asset
def _core_rus12__yearly_statement_of_operations(raw_rus12__statement_of_operations):
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


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_investments(
    raw_rus12__investments: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the investments table."""
    df = rus.early_transform(
        raw_df=raw_rus12__investments,
        boolean_columns_to_fix=["for_rural_development"],
    )
    # Spot fix bad investment type code values that is 0 and should be 1.
    # 0 is not a valid code and the same description was later listed as 1.
    mask = (
        (df.borrower_id_rus == "KY0059")
        & (df.report_date == "2006-12-01")
        & (
            df.investment_description
            == "Temporary Investments - Cooperative Finance Corp"
        )
    )
    assert len(df.loc[mask]) == 1, (
        "Expected exactly one record to be affected by this spot fix."
    )
    df.loc[mask, "investment_type_code"] = 1

    # TO-DO: clean up property_type field
    return df


@asset(io_manager_key="pudl_io_manager")
def core_rus12__yearly_external_financial_risk_ratio(
    raw_rus12__external_financial_risk_ratio: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the raw_rus12__external_financial_risk_ratio table."""
    df = rus.early_transform(raw_df=raw_rus12__external_financial_risk_ratio)
    df["external_financial_risk_ratio"] = df["external_financial_risk_ratio"]
    return df


@asset(io_manager_key="pudl_io_manager")
def _core_rus12__yearly_plant_costs(
    raw_rus12__combined_cycle_plant_costs: pd.DataFrame,
    raw_rus12__hydro_plant_costs: pd.DataFrame,
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
        "hydro": raw_rus12__hydro_plant_costs,
        "internal_combustion": raw_rus12__internal_combustion_plant_costs,
        "nuclear": raw_rus12__nuclear_plant_costs,
        "steam": raw_rus12__steam_plant_costs,
    }
    df_outs = {}
    for plant_type, raw_table in plant_cost_tables.items():
        df = rus.early_transform(raw_df=raw_table)
        # there are duplicates for the "Walter Scott" steam plant
        if plant_type != "steam":
            rus.early_check_pk(
                df, pk_early=["report_date", "borrower_id_rus", "plant_name_rus"]
            )
        data_cols = (
            ["cost", "cost_per_mmbtu", "cost_per_mwh"]
            if plant_type not in ["hydro", "nuclear"]
            else ["cost", "cost_per_mwh"]
        )
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
    df_all["is_total"] = df_all.cost_type.str.contains("total|net|less") | (
        df_all.cost_group == "total"
    )
    return df_all


def drop_bad_ownership_plant(df):
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


def fix_string_unit_id_rus(df):
    """Fix unit_id_rus's bad string IDs.

    There are two instances of unit_id_rus's that have string values in them.
    Based on pre-cleaned data, we were able to clearly identify that we can use
    just the numeric values in these bad strings. This enables us to have an integer
    type for this unit_id_rus column.
    """
    df.unit_id_rus = df.unit_id_rus.astype(pd.StringDtype())
    assert len(df[df.unit_id_rus.str.contains("WSL")]) <= 4
    df.unit_id_rus = (
        df.unit_id_rus.replace({"WSL GT 12": "12", "WSL ST 10": "10"})
        # then actually convert the dtype to make sure
        # it can be an int. convert to a float first because
        # this column could have been converted into an object with
        # floaty string with things like "2.0" that don't love being
        # directly converted into an int
        .astype(float)
        .astype(pd.Int64Dtype())
    )
    return df


@multi_asset(
    outs={
        "_core_rus12__yearly_plant_operations_by_plant": AssetOut(),
        "_core_rus12__yearly_plant_operations_by_borrower": AssetOut(),
    }
)
def _core_rus12__yearly_plant_operations(
    raw_rus12__combined_cycle_plant_operations: pd.DataFrame,
    raw_rus12__hydro_plant_operations: pd.DataFrame,
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
    raw_plant_operations_tables = {
        "combined_cycle": raw_rus12__combined_cycle_plant_operations,
        "hydro": raw_rus12__hydro_plant_operations,
        "internal_combustion": raw_rus12__internal_combustion_plant_operations,
        "nuclear": raw_rus12__nuclear_plant_operations,
        "steam": raw_rus12__steam_plant_operations,
    }

    df_outs = {}
    for plant_type, raw_table in raw_plant_operations_tables.items():
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
    for old_unit in ["1000_lbs", "1000_cubic_feet", "1000_gallons"]:
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

    df = (
        df.astype(
            {
                "is_full_ownership_portion": pd.BooleanDtype(),
                "is_partly_owned_by_borrower": pd.BooleanDtype(),
            }
        )
        .pipe(drop_bad_ownership_plant)
        .pipe(fix_string_unit_id_rus)
    )

    # for old years, there is no is_partly_owned_by_borrower column and no
    # accompanying documentation. We assume if its a full ownership portion,
    # it should go in both. if not full ownership portion  it should go in by borrower.
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
    # Some validation checks...
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
    # we want to check whether or not we are loosing any records:
    both = pd.concat(
        [df_by_borrower.set_index(idx_check), df_by_plant.set_index(idx_check)],
        axis="index",
        join="outer",
        copy=False,
    )
    # There is some overlap between the by_borrower and by_plant records but we
    # should have all of the original df records
    assert df.set_index(idx_check).index.difference(both.index, sort=True).empty
    # Also, there are a small number of plants that have duplicated values...
    # which seem hard to reconcile.
    assert len(df_by_borrower[df_by_borrower.duplicated(idx_check)]) < 35
    assert len(df_by_plant[df_by_plant.duplicated(idx_check)]) < 38

    return (
        Output(
            output_name="_core_rus12__yearly_plant_operations_by_plant",
            value=df_by_plant,
        ),
        Output(
            output_name="_core_rus12__yearly_plant_operations_by_borrower",
            value=df_by_borrower,
        ),
    )


######################################
# HARVESTING aka NORMALIZATION
######################################
# The USDA would be proud of this name


_CORE_RUS12_TABLES = [
    "_core_rus12__scd_borrowers",
    "_core_rus12__yearly_balance_sheet_assets",
    "_core_rus12__yearly_balance_sheet_liabilities",
    "_core_rus12__yearly_lines_stations_labor_materials_cost",
    "_core_rus12__yearly_loans",
    "_core_rus12__yearly_long_term_debt",
    "_core_rus12__yearly_meeting_and_board",
    "_core_rus12__yearly_plant_costs",
    "_core_rus12__yearly_plant_labor",
    "_core_rus12__yearly_plant_operations_by_borrower",
    "_core_rus12__yearly_plant_operations_by_plant",
    "_core_rus12__yearly_renewable_plants",
    "_core_rus12__yearly_sources_and_distribution",
    "_core_rus12__yearly_sources_and_distribution_by_plant_type",
    "_core_rus12__yearly_statement_of_operations",
]


@asset(
    ins={table_name: AssetIn() for table_name in _CORE_RUS12_TABLES},
    io_manager_key="pudl_io_manager",
    config_schema={
        "debug": Field(
            bool,
            default_value=False,
            description=(
                "If True, allow inconsistent values in harvested columns and "
                "produce additional debugging output."
            ),
        ),
    },
)
def core_rus12__entity_borrowers(context, **clean_dfs):
    """Harvesting IDs & consistent static attributes for RUS12 entity."""
    entity = rus.RusEntity.BORROWERS
    logger.info("Harvesting IDs & consistent static attributes for RUS Borrowers")
    # We want **all** borrowers to have non-null names in this entity
    # table. They aren't always super consistent over time, but we have
    # vetted them (see https://github.com/catalyst-cooperative/pudl/pull/5056#issuecomment-4008247047)
    # and thus decided to set the threshold for consistency strictness
    # at 0% (instead of the default 70%) so we the most consistent value
    # no matter what.
    special_case_strictness = {"borrower_name_rus": 0}
    # We only need the entity table, but the harvesting process
    # always produces entity (aka static) as annual (aka scd) tables.
    # as well as a helpful-for-debugging dictionary of dfs for all
    # values columns we are harvesting
    entity_df, annual_df, _col_dfs = harvest_entity_tables(
        entity,
        clean_dfs,
        special_case_strictness=special_case_strictness,
        debug=context.op_config["debug"],
    )

    return entity_df


finished_rus_assets = [
    rus.finished_rus_asset_factory(
        table_name=_core_table_name.removeprefix("_"),
        _core_table_name=_core_table_name,
        io_manager_key="pudl_io_manager",
    )
    for _core_table_name in _CORE_RUS12_TABLES
    # Don't attempt to core-ify this table
    if _core_table_name not in ["_core_rus12__scd_borrowers"]
]
