"""Transform NREL ATB data into well normalized, cleaned tables."""

from typing import Self

import numpy as np
import pandas as pd
from dagster import AssetCheckResult, asset, asset_check
from pydantic import BaseModel, field_validator

import pudl.helpers as helpers
from pudl import logging_helpers

logger = logging_helpers.get_logger(__name__)

IDX_ALL = [
    "report_year",
    "model_case_nrelatb",
    "model_tax_credit_case_nrelatb",
    "projection_year",
    "cost_recovery_period_years",
    "scenario_atb",
    "technology_description",
    "technology_description_detail_1",
    "technology_description_detail_2",
    "core_metric_parameter",
]
"""Expected primary key columns for the raw nrelatb data.

The normalized core tables we are trying to build will have primary keys which

are a subset of these columns.
"""


class TableNormalizer(BaseModel):
    """Info needed to convert a selection of the raw NREL table into a normalized table."""

    idx: list[str]
    """Primary key columns of normalized subset table."""
    columns: list[str]
    """Columns reported in raw NREL table that are unique to the :attr:`idx`."""


def transform_normalize(nrelatb: pd.DataFrame, normalizer: TableNormalizer):
    """Normalize a subset of the NREL ATB data into a small table.

    Given a :class:`TableNormalizer` with a set of primary keys (``idx``) and a
    list of columns (``columns``), build a table with just those primary key and
    data columns from the larger ATB semi-processed table (output of
    :func:`_core_nrelatb__transform_start`). Ensure that the output table is
    unique based on the primary keys.
    """
    smol_table = nrelatb[normalizer.idx + normalizer.columns].drop_duplicates()
    if not (
        idx_dupes := smol_table[smol_table.duplicated(normalizer.idx, keep=False)]
    ).empty:
        raise AssertionError(
            f"Found duplicate records on expected primary keys ({normalizer.idx}) and"
            f"expected none. Duplicates:\n{idx_dupes}"
        )
    # drop the fully null columns bc some years there are no values in these
    # columns and there is no use keeping these fully null records
    return smol_table.dropna(how="all", subset=normalizer.columns)


class Normalizer(BaseModel):
    """Class that defines how to normalize all of the NREL tables that get the :func:`transform_normalize` treatment.

    There are several columns in the raw ATB data that are not a part of the primary keys
    for the tables that get the :func:`transform_unstack` treatment. This class helps us
    build these smaller tables which have a smaller subset of primary key columns.
    """

    units: TableNormalizer = TableNormalizer(
        idx=["core_metric_parameter"],
        columns=["units"],
    )
    technology_status: TableNormalizer = TableNormalizer(
        idx=[
            "report_year",
            "technology_description",
            "technology_description_detail_1",
            "technology_description_detail_2",
        ],
        columns=["is_technology_mature", "is_default"],
    )


class TableUnstacker(BaseModel):
    """Info needed to unstack a portion of the NREL ATB table.

    This class defines a portion of the raw ATB table to get the :func:`transform_unstack`
    treatment. The set of tables which get this treatment are defined in :class:`Unstacker`.
    """

    idx: list[str]
    core_metric_parameters: list[str]
    """Values from the ``core_metric_parameter`` column to be included in this unstack."""

    @field_validator("idx")
    @classmethod
    def idx_are_same_or_subset_of_idx_all(cls: Self, idx: list[str]):
        """Are the :attr:`idx` columns either the same as or a subset of :attr:`IDX_ALL`?"""
        assert set(idx).issubset(IDX_ALL)
        return idx

    @property
    def idx_unstacked(self: Self) -> list[str]:
        """Primary key columns after the table is unstacked.

        All of the columns in :attr:`idx` except core_metric_parameter.
        """
        return [c for c in self.idx if c != "core_metric_parameter"]


def transform_unstack(
    nrelatb: pd.DataFrame, table_unstacker: TableUnstacker
) -> pd.DataFrame:
    """Generic unstacking function to convert ATB data from a skinny to wider format.

    This function applies :func:`pandas.unstack` to a subset of values
    for ``core_metric_parameter`` (via :attr:`TableUnstacker.core_metric_parameters`)
    with different primary keys (via :attr:`TableUnstacker.idx`). If the set of given
    ``core_metric_parameters`` result in non-unique values for the primary keys,
    :func:`pandas.unstack` will raise an error.

    """
    nrelatb_unstacked = (
        nrelatb.loc[
            nrelatb.core_metric_parameter.isin(table_unstacker.core_metric_parameters),
            table_unstacker.idx + ["value"],
        ]
        .drop_duplicates()
        .set_index(table_unstacker.idx)
        .sort_index()
        .unstack(level="core_metric_parameter")
    )
    nrelatb_unstacked.columns = nrelatb_unstacked.columns.droplevel()
    return nrelatb_unstacked.reset_index()


class Unstacker(BaseModel):
    """Class that defines how to unstack the raw ATB table into all of the tidy core tables.

    The ATB data is reported in a very skinny format that enables the raw data to have the
    same schema over time. The ``core_metric_parameter`` column contains a string which
    indicates what type of data is being reported in the ``value`` column.

    We want the strings in ``core_metric_parameter`` to end up as column names in the
    tables, so that each column represents a unique type of data. In the end, there will
    be one column containing values from the ``value`` column for each unique
    ``core_metric_parameter``. A quirk with ATB is that different ``core_metric_parameter``
    have different set of primary keys. Subsets of the ``core_metric_parameter`` have
    unique values across the data given specific primary keys.

    The convention for ATB data is to use an asterisk in the key columns as a wildcard.
    Generally when an asterisk is in one the ``IDX_ALL`` columns, the corresponding
    ``core_metric_parameter`` should be associated with a table without that column
    as one of its ``idx`` - thus in effect dropping these asterisks from the data.
    Once these tables are in their core tidy format, they can be merged back together
    using the primary keys.

    This class defines all of the tables in the ATB data that get the
    :func:`transform_unstack` treatment.
    """

    rate_table: TableUnstacker = TableUnstacker(
        idx=[
            "report_year",
            "model_case_nrelatb",
            "projection_year",
            "technology_description",
            "core_metric_parameter",
        ],
        core_metric_parameters=[
            "inflation_rate",
            "interest_rate_during_construction_nominal",
            "tax_rate_federal_state",
            "interest_rate_calculated_real",
            "interest_rate_nominal",
            "rate_of_return_on_equity_nominal",
            "rate_of_return_on_equity_calculated_real",
        ],
    )
    scenario_table: TableUnstacker = TableUnstacker(
        idx=rate_table.idx
        + [
            "scenario_atb",
            "model_tax_credit_case_nrelatb",
            "cost_recovery_period_years",
        ],
        core_metric_parameters=[
            "debt_fraction",
            "wacc_real",
            "wacc_nominal",
            "capital_recovery_factor",
            "fixed_charge_rate",
        ],
    )
    tech_detail_table: TableUnstacker = TableUnstacker(
        idx=scenario_table.idx
        + [
            "technology_description_detail_1",
            "technology_description_detail_2",
        ],
        core_metric_parameters=[
            "capex_per_kw",
            "capacity_factor",
            "opex_fixed_per_kw",
            "levelized_cost_of_energy_per_mwh",
            "fuel_cost_per_mwh",
            "opex_variable_per_mwh",
            # 2023 only core_metric_parameters
            "heat_rate_mmbtu_per_mwh",
            "capex_construction_finance_factor",
            "capex_grid_connection_per_kw",
            "capex_overnight_per_kw",
            "capex_overnight_additional_per_kw",
            "heat_rate_penalty",
            "net_output_penalty",
        ],
    )

    @property
    def core_metric_parameters_all(self: Self) -> list[str]:
        """Compilation of all of the parameter values from each of the tables.

        Also check if there are no duplicate core_metric_parameters. We expect all of the parameters
        across the :class:`TableUnstacker` to be unique.
        """
        core_metric_parameters_all = (
            self.rate_table.core_metric_parameters
            + self.scenario_table.core_metric_parameters
            + self.tech_detail_table.core_metric_parameters
        )
        assert not {
            x
            for x in core_metric_parameters_all
            if core_metric_parameters_all.count(x) > 1
        }
        return core_metric_parameters_all


@asset
def _core_nrelatb__transform_start(raw_nrelatb__data):
    """Transform raw NREL ATB data into semi-clean but still very skinny table."""
    ## EARLY CLEANING/DEDUPING
    # rename dict of semi-cleaned param name -> desired pudl name
    # all the core_metric_parameters are here even if we aren't renaming mostly
    # to make sure we've got em all.
    core_metric_parameters_rename = {
        "gcc": "capex_grid_connection_per_kw",
        "interest_rate_nominal": "interest_rate_nominal",
        "debt_fraction": "debt_fraction",
        "inflation_rate": "inflation_rate",
        "calculated_interest_rate_real": "interest_rate_calculated_real",
        "fixed_o&m": "opex_fixed_per_kw",
        "fcr": "fixed_charge_rate",
        "crf": "capital_recovery_factor",
        "tax_rate_(federal_and_state)": "tax_rate_federal_state",
        "calculated_rate_of_return_on_equity_real": "rate_of_return_on_equity_calculated_real",
        "heat_rate": "heat_rate_mmbtu_per_mwh",
        "net_output_penalty": "net_output_penalty",
        "variable_o&m": "opex_variable_per_mwh",
        "interest_during_construction_-_nominal": "interest_rate_during_construction_nominal",
        "cf": "capacity_factor",
        "capex": "capex_per_kw",
        "fuel": "fuel_cost_per_mwh",
        "rate_of_return_on_equity_nominal": "rate_of_return_on_equity_nominal",
        "occ": "capex_overnight_per_kw",
        # this only applies to technology_description's Coal_Retrofits & NaturalGas_Retrofits (maybe we should combine)
        "additional_occ": "capex_overnight_additional_per_kw",
        "wacc_nominal": "wacc_nominal",
        "wacc_real": "wacc_real",
        "cfc": "capex_construction_finance_factor",
        "lcoe": "levelized_cost_of_energy_per_mwh",
        "heat_rate_penalty": "heat_rate_penalty",
    }
    rename_dict = {
        "core_metric_variable_year": "projection_year",
        "core_metric_case": "model_case_nrelatb",
        "tax_credit_case": "model_tax_credit_case_nrelatb",
    }
    nrelatb = (
        raw_nrelatb__data.replace([""], pd.NA)
        .rename(columns=rename_dict)
        .pipe(
            helpers.fix_boolean_columns,
            boolean_columns_to_fix=list(raw_nrelatb__data.filter(regex=r"^is_")),
        )
        ## clean & rename values in core_metric_parameters
        .pipe(helpers.simplify_strings, ["core_metric_parameter"])
        .pipe(helpers.cleanstrings_snake, cols=["core_metric_parameter"])
        .replace({"core_metric_parameter": core_metric_parameters_rename})
        # we are dropping records that are completely dupes - not just dupes based on IDX_ALL
        .drop_duplicates(keep="first")
    )
    # In 2024, we see many records which are completely identical except for their core
    # metric key, which we are not currently using as a primary key due to changing,
    # unexpected and undocumented behaviors. We should drop these duplicates.
    # See issues #3506 and #3576 for an exploration of this column.
    logger.info(
        f"Dropping {sum(nrelatb.duplicated(nrelatb.columns.difference(['core_metric_key']))):,} records where only difference is the core_metric_key."
    )
    nrelatb = nrelatb.drop_duplicates(
        subset=nrelatb.columns.difference(["core_metric_key"])
    )

    assert not any(nrelatb.duplicated(IDX_ALL)), (
        f"Duplicated: {nrelatb[nrelatb.duplicated(IDX_ALL)]}"
    )

    # ensure that we have the same set of parameters in the unstackers and in the rename
    params_found = set(nrelatb.core_metric_parameter.unique())
    params_rename = set(core_metric_parameters_rename.values())
    params_unstacker = set(Unstacker().core_metric_parameters_all)
    for params in [params_rename, params_unstacker]:
        if param_differences := params_found.symmetric_difference(params):
            raise AssertionError(
                "Found different parameters in the NREL ``core_metric_parameter`` column "
                "compared to the params in the TableUnstackers or core_metric_parameters_rename:\n"
                f"{param_differences=}"
            )
    return (
        nrelatb.convert_dtypes()
        .astype({"projection_year": float})
        .astype({"projection_year": pd.Int64Dtype()})
    )


@asset(io_manager_key="pudl_io_manager")
def core_nrelatb__yearly_projected_financial_cases(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the data defining the assumptions for the ATB financial cases.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(_core_nrelatb__transform_start, Unstacker().rate_table)
    return df


@asset(io_manager_key="pudl_io_manager")
def core_nrelatb__yearly_projected_financial_cases_by_scenario(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the data defining the assumptions for the ATB financial cases which vary by scenario.

    Right now, this unstacks the table and applies :func:`broadcast_fixed_charge_rate_across_tech_detail`.
    """
    unstack_scenario = Unstacker().scenario_table

    # To handle errors in unstack where both the N/A tax credit case and the ITC tax
    # credit case have the same value, temporarily fill the NA here with none.
    df = (
        transform_unstack(_core_nrelatb__transform_start, unstack_scenario)
        .pipe(
            broadcast_fixed_charge_rate_across_tech_detail,
            unstack_scenario.idx_unstacked,
        )
        .pipe(broadcast_asterisk_cost_recovery_period_years, unstack_scenario)
    )
    return df


def broadcast_fixed_charge_rate_across_tech_detail(
    nrelatb_unstacked: pd.DataFrame, idx_broadcast: list[str]
) -> pd.DataFrame:
    """For older years, broadcast the ``fixed_charge_rate`` parameter across the technical detail columns.

    We want the table schema to be consistent for all years of ATB data. Mostly the parameters
    have the same primary keys across all of the years. But the ``fixed_charge_rate`` parameter is the
    only exception. For the older years (pre-2023), the FCR parameter is not variable
    based on tech detail so we are going to broadcast the pre-2023 ``fixed_charge_rate`` values across the
    tech details that exist in the data.
    """
    mask_broadcast = (
        nrelatb_unstacked.fixed_charge_rate.notnull()
        & (nrelatb_unstacked.report_year < 2023)
        # There are weirdly some fcr nuclear records that have a bunch of data in the
        # other core param columns.
        # Bc these records are not sparse, we do not want to broadcast these records.
        # We use the columns of nrelatb_unstacked bc those are the core_metric_parameters names.
        & nrelatb_unstacked[[c for c in nrelatb_unstacked if c != "fixed_charge_rate"]]
        .isna()
        .all(axis=1)
    )
    core_metric_parameters = ["fixed_charge_rate"]
    nrelatb_unstacked_fcr_broadcast = _broadcast_core_metric_parameters(
        nrelatb_unstacked,
        mask_broadcast,
        core_metric_parameters,
        idx_broadcast,
    )
    return nrelatb_unstacked_fcr_broadcast


def broadcast_asterisk_cost_recovery_period_years(
    nrelatb_unstacked, unstack_scenario: TableUnstacker
):
    """Broadcast the asterisk (wildcard) ``cost_recovery_period_years``.

    Most of the records in this unstacked table have values in the
    ``cost_recovery_period_years`` column, but before broadcasting, about 15% of the
    table has an ``*`` in this column. This is a part of the tables primary key and
    because we know ``*`` in a primary key effectively means wildcard, we want to
    broadcast the records with an asterisk across the rest of the data. Unfortunately,
    there are still ~5% of records w/ ``*`` that don't have associated records in the
    rest of the data (the are left_only records in :func:`_broadcast_core_metric_parameters`)
    so they end up with nulls in the ``cost_recovery_period_years`` column.

    Probably we could treat ``cost_recovery_period_years`` as a categorical column and/or
    figure out ways to fill in these nulls with the right set of merge keys.

    """
    mask_broadcast = nrelatb_unstacked.cost_recovery_period_years == "*"
    idx_broadcast = [
        i for i in unstack_scenario.idx_unstacked if i != "cost_recovery_period_years"
    ]
    core_metric_parameters = unstack_scenario.core_metric_parameters
    nrelatb_unstacked_broadcast = _broadcast_core_metric_parameters(
        nrelatb_unstacked,
        mask_broadcast,
        core_metric_parameters,
        idx_broadcast,
    )
    return nrelatb_unstacked_broadcast


def _broadcast_core_metric_parameters(
    nrelatb_unstacked: pd.DataFrame,
    mask_broadcast: pd.Series,
    core_metric_parameters: list[str],
    idx_broadcast: list[str],
) -> pd.DataFrame:
    """Broadcast a section of a table and fillna with the broadcasted values.

    Args:
        nrelatb_unstacked: the unstacked ATB table which has values to broadcast.
        mask_broadcast: a series with the same index as nrelatb_unstacked and boolean
            values where the True's are the records that you want to broadcast.
        core_metric_parameters: the list of core_metric_parameter columns in
            nrelatb_unstacked which you want to extract values from the broadcasted
            records.
        idx_broadcast: the columns to merge ``on``.
    """
    nrelatb_unstacked_broadcast = pd.merge(
        nrelatb_unstacked[~mask_broadcast],
        nrelatb_unstacked.loc[mask_broadcast, idx_broadcast + core_metric_parameters],
        on=idx_broadcast,
        how="outer",
        validate="m:1",
        suffixes=("", "_broadcast"),
    )

    for param in core_metric_parameters:
        nrelatb_unstacked_broadcast.loc[:, param] = nrelatb_unstacked_broadcast[
            param
        ].fillna(nrelatb_unstacked_broadcast[f"{param}_broadcast"])

    nrelatb_unstacked_broadcast = nrelatb_unstacked_broadcast.drop(
        columns=[f"{param}_broadcast" for param in core_metric_parameters]
    )
    return nrelatb_unstacked_broadcast


@asset(io_manager_key="pudl_io_manager")
def core_nrelatb__yearly_projected_cost_performance(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the yearly NREL ATB cost and performance projections.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(
        _core_nrelatb__transform_start, Unstacker().tech_detail_table
    )
    return df


@asset
def _core_nrelatb__yearly_units(
    _core_nrelatb__transform_start: pd.DataFrame,
) -> pd.DataFrame:
    """Transform a table of units by ``core_metric_parameter``.

    This asset is created mostly to ensure that the input units do not
    vary within one ``core_metric_parameter``. If they do vary, we will
    need to standardize the units of that parameter.
    """
    # clean up the units column so we can verify the units are consistent across the params
    units = _core_nrelatb__transform_start.assign(
        units=lambda x: x.units.str.lower()
    ).pipe(transform_normalize, Normalizer().units)
    assert not units.duplicated().any()
    return units


@asset(io_manager_key="pudl_io_manager")
def core_nrelatb__yearly_technology_status(
    _core_nrelatb__transform_start: pd.DataFrame,
) -> pd.DataFrame:
    """Transform a small table of statuses of different technology types."""
    return transform_normalize(
        _core_nrelatb__transform_start, Normalizer().technology_status
    )


@asset_check(asset=core_nrelatb__yearly_projected_cost_performance, blocking=True)
def null_cols_cost_performance(df):
    """Check for the prevalence of nulls in the core_nrelatb__yearly_projected_cost_performance."""
    nulls = pd.DataFrame(
        df.set_index(Unstacker().tech_detail_table.idx_unstacked).isnull().sum(axis=0)
        / len(df),
        columns=["null_count"],
    )
    # TODO 2024-05-13: this check could remove the know to be super
    # full of null parameters in check_technology_specific_parameters
    if not np.mean(nulls) < 0.64:
        raise AssertionError(
            "We expect the table to have an average of ~63% nulls, "
            f"but we found {np.mean(nulls):.1%}"
        )
    return AssetCheckResult(passed=True)


@asset_check(asset=core_nrelatb__yearly_projected_cost_performance, blocking=True)
def check_technology_specific_parameters(df):
    """Some parameters in the cost performance table only pertain to some technologies."""
    tech_specific_params = [
        {
            "technology_descriptions": {"Coal_Retrofits", "NaturalGas_Retrofits"},
            "params": [
                "capex_overnight_additional_per_kw",
                "net_output_penalty",
                "heat_rate_penalty",
            ],
        },
        {
            "technology_descriptions": {
                "Biopower",
                "Coal_FE",
                "Coal_Retrofits",
                "NaturalGas_FE",
                "NaturalGas_Retrofits",
                "Nuclear",
            },
            "params": ["heat_rate_mmbtu_per_mwh"],
        },
    ]
    for tech_specific_param in tech_specific_params:
        technology_descriptors = set(
            df[
                df[tech_specific_param["params"]].notnull().all(axis=1)
            ].technology_description.unique()
        )
        assert (
            tech_specific_param["technology_descriptions"] == technology_descriptors
        ), (
            f"{tech_specific_param['technology_descriptions']} does not equal {technology_descriptors}"
        )
    return AssetCheckResult(passed=True)
