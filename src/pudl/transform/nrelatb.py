"""Transform NREL ATB data into well normalized, cleaned tables."""

from typing import Self

import pandas as pd
from dagster import asset
from pydantic import BaseModel, field_validator

import pudl.helpers as helpers

IDX_ALL = [
    "report_year",
    "core_metric_case",
    "core_metric_variable_year",
    "cost_recovery_period_years",
    "scenario_atb",
    "technology_description",
    "technology_description_detail_1",
    "technology_description_detail_2",
    "core_metric_parameter",
]
"""Expected primary key columns for the raw nrelatb data.

The normalized core tables we are trying to build will result in a subset of these columns.
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
    unquie absed on the primary keys.
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

    revisions: TableNormalizer = TableNormalizer(
        idx=["report_year"],
        columns=["revision_num", "update_date"],
    )
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
    """Info needed to unstack a portion of the NREL ATB table."""

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
    """Generic unstacking function to convert ATB data from skinny to wider.

    The ATB data is reported in a very skinny format. There is one ``value``
    column and those values are labeled with the ``core_metric_parameter`` column
    which indicates what type of ``value`` is being reported. We want the info in
    ``core_metric_parameter`` to end up as columns in the tables - so there will
    be one column containing values from the ``value`` column each unique
    ``core_metric_parameter``.

    A quirk with ATB is that different ``core_metric_parameter`` have different
    set of primary keys. This function applies :func:`pd.unstack` to a subset of values
    for ``core_metric_parameter`` (via :attr:`TableUnstacker.core_metric_parameters`)
    with differnt primary keys (via :attr:`TableUnstacker.idx`). If the set of given
    ``core_metric_parameters`` are result in non-unique values for the primary keys,
    :func:`pd.unstack` wil raise an error.

    """
    nrelatb_unstacked = (
        nrelatb[
            nrelatb.core_metric_parameter.isin(table_unstacker.core_metric_parameters)
        ]
        .set_index(table_unstacker.idx)
        .sort_index()[["value"]]
        .drop_duplicates()
        .unstack(level="core_metric_parameter")
    )
    nrelatb_unstacked.columns = nrelatb_unstacked.columns.droplevel()
    return nrelatb_unstacked


class Unstacker(BaseModel):
    """Class that defines how to unstack the :func:`transform_start` table into all of the tidy NREL tables."""

    rate_table: TableUnstacker = TableUnstacker(
        idx=[
            "report_year",
            "core_metric_case",
            "core_metric_variable_year",
            "cost_recovery_period_years",
            "technology_description",
            "core_metric_parameter",
        ],
        core_metric_parameters=[
            "inflation_rate",
            "interest_during_construction_nominal",
            "tax_rate_federal_and_state",
            "interest_rate_calculated_real",
            "interest_rate_nominal",
            "rate_of_return_on_equity_nominal",
            "rate_of_return_on_equity_calculated_real",
        ],
    )
    scenario_table: TableUnstacker = TableUnstacker(
        idx=rate_table.idx + ["scenario_atb"],
        core_metric_parameters=[
            "debt_fraction",
            "wacc_real",
            "wacc_nominal",
            "capital_recovery_factor",
            "fuel_cost",
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
            "capex",
            "capacity_factor",
            "opex_fixed",
            "levelized_cost_of_energy",
            "opex_variable",
            # 2023 only core_metric_parameters
            "capex_overnight_additional",
            "construction_finance_factor",
            "grid_connection_cost",
            "heat_rate",
            "heat_rate_penalty",
            "net_output_penalty",
            "capex_overnight",
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


def broadcast_fixed_charge_rate_across_tech_detail(
    nrelatb_unstacked: pd.DataFrame, idx_unstacked: list[str]
) -> pd.DataFrame:
    """For older years, broadcast the ``fixed_charge_rate`` parameter across the technical detail columns.

    We want to table schema to be consistent for all years of ATB data. Mostly the parameters
    have the same primary keys across all of the years. But the ``fixed_charge_rate`` parameter is the
    only exception. For the older years (pre-2023), the FCR parameter is not variable
    based on tech detail so we are going to broadcast the pre-2023 ``fixed_charge_rate`` values across the
    tech details that exist in the data.
    """
    nrelatb_unstacked_fcr = nrelatb_unstacked.reset_index()
    mask_fcr = (
        nrelatb_unstacked_fcr.fixed_charge_rate.notnull()
        & (nrelatb_unstacked_fcr.report_year < 2023)
        # There are weirdly some fcr nuclear records that have a bunch of data in the
        # other core param columns.
        # Bc these records are not sparse, we do not want to broadcast these records.
        # We use the columns of nrelatb_unstacked bc those are the core_metric_parameters names.
        & nrelatb_unstacked_fcr[
            [c for c in nrelatb_unstacked if c != "fixed_charge_rate"]
        ]
        .isna()
        .all(axis=1)
    )
    idx_fcr = [
        c
        for c in idx_unstacked
        if c
        not in ["technology_description_detail_1", "technology_description_detail_2"]
    ]
    nrelatb_unstacked_fcr_broadcast = (
        pd.merge(
            nrelatb_unstacked_fcr[~mask_fcr],
            nrelatb_unstacked_fcr.loc[mask_fcr, idx_fcr + ["fixed_charge_rate"]],
            on=idx_fcr,
            how="outer",
            validate="m:1",
            suffixes=("", "_broadcast"),
        )
        .assign(
            fixed_charge_rate=lambda x: x.fixed_charge_rate.fillna(
                x.fixed_charge_rate_broadcast
            )
        )
        .drop(columns=["fixed_charge_rate_broadcast"])
    )
    return nrelatb_unstacked_fcr_broadcast.set_index(idx_unstacked)


@asset
def _core_nrelatb__transform_start(raw_nrelatb__data):
    """Transform raw NREL ATB data into semi-clean but still very skinny table."""
    ## EARLY CLEANING/DEDUPING
    # rename dict f semi-cleaned param name -> desired pudl name
    # all the core_metric_parameters are here even if we aren't renaming mostly
    # to make sure we've got em all.
    core_metric_parameters_rename = {
        "gcc": "grid_connection_cost",
        "interest_rate_nominal": "interest_rate_nominal",
        "debt_fraction": "debt_fraction",
        "inflation_rate": "inflation_rate",
        "calculated_interest_rate_real": "interest_rate_calculated_real",
        "fixed_o&m": "opex_fixed",
        "fcr": "fixed_charge_rate",
        "crf": "capital_recovery_factor",
        "tax_rate_(federal_and_state)": "tax_rate_federal_and_state",
        "calculated_rate_of_return_on_equity_real": "rate_of_return_on_equity_calculated_real",
        "occ": "capex_overnight",
        "heat_rate": "heat_rate",  # This does not have a unit, but we are going to extract units
        "net_output_penalty": "net_output_penalty",
        "variable_o&m": "opex_variable",
        "interest_during_construction_-_nominal": "interest_during_construction_nominal",
        "cf": "capacity_factor",
        "capex": "capex",
        "fuel": "fuel_cost",  # This does not have a unit, but we are going to extract units
        "rate_of_return_on_equity_nominal": "rate_of_return_on_equity_nominal",
        "additional_occ": "capex_overnight_additional",
        "wacc_nominal": "wacc_nominal",
        "wacc_real": "wacc_real",
        "cfc": "construction_finance_factor",
        "lcoe": "levelized_cost_of_energy",
        "heat_rate_penalty": "heat_rate_penalty",
    }
    nrelatb = (
        raw_nrelatb__data.replace(["*", ""], pd.NA)
        ## clean & rename values in core_metric_parameters
        .pipe(helpers.simplify_strings, ["core_metric_parameter"])
        .pipe(helpers.cleanstrings_snake, cols=["core_metric_parameter"])
        .replace({"core_metric_parameter": core_metric_parameters_rename})
        # we are dropping records that are completely dupes - not just dupes based on IDX_ALL
        .drop_duplicates(keep="first")
    )
    assert not any(nrelatb.duplicated(IDX_ALL))

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
    return nrelatb.convert_dtypes()


@asset
def _core_nrelatb__yearly_projections_rates(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the yearly rates table.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(_core_nrelatb__transform_start, Unstacker().rate_table)
    return df


@asset
def _core_nrelatb__yearly_projections_by_scenario(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the yearly NREL ATB projections by scenario table.

    Right now, this unstacks the table and applies :func:`broadcast_fixed_charge_rate_across_tech_detail`.
    """
    unstack_scenario = Unstacker().scenario_table
    df = transform_unstack(_core_nrelatb__transform_start, unstack_scenario).pipe(
        broadcast_fixed_charge_rate_across_tech_detail, unstack_scenario.idx_unstacked
    )
    return df


@asset
def _core_nrelatb__yearly_projections_by_technology_detail(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the yearly NREL ATB projections by technology detail.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(
        _core_nrelatb__transform_start, Unstacker().tech_detail_table
    )
    return df


@asset
def _core_nrelatb__yearly_revisions(
    _core_nrelatb__transform_start: pd.DataFrame,
) -> pd.DataFrame:
    """Transform small table including which revision the data pertains to and when it was updated."""
    return transform_normalize(_core_nrelatb__transform_start, Normalizer().revisions)


@asset
def _core_nrelatb__yearly_units(
    _core_nrelatb__transform_start: pd.DataFrame,
) -> pd.DataFrame:
    """Transform a table of units by ``core_metric_parameter``."""
    # clean up the units column so we can verify the units are consistent across the params
    units = _core_nrelatb__transform_start.assign(
        units=lambda x: x.units.str.lower()
    ).pipe(transform_normalize, Normalizer().units)
    return units


@asset
def _core_nrelatb__yearly_technology_status(
    _core_nrelatb__transform_start: pd.DataFrame,
) -> pd.DataFrame:
    """Transform a small table of statuses of different technology types."""
    return transform_normalize(
        _core_nrelatb__transform_start, Normalizer().technology_status
    )
