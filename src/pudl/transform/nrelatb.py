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
    """Info needed to convert a selection of a the raw NREL table into a normalized table."""

    idx: list[str]
    """Primary key columns of normalized subset table."""
    columns: list[str]
    """Columns reported in raw NREL table that are unique to the :attr:`idx`."""


def transform_normlize(nrelatb: pd.DataFrame, normalizer: TableNormalizer):
    """Normalize a subset of the NREL ATB data into a small tables."""
    smol_table = nrelatb[normalizer.idx + normalizer.columns].drop_duplicates()
    assert smol_table[smol_table.duplicated(normalizer.idx, keep=False)].empty
    return smol_table.dropna(how="all", subset=normalizer.columns)


class Normalizer(BaseModel):
    """Class that defines how to normalize all of the NREL tables that get the :func:`transform_normlize` treatment."""

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
    params: list[str]
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
    """Generic unstacking function to convert ATB data from skinny to wider."""
    param_mask = nrelatb.core_metric_parameter.isin(table_unstacker.params)
    # assert not nrelatb[param_mask].duplicated(subset=table_unstacker.idx).any()

    nrelatb_unstacked = (
        nrelatb[param_mask]
        .set_index(table_unstacker.idx)
        .sort_index()[["value"]]
        .drop_duplicates()
        .unstack(level="core_metric_parameter")
    )
    nrelatb_unstacked.columns = nrelatb_unstacked.columns.droplevel()
    # this next step regarding the fcr param is only applicable to
    # the table w/ the fcr param:
    if "fcr" in nrelatb_unstacked:
        nrelatb_unstacked = broadcast_fcr_param_across_tech_detail(
            nrelatb_unstacked, table_unstacker.idx_unstacked
        )
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
        params=[
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
        params=[
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
        params=[
            "capex",
            "capacity_factor",
            "opex_fixed",
            "levelized_cost_of_energy",
            "opex_variable",
            # 2023 only params
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
    def params_all(self: Self) -> list[str]:
        """Compilation of all of the parameter values from each of the tables.

        Also check if there are no duplicate params. We expect all of the parameters
        across the :class:`TableUnstacker` to be unique.
        """
        params_all = (
            self.rate_table.params
            + self.scenario_table.params
            + self.tech_detail_table.params
        )
        assert not {x for x in params_all if params_all.count(x) > 1}
        return params_all


def broadcast_fcr_param_across_tech_detail(
    nrelatb_unstacked: pd.DataFrame, idx_unstacked: list[str]
) -> pd.DataFrame:
    """For older years, broadcast the FCR parameter across the technical detail columns.

    We want to table schema to be consistent for all years of ATB data. Mostly the parameters
    have the same primary keys across all of the years. But the ``fcr`` parameter is the
    only exception. For the older years (pre-2023), the FCR parameter is not variable
    based on tech detail so we are going to broadcast the pre-2023 fcr values acorss the
    tech details that exist in the data.
    """
    nrelatb_unstacked_fcr = nrelatb_unstacked.reset_index()
    mask_fcr = (
        nrelatb_unstacked_fcr.fcr.notnull()
        & (nrelatb_unstacked_fcr.report_year < 2023)
        # there are weirdly some fcr nuclear records that have a bunch of data
        # bc these records are not sparse, we do not want to broadcast these records
        # we use the columns of nrelatb_unstacked bc those of the params names
        & (
            nrelatb_unstacked_fcr[[c for c in nrelatb_unstacked if c != "fcr"]]
            .isna()
            .all(axis=1)
        )
    )
    # there are a small number of records that have "Nuclear" tech detail col
    # we are going to drop this detail bc it is always the same as the tech description
    assert nrelatb_unstacked_fcr.loc[
        mask_fcr
        & nrelatb_unstacked_fcr.technology_description_detail_1.notnull()
        & (
            nrelatb_unstacked_fcr.technology_description
            != nrelatb_unstacked_fcr.technology_description_detail_1
        )
    ].empty

    idx_fcr = [
        c
        for c in idx_unstacked
        if c
        not in ["technology_description_detail_1", "technology_description_detail_2"]
    ]
    nrelatb_unstacked_fcr_broadcast = (
        pd.merge(
            nrelatb_unstacked_fcr[~mask_fcr],
            nrelatb_unstacked_fcr.loc[mask_fcr, idx_fcr + ["fcr"]],
            on=idx_fcr,
            how="outer",
            validate="m:1",
            suffixes=("", "_broadcast"),
        )
        .assign(fcr=lambda x: x.fcr.fillna(x.fcr_broadcast))
        .drop(columns=["fcr_broadcast"])
    )
    return nrelatb_unstacked_fcr_broadcast.set_index(idx_unstacked)


@asset
def _core_nrelatb__transform_start(raw_nrelatb__data):
    """Transform raw NREL ATB data into semi-clean but still very skinny table."""
    ## EARLY CLEANING/DEDUPING
    # rename dict f semi-cleaned param name -> desired pudl name
    # all the params are here even if we aren't renaming mostly
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
        .pipe(
            helpers.fix_boolean_columns,
            boolean_columns_to_fix=list(raw_nrelatb__data.filter(regex=r"^is_")),
        )
        ## clean & rename values in core_metric_parameters
        .pipe(helpers.simplify_strings, ["core_metric_parameter"])
        .pipe(helpers.cleanstrings_snake, cols=["core_metric_parameter"])
        .replace({"core_metric_parameter": core_metric_parameters_rename})
        # we are droping dropping records that are completely dupes - not just
        # dupes based on IDX_ALL
        .drop_duplicates(keep="first")
    )
    assert not any(nrelatb.duplicated(IDX_ALL))

    # ensure that we have the same set of parameters in the unstackers and in the rename
    params_found = set(nrelatb.core_metric_parameter.unique())
    params_rename = set(core_metric_parameters_rename.values())
    params_unstacker = set(Unstacker().params_all)
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
    """Transform the yearly NREL ATB scenario table.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(_core_nrelatb__transform_start, Unstacker().scenario_table)
    return df


@asset
def _core_nrelatb__yearly_projections_by_technology_detail(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the yearly technology detail table.

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
    """Transform small table with the r."""
    return transform_normlize(_core_nrelatb__transform_start, Normalizer().revisions)


@asset
def _core_nrelatb__yearly_units(
    _core_nrelatb__transform_start: pd.DataFrame,
) -> pd.DataFrame:
    # clean up the units column so we can verify the units are consistent across the params
    units = _core_nrelatb__transform_start.assign(
        units=lambda x: x.units.str.lower()
    ).pipe(transform_normlize, Normalizer().units)
    return units


@asset
def _core_nrelatb__yearly_technology_status(
    _core_nrelatb__transform_start: pd.DataFrame,
) -> pd.DataFrame:
    return transform_normlize(
        _core_nrelatb__transform_start, Normalizer().technology_status
    )
