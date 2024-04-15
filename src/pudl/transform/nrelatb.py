"""Transform NREL ATB data into well normalized, cleaned tables."""

from typing import Self

import pandas as pd
from dagster import asset
from pydantic import BaseModel, field_validator

import pudl.helpers as helpers

IDX_ALL = [
    "report_year",
    "mystery_code",
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


class Unstacker(BaseModel):
    """Class that defines how to unstack the :func:`transform_start` table into all of the tidy NREL tables."""

    rate_table: TableUnstacker
    scenario_table: TableUnstacker
    tech_detail_table: TableUnstacker

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


def unstacker_of_tables() -> Unstacker:
    """Build an instance of :class:`Unstacker` for the core NREL ATB tables.

    This function is soley for easy access to the reshaping parameters.
    """
    unstacker_rate_table = TableUnstacker(
        idx=[
            "report_year",
            "mystery_code",
            "core_metric_case",
            "core_metric_variable_year",
            "cost_recovery_period_years",
            "technology_description",
            "core_metric_parameter",
        ],
        params=[
            "inflation rate",
            "interest during construction - nominal",
            "tax rate (federal and state)",
        ],
    )
    unstacker_scenario_table = TableUnstacker(
        idx=unstacker_rate_table.idx + ["scenario_atb"],
        params=[
            "interest rate nominal",
            "debt fraction",
            "wacc real",
            "wacc nominal",
            "calculated interest rate real",
            "rate of return on equity nominal",
            "calculated rate of return on equity real",
            # 2023 only param
            "crf",
        ],
    )
    unstacker_detail_table = TableUnstacker(
        idx=unstacker_scenario_table.idx
        + [
            "technology_description_detail_1",
            "technology_description_detail_2",
        ],
        params=[
            "capex",
            "cf",
            "fixed o&m",
            "fuel",
            "lcoe",
            "variable o&m",
            "fcr",
            # 2023 only params
            "additional occ",
            "cfc",
            "gcc",
            "heat rate",
            "heat rate penalty",
            "net output penalty",
            "occ",
        ],
    )
    unstack_tables = Unstacker(
        rate_table=unstacker_rate_table,
        scenario_table=unstacker_scenario_table,
        tech_detail_table=unstacker_detail_table,
    )
    return unstack_tables


@asset
def _core_nrelatb__transform_start(raw_nrelatb__data):
    """Transform raw NREL ATB data into semi."""
    ## EARLY CLEANING/DEDUPING
    raw_nrelatb__data["mystery_code"] = raw_nrelatb__data.core_metric_key.str[0]
    raw_nrelatb__data = raw_nrelatb__data.replace("*", pd.NA)
    raw_nrelatb__data = helpers.simplify_strings(
        raw_nrelatb__data, ["core_metric_parameter"]
    ).convert_dtypes()

    # Remove some duplicate values. they are complete dupes
    dupe_mask = raw_nrelatb__data.duplicated(subset=IDX_ALL, keep=False)
    # check if all of the values in the comp
    assert all(
        raw_nrelatb__data[dupe_mask]
        .groupby(by=IDX_ALL, dropna=False)[["value"]]
        .nunique()
        .value
        == 1
    )
    # we are droping dropping records that are completely dupes - not just
    # dupes based on IDX_ALL
    nrelatb = pd.concat(
        [
            raw_nrelatb__data[~dupe_mask],
            raw_nrelatb__data[dupe_mask].drop_duplicates(keep="first"),
        ]
    )
    assert not any(nrelatb.duplicated(IDX_ALL))

    # ensure that we have the same set of parameters in the unstackers
    params_found = set(nrelatb.core_metric_parameter.unique())
    if param_differences := params_found.symmetric_difference(
        set(unstacker_of_tables().params_all)
    ):
        raise AssertionError(
            "Found different parameters in the raw NREL ``core_metric_parameter`` column "
            f"compared to the params in the TableUnstackers: {param_differences=}"
        )
    return nrelatb


def transform_unstack(
    nrelatb: pd.DataFrame, table_unstacker: TableUnstacker
) -> pd.DataFrame:
    """Unstack."""
    mask = nrelatb.core_metric_parameter.isin(table_unstacker.params)
    assert not nrelatb[mask].duplicated(subset=table_unstacker.idx).any()

    nrelatb_unstacked = (
        nrelatb[mask]
        .set_index(table_unstacker.idx)
        .sort_index()[["value"]]
        .unstack(
            level="core_metric_parameter",
        )
    )
    nrelatb_unstacked.columns = nrelatb_unstacked.columns.droplevel()
    # this next step regarding the fcr param is only applicable to
    # the table w/ the fcr param:
    if "fcr" in nrelatb_unstacked:
        nrelatb_unstacked = broadcast_fcr_param_across_tech_detail(
            nrelatb_unstacked, table_unstacker.idx_unstacked
        )
    return nrelatb_unstacked


@asset
def _core_nrelatb__yearly_rates(_core_nrelatb__transform_start) -> pd.DataFrame:
    """Transform the yearly rates table.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(
        _core_nrelatb__transform_start, unstacker_of_tables().rate_table
    )
    return df


@asset
def _core_nrelatb__yearly_scenario(_core_nrelatb__transform_start) -> pd.DataFrame:
    """Transform the yearly NREL ATB scenario table.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(
        _core_nrelatb__transform_start, unstacker_of_tables().scenario_table
    )
    return df


@asset
def _core_nrelatb__yearly_technology_detail(
    _core_nrelatb__transform_start,
) -> pd.DataFrame:
    """Transform the yearly technology detail table.

    Right now, this just unstacks the table.
    """
    df = transform_unstack(
        _core_nrelatb__transform_start, unstacker_of_tables().tech_detail_table
    )
    return df


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
