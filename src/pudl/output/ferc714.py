"""Functions & classes for compiling derived aspects of the FERC Form 714 data."""

from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
from dagster import Field, asset

import pudl
from pudl.analysis.service_territory import utility_ids_all_eia
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)

ASSOCIATIONS: list[dict[str, Any]] = [
    # MISO: Midwest Indep System Operator
    {"id": 56669, "from": 2011, "to": [2009, 2010]},
    # SWPP: Southwest Power Pool
    {"id": 59504, "from": 2014, "to": [2006, 2009], "exclude": ["NE"]},
    {"id": 59504, "from": 2014, "to": [2010, 2013]},
    # LGEE: LG&E and KU Services Company
    {"id": 11249, "from": 2014, "to": [2006, 2013]},
    # (no code): Entergy
    {"id": 12506, "from": 2012, "to": [2013, 2013]},
    # (no code): American Electric Power Co Inc
    {"id": 829, "from": 2008, "to": [2009, 2013]},
    # PJM: PJM Interconnection LLC
    {"id": 14725, "from": 2011, "to": [2006, 2010]},
    # BANC: Balancing Authority of Northern California
    {"id": 16534, "from": 2013, "to": [2012, 2012]},
    # SPS: Southwestern Public Service
    {"id": 17718, "from": 2010, "to": [2006, 2009]},
    # Nevada Power Company
    {"id": 13407, "from": 2009, "to": [2006, 2008]},
    {"id": 13407, "from": 2013, "to": [2014, 2019]},
]
"""Adjustments to balancing authority-utility associations from EIA 861.

The changes are applied locally to EIA 861 tables.

* `id` (int): EIA balancing authority identifier (`balancing_authority_id_eia`).
* `from` (int): Reference year, to use as a template for target years.
* `to` (List[int]): Target years, in the closed interval format [minimum, maximum].
  Rows in `core_eia861__yearly_balancing_authority` are added (if missing) for every target year
  with the attributes from the reference year.
  Rows in `core_eia861__assn_balancing_authority` are added (or replaced, if existing)
  for every target year with the utility associations from the reference year.
  Rows in `core_eia861__yearly_service_territory` are added (if missing) for every target year
  with the nearest year's associated utilities' counties.
* `exclude` (Optional[List[str]]): Utilities to exclude, by state (two-letter code).
  Rows are excluded from `core_eia861__assn_balancing_authority` with target year and state.
"""

UTILITIES: list[dict[str, Any]] = [
    # (no code): Pacific Gas & Electric Co
    {"id": 14328, "reassign": True},
    # (no code): San Diego Gas & Electric Co
    {"id": 16609, "reassign": True},
    # (no code): Dayton Power & Light Co
    {"id": 4922, "reassign": True},
    # (no code): Consumers Energy Company
    # NOTE: 2003-2006 parent to 40211, which is never child to parent BA (12427),
    # (and 40211 never reports in core_eia861__yearly_service_territory) so don't reassign.
    {"id": 4254},
]
"""Balancing authorities to treat as utilities in associations from EIA 861.

The changes are applied locally to EIA 861 tables.

* `id` (int): EIA balancing authority (BA) identifier (`balancing_authority_id_eia`).
  Rows for `id` are removed from `core_eia861__yearly_balancing_authority`.
* `reassign` (Optional[bool]): Whether to reassign utilities to parent BAs.
  Rows for `id` as BA in `core_eia861__assn_balancing_authority` are removed.
  Utilities assigned to `id` for a given year are reassigned
  to the BAs for which `id` is an associated utility.
* `replace` (Optional[bool]): Whether to remove rows where `id` is a utility in
  `core_eia861__assn_balancing_authority`. Applies only if `reassign=True`.
"""

################################################################################
# Helper functions
################################################################################


def categorize_eia_code(
    eia_codes: list[int],
    ba_ids: list[int],
    util_ids: list[int],
    priority: str = "balancing_authority",
) -> pd.DataFrame:
    """Categorize FERC 714 ``eia_codes`` as either balancing authority or utility IDs.

    Most FERC 714 respondent IDs are associated with an ``eia_code`` which refers to
    either a ``balancing_authority_id_eia`` or a ``utility_id_eia`` but no indication
    as to which type of ID each one is. This is further complicated by the fact
    that EIA uses the same numerical ID to refer to the same entity in most but not all
    cases, when that entity acts as both a utility and as a balancing authority.

    This function associates a ``respondent_type`` of ``utility``,
    ``balancing_authority`` or ``pandas.NA`` with each input ``eia_code`` using the
    following rules:

    * If a ``eia_code`` appears only in ``util_ids`` the ``respondent_type`` will be
      ``utility``.
    * If ``eia_code`` appears only in ``ba_ids`` the ``respondent_type`` will be
      assigned ``balancing_authority``.
    * If ``eia_code`` appears in neither set of IDs, ``respondent_type`` will be
      assigned ``pandas.NA``.
    * If ``eia_code`` appears in both sets of IDs, then whichever ``respondent_type``
      has been selected with the ``priority`` flag will be assigned.

    Note that the vast majority of ``balancing_authority_id_eia`` values also show up
    as ``utility_id_eia`` values, but only a small subset of the ``utility_id_eia``
    values are associated with balancing authorities. If you use
    ``priority="utility"`` you should probably also be specifically compiling the list
    of Utility IDs because you know they should take precedence. If you use utility
    priority with all utility IDs

    Args:
        eia_codes: A collection of IDs which may be either
            associated with EIA balancing authorities or utilities, to be categorized.
        ba_ids_eia: A collection of IDs which should be
            interpreted as belonging to EIA Balancing Authorities.
        util_ids_eia: A collection of IDs which should be
            interpreted as belonging to EIA Utilities.
        priority: Which respondent_type to give priority to if the eia_code shows
            up in both util_ids_eia and ba_ids_eia. Must be one of "utility" or
            "balancing_authority". The default is "balancing_authority".

    Returns:
        A DataFrame containing 2 columns: ``eia_code`` and ``respondent_type``.
    """
    if priority == "balancing_authority":
        primary = "balancing_authority"
        secondary = "utility"
    elif priority == "utility":
        primary = "utility"
        secondary = "balancing_authority"
    else:
        raise ValueError(
            f"Invalid respondent type {priority} chosen as priority."
            "Must be either 'utility' or 'balancing_authority'."
        )

    eia_codes = pd.DataFrame(eia_codes, columns=["eia_code"]).drop_duplicates()
    ba_ids = (
        pd.Series(ba_ids, name="balancing_authority_id_eia")
        .drop_duplicates()
        .convert_dtypes()
    )
    util_ids = (
        pd.Series(util_ids, name="utility_id_eia").drop_duplicates().convert_dtypes()
    )

    df = eia_codes.merge(
        ba_ids, left_on="eia_code", right_on="balancing_authority_id_eia", how="left"
    ).merge(util_ids, left_on="eia_code", right_on="utility_id_eia", how="left")
    df.loc[df[f"{primary}_id_eia"].notnull(), "respondent_type"] = primary
    df.loc[
        (df[f"{secondary}_id_eia"].notnull()) & (df[f"{primary}_id_eia"].isnull()),
        "respondent_type",
    ] = secondary
    df = df.astype({"respondent_type": pd.StringDtype()}).loc[
        :, ["eia_code", "respondent_type"]
    ]
    return df


#################################################################################
# Functions to compute analysis assets.
################################################################################


def filled_core_eia861__yearly_balancing_authority(
    core_eia861__yearly_balancing_authority: pd.DataFrame,
) -> pd.DataFrame:
    """Modified core_eia861__yearly_balancing_authority table.

    This function adds rows for each balancing authority-year pair missing from the
    cleaned core_eia861__yearly_balancing_authority table, using a dictionary of manual fixes. It
    uses the reference year as a template. The function also removes balancing
    authorities that are manually categorized as utilities.
    """
    df = core_eia861__yearly_balancing_authority
    index = ["balancing_authority_id_eia", "report_date"]
    dfi = df.set_index(index)
    # Prepare reference rows
    keys = [(fix["id"], pd.Timestamp(fix["from"], 1, 1)) for fix in ASSOCIATIONS]
    refs = dfi.loc[keys].reset_index().to_dict("records")
    # Build table of new rows
    # Insert row for each target balancing authority-year pair
    # missing from the original table, using the reference year as a template.
    rows: list[dict[str, Any]] = []
    for ref, fix in zip(refs, ASSOCIATIONS, strict=True):
        for year in range(fix["to"][0], fix["to"][1] + 1):
            key = (fix["id"], pd.Timestamp(year, 1, 1))
            if key not in dfi.index:
                rows.append({**ref, "report_date": key[1]})
    df = pd.concat(
        [df, apply_pudl_dtypes(pd.DataFrame(rows), group="eia")], axis="index"
    )
    # Remove balancing authorities treated as utilities
    mask = df["balancing_authority_id_eia"].isin([util["id"] for util in UTILITIES])
    return apply_pudl_dtypes(df[~mask], group="eia")


def filled_core_eia861__assn_balancing_authority(
    core_eia861__assn_balancing_authority: pd.DataFrame,
) -> pd.DataFrame:
    """Modified core_eia861__assn_balancing_authority table.

    This function adds rows for each balancing authority-year pair missing from the
    cleaned core_eia861__assn_balancing_authority table, using a dictionary of manual fixes.
    It uses the reference year as a template. The function also reassigns balancing
    authorities that are manually categorized as utilities to their parent balancing
    authorities.
    """
    df = core_eia861__assn_balancing_authority
    # Prepare reference rows
    refs = []
    for fix in ASSOCIATIONS:
        mask = df["balancing_authority_id_eia"].eq(fix["id"]).to_numpy(bool)
        mask[mask] = df["report_date"][mask].eq(pd.Timestamp(fix["from"], 1, 1))
        ref = df[mask]
        if "exclude" in fix:
            # Exclude utilities by state
            mask = ~ref["state"].isin(fix["exclude"])
            ref = ref[mask]
        refs.append(ref)
    # Build a table of new rows
    # Insert (or overwrite) rows for each target balancing authority-year pair,
    # using the reference year as a template.
    replaced = np.zeros(df.shape[0], dtype=bool)
    tables = []
    for ref, fix in zip(refs, ASSOCIATIONS, strict=True):
        for year in range(fix["to"][0], fix["to"][1] + 1):
            key = fix["id"], pd.Timestamp(year, 1, 1)
            mask = df["balancing_authority_id_eia"].eq(key[0]).to_numpy(bool)
            mask[mask] = df["report_date"][mask].eq(key[1])
            tables.append(ref.assign(report_date=key[1]))
            replaced |= mask
    # Append to original table with matching rows removed
    df = pd.concat([df[~replaced], apply_pudl_dtypes(pd.concat(tables), group="eia")])
    # Remove balancing authorities treated as utilities
    mask = np.zeros(df.shape[0], dtype=bool)
    tables = []
    for util in UTILITIES:
        is_parent = df["balancing_authority_id_eia"].eq(util["id"])
        mask |= is_parent
        # Associated utilities are reassigned to parent balancing authorities
        if util.get("reassign"):
            # Ignore when entity is child to itself
            is_child = ~is_parent & df["utility_id_eia"].eq(util["id"])
            # Build table associating parents to children of entity
            table = (
                df[is_child]
                .merge(
                    df[is_parent & ~df["utility_id_eia"].eq(util["id"])],
                    left_on=["report_date", "utility_id_eia"],
                    right_on=["report_date", "balancing_authority_id_eia"],
                )
                .drop(
                    columns=[
                        "utility_id_eia_x",
                        "state_x",
                        "balancing_authority_id_eia_y",
                    ]
                )
                .rename(
                    columns={
                        "balancing_authority_id_eia_x": "balancing_authority_id_eia",
                        "utility_id_eia_y": "utility_id_eia",
                        "state_y": "state",
                    }
                )
            )
            tables.append(table)
            if util.get("replace"):
                mask |= is_child
    return (
        pd.concat([df[~mask]] + tables)
        .drop_duplicates()
        .pipe(apply_pudl_dtypes, group="eia")
    )


def filled_service_territory_eia861(
    core_eia861__assn_balancing_authority: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
) -> pd.DataFrame:
    """Modified core_eia861__yearly_service_territory table.

    This function adds rows for each balancing authority-year pair missing from the
    cleaned core_eia861__yearly_service_territory table, using a dictionary of manual fixes. It also
    drops utility-state combinations which are missing counties across all years of
    data, fills records missing counties with the nearest year of county data for the
    same utility and state.
    """
    index = ["utility_id_eia", "state", "report_date"]
    # Select relevant balancing authority-utility associations
    assn = filled_core_eia861__assn_balancing_authority(
        core_eia861__assn_balancing_authority
    )
    selected = np.zeros(assn.shape[0], dtype=bool)
    for fix in ASSOCIATIONS:
        years = [fix["from"], *range(fix["to"][0], fix["to"][1] + 1)]
        dates = [pd.Timestamp(year, 1, 1) for year in years]
        mask = assn["balancing_authority_id_eia"].eq(fix["id"]).to_numpy(bool)
        mask[mask] = assn["report_date"][mask].isin(dates)
        selected |= mask
    # Reformat as unique utility-state-year
    assn = assn[selected][index].drop_duplicates()
    # Select relevant service territories
    mdf = assn.merge(core_eia861__yearly_service_territory, how="left")
    # Drop utility-state with no counties for all years
    grouped = mdf.groupby(["utility_id_eia", "state"])["county_id_fips"]
    mdf = mdf[grouped.transform("count").gt(0)]
    # Fill missing utility-state-year with nearest year with counties
    grouped = mdf.groupby(index)["county_id_fips"]
    missing = mdf[grouped.transform("count").eq(0)].to_dict("records")
    has_county = mdf["county_id_fips"].notna()
    tables = []
    for row in missing:
        mask = (
            mdf["utility_id_eia"].eq(row["utility_id_eia"])
            & mdf["state"].eq(row["state"])
            & has_county
        )
        years = mdf["report_date"][mask].drop_duplicates()
        # Match to nearest year
        idx = (years - row["report_date"]).abs().idxmin()
        mask &= mdf["report_date"].eq(years[idx])
        tables.append(mdf[mask].assign(report_date=row["report_date"]))
    return pd.concat([core_eia861__yearly_service_territory] + tables).pipe(
        apply_pudl_dtypes, group="eia"
    )


@asset(
    compute_kind="Python",
    required_resource_keys={"dataset_settings"},
)
def _out_ferc714__annualized_respondents(
    context,
    core_ferc714__respondent_id: pd.DataFrame,
) -> pd.DataFrame:
    """Broadcast respondent data across all years with reported demand.

    The FERC 714 Respondent IDs and names are reported in their own table, without any
    refence to individual years, but much of the information we are associating with
    them varies annually. This method creates an annualized version of the respondent
    table, with each respondent having an entry corresponding to every year for which
    FERC 714 has been processed. This means that many of the respondents will end up
    having entries for years in which they reported no demand, and that's fine.
    They can be filtered later.
    """
    if "report_date" in core_ferc714__respondent_id.columns:
        raise AssertionError("report_date already present, can't be added again!")

    ferc714_settings = context.resources.dataset_settings.ferc714
    report_dates = pd.DataFrame(
        {"report_date": pd.to_datetime(sorted(ferc714_settings.years), format="%Y")}
    )
    return core_ferc714__respondent_id.merge(report_dates, how="cross")


@asset(
    config_schema={
        "priority": Field(
            str,
            default_value="balancing_authority",
            description=(
                "Which type of entity should take priority in the categorization of "
                "FERC 714 respondents. Must be either ``utility`` or "
                "``balancing_authority.`` The default is ``balancing_authority``."
            ),
        ),
    },
    compute_kind="Python",
)
def _out_ferc714__categorized_respondents(
    context,
    core_ferc714__respondent_id: pd.DataFrame,
    out_eia__yearly_utilities: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
    core_eia861__yearly_balancing_authority: pd.DataFrame,
    _out_ferc714__annualized_respondents: pd.DataFrame,
) -> pd.DataFrame:
    """Annualized respondents with ``respondent_type`` assigned if possible.

    Categorize each respondent as either a ``utility`` or a ``balancing_authority``
    using the parameters stored in the instance of the class. While categorization
    can also be done without annualizing, this function annualizes as well, since we
    are adding the ``respondent_type`` in order to be able to compile service
    territories for the respondent, which vary annually.
    """
    priority = context.op_config["priority"]

    logger.info("Categorizing EIA codes associated with FERC-714 Respondents.")

    bal_auth = filled_core_eia861__yearly_balancing_authority(
        core_eia861__yearly_balancing_authority
    )
    utilids_all_eia = utility_ids_all_eia(
        out_eia__yearly_utilities, core_eia861__yearly_service_territory
    )

    categorized = categorize_eia_code(
        core_ferc714__respondent_id.eia_code.dropna().unique(),
        ba_ids=bal_auth.balancing_authority_id_eia.dropna().unique(),
        util_ids=utilids_all_eia.utility_id_eia,
        priority=priority,
    )
    logger.info(
        "Merging categorized EIA codes with annualized FERC-714 Respondent data."
    )
    categorized = pd.merge(
        categorized, _out_ferc714__annualized_respondents, how="right"
    )
    # Names, ids, and codes for BAs identified as FERC 714 respondents
    # NOTE: this is not *strictly* correct, because the EIA BAs are not
    # eternal and unchanging.  There's at least one case in which the BA
    # associated with a given ID had a code and name change between years
    # after it changed hands. However, not merging on report_date in
    # addition to the balancing_authority_id_eia / eia_code fields ensures
    # that all years are populated for all BAs, which keeps them analogous
    # to the Utiliies in structure. Sooo.... it's fine for now.
    logger.info("Selecting FERC-714 Balancing Authority respondents.")
    ba_respondents = categorized.query("respondent_type=='balancing_authority'")
    logger.info(
        "Merging FERC-714 Balancing Authority respondents with BA id/code/name "
        "information from EIA-861."
    )
    ba_respondents = pd.merge(
        ba_respondents,
        bal_auth[
            [
                "balancing_authority_id_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
            ]
        ].drop_duplicates(
            subset=[
                "balancing_authority_id_eia",
            ]
        ),
        how="left",
        left_on="eia_code",
        right_on="balancing_authority_id_eia",
    )
    logger.info("Selecting names and IDs for FERC-714 Utility respondents.")
    util_respondents = categorized.query("respondent_type=='utility'")
    logger.info("Merging FERC-714 Utility respondents with service territory.")
    util_respondents = pd.merge(
        util_respondents,
        utilids_all_eia,
        how="left",
        left_on="eia_code",
        right_on="utility_id_eia",
    )
    logger.info(
        "Concatenating categorized FERC-714 respondents:"
        f"{len(ba_respondents)} BA records, "
        f"{len(util_respondents)} utility records, "
        f"{sum(categorized.respondent_type.isnull())} uncategorized records."
    )
    categorized = pd.concat(
        [
            ba_respondents,
            util_respondents,
            # Uncategorized respondents w/ no respondent_type:
            categorized[categorized.respondent_type.isnull()],
        ]
    )
    categorized = apply_pudl_dtypes(categorized)
    return categorized


@asset(
    config_schema={
        "limit_by_state": Field(
            bool,
            default_value=True,
            description=(
                "Whether to limit respondent service territories to the states where "
                "they have documented activity in the EIA 861. Currently this is only "
                "implemented for Balancing Authorities."
            ),
        ),
    },
    compute_kind="pandas",
    io_manager_key="pudl_io_manager",
)
def out_ferc714__respondents_with_fips(
    context,
    _out_ferc714__categorized_respondents: pd.DataFrame,
    core_eia861__assn_balancing_authority: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
    core_eia861__assn_utility: pd.DataFrame,
) -> pd.DataFrame:
    """Annual respondents with the county FIPS IDs for their service territories.

    Given the ``respondent_type`` associated with each respondent (either ``utility`` or
    ``balancing_authority``) compile a list of counties that are part of their service
    territory on an annual basis, and merge those into the annualized respondent table.
    This results in a very long dataframe, since there are thousands of counties and
    many of them are served by more than one entity.

    Currently respondents categorized as ``utility`` will include any county that
    appears in the ``core_eia861__yearly_service_territory`` table in association with
    that utility ID in each year, while for ``balancing_authority`` respondents, some
    counties can be excluded based on state (if ``limit_by_state==True``).

    """
    assn = filled_core_eia861__assn_balancing_authority(
        core_eia861__assn_balancing_authority
    )
    st_eia861 = filled_service_territory_eia861(
        core_eia861__assn_balancing_authority, core_eia861__yearly_service_territory
    )

    # Generate the BA:FIPS relation:
    ba_counties = pd.merge(
        _out_ferc714__categorized_respondents.query(
            "respondent_type=='balancing_authority'"
        ),
        pudl.analysis.service_territory.get_territory_fips(
            ids=_out_ferc714__categorized_respondents.balancing_authority_id_eia.unique(),
            assn=assn,
            assn_col="balancing_authority_id_eia",
            core_eia861__yearly_service_territory=st_eia861,
            limit_by_state=context.op_config["limit_by_state"],
        ),
        on=["report_date", "balancing_authority_id_eia"],
        how="left",
    )
    # Generate the Util:FIPS relation:
    util_counties = pd.merge(
        _out_ferc714__categorized_respondents.query("respondent_type=='utility'"),
        pudl.analysis.service_territory.get_territory_fips(
            ids=_out_ferc714__categorized_respondents.utility_id_eia.unique(),
            assn=core_eia861__assn_utility,
            assn_col="utility_id_eia",
            core_eia861__yearly_service_territory=st_eia861,
            limit_by_state=context.op_config["limit_by_state"],
        ),
        on=["report_date", "utility_id_eia"],
        how="left",
    )
    logger.info(
        "Concatenating georeferenced FERC-714 respondents:"
        f"{len(ba_counties)} BA records, "
        f"{len(util_counties)} utility records, "
        f"{sum(_out_ferc714__categorized_respondents.respondent_type.isnull())} uncategorized records."
    )
    fipsified = pd.concat(
        [
            ba_counties,
            util_counties,
            _out_ferc714__categorized_respondents[
                _out_ferc714__categorized_respondents.respondent_type.isnull()
            ],
        ]
    ).pipe(apply_pudl_dtypes)
    return fipsified


@asset(
    op_tags={"memory-use": "high"},
    compute_kind="pandas",
)
def _out_ferc714__georeferenced_counties(
    out_ferc714__respondents_with_fips: pd.DataFrame,
    _core_censusdp1tract__counties: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Annual respondents with all associated county-level geometries.

    Given the county FIPS codes associated with each respondent in each year, pull in
    associated geometries from the US Census DP1 dataset, so we can do spatial analyses.
    This keeps each county record independent -- so there will be many records for each
    respondent in each year. This is fast, and still good for mapping, and retains all
    of the FIPS IDs so you can also still do ID based analyses.
    """
    counties_gdf = pudl.analysis.service_territory.add_geometries(
        out_ferc714__respondents_with_fips,
        census_gdf=_core_censusdp1tract__counties,
    ).pipe(apply_pudl_dtypes)
    return counties_gdf


@asset(compute_kind="pandas")
def _out_ferc714__georeferenced_respondents(
    out_ferc714__respondents_with_fips: pd.DataFrame,
    out_ferc714__summarized_demand: pd.DataFrame,
    _core_censusdp1tract__counties: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Annual respondents with a single all-encompassing geometry for each year.

    Given the county FIPS codes associated with each responent in each year, compile a
    geometry for the respondent's entire service territory annually. This results in
    just a single record per respondent per year, but is computationally expensive and
    you lose the information about what all counties are associated with the respondent
    in that year. But it's useful for merging in other annual data like total demand, so
    you can see which respondent-years have both reported demand and decent geometries,
    calculate their areas to see if something changed from year to year, etc.
    """
    respondents_gdf = (
        pudl.analysis.service_territory.add_geometries(
            out_ferc714__respondents_with_fips,
            census_gdf=_core_censusdp1tract__counties,
            dissolve=True,
            dissolve_by=["report_date", "respondent_id_ferc714"],
        )
        .merge(
            out_ferc714__summarized_demand[
                ["report_date", "respondent_id_ferc714", "demand_annual_mwh"]
            ]
        )
        .pipe(apply_pudl_dtypes)
    )
    return respondents_gdf


@asset(
    io_manager_key="pudl_io_manager",
    op_tags={"memory-use": "high"},
    compute_kind="pandas",
)
def out_ferc714__summarized_demand(
    _out_ferc714__annualized_respondents: pd.DataFrame,
    out_ferc714__hourly_planning_area_demand: pd.DataFrame,
    _out_ferc714__categorized_respondents: pd.DataFrame,
    _out_ferc714__georeferenced_counties: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Compile annualized, categorized respondents and summarize values.

    Calculated summary values include:
    * Total reported electricity demand per respondent (``demand_annual_mwh``)
    * Reported per-capita electrcity demand (``demand_annual_per_capita_mwh``)
    * Population density (``population_density_km2``)
    * Demand density (``demand_density_mwh_km2``)

    These metrics are helpful identifying suspicious changes in the compiled annual
    geometries for the planning areas.
    """
    demand_annual = (
        pd.merge(
            _out_ferc714__annualized_respondents,
            out_ferc714__hourly_planning_area_demand.loc[
                :, ["report_date", "respondent_id_ferc714", "demand_mwh"]
            ],
            on=["report_date", "respondent_id_ferc714"],
            how="left",
        )
        .groupby(["report_date", "respondent_id_ferc714"], as_index=False)[
            ["demand_mwh"]
        ]
        .sum(min_count=1)
        .rename(columns={"demand_mwh": "demand_annual_mwh"})
        .merge(
            _out_ferc714__georeferenced_counties.groupby(
                ["report_date", "respondent_id_ferc714"], as_index=False
            )[["population", "area_km2"]].sum(min_count=1),
            on=["report_date", "respondent_id_ferc714"],
            how="left",
        )
        .assign(
            population_density_km2=lambda x: x.population / x.area_km2,
            demand_annual_per_capita_mwh=lambda x: x.demand_annual_mwh / x.population,
            demand_density_mwh_km2=lambda x: x.demand_annual_mwh / x.area_km2,
        )
    )
    # Merge respondent categorizations into the annual demand
    demand_summary = pd.merge(
        demand_annual, _out_ferc714__categorized_respondents, how="left"
    ).pipe(apply_pudl_dtypes)
    return demand_summary
